package commit

import (
	"errors"
	"fmt"
	"runtime"
	"sync"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/rlp"
	"github.com/ava-labs/libevm/trie"
	"github.com/ava-labs/libevm/trie/trienode"
	"github.com/ava-labs/libevm/triedb/database"
)

// SeekFunc returns the first row of the ROLLED flat state (the state the
// File was rolled from, not the live one) whose contract key is >= prefix;
// nil when there is none. It must be safe for concurrent calls.
//
// It is how a leaf is read: the file holds no leaves, and the trie needs an
// untouched leaf's content when a new key splits it or a delete merges it
// upward, and the key remainder of the leaf being updated.
type SeekFunc func(prefix []byte) (key, value []byte)

// Dirty is the in-memory overlay of trie nodes changed since the last roll.
// Apply queues contract writes; Root recomputes the state root touching only
// the dirty paths and retains the produced nodes for the next round.
type Dirty struct {
	f     *File
	seek  SeekFunc
	root  common.Hash
	nodes map[common.Hash]map[string][]byte // owner -> path -> blob; nil = deleted
	bytes int
	acct  map[common.Hash]*pending

	// Workers bounds the storage-trie hashing pool (default GOMAXPROCS).
	Workers int
}

type pending struct {
	row   []byte // contract account value; nil when only slots were applied
	del   bool   // the last account write was a delete
	wiped bool   // a delete happened this round: storage starts empty
	slots map[common.Hash][]byte
}

// NewDirty starts an empty overlay over f.
func NewDirty(f *File, seek SeekFunc) *Dirty {
	d := &Dirty{seek: seek, Workers: runtime.GOMAXPROCS(0)}
	d.Reset(f)
	return d
}

// Reset drops every retained node and rebinds the overlay to a freshly
// rolled file.
func (d *Dirty) Reset(f *File) {
	d.f = f
	d.root = f.Root()
	d.nodes = map[common.Hash]map[string][]byte{}
	d.bytes = 0
	d.acct = map[common.Hash]*pending{}
}

// Bytes is the approximate size of the retained dirty nodes.
func (d *Dirty) Bytes() int { return d.bytes }

// Apply queues one contract write. Keys may come in any order; an empty
// value deletes, and deleting an account drops its slots.
func (d *Dirty) Apply(key, value []byte) error {
	switch {
	case len(key) == 33 && key[32] == 0:
		p := d.touch(common.BytesToHash(key[:32]))
		if len(value) == 0 {
			*p = pending{del: true, wiped: true}
		} else {
			p.row, p.del = append([]byte(nil), value...), false
		}
	case len(key) == 65 && key[32] == 1:
		p := d.touch(common.BytesToHash(key[:32]))
		if p.slots == nil {
			p.slots = map[common.Hash][]byte{}
		}
		p.slots[common.BytesToHash(key[33:])] = append([]byte{}, value...)
	default:
		return fmt.Errorf("commit: malformed key %x", key)
	}
	return nil
}

func (d *Dirty) touch(h common.Hash) *pending {
	p := d.acct[h]
	if p == nil {
		p = &pending{}
		d.acct[h] = p
	}
	return p
}

type job struct {
	hash common.Hash
	p    *pending
	cur  *accountLeaf
	root common.Hash
	set  *trienode.NodeSet
	err  error
}

// Root applies the queued writes and returns the new state root. Storage
// tries are hashed in parallel, the account trie after them.
func (d *Dirty) Root() (common.Hash, error) {
	acc, err := trie.New(trie.TrieID(d.root), d)
	if err != nil {
		return common.Hash{}, err
	}
	jobs := make([]*job, 0, len(d.acct))
	var work []*job
	for h, p := range d.acct {
		j := &job{hash: h, p: p}
		jobs = append(jobs, j)
		if p.del {
			continue
		}
		val, err := acc.Get(h[:])
		if err != nil {
			return common.Hash{}, err
		}
		if len(val) > 0 {
			j.cur = new(accountLeaf)
			if err := rlp.DecodeBytes(val, j.cur); err != nil {
				return common.Hash{}, err
			}
		}
		if j.cur == nil && p.row == nil {
			return common.Hash{}, fmt.Errorf("commit: slots written for missing account %x", h)
		}
		j.root = types.EmptyRootHash
		if j.cur != nil && !p.wiped {
			j.root = j.cur.Root
		}
		if len(p.slots) > 0 {
			work = append(work, j)
		}
	}
	var wg sync.WaitGroup
	sem := make(chan struct{}, max(d.Workers, 1))
	for _, j := range work {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			j.root, j.set, j.err = d.storage(j)
		}()
	}
	wg.Wait()
	for _, j := range work {
		if j.err != nil {
			return common.Hash{}, j.err
		}
		d.merge(j.set)
	}
	for _, j := range jobs {
		if j.p.del {
			if err := acc.Delete(j.hash[:]); err != nil {
				return common.Hash{}, err
			}
			for _, b := range d.nodes[j.hash] {
				d.bytes -= len(b)
			}
			delete(d.nodes, j.hash)
			continue
		}
		leaf := accountLeaf{Root: j.root}
		if j.p.row != nil {
			var row accountRow
			if err := rlp.DecodeBytes(j.p.row, &row); err != nil {
				return common.Hash{}, err
			}
			leaf.Nonce, leaf.Balance, leaf.CodeHash = row.Nonce, row.Balance, row.CodeHash
		} else {
			leaf.Nonce, leaf.Balance, leaf.CodeHash = j.cur.Nonce, j.cur.Balance, j.cur.CodeHash
		}
		val, err := rlp.EncodeToBytes(&leaf)
		if err != nil {
			return common.Hash{}, err
		}
		if err := acc.Update(j.hash[:], val); err != nil {
			return common.Hash{}, err
		}
	}
	root, set, err := acc.Commit(false)
	if err != nil {
		return common.Hash{}, err
	}
	d.merge(set)
	d.root = root
	d.acct = map[common.Hash]*pending{}
	return root, nil
}

func (d *Dirty) storage(j *job) (common.Hash, *trienode.NodeSet, error) {
	t, err := trie.New(trie.StorageTrieID(d.root, j.hash, j.root), d)
	if err != nil {
		return common.Hash{}, nil, err
	}
	for k, v := range j.p.slots {
		if len(v) == 0 {
			err = t.Delete(k[:])
		} else {
			var enc []byte
			enc, err = rlp.EncodeToBytes(v)
			if err == nil {
				err = t.Update(k[:], enc)
			}
		}
		if err != nil {
			return common.Hash{}, nil, err
		}
	}
	return t.Commit(false)
}

func (d *Dirty) merge(set *trienode.NodeSet) {
	if set == nil {
		return
	}
	m := d.nodes[set.Owner]
	if m == nil {
		m = map[string][]byte{}
		d.nodes[set.Owner] = m
	}
	for path, n := range set.Nodes {
		d.bytes += len(n.Blob) - len(m[path])
		m[path] = n.Blob
	}
}

// Reader and Node make Dirty a database.Database for trie.New: dirty nodes
// first, then the file, then a leaf fabricated from the rolled flat row.
func (d *Dirty) Reader(common.Hash) (database.Reader, error) { return d, nil }
func (d *Dirty) Preimage(common.Hash) []byte                 { return nil }
func (d *Dirty) InsertPreimage(map[common.Hash][]byte)       {}

func (d *Dirty) Node(owner common.Hash, path []byte, _ common.Hash) ([]byte, error) {
	if m := d.nodes[owner]; m != nil {
		if blob, ok := m[string(path)]; ok {
			return blob, nil
		}
	}
	if blob, ok := d.f.Node(owner, path); ok {
		return blob, nil
	}
	return d.leaf(owner, path)
}

var errNoLeaf = errors.New("commit: no flat row under the requested trie path")

// leaf rebuilds the leaf node at path from the rolled flat state.
func (d *Dirty) leaf(owner common.Hash, path []byte) ([]byte, error) {
	packed := packNibbles(path)
	var prefix []byte
	if owner == (common.Hash{}) {
		prefix = packed
	} else {
		prefix = append(append(append([]byte{}, owner[:]...), 1), packed...)
	}
	key, val := d.seek(prefix)
	var hashed, value []byte
	switch {
	case owner == (common.Hash{}) && len(key) == 33 && key[32] == 0:
		hashed = key[:32]
		var row accountRow
		if err := rlp.DecodeBytes(val, &row); err != nil {
			return nil, err
		}
		leaf := accountLeaf{Nonce: row.Nonce, Balance: row.Balance, CodeHash: row.CodeHash}
		var ok bool
		if leaf.Root, ok = d.f.StorageRoot(common.BytesToHash(hashed)); !ok {
			// One slot, or none: the root is the leaf's own hash, or empty.
			k, v := d.seek(append(append([]byte{}, hashed...), 1))
			if len(k) == 65 && k[32] == 1 && string(k[:32]) == string(hashed) {
				enc, _ := rlp.EncodeToBytes(v)
				st := trie.NewStackTrie(nil)
				st.Update(k[33:], enc)
				leaf.Root = st.Hash()
			}
		}
		value, _ = rlp.EncodeToBytes(&leaf)
	case owner != (common.Hash{}) && len(key) == 65 && key[32] == 1 && string(key[:32]) == string(owner[:]):
		hashed = key[33:]
		value, _ = rlp.EncodeToBytes(val)
	default:
		return nil, errNoLeaf
	}
	nib := keyToNibbles(hashed)
	if len(nib) < len(path) || string(nib[:len(path)]) != string(path) {
		return nil, errNoLeaf
	}
	return rlp.EncodeToBytes([]any{hexToCompact(nib[len(path):], true), value})
}
