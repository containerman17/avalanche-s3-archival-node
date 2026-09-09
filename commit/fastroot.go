package commit

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/crypto"
	"github.com/ava-labs/libevm/rlp"
	"github.com/ava-labs/libevm/trie/trienode"
)

// fastStorage is the parallel replacement for one contract's storage-trie
// update. libevm's Trie applies 50k inserts on one goroutine, resolving and
// decoding nodes on every path; here the dirty keys are split by their first
// nibble and the 16 subtrees are updated and hashed concurrently, in memory.
// Output matches trie.Commit: the new root and a NodeSet of changed nodes by
// path, deleted paths included. Small batches and tries without a branch at
// the root fall back to libevm (same result, no parallelism to gain).
//
// ponytail: prototype for the big-block measurement; the account trie and
// the roll still go through libevm.

// fastMinSlots is the dirty-slot count from which a contract takes this path.
var fastMinSlots = 256

// Node shapes. A ref is what a parent stores for a child: a 32-byte hash, or
// the raw RLP of an embedded node under 32 bytes.
type (
	fnode  interface{}
	fhash  []byte   // 32-byte hash of a node not loaded (or unchanged)
	fembed []byte   // raw RLP of an embedded child not loaded
	fvalue []byte   // leaf value
	fshort struct { // extension or leaf
		key  []byte // nibbles
		val  fnode
		orig fnode // the ref this node was loaded from; nil when new
	}
	fbranch struct {
		child [16]fnode
		val   []byte
		orig  fnode
	}
)

// fresolver reads nodes for one worker; loaded collects the paths it read.
type fresolver struct {
	d      *Dirty
	owner  common.Hash
	loaded map[string]struct{}
}

func (r *fresolver) load(path []byte, ref fnode) (fnode, error) {
	var blob []byte
	switch ref := ref.(type) {
	case fhash:
		var err error
		blob, err = r.d.Node(r.owner, path, common.BytesToHash(ref))
		if err != nil {
			return nil, err
		}
		r.loaded[string(path)] = struct{}{}
	case fembed:
		blob = ref
	default:
		return ref, nil
	}
	n, err := fdecode(blob)
	switch n := n.(type) {
	case *fshort:
		n.orig = ref
	case *fbranch:
		n.orig = ref
	}
	return n, err
}

// fdecode parses one RLP node; children stay refs.
func fdecode(blob []byte) (fnode, error) {
	elems, _, err := rlp.SplitList(blob)
	if err != nil {
		return nil, err
	}
	n, err := rlp.CountValues(elems)
	if err != nil {
		return nil, err
	}
	switch n {
	case 2:
		kbuf, rest, err := rlp.SplitString(elems)
		if err != nil {
			return nil, err
		}
		key := compactToHex(kbuf)
		if kbuf[0]&0x20 != 0 { // leaf
			val, _, err := rlp.SplitString(rest)
			if err != nil {
				return nil, err
			}
			return &fshort{key: key, val: fvalue(bytes.Clone(val))}, nil
		}
		ref, err := fref(rest)
		if err != nil {
			return nil, err
		}
		return &fshort{key: key, val: ref}, nil
	case 17:
		b := &fbranch{}
		rest := elems
		for i := 0; i < 16; i++ {
			_, _, cnt, err := rlp.Split(rest)
			if err != nil {
				return nil, err
			}
			item := rest[:len(rest)-len(cnt)]
			if len(item) > 1 { // not the empty string 0x80
				if b.child[i], err = fref(item); err != nil {
					return nil, err
				}
			}
			rest = cnt
		}
		val, _, err := rlp.SplitString(rest)
		if err != nil {
			return nil, err
		}
		if len(val) > 0 {
			b.val = bytes.Clone(val)
		}
		return b, nil
	}
	return nil, fmt.Errorf("commit: node with %d items", n)
}

// fref reads one child item: a hash string or an embedded list.
func fref(item []byte) (fnode, error) {
	kind, content, rest, err := rlp.Split(item)
	if err != nil {
		return nil, err
	}
	switch kind {
	case rlp.String:
		if len(content) == 0 {
			return nil, nil
		}
		return fhash(bytes.Clone(content)), nil
	case rlp.List:
		return fembed(bytes.Clone(item[:len(item)-len(rest)])), nil
	}
	return nil, fmt.Errorf("commit: child of kind %d", kind)
}

// cat returns a fresh slice a+b (paths must not alias across siblings).
func cat(a []byte, b ...byte) []byte {
	out := make([]byte, 0, len(a)+len(b))
	return append(append(out, a...), b...)
}

func fprefixLen(a, b []byte) int {
	i := 0
	for i < len(a) && i < len(b) && a[i] == b[i] {
		i++
	}
	return i
}

// finsert follows trie.insert. key has no terminator; prefix is the path of n.
func (r *fresolver) finsert(n fnode, prefix, key []byte, value fnode) (fnode, error) {
	if len(key) == 0 {
		return value, nil
	}
	switch n := n.(type) {
	case nil:
		return &fshort{key: bytes.Clone(key), val: value}, nil
	case *fshort:
		m := fprefixLen(key, n.key)
		if m == len(n.key) {
			nn, err := r.finsert(n.val, cat(prefix, key[:m]...), key[m:], value)
			if err != nil {
				return nil, err
			}
			n.val = nn
			return n, nil
		}
		b := &fbranch{}
		var err error
		b.child[n.key[m]], err = r.finsert(nil, cat(prefix, n.key[:m+1]...), n.key[m+1:], n.val)
		if err != nil {
			return nil, err
		}
		b.child[key[m]], err = r.finsert(nil, cat(prefix, key[:m+1]...), key[m+1:], value)
		if err != nil {
			return nil, err
		}
		if m == 0 {
			return b, nil
		}
		return &fshort{key: bytes.Clone(key[:m]), val: b}, nil
	case *fbranch:
		nn, err := r.finsert(n.child[key[0]], cat(prefix, key[0]), key[1:], value)
		if err != nil {
			return nil, err
		}
		n.child[key[0]] = nn
		return n, nil
	case fhash, fembed:
		rn, err := r.load(prefix, n)
		if err != nil {
			return nil, err
		}
		return r.finsert(rn, prefix, key, value)
	}
	return nil, fmt.Errorf("commit: insert into %T", n)
}

// fdelete follows trie.delete.
func (r *fresolver) fdelete(n fnode, prefix, key []byte) (fnode, error) {
	switch n := n.(type) {
	case nil:
		return nil, nil
	case fvalue:
		return nil, nil
	case *fshort:
		m := fprefixLen(key, n.key)
		if m < len(n.key) {
			return n, nil
		}
		if m == len(key) {
			return nil, nil
		}
		child, err := r.fdelete(n.val, cat(prefix, key[:len(n.key)]...), key[len(n.key):])
		if err != nil {
			return nil, err
		}
		if c, ok := child.(*fshort); ok {
			return &fshort{key: append(bytes.Clone(n.key), c.key...), val: c.val}, nil
		}
		n.val = child
		return n, nil
	case *fbranch:
		nn, err := r.fdelete(n.child[key[0]], cat(prefix, key[0]), key[1:])
		if err != nil {
			return nil, err
		}
		n.child[key[0]] = nn
		pos := -1
		for i, c := range n.child {
			if c != nil {
				if pos == -1 {
					pos = i
				} else {
					pos = -2
					break
				}
			}
		}
		if n.val != nil {
			if pos == -1 {
				pos = 16
			} else {
				pos = -2
			}
		}
		if pos >= 0 {
			if pos != 16 {
				c, err := r.load(cat(prefix, byte(pos)), n.child[pos])
				if err != nil {
					return nil, err
				}
				if cs, ok := c.(*fshort); ok {
					return &fshort{key: append([]byte{byte(pos)}, cs.key...), val: cs.val}, nil
				}
				return &fshort{key: []byte{byte(pos)}, val: c}, nil
			}
			return &fshort{key: []byte{16}, val: fvalue(n.val)}, nil
		}
		return n, nil
	case fhash, fembed:
		rn, err := r.load(prefix, n)
		if err != nil {
			return nil, err
		}
		return r.fdelete(rn, prefix, key)
	}
	return nil, fmt.Errorf("commit: delete from %T", n)
}

// fcommit encodes and hashes a subtree, recording stored nodes by path and
// every present path (so deletions can be derived), and returns the ref the
// parent stores. Unloaded refs are returned as they are.
type fout struct {
	nodes   map[string][]byte // path -> blob of stored (hashed) nodes
	hashes  map[string][]byte // path -> keccak of that blob
	present map[string]struct{}
}

func newOut() *fout {
	return &fout{nodes: map[string][]byte{}, hashes: map[string][]byte{}, present: map[string]struct{}{}}
}

func (o *fout) commit(n fnode, path []byte) fnode {
	switch n := n.(type) {
	case nil, fhash, fembed:
		return n
	case fvalue:
		return n
	case *fshort:
		var val []byte
		v, leaf := n.val.(fvalue)
		if leaf {
			val = rlpString(v)
		} else {
			val = rlpRef(o.commit(n.val, cat(path, n.key...)))
		}
		return o.store(path, rlpList(rlpString(hexToCompact(n.key, leaf)), val), n.orig)
	case *fbranch:
		var body []byte
		for i, c := range n.child {
			body = append(body, rlpRef(o.commit(c, cat(path, byte(i))))...)
		}
		body = append(body, rlpString(n.val)...)
		return o.store(path, rlpList(body), n.orig)
	}
	panic(fmt.Sprintf("commit: encode %T", n))
}

// store records a present node and, when it changed against what it was
// loaded from, its blob; returns the ref for the parent.
func (o *fout) store(path []byte, enc []byte, orig fnode) fnode {
	o.present[string(path)] = struct{}{}
	if len(enc) < 32 {
		return fembed(enc)
	}
	h := crypto.Keccak256(enc)
	if oh, ok := orig.(fhash); ok && bytes.Equal(oh, h) {
		return orig
	}
	o.nodes[string(path)] = enc
	o.hashes[string(path)] = h
	return fhash(h)
}

// Minimal RLP: strings and lists of already-encoded items.
func rlpString(b []byte) []byte {
	if len(b) == 1 && b[0] < 0x80 {
		return b
	}
	return append(rlpHeader(0x80, len(b)), b...)
}

func rlpList(items ...[]byte) []byte {
	n := 0
	for _, it := range items {
		n += len(it)
	}
	out := rlpHeader(0xc0, n)
	for _, it := range items {
		out = append(out, it...)
	}
	return out
}

func rlpHeader(base byte, n int) []byte {
	if n < 56 {
		return []byte{base + byte(n)}
	}
	var be []byte
	for v := n; v > 0; v >>= 8 {
		be = append([]byte{byte(v)}, be...)
	}
	return append([]byte{base + 55 + byte(len(be))}, be...)
}

func rlpRef(ref fnode) []byte {
	switch ref := ref.(type) {
	case nil:
		return []byte{0x80}
	case fhash:
		return rlpString(ref)
	case fembed:
		return ref
	}
	panic(fmt.Sprintf("commit: ref %T", ref))
}

// fastStorage updates one contract's storage trie from j.p.slots in parallel.
// ok=false means the caller should use libevm (root not a branch).
func (d *Dirty) fastStorage(j *job) (common.Hash, *trienode.NodeSet, bool, error) {
	if j.root == types.EmptyRootHash {
		return common.Hash{}, nil, false, nil // nothing to split; libevm builds it
	}
	r := &fresolver{d: d, owner: j.hash, loaded: map[string]struct{}{}}
	root, err := r.load(nil, fhash(j.root.Bytes()))
	if err != nil {
		return common.Hash{}, nil, false, err
	}
	rb, ok := root.(*fbranch)
	if !ok {
		return common.Hash{}, nil, false, nil
	}
	// Bucket dirty keys by first nibble.
	type kv struct {
		key []byte // nibbles, no terminator
		val []byte // nil = delete
	}
	var buckets [16][]kv
	for k, v := range j.p.slots {
		nib := keyToNibbles(k[:])
		var enc []byte
		if len(v) > 0 {
			enc, _ = rlp.EncodeToBytes(v)
		}
		buckets[nib[0]] = append(buckets[nib[0]], kv{nib, enc})
	}
	outs := make([]*fout, 16)
	loaded := make([]map[string]struct{}, 16)
	errs := make([]error, 16)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		if len(buckets[i]) == 0 {
			continue
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			w := &fresolver{d: d, owner: j.hash, loaded: map[string]struct{}{}}
			loaded[i] = w.loaded
			prefix := []byte{byte(i)}
			n := rb.child[i]
			var err error
			for _, e := range buckets[i] {
				if e.val == nil {
					n, err = w.fdelete(n, prefix, e.key[1:])
				} else {
					n, err = w.finsert(n, prefix, e.key[1:], fvalue(e.val))
				}
				if err != nil {
					errs[i] = err
					return
				}
			}
			o := newOut()
			rb.child[i] = o.commit(n, prefix)
			outs[i] = o
		}(i)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return common.Hash{}, nil, false, err
		}
	}
	// A branch that lost children to deletions would need collapsing; that
	// changes the root shape, which the bucket split cannot express. Fall
	// back to libevm for that rare case.
	kids := 0
	for _, c := range rb.child {
		if c != nil {
			kids++
		}
	}
	if kids < 2 {
		return common.Hash{}, nil, false, nil
	}
	top := newOut()
	ref := top.commit(rb, nil)
	h, ok := ref.(fhash)
	if !ok {
		return common.Hash{}, nil, false, nil
	}
	set := trienode.NewNodeSet(j.hash)
	present := map[string]struct{}{"": {}}
	for _, o := range append(outs, top) {
		if o == nil {
			continue
		}
		for p, blob := range o.nodes {
			set.AddNode([]byte(p), trienode.New(common.BytesToHash(o.hashes[p]), blob))
		}
		for p := range o.present {
			present[p] = struct{}{}
		}
	}
	for _, l := range append(loaded, r.loaded) {
		for p := range l {
			if _, ok := present[p]; !ok {
				set.AddNode([]byte(p), trienode.NewDeleted())
			}
		}
	}
	return common.BytesToHash(h), set, true, nil
}
