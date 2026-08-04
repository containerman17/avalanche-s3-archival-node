package state

import (
	"errors"
	"sort"
	"strings"
	"sync"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/ethdb"
)

// errNotFound is returned by Get for missing keys. rawdb callers treat any
// error as "not found" (mirrors the pebble sentinel in the reference impl).
var errNotFound = errors.New("state ethdb: not found")

// EthDB returns a libevm ethdb.KeyValueStore over the Store: contract code
// (rawdb key 'c'+hash) routes to Store.Code, i.e. code.log and then the sealed
// epochs, every other key to misc.log.
// This is the durable backing rawdb needs because Firewood explicitly
// delegates code storage to rawdb. Lifetime is tied to the Store.
func (s *Store) EthDB() ethdb.KeyValueStore {
	return &ethKV{s: s}
}

type ethKV struct {
	mu sync.Mutex
	s  *Store
}

// isCodeKey matches libevm rawdb's code key layout: 'c' + 32-byte hash.
func isCodeKey(k []byte) (common.Hash, bool) {
	if len(k) == 33 && k[0] == 'c' {
		return common.BytesToHash(k[1:]), true
	}
	return common.Hash{}, false
}

func (a *ethKV) Has(key []byte) (bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if h, ok := isCodeKey(key); ok {
		_, found, err := a.s.Code(h)
		return found, err
	}
	_, ok := a.s.misc.Get(key)
	return ok, nil
}

func (a *ethKV) Get(key []byte) ([]byte, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if h, ok := isCodeKey(key); ok {
		blob, ok, err := a.s.Code(h)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, errNotFound
		}
		return blob, nil
	}
	if v, ok := a.s.misc.Get(key); ok {
		return v, nil
	}
	return nil, errNotFound
}

func (a *ethKV) Put(key, value []byte) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if h, ok := isCodeKey(key); ok {
		return a.s.code.Put(h, value)
	}
	return a.s.misc.Put(key, value)
}

func (a *ethKV) Delete(key []byte) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, ok := isCodeKey(key); ok {
		// Code is content-addressed and never garbage-collected here.
		return nil
	}
	return a.s.misc.Delete(key)
}

func (a *ethKV) Stat(string) (string, error) {
	return "", errors.New("state ethdb: stat not supported")
}

func (a *ethKV) Compact([]byte, []byte) error { return nil }

func (a *ethKV) NewSnapshot() (ethdb.Snapshot, error) {
	return nil, errors.New("state ethdb: snapshot not supported")
}

func (a *ethKV) Close() error { return nil } // owned by Store

// --- batches ---------------------------------------------------------------

type ethKVBatch struct {
	a    *ethKV
	ops  []kvOp
	size int
}

type kvOp struct {
	key, value []byte
	del        bool
}

func (a *ethKV) NewBatch() ethdb.Batch { return &ethKVBatch{a: a} }

func (a *ethKV) NewBatchWithSize(int) ethdb.Batch { return &ethKVBatch{a: a} }

func (b *ethKVBatch) Put(key, value []byte) error {
	k := make([]byte, len(key))
	copy(k, key)
	v := make([]byte, len(value))
	copy(v, value)
	b.ops = append(b.ops, kvOp{key: k, value: v})
	b.size += len(key) + len(value) + 1
	return nil
}

func (b *ethKVBatch) Delete(key []byte) error {
	k := make([]byte, len(key))
	copy(k, key)
	b.ops = append(b.ops, kvOp{key: k, del: true})
	b.size += len(key) + 1
	return nil
}

func (b *ethKVBatch) ValueSize() int { return b.size }

func (b *ethKVBatch) Write() error {
	for _, op := range b.ops {
		if op.del {
			if err := b.a.Delete(op.key); err != nil {
				return err
			}
			continue
		}
		if err := b.a.Put(op.key, op.value); err != nil {
			return err
		}
	}
	return nil
}

func (b *ethKVBatch) Reset() {
	b.ops = b.ops[:0]
	b.size = 0
}

func (b *ethKVBatch) Replay(w ethdb.KeyValueWriter) error {
	for _, op := range b.ops {
		if op.del {
			if err := w.Delete(op.key); err != nil {
				return err
			}
			continue
		}
		if err := w.Put(op.key, op.value); err != nil {
			return err
		}
	}
	return nil
}

// --- iterator ---------------------------------------------------------------

// NewIterator iterates a snapshot of matching keys sorted ascending. Rarely
// used in our flow (nothing in the executor path iterates); correctness over
// speed.
func (a *ethKV) NewIterator(prefix, start []byte) ethdb.Iterator {
	a.mu.Lock()
	defer a.mu.Unlock()
	// The two store locks, not a.mu, are what makes this safe beside a live
	// executor (a.mu is per-EthDB()-instance, and serve --follow holds two).
	a.s.misc.mu.Lock()
	defer a.s.misc.mu.Unlock()
	a.s.code.mu.Lock()
	defer a.s.code.mu.Unlock()
	lower := string(prefix) + string(start)
	var keys []string
	for k := range a.s.misc.m {
		if strings.HasPrefix(k, string(prefix)) && k >= lower {
			keys = append(keys, k)
		}
	}
	if len(prefix) == 0 || prefix[0] == 'c' {
		for h := range a.s.code.idx {
			k := string(append([]byte{'c'}, h[:]...))
			if strings.HasPrefix(k, string(prefix)) && k >= lower {
				keys = append(keys, k)
			}
		}
	}
	sort.Strings(keys)
	return &ethKVIter{a: a, keys: keys, pos: -1}
}

type ethKVIter struct {
	a    *ethKV
	keys []string
	pos  int
	err  error
}

func (i *ethKVIter) Next() bool {
	i.pos++
	return i.pos < len(i.keys)
}

func (i *ethKVIter) Error() error { return i.err }

func (i *ethKVIter) Key() []byte {
	if i.pos < 0 || i.pos >= len(i.keys) {
		return nil
	}
	return []byte(i.keys[i.pos])
}

func (i *ethKVIter) Value() []byte {
	k := i.Key()
	if k == nil {
		return nil
	}
	v, err := i.a.Get(k)
	if err != nil {
		i.err = err
		return nil
	}
	return v
}

func (i *ethKVIter) Release() {}
