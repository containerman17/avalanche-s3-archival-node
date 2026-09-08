// Package pruned implements a revision backend for subnet-evm over latest and
// commit. It stores current state and a bounded recovery journal.
package pruned

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/rawdb"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/libevm/stateconf"
	"github.com/ava-labs/libevm/trie/trienode"
	"github.com/ava-labs/libevm/trie/triestate"
	"github.com/ava-labs/libevm/triedb"
	"github.com/ava-labs/libevm/triedb/database"

	"github.com/containerman17/avalanche-s3-archival-node/commit"
	"github.com/containerman17/avalanche-s3-archival-node/latest"
)

var ErrPruned = errors.New("epochdb: state revision is unavailable")

type Config struct {
	Dir            string
	Retain         uint64
	CommitInterval uint64
	// JournalLimit bounds recovery work; a checkpoint replaces the old journal.
	JournalLimit int64
}

type operation struct{ key, value []byte }
type nodeKey struct {
	owner common.Hash
	path  string
}
type nodeValue struct {
	blob []byte
	err  error
}

type revision struct {
	root, block       common.Hash
	height, sequence  uint64
	parent            *revision
	ops               []operation
	flat              map[string][]byte
	wiped             map[common.Hash]bool
	nodes             map[nodeKey][]byte
	undoFlat          map[string][]byte
	undoNodes         map[nodeKey]nodeValue
	accepted, invalid bool
}

type candidateKey struct{ parent, root common.Hash }

type DB struct {
	mu       sync.RWMutex
	cfg      Config
	run      *latest.Run
	file     *commit.File
	overlay  *latest.Overlay
	dirty    *commit.Dirty
	owners   map[common.Hash]bool
	current  *revision
	history  []*revision
	blocks   map[common.Hash]*revision
	roots    map[common.Hash][]*revision
	possible map[candidateKey]*revision
	wal      *os.File
	lock     *os.File
	walBytes int64
	gen      uint64
	closed   bool
	fault    error
}

var _ triedb.DBOverride = (*DB)(nil)

func New(cfg Config) (*DB, error) {
	if cfg.Dir == "" {
		return nil, errors.New("epochdb: state directory is required")
	}
	if cfg.Retain == 0 {
		cfg.Retain = 32
	}
	if cfg.Retain < 2 {
		return nil, errors.New("epochdb: retain must be at least two revisions")
	}
	if cfg.CommitInterval == 0 {
		cfg.CommitInterval = cfg.Retain - 1
	}
	cfg.CommitInterval = min(cfg.CommitInterval, cfg.Retain-1)
	if cfg.JournalLimit == 0 {
		cfg.JournalLimit = 64 << 20
	}
	if cfg.JournalLimit < 1 {
		return nil, errors.New("epochdb: journal limit must be positive")
	}
	if err := os.MkdirAll(cfg.Dir, 0755); err != nil {
		return nil, err
	}
	d := &DB{cfg: cfg, overlay: latest.NewOverlay(), owners: map[common.Hash]bool{}, blocks: map[common.Hash]*revision{}, roots: map[common.Hash][]*revision{}, possible: map[candidateKey]*revision{}}
	if err := d.restore(); err != nil {
		d.closeFiles()
		return nil, err
	}
	return d, nil
}

func (*DB) Scheme() string { return rawdb.HashScheme }
func (d *DB) Initialized(common.Hash) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.current != nil && d.current.root != types.EmptyRootHash
}
func (d *DB) Size() (common.StorageSize, common.StorageSize) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.closed {
		return 0, 0
	}
	return 0, common.StorageSize(d.overlay.Bytes() + d.dirty.Bytes())
}
func (*DB) Reference(common.Hash, common.Hash) {}
func (*DB) Dereference(common.Hash)            {}
func (*DB) Cap(common.StorageSize) error       { return nil }

func (d *DB) check() error {
	if d.closed {
		return errors.New("epochdb: database is closed")
	}
	return d.fault
}

func (d *DB) valid(r *revision) bool {
	return r != nil && !r.invalid && (!r.accepted || d.current.sequence-r.sequence < d.cfg.Retain)
}

func (d *DB) find(root common.Hash) (*revision, error) {
	if err := d.check(); err != nil {
		return nil, err
	}
	if d.current.root == root {
		return d.current, nil
	}
	list := d.roots[root]
	for i := len(list) - 1; i >= 0; i-- {
		if d.valid(list[i]) {
			return list[i], nil
		}
	}
	return nil, fmt.Errorf("%w: %s", ErrPruned, root)
}

func (d *DB) open(root common.Hash) (*revision, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.find(root)
}

func (d *DB) currentGet(key []byte) []byte {
	v, _ := latest.NewView(d.overlay, d.run).Get(key)
	return bytes.Clone(v)
}

func (d *DB) getAt(r *revision, key []byte) ([]byte, error) {
	if !d.valid(r) {
		return nil, ErrPruned
	}
	for !r.accepted {
		if v, ok := r.flat[string(key)]; ok {
			return bytes.Clone(v), nil
		}
		if len(key) == 65 && r.wiped[common.BytesToHash(key[:32])] {
			return nil, nil
		}
		r = r.parent
		if !d.valid(r) {
			return nil, ErrPruned
		}
	}
	value := d.currentGet(key)
	for i := len(d.history) - 1; i >= 0 && d.history[i].sequence > r.sequence; i-- {
		if old, ok := d.history[i].undoFlat[string(key)]; ok {
			value = bytes.Clone(old)
		}
	}
	return value, nil
}

func (d *DB) get(r *revision, key []byte) ([]byte, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if err := d.check(); err != nil {
		return nil, err
	}
	return d.getAt(r, key)
}

func (d *DB) nodeAt(r *revision, owner common.Hash, path []byte, hash common.Hash) ([]byte, error) {
	if !d.valid(r) {
		return nil, ErrPruned
	}
	k := nodeKey{owner, string(path)}
	for !r.accepted {
		if v, ok := r.nodes[k]; ok {
			return bytes.Clone(v), nil
		}
		r = r.parent
		if !d.valid(r) {
			return nil, ErrPruned
		}
	}
	v, err := d.dirty.Node(owner, path, hash)
	for i := len(d.history) - 1; i >= 0 && d.history[i].sequence > r.sequence; i-- {
		if old, ok := d.history[i].undoNodes[k]; ok {
			v, err = old.blob, old.err
		}
	}
	return bytes.Clone(v), err
}

// internalReader is used while the database lock is held by proposal hashing.
type internalReader struct {
	db       *DB
	revision *revision
}

func (r internalReader) Node(owner common.Hash, path []byte, hash common.Hash) ([]byte, error) {
	return r.db.nodeAt(r.revision, owner, path, hash)
}

type publicReader struct{ internalReader }

func (r publicReader) Node(owner common.Hash, path []byte, hash common.Hash) ([]byte, error) {
	r.db.mu.RLock()
	defer r.db.mu.RUnlock()
	if err := r.db.check(); err != nil {
		return nil, err
	}
	return r.internalReader.Node(owner, path, hash)
}
func (d *DB) Reader(root common.Hash) (database.Reader, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	r, err := d.find(root)
	if err != nil {
		return nil, err
	}
	return publicReader{internalReader{d, r}}, nil
}

func makeRevision(parent *revision, ops []operation) *revision {
	r := &revision{parent: parent, flat: map[string][]byte{}, wiped: map[common.Hash]bool{}, nodes: map[nodeKey][]byte{}}
	for _, op := range ops {
		op = operation{bytes.Clone(op.key), bytes.Clone(op.value)}
		r.ops = append(r.ops, op)
		if len(op.key) == 33 && len(op.value) == 0 {
			owner := common.BytesToHash(op.key[:32])
			r.wiped[owner] = true
			for k := range r.flat {
				if len(k) == 65 && bytes.Equal([]byte(k[:32]), owner[:]) {
					delete(r.flat, k)
				}
			}
		}
		r.flat[string(op.key)] = op.value
	}
	return r
}

func (d *DB) compute(parent *revision, ops []operation) (*revision, error) {
	if !d.valid(parent) {
		return nil, ErrPruned
	}
	r := makeRevision(parent, ops)
	layer := commit.NewLayer(parent.root, internalReader{d, parent})
	for _, op := range ops {
		if err := layer.Apply(op.key, op.value); err != nil {
			return nil, err
		}
	}
	root, err := layer.Root()
	if err != nil {
		return nil, err
	}
	r.root = root
	for _, n := range layer.NodeChanges() {
		r.nodes[nodeKey{n.Owner, string(n.Path)}] = n.Blob
	}
	return r, nil
}

func (d *DB) propose(parent *revision, ops []operation) (common.Hash, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.check(); err != nil {
		return common.Hash{}, err
	}
	r, err := d.compute(parent, ops)
	if err != nil {
		return common.Hash{}, err
	}
	d.possible[candidateKey{parent.root, r.root}] = r
	return r.root, nil
}

func (d *DB) Update(root, parent common.Hash, height uint64, _ *trienode.MergedNodeSet, _ *triestate.Set, opts ...stateconf.TrieDBUpdateOption) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.check(); err != nil {
		return err
	}
	parentHash, blockHash, ok := stateconf.ExtractTrieDBUpdatePayload(opts...)
	if !ok {
		return fmt.Errorf("epochdb: missing block identity at height %d", height)
	}
	if old := d.blocks[blockHash]; old != nil && d.valid(old) {
		if old.root != root || old.height != height {
			return errors.New("epochdb: conflicting block identity")
		}
		clear(d.possible)
		return nil
	}
	p := d.blocks[parentHash]
	if p == nil || !d.valid(p) || p.root != parent {
		return fmt.Errorf("epochdb: unknown parent %s at height %d", parentHash, height)
	}
	if height != p.height+1 && !(height == 0 && parentHash == (common.Hash{}) && p.sequence == 0) {
		return errors.New("epochdb: nonconsecutive proposal height")
	}
	// Distinct block hashes may describe the same state transition. Preserve
	// both identities while committing the transition only once.
	for _, existing := range d.roots[root] {
		if !existing.accepted && !existing.invalid && existing.parent == p && existing.height == height {
			d.blocks[blockHash] = existing
			clear(d.possible)
			return nil
		}
	}
	r := d.possible[candidateKey{parent, root}]
	if r == nil && root == parent {
		r = makeRevision(p, nil)
		r.root = root
	}
	if r == nil {
		return fmt.Errorf("epochdb: no computed proposal for root %s", root)
	}
	r.parent = p
	r.block = blockHash
	r.height = height
	d.blocks[blockHash] = r
	d.roots[root] = append(d.roots[root], r)
	clear(d.possible)
	return nil
}

func (d *DB) Commit(root common.Hash, _ bool) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.check(); err != nil {
		return err
	}
	var selected *revision
	for _, r := range d.roots[root] {
		if r.accepted || r.invalid || r.parent != d.current {
			continue
		}
		if selected != nil {
			return errors.New("epochdb: ambiguous accepted proposal")
		}
		selected = r
	}
	if selected == nil {
		return fmt.Errorf("epochdb: no accepted child for root %s", root)
	}
	selected.sequence = d.current.sequence + 1
	if err := d.appendJournal(selected); err != nil {
		d.fault = err
		return err
	}
	if err := d.accept(selected); err != nil {
		d.fault = err
		return err
	}
	if d.walBytes >= d.cfg.JournalLimit || d.overlay.Bytes()+d.dirty.Bytes() > 256<<20 {
		if err := d.checkpoint(); err != nil {
			d.fault = err
			return err
		}
	}
	return nil
}

func (d *DB) accept(r *revision) error {
	r.undoFlat = map[string][]byte{}
	r.undoNodes = map[nodeKey]nodeValue{}
	changes := make([]commit.NodeChange, 0, len(r.nodes))
	for k, v := range r.nodes {
		old, err := d.dirty.Node(k.owner, []byte(k.path), common.Hash{})
		r.undoNodes[k] = nodeValue{bytes.Clone(old), err}
		changes = append(changes, commit.NodeChange{Owner: k.owner, Path: []byte(k.path), Blob: v})
	}
	for owner := range r.wiped {
		lo := append(bytes.Clone(owner[:]), 1)
		hi := append(bytes.Clone(owner[:]), 2)
		if !d.owners[owner] && !latest.NewView(nil, d.run).Iter(lo, hi).Next() {
			continue
		}
		it := latest.NewView(d.overlay, d.run).Iter(lo, hi)
		var keys [][]byte
		for it.Next() {
			keys = append(keys, bytes.Clone(it.Key()))
		}
		if err := it.Err(); err != nil {
			return err
		}
		for _, k := range keys {
			r.undoFlat[string(k)] = d.currentGet(k)
			d.overlay.Put(k, nil)
		}
		delete(d.owners, owner)
	}
	for k, v := range r.flat {
		if _, ok := r.undoFlat[k]; !ok {
			r.undoFlat[k] = d.currentGet([]byte(k))
		}
		d.overlay.Put([]byte(k), v)
		if len(k) == 65 && len(v) > 0 {
			d.owners[common.BytesToHash([]byte(k[:32]))] = true
		}
	}
	if err := d.dirty.ApplyNodes(r.root, changes); err != nil {
		return err
	}
	old := d.current
	r.accepted = true
	d.current = r
	d.history = append(d.history, r)
	// A decision invalidates sibling branches, including their descendants.
	for _, candidate := range d.blocks {
		if candidate.accepted {
			continue
		}
		p := candidate
		for p.parent != nil && !p.parent.accepted {
			p = p.parent
		}
		if p.parent == old {
			candidate.invalid = true
		}
	}
	r.parent = nil
	r.ops = nil
	r.flat = nil
	r.wiped = nil
	r.nodes = nil
	for len(d.history) > int(d.cfg.Retain) {
		retired := d.history[0]
		retired.invalid = true
		retired.undoFlat = nil
		retired.undoNodes = nil
		d.history = d.history[1:]
	}
	for hash, rev := range d.blocks {
		if !d.valid(rev) {
			delete(d.blocks, hash)
		}
	}
	for root, list := range d.roots {
		keep := list[:0]
		for _, rev := range list {
			if d.valid(rev) {
				keep = append(keep, rev)
			}
		}
		if len(keep) == 0 {
			delete(d.roots, root)
		} else {
			d.roots[root] = keep
		}
	}
	return nil
}

func (d *DB) SetHashAndHeight(hash common.Hash, height uint64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return
	}
	if height != d.current.height {
		d.fault = fmt.Errorf("epochdb: recovery height %d differs from current state height %d", height, d.current.height)
		return
	}
	delete(d.blocks, d.current.block)
	d.current.block = hash
	d.current.height = height
	d.blocks[hash] = d.current
}

func (d *DB) ClearAll() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.check(); err != nil {
		return err
	}
	if d.current.root == types.EmptyRootHash {
		d.current.block = common.Hash{}
		d.current.height = 0
		d.blocks = map[common.Hash]*revision{{}: d.current}
		return nil
	}
	return errors.New("epochdb: refusing to discard persisted state during genesis recovery")
}

func (d *DB) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil
	}
	err := d.wal.Sync()
	d.closed = true
	d.closeFiles()
	return err
}

func (d *DB) closeFiles() {
	if d.wal != nil {
		d.wal.Close()
	}
	if d.run != nil {
		d.run.Close()
	}
	if d.file != nil {
		d.file.Close()
	}
	if d.lock != nil {
		d.lock.Close()
	}
}

func (d *DB) runPath(gen uint64) string { return filepath.Join(d.cfg.Dir, fmt.Sprintf("run.%d", gen)) }
func (d *DB) triePath(gen uint64) string {
	return filepath.Join(d.cfg.Dir, fmt.Sprintf("trie.%d", gen))
}
