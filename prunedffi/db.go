//go:build subnetbench

// Package prunedffi is the Rust pruned state backend (rust/prunedffi) behind
// the same subnet-evm interfaces the Go pruned package implements. Build the
// static library first: cargo build --release in rust/prunedffi.
package prunedffi

// #cgo CFLAGS: -I${SRCDIR}/../rust/prunedffi
// #cgo LDFLAGS: -L${SRCDIR}/../rust/prunedffi/target/release -lprunedffi -lm -ldl -lpthread
// // Firewood's staticlib carries its own Rust std; both define rust_eh_personality.
// #cgo linux LDFLAGS: -Wl,--allow-multiple-definition
// #include <stdlib.h>
// #include "prunedffi.h"
// #cgo noescape pf_propose
// #cgo nocallback pf_propose
// #cgo noescape pf_update
// #cgo nocallback pf_update
// #cgo noescape pf_commit
// #cgo nocallback pf_commit
// #cgo noescape pf_get_account
// #cgo nocallback pf_get_account
// #cgo noescape pf_get_storage
// #cgo nocallback pf_get_storage
// #cgo noescape pf_has_root
// #cgo nocallback pf_has_root
// #cgo noescape pf_current
// #cgo nocallback pf_current
import "C"

import (
	"errors"
	"fmt"
	"runtime"
	"unsafe"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/rawdb"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/libevm/stateconf"
	"github.com/ava-labs/libevm/trie/trienode"
	"github.com/ava-labs/libevm/trie/triestate"
	"github.com/ava-labs/libevm/triedb"
	"github.com/ava-labs/libevm/triedb/database"
)

var ErrPruned = errors.New("prunedffi: state revision is unavailable")

type Config struct {
	Dir            string
	Retain         uint64
	CommitInterval uint64
	JournalLimit   int64
}

type DB struct {
	h *C.Db
}

var _ triedb.DBOverride = (*DB)(nil)

func status(s C.struct_pf_status) error {
	switch s.code {
	case 0:
		return nil
	case 1:
		return ErrPruned
	default:
		err := errors.New("prunedffi: " + C.GoString(s.msg))
		C.pf_free_status(s)
		return err
	}
}

func hashPtr(h *common.Hash) *C.uint8_t { return (*C.uint8_t)(unsafe.Pointer(&h[0])) }

func New(cfg Config) (*DB, error) {
	if cfg.Dir == "" {
		return nil, errors.New("prunedffi: state directory is required")
	}
	dir := C.CString(cfg.Dir)
	defer C.free(unsafe.Pointer(dir))
	var h *C.Db
	if err := status(C.pf_open(dir, C.uint64_t(cfg.Retain), C.uint64_t(cfg.CommitInterval), C.int64_t(cfg.JournalLimit), &h)); err != nil {
		return nil, err
	}
	return &DB{h: h}, nil
}

func (*DB) Scheme() string { return rawdb.HashScheme }
func (d *DB) Initialized(common.Hash) bool {
	root, _, _ := d.head()
	return root != types.EmptyRootHash
}
func (d *DB) Size() (common.StorageSize, common.StorageSize) {
	if d.h == nil {
		return 0, 0
	}
	return 0, common.StorageSize(C.pf_size(d.h))
}
func (*DB) Reference(common.Hash, common.Hash) {}
func (*DB) Dereference(common.Hash)            {}
func (*DB) Cap(common.StorageSize) error       { return nil }

func (d *DB) head() (root, block common.Hash, height uint64) {
	var out C.struct_pf_head
	C.pf_current(d.h, &out)
	copy(root[:], C.GoBytes(unsafe.Pointer(&out.root[0]), 32))
	copy(block[:], C.GoBytes(unsafe.Pointer(&out.block[0]), 32))
	return root, block, uint64(out.height)
}

// open validates that root names a retained revision.
func (d *DB) open(root common.Hash) error {
	return status(C.pf_has_root(d.h, hashPtr(&root)))
}

func (d *DB) propose(parent common.Hash, ops []byte) (common.Hash, error) {
	var root common.Hash
	var p *C.uint8_t
	if len(ops) > 0 {
		p = (*C.uint8_t)(unsafe.Pointer(&ops[0]))
	}
	err := status(C.pf_propose(d.h, hashPtr(&parent), p, C.size_t(len(ops)), hashPtr(&root)))
	runtime.KeepAlive(ops)
	return root, err
}

func (d *DB) getAccount(root, key common.Hash) ([]byte, error) {
	var out C.struct_pf_value
	if err := status(C.pf_get_account(d.h, hashPtr(&root), hashPtr(&key), &out)); err != nil {
		return nil, err
	}
	if out.len == 0 {
		return nil, nil
	}
	return C.GoBytes(unsafe.Pointer(&out.data[0]), C.int(out.len)), nil
}

func (d *DB) getStorage(root, key, slot common.Hash) ([]byte, error) {
	var out C.struct_pf_value
	if err := status(C.pf_get_storage(d.h, hashPtr(&root), hashPtr(&key), hashPtr(&slot), &out)); err != nil {
		return nil, err
	}
	if out.len == 0 {
		return nil, nil
	}
	return C.GoBytes(unsafe.Pointer(&out.data[0]), C.int(out.len)), nil
}

type reader struct{}

// Node is never used: the state accessor serves every trie read flat.
func (reader) Node(common.Hash, []byte, common.Hash) ([]byte, error) {
	return nil, errors.New("prunedffi: trie node reads are unsupported")
}

func (d *DB) Reader(root common.Hash) (database.Reader, error) {
	if err := d.open(root); err != nil {
		return nil, err
	}
	return reader{}, nil
}

func (d *DB) Update(root, parent common.Hash, height uint64, _ *trienode.MergedNodeSet, _ *triestate.Set, opts ...stateconf.TrieDBUpdateOption) error {
	parentHash, blockHash, ok := stateconf.ExtractTrieDBUpdatePayload(opts...)
	if !ok {
		return fmt.Errorf("prunedffi: missing block identity at height %d", height)
	}
	return status(C.pf_update(d.h, hashPtr(&root), hashPtr(&parent), C.uint64_t(height), hashPtr(&parentHash), hashPtr(&blockHash)))
}

func (d *DB) Commit(root common.Hash, _ bool) error {
	return status(C.pf_commit(d.h, hashPtr(&root)))
}

func (d *DB) SetHashAndHeight(hash common.Hash, height uint64) {
	C.pf_set_hash_and_height(d.h, hashPtr(&hash), C.uint64_t(height))
}

func (d *DB) ClearAll() error { return status(C.pf_clear_all(d.h)) }

func (d *DB) checkpoint() error { return status(C.pf_checkpoint(d.h)) }

func (d *DB) Close() error {
	if d.h == nil {
		return nil
	}
	err := status(C.pf_close(d.h))
	d.h = nil
	return err
}
