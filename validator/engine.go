package validator

/*
#cgo CFLAGS: -I${SRCDIR}/../rs/ffi -I${SRCDIR}/../cmd/epochdb-validator/stub
#cgo !epochdb_stub LDFLAGS: -L${SRCDIR}/../rs/target/release -lepochdb_engine -lm -ldl -lpthread -Wl,--allow-multiple-definition
// --allow-multiple-definition: avalanchego's bls (supranational blst, cgo) and the
// engine's blst crate both define the blst assembly symbols; the linker keeps the
// Go side's copy. Same code, same ABI. Open item for rs/ffi: localize blst in the
// staticlib and drop the flag.
#include <stdlib.h>
#include "epochdb_engine.h"
*/
import "C"

import (
	"errors"
	"fmt"
	"sync/atomic"
	"unsafe"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/libevm/common"
)

// engine is the cgo edge: one method per ABI function, every payload passed
// as pointer + length, every result copied out of the Rust buffer once and
// freed. crossings counts calls per kind for the per-block log line.
type engine struct {
	p         *C.epochdb_engine
	crossings [nCrossing]atomic.Uint64
}

type crossing int

const (
	xParse crossing = iota
	xVerify
	xAccept
	xReject
	xBuild
	xAccount
	xHeader
	xRPC
	xOther
	nCrossing
)

var crossingNames = [nCrossing]string{"parse", "verify", "accept", "reject", "build", "account", "header", "rpc", "other"}

func cptr(b []byte) *C.uint8_t {
	if len(b) == 0 {
		return nil
	}
	return (*C.uint8_t)(unsafe.Pointer(&b[0]))
}

// c32 is an input id; out32 an output slot.
func c32(id ids.ID) *C.uint8_t    { return (*C.uint8_t)(unsafe.Pointer(&id[0])) }
func out32(id *ids.ID) *C.uint8_t { return (*C.uint8_t)(unsafe.Pointer(&id[0])) }

// take copies a Rust buffer into Go memory and frees it.
func take(b *C.epochdb_buf) []byte {
	if b.ptr == nil || b.len == 0 {
		C.epochdb_buf_free(b)
		return nil
	}
	out := C.GoBytes(unsafe.Pointer(b.ptr), C.int(b.len))
	C.epochdb_buf_free(b)
	return out
}

func open(dataDir string, genesis, upgrade, config []byte, chainID, subnetID ids.ID, networkID uint32) (*engine, error) {
	dir := []byte(dataDir)
	var errBuf C.epochdb_buf
	p := C.epochdb_open(cptr(dir), C.size_t(len(dir)), cptr(genesis), C.size_t(len(genesis)),
		cptr(upgrade), C.size_t(len(upgrade)), cptr(config), C.size_t(len(config)),
		c32(chainID), c32(subnetID), C.uint32_t(networkID), &errBuf)
	if p == nil {
		return nil, fmt.Errorf("epochdb_open: %s", take(&errBuf))
	}
	return &engine{p: p}, nil
}

func (e *engine) close() { C.epochdb_close(e.p) }

func (e *engine) err(fn string, rc C.int) error {
	if rc == 0 {
		return nil
	}
	var b C.epochdb_buf
	C.epochdb_last_error(e.p, &b)
	return fmt.Errorf("%s: rc=%d: %s", fn, int(rc), take(&b))
}

func (e *engine) setState(st uint32) error {
	e.crossings[xOther].Add(1)
	return e.err("epochdb_set_state", C.epochdb_set_state(e.p, C.uint32_t(st)))
}

type blockMeta struct {
	id, parent ids.ID
	height     uint64
	time       uint64
}

func (e *engine) parse(raw []byte) (blockMeta, error) {
	e.crossings[xParse].Add(1)
	var out C.epochdb_block_meta
	if err := e.err("epochdb_parse", C.epochdb_parse(e.p, cptr(raw), C.size_t(len(raw)), &out)); err != nil {
		return blockMeta{}, err
	}
	var m blockMeta
	m.id = *(*ids.ID)(unsafe.Pointer(&out.id[0]))
	m.parent = *(*ids.ID)(unsafe.Pointer(&out.parent[0]))
	m.height, m.time = uint64(out.height), uint64(out.timestamp)
	return m, nil
}

func (e *engine) verify(id ids.ID, pchainHeight uint64) (root common.Hash, gasUsed, txs uint64, err error) {
	e.crossings[xVerify].Add(1)
	var out C.epochdb_verify_out
	if err = e.err("epochdb_verify", C.epochdb_verify(e.p, c32(id), C.uint64_t(pchainHeight), &out)); err != nil {
		return
	}
	root = *(*common.Hash)(unsafe.Pointer(&out.state_root[0]))
	return root, uint64(out.gas_used), uint64(out.tx_count), nil
}

func (e *engine) accept(id ids.ID) error {
	e.crossings[xAccept].Add(1)
	return e.err("epochdb_accept", C.epochdb_accept(e.p, c32(id)))
}

func (e *engine) reject(id ids.ID) error {
	e.crossings[xReject].Add(1)
	return e.err("epochdb_reject", C.epochdb_reject(e.p, c32(id)))
}

func (e *engine) lastAccepted() (ids.ID, uint64, error) {
	e.crossings[xOther].Add(1)
	var id ids.ID
	var h C.uint64_t
	err := e.err("epochdb_last_accepted", C.epochdb_last_accepted(e.p, out32(&id), &h))
	return id, uint64(h), err
}

var errNotFound = errors.New("not found")

func (e *engine) blockIDAtHeight(h uint64) (ids.ID, error) {
	e.crossings[xOther].Add(1)
	var id ids.ID
	if rc := C.epochdb_block_id_at_height(e.p, C.uint64_t(h), out32(&id)); rc != 0 {
		return ids.Empty, errNotFound
	}
	return id, nil
}

func (e *engine) getBlock(id ids.ID) ([]byte, error) {
	e.crossings[xOther].Add(1)
	var b C.epochdb_buf
	if rc := C.epochdb_get_block(e.p, c32(id), &b); rc != 0 {
		return nil, errNotFound
	}
	return take(&b), nil
}

type buildOut struct {
	block     []byte
	id        ids.ID
	gasUsed   uint64
	included  uint64
	skipped   []byte
	needsMore bool
}

// build: txs is the RLP list of candidate tx envelopes in the miner's order.
func (e *engine) build(parent ids.ID, timestampMS uint64, coinbase common.Address, pchainHeight uint64, txs []byte) (buildOut, error) {
	e.crossings[xBuild].Add(1)
	var out C.epochdb_build_out
	rc := C.epochdb_build(e.p, c32(parent), C.uint64_t(timestampMS), (*C.uint8_t)(unsafe.Pointer(&coinbase[0])),
		C.uint64_t(pchainHeight), cptr(txs), C.size_t(len(txs)), &out)
	if err := e.err("epochdb_build", rc); err != nil {
		return buildOut{}, err
	}
	return buildOut{
		block:     take(&out.block_bytes),
		id:        *(*ids.ID)(unsafe.Pointer(&out.id[0])),
		gasUsed:   uint64(out.gas_used),
		included:  uint64(out.included_count),
		skipped:   take(&out.skipped),
		needsMore: out.needs_more != 0,
	}, nil
}

// accountState reads nonce + balance of n addresses at a block's state in
// one crossing (zero id = the accepted head). Result: n x (u64 LE nonce,
// 32-byte BE balance).
func (e *engine) accountState(addrs []common.Address, block ids.ID) ([]byte, error) {
	e.crossings[xAccount].Add(1)
	var b C.epochdb_buf
	var p *C.uint8_t
	if len(addrs) > 0 {
		p = (*C.uint8_t)(unsafe.Pointer(&addrs[0][0]))
	}
	if err := e.err("epochdb_account_state", C.epochdb_account_state(e.p, p, C.size_t(len(addrs)), c32(block), &b)); err != nil {
		return nil, err
	}
	out := take(&b)
	if len(out) != 40*len(addrs) {
		return nil, fmt.Errorf("epochdb_account_state: %d bytes for %d addresses", len(out), len(addrs))
	}
	return out, nil
}

func (e *engine) headHeader() ([]byte, error) {
	e.crossings[xHeader].Add(1)
	var b C.epochdb_buf
	if err := e.err("epochdb_head_header", C.epochdb_head_header(e.p, &b)); err != nil {
		return nil, err
	}
	return take(&b), nil
}

func (e *engine) rpc(body []byte) ([]byte, error) {
	e.crossings[xRPC].Add(1)
	var b C.epochdb_buf
	if err := e.err("epochdb_rpc", C.epochdb_rpc(e.p, cptr(body), C.size_t(len(body)), &b)); err != nil {
		return nil, err
	}
	return take(&b), nil
}

func (e *engine) health() ([]byte, error) {
	e.crossings[xOther].Add(1)
	var b C.epochdb_buf
	if err := e.err("epochdb_health", C.epochdb_health(e.p, &b)); err != nil {
		return nil, err
	}
	return take(&b), nil
}

// snapshot returns the crossing counters.
func (e *engine) snapshot() (out [nCrossing]uint64) {
	for i := range out {
		out[i] = e.crossings[i].Load()
	}
	return out
}
