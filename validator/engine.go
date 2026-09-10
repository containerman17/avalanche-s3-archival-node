package validator

/*
#cgo CFLAGS: -I${SRCDIR}/../rs/ffi -I${SRCDIR}/../cmd/epochdb-validator/stub
#cgo !epochdb_stub LDFLAGS: -L${SRCDIR}/../rs/target/release -lepochdb_engine -lm -ldl -lpthread
// The archive must be localized (rs/ffi/localize.sh) so its blst, secp256k1 and
// Rust runtime symbols do not collide with avalanchego's bls and firewood.
#include <stdlib.h>
#include "epochdb_engine.h"
*/
import "C"

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/rlp"
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
	xPool
	xOther
	nCrossing
)

var crossingNames = [nCrossing]string{"parse", "verify", "accept", "reject", "build", "account", "header", "rpc", "pool", "other"}

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

// errNotFound: the engine does not know the id / height (rc EPOCHDB_ENOTFOUND).
// Any other failure is a real error and must not be mistaken for it.
var errNotFound = errors.New("not found")

const rcNotFound = -3 // EPOCHDB_ENOTFOUND

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
	rc := C.epochdb_get_block(e.p, c32(id), &b)
	if rc == rcNotFound {
		return nil, errNotFound
	}
	if err := e.err("epochdb_get_block", rc); err != nil {
		return nil, err
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
	// Engine phases in ns (see epochdb_build_out.phase_ns), plus the Go-side
	// copy-out of the result buffers at the end.
	phaseNS [nEnginePhase + 1]time.Duration
}

// enginePhaseNames name epochdb_build_out.phase_ns, then Go's copy-out.
var enginePhaseNames = [nEnginePhase + 1]string{"decode", "template", "recover", "exec", "finish", "root", "assemble", "cache", "tree", "outbuf", "copyout"}

const nEnginePhase = 10

// build: txs is the RLP list of candidate tx envelopes in the miner's order,
// senders their recovered senders (20 bytes each; nil = the engine recovers);
// txs nil = the engine takes the candidates from its own pool.
func (e *engine) build(parent ids.ID, timestampMS uint64, coinbase common.Address, pchainHeight uint64, txs, senders []byte) (buildOut, error) {
	e.crossings[xBuild].Add(1)
	var out C.epochdb_build_out
	rc := C.epochdb_build(e.p, c32(parent), C.uint64_t(timestampMS), (*C.uint8_t)(unsafe.Pointer(&coinbase[0])),
		C.uint64_t(pchainHeight), cptr(txs), C.size_t(len(txs)), cptr(senders), C.size_t(len(senders)), &out)
	if err := e.err("epochdb_build", rc); err != nil {
		return buildOut{}, err
	}
	t := time.Now()
	o := buildOut{
		block:     take(&out.block_bytes),
		id:        *(*ids.ID)(unsafe.Pointer(&out.id[0])),
		gasUsed:   uint64(out.gas_used),
		included:  uint64(out.included_count),
		skipped:   take(&out.skipped),
		needsMore: out.needs_more != 0,
	}
	for i := 0; i < nEnginePhase; i++ {
		o.phaseNS[i] = time.Duration(out.phase_ns[i])
	}
	o.phaseNS[nEnginePhase] = time.Since(t)
	return o, nil
}

// poolResult: one epochdb_pool_add answer.
type poolResult struct {
	code byte
	hash common.Hash
}

// Pool admission codes (ABI.md).
const (
	poolOK       = 0
	poolKnown    = 1
	poolReplaced = 2
)

var poolCodeText = [...]string{"ok", "already known", "replaced", "transaction underpriced", "nonce too low", "insufficient funds for gas * price + value",
	"exceeds block gas limit", "intrinsic gas too low", "invalid sender", "txpool is full", "invalid transaction"}

func (r poolResult) ok() bool { return r.code == poolOK || r.code == poolReplaced }
func (r poolResult) err() error {
	if r.ok() {
		return nil
	}
	if int(r.code) < len(poolCodeText) {
		return errors.New(poolCodeText[r.code])
	}
	return fmt.Errorf("pool code %d", r.code)
}

// poolAdd admits tx envelopes (MarshalBinary form) in one crossing.
func (e *engine) poolAdd(txs [][]byte, local bool) ([]poolResult, error) {
	e.crossings[xPool].Add(1)
	raw, err := rlp.EncodeToBytes(txs)
	if err != nil {
		return nil, err
	}
	var b C.epochdb_buf
	var l C.uint8_t
	if local {
		l = 1
	}
	if err := e.err("epochdb_pool_add", C.epochdb_pool_add(e.p, cptr(raw), C.size_t(len(raw)), l, &b)); err != nil {
		return nil, err
	}
	out := take(&b)
	if len(out) != 33*len(txs) {
		return nil, fmt.Errorf("epochdb_pool_add: %d bytes for %d txs", len(out), len(txs))
	}
	res := make([]poolResult, len(txs))
	for i := range res {
		res[i].code = out[i*33]
		copy(res[i].hash[:], out[i*33+1:i*33+33])
	}
	return res, nil
}

func (e *engine) poolStatus() (pending, queued uint64) {
	e.crossings[xPool].Add(1)
	var p, q C.uint64_t
	C.epochdb_pool_status(e.p, &p, &q)
	return uint64(p), uint64(q)
}

func (e *engine) poolHas(hash ids.ID) bool {
	e.crossings[xPool].Add(1)
	var out C.uint8_t
	C.epochdb_pool_has(e.p, c32(hash), &out)
	return out != 0
}

// poolContent: the pool's tx envelopes, pending then queued, of one address
// or of all (nil); limit per half (0 = all).
func (e *engine) poolContent(addr *common.Address, limit int) ([][]byte, error) {
	e.crossings[xPool].Add(1)
	var b C.epochdb_buf
	var a *C.uint8_t
	if addr != nil {
		a = (*C.uint8_t)(unsafe.Pointer(&addr[0]))
	}
	if err := e.err("epochdb_pool_content", C.epochdb_pool_content(e.p, a, C.size_t(limit), &b)); err != nil {
		return nil, err
	}
	return decodeEnvelopes(take(&b))
}

// poolNonce: the pool's nonce for addr (state nonce + executable txs); ok
// is false when the pool holds nothing of the address.
func (e *engine) poolNonce(addr common.Address) (uint64, bool) {
	e.crossings[xPool].Add(1)
	var n C.uint64_t
	rc := C.epochdb_pool_nonce(e.p, (*C.uint8_t)(unsafe.Pointer(&addr[0])), &n)
	return uint64(n), rc == 0
}

// poolWait blocks until the pool holds an executable tx or the timeout
// passes; true when it does.
func (e *engine) poolWait(timeout time.Duration) bool {
	e.crossings[xPool].Add(1)
	var out C.uint8_t
	C.epochdb_pool_wait(e.p, C.uint64_t(timeout/time.Millisecond), &out)
	return out != 0
}

// poolDrainGossip: every tx admitted since the previous call.
func (e *engine) poolDrainGossip() ([][]byte, error) {
	e.crossings[xPool].Add(1)
	var b C.epochdb_buf
	if err := e.err("epochdb_pool_drain_gossip", C.epochdb_pool_drain_gossip(e.p, &b)); err != nil {
		return nil, err
	}
	return decodeEnvelopes(take(&b))
}

// decodeEnvelopes: the pool's RLP list of byte strings (empty buffer = none).
func decodeEnvelopes(raw []byte) ([][]byte, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var out [][]byte
	if err := rlp.DecodeBytes(raw, &out); err != nil {
		return nil, fmt.Errorf("pool envelopes: %w", err)
	}
	return out, nil
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

// poolDup is the pool's pool_dup_total (txs answered Known from the hash
// alone, no decode or recovery), read from the health JSON.
func (e *engine) poolDup() uint64 {
	raw, err := e.health()
	if err != nil {
		return 0
	}
	var h struct {
		Dup uint64 `json:"pool-dup"`
	}
	_ = json.Unmarshal(raw, &h)
	return h.Dup
}
