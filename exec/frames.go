package exec

// STORED CALL FRAMES, THE PRODUCTION CAPTURE (DESIGN.md, "Stored call
// frames"). Every call frame of every transaction is recorded at execution
// into the `itx` tail family, in the epoch encoding VERBATIM, so seal copies
// bytes and never runs an EVM. This replaces the count-only tracer and the
// bounded byte sampler that measured the design inputs (10.4 frames/tx,
// 209 B/frame raw); both are deleted, their numbers live in DESIGN.md.
//
// THE TRACER IS ALWAYS ON. There is no env var: frames are the one thing not
// derivable from stored bytes, so a corpus captured without them is a corpus
// that has to be re-executed. The measured cost is 1.3-4% (this libevm pin
// dispatches per OPCODE even when every hook is a no-op).
//
// TWO RECORDS COME OUT OF ONE PASS, both per block:
//
//   - THE FRAMES RECORD, which is stored in the epoch. Per transaction that
//     has at least one nested frame: uvarint txIndex, uvarint frameCount,
//     then the frames in EXECUTION (enter) order. Enter order plus depth is
//     a pre-order DFS, which is exactly what a call tracer replays; the
//     sampler's exit order carried the same bytes.
//
//   - THE PARTICIPANTS RECORD, which is the ADDRESS INDEX's input and is
//     dropped once the epoch is sealed. One group per transaction, in tx
//     order, so a group's position IS its tx index: uvarint nAddr then the
//     20-byte addresses, first-appearance order, deduped within the tx.
//
// PARTICIPANTS COME FROM THE TRACER, NOT FROM SEAL. CaptureStart hands over
// the top-level (from, to), where `to` is the CREATED CONTRACT ADDRESS for a
// creation, and every CaptureEnter hands over a nested (from, to) including
// CREATE/CREATE2. So the sender, the recipient, the created address and every
// frame participant are all in hand at execution time. This is why seal does
// no ECDSA recovery: the ~90s/8M-tx epoch DESIGN.md budgeted for it is not
// spent, because the EVM already recovered every sender.
//
// KNOWN HOLE, unchanged from the design's list: SAE blocks (saexec.Execute
// hardcodes vm.Config{} at this pin, so there is no tracer seam), coreth
// atomic txs, subnet-evm's nativeMinter and SELFDESTRUCT. Seal REFUSES a
// tx-bearing block with no frames record rather than sealing a silent hole.

import (
	"encoding/binary"
	"math/big"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/vm"

	"github.com/containerman17/epochdb/state"
)

// frames is the process's capture. One executor per process (DESIGN.md, one
// chain per process) and one execution goroutine, so this is a plain var with
// no lock, exactly as the sampler it replaces was.
var frames = newFrameCapture()

// frameTracer is what goes into vm.Config for every transaction.
func frameTracer() vm.EVMLogger { return frameLogger{frames} }

type frameCapture struct {
	rec   []byte // this block's frames record
	parts []byte // this block's participants record
	nTx   int    // transactions seen in this block

	// Per transaction. arena backs every input/output/value copy (the EVM
	// reuses its own buffers), so a frame is a fixed-size struct and the
	// whole tx costs one growing byte slice instead of two allocations per
	// frame.
	txIdx int
	arena []byte
	cur   []openFrame
	stack []int // indexes into cur of the frames still open
	addrs []common.Address
	seen  map[common.Address]struct{}
}

// openFrame is one CaptureEnter waiting for its CaptureExit.
type openFrame struct {
	kind, depth    byte
	from, to       common.Address
	valOff, valLen int
	gas, gasUsed   uint64
	inOff, inLen   int
	outOff, outLen int
	failed         bool
}

func newFrameCapture() *frameCapture {
	return &frameCapture{txIdx: -1, seen: map[common.Address]struct{}{}}
}

// begin resets the capture for one block. Called before the EVM runs, which
// makes a re-executed block (the batch bisect, the crash walk-back) produce
// the same record as the first run rather than appending to it.
func (c *frameCapture) begin() {
	c.rec, c.parts, c.nTx, c.txIdx = c.rec[:0], c.parts[:0], 0, -1
	c.resetTx()
}

func (c *frameCapture) resetTx() {
	c.arena, c.cur, c.stack, c.addrs = c.arena[:0], c.cur[:0], c.stack[:0], c.addrs[:0]
	clear(c.seen)
}

// record returns the block's tail-family record, nil when the block had no
// transactions. The bytes are a fresh copy, so the batched executor can hold
// one per buffered block while the capture moves on.
func (c *frameCapture) record() []byte {
	if c.nTx == 0 {
		return nil
	}
	return state.EncodeTailItx(c.rec, c.parts)
}

// intern copies b into the per-tx arena.
func (c *frameCapture) intern(b []byte) (off, n int) {
	if len(b) == 0 {
		return 0, 0
	}
	off = len(c.arena)
	c.arena = append(c.arena, b...)
	return off, len(b)
}

func (c *frameCapture) participant(a common.Address) {
	if _, dup := c.seen[a]; dup {
		return
	}
	c.seen[a] = struct{}{}
	c.addrs = append(c.addrs, a)
}

// txDone flushes one transaction into the two block records. Every
// transaction contributes a participants group, even an empty one, because a
// group's POSITION is its tx index.
func (c *frameCapture) txDone() {
	c.parts = binary.AppendUvarint(c.parts, uint64(len(c.addrs)))
	for _, a := range c.addrs {
		c.parts = append(c.parts, a[:]...)
	}
	c.nTx++
	if len(c.cur) > 0 {
		c.rec = binary.AppendUvarint(c.rec, uint64(c.txIdx))
		c.rec = binary.AppendUvarint(c.rec, uint64(len(c.cur)))
		for i := range c.cur {
			c.rec = c.appendFrame(c.rec, &c.cur[i])
		}
	}
	c.resetTx()
}

// appendFrame writes one frame in the measured encoding: kind, depth, from,
// to, value, gas, gasUsed, err, input, output.
func (c *frameCapture) appendFrame(b []byte, f *openFrame) []byte {
	b = append(b, f.kind, f.depth)
	b = append(b, f.from[:]...)
	b = append(b, f.to[:]...)
	b = binary.AppendUvarint(b, uint64(f.valLen))
	b = append(b, c.arena[f.valOff:f.valOff+f.valLen]...)
	b = binary.AppendUvarint(b, f.gas)
	b = binary.AppendUvarint(b, f.gasUsed)
	if f.failed {
		b = append(b, 1)
	} else {
		b = append(b, 0)
	}
	b = binary.AppendUvarint(b, uint64(f.inLen))
	b = append(b, c.arena[f.inOff:f.inOff+f.inLen]...)
	b = binary.AppendUvarint(b, uint64(f.outLen))
	return append(b, c.arena[f.outOff:f.outOff+f.outLen]...)
}

// frameLogger is the vm.EVMLogger seam. The per-opcode hooks stay empty: this
// pin has no hook-shaped tracer, so the only cheap thing to do is nothing.
type frameLogger struct{ c *frameCapture }

func (l frameLogger) CaptureTxStart(uint64) {
	l.c.txIdx++
	l.c.resetTx()
}

func (l frameLogger) CaptureTxEnd(uint64) { l.c.txDone() }

// CaptureStart is the TOP-LEVEL frame, which is never stored (it IS the
// transaction), but its two addresses are participants: `from` is the sender
// the EVM already recovered, `to` is the recipient or, for a creation, the
// created contract address.
func (l frameLogger) CaptureStart(_ *vm.EVM, from, to common.Address, _ bool, _ []byte, _ uint64, _ *big.Int) {
	l.c.participant(from)
	l.c.participant(to)
}

func (l frameLogger) CaptureEnd([]byte, uint64, error) {}

func (l frameLogger) CaptureEnter(typ vm.OpCode, from, to common.Address, input []byte, gas uint64, value *big.Int) {
	c := l.c
	c.participant(from)
	c.participant(to)
	f := openFrame{kind: byte(typ), depth: byte(len(c.stack)), from: from, to: to, gas: gas}
	if value != nil && value.Sign() != 0 {
		// DELEGATECALL carries the PARENT's value by design: excluded by a
		// read-time transfer filter, never by capture (DESIGN.md).
		f.valOff, f.valLen = c.intern(value.Bytes())
	}
	f.inOff, f.inLen = c.intern(input)
	c.stack = append(c.stack, len(c.cur))
	c.cur = append(c.cur, f)
}

func (l frameLogger) CaptureExit(output []byte, gasUsed uint64, err error) {
	c := l.c
	if len(c.stack) == 0 {
		return
	}
	f := &c.cur[c.stack[len(c.stack)-1]]
	c.stack = c.stack[:len(c.stack)-1]
	f.gasUsed = gasUsed
	f.failed = err != nil
	f.outOff, f.outLen = c.intern(output)
}

func (frameLogger) CaptureState(uint64, vm.OpCode, uint64, uint64, *vm.ScopeContext, []byte, int, error) {
}
func (frameLogger) CaptureFault(uint64, vm.OpCode, uint64, uint64, *vm.ScopeContext, int, error) {}
