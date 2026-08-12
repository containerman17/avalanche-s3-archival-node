package exec

// STORED CALL FRAMES, THE PRODUCTION CAPTURE (DESIGN, "Principles": traces are
// stored, and capture failure is death). Every call frame of every transaction
// is recorded at execution into the `itx/<txnum>` chain-section family. In
// storage v0 that is ONE MORE STREAMED FAMILY: no groups, no index sections, no
// sort, it arrives in TxNum order exactly like `tx/` and `rcpt/`.
//
// THE TRACER IS ALWAYS ON. There is no env var: frames are the one thing not
// derivable from stored bytes, so a corpus captured without them is a corpus
// that has to be re-executed. Measured free on wall clock (66.7s with against
// 67.6s without over the same corpus).
//
// THE RECORD, per transaction: uvarint frameCount, then the frames in ENTER
// order. Enter order plus depth is a pre-order DFS, which is exactly what a
// call tracer replays. The TOP-LEVEL FRAME IS EXCLUDED because it IS the
// transaction, but its two addresses are participants.
//
// DETERMINISM NOTES, carried from the V1 capture work: DELEGATECALL frames
// carry the PARENT's value by design (excluded by a read-time transfer filter,
// never by capture); SELFDESTRUCT has no hook at this pin. Frames follow
// execution exactly, so a config that would change them diverges the state root
// first, which the executor hard-stops on.

import (
	"encoding/binary"
	"math/big"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/vm"
)

// frames is the process's capture. One executor per process (DESIGN, one chain
// per process) and one execution goroutine, so this is a plain var with no lock.
var frames = newFrameCapture()

// frameTracer is what goes into vm.Config for every transaction.
func frameTracer() vm.EVMLogger { return frameLogger{frames} }

type frameCapture struct {
	// armed says the tracer actually ran for the transaction in flight. It is
	// what turns "this path forgot the tracer" from a silent hole into the
	// documented death: see Executor.captureTx.
	armed bool

	// Per transaction. arena backs every input/output/value copy (the EVM
	// reuses its own buffers), so a frame is a fixed-size struct and the whole
	// tx costs one growing byte slice instead of two allocations per frame.
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
	return &frameCapture{seen: map[common.Address]struct{}{}}
}

func (c *frameCapture) resetTx() {
	c.arena, c.cur, c.stack, c.addrs = c.arena[:0], c.cur[:0], c.stack[:0], c.addrs[:0]
	clear(c.seen)
	c.armed = false
}

// take serialises the transaction in flight and starts the next one. The bytes
// are a fresh copy, so the batched executor can hold one per buffered block
// while the capture moves on. ok=false means the tracer never ran, which is a
// hole and is handled as death by the caller.
func (c *frameCapture) take() (rec []byte, addrs [][]byte, ok bool) {
	if !c.armed {
		return nil, nil, false
	}
	if len(c.cur) > 0 {
		rec = binary.AppendUvarint(nil, uint64(len(c.cur)))
		for i := range c.cur {
			rec = c.appendFrame(rec, &c.cur[i])
		}
	}
	for _, a := range c.addrs {
		addrs = append(addrs, a.Bytes())
	}
	c.resetTx()
	return rec, addrs, true
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

// appendFrame writes one frame: kind, depth, from, to, value, gas, gasUsed,
// err, input, output.
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
	l.c.resetTx()
	l.c.armed = true
}

func (l frameLogger) CaptureTxEnd(uint64) {}

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
		// read-time transfer filter, never by capture (DESIGN).
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
