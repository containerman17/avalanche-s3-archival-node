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
//
// ONE CALL IS ANNOUNCED TWICE when a stateful precompile calls back into the
// EVM: see reannounced below. That is a libevm shape, not a capture defect, and
// collapsing it is what keeps a NativeAssetCall transaction honest.

import (
	"bytes"
	"encoding/binary"
	"fmt"
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
// while the capture moves on.
//
// A NON-EMPTY why IS A HOLE AND THE CALLER TURNS IT INTO DEATH. There are two
// of them, and both are "the trace is not what execution did":
//
//   - the tracer never ran at all (armed is false), which at this pin means the
//     saexec seam;
//   - the tracer ran but the enter/exit stack did not close, so the open frames
//     carry gasUsed 0, no output and no error flag. That is INDISTINGUISHABLE
//     from a real zero-gas success once stored, which makes it exactly the
//     silent hole the fail-stop exists to refuse. The one shape at this pin
//     that reaches it legitimately is the precompile re-announcement, and
//     reannounced below folds that away rather than storing it, so anything
//     still open here is an enter/exit contract this capture does not model.
func (c *frameCapture) take() (rec []byte, addrs [][]byte, why string) {
	if !c.armed {
		return nil, nil, "the frame tracer never ran for this transaction"
	}
	if len(c.stack) != 0 {
		// NAME THE FRAME. "some frame never closed" cost a full wrong-turn
		// diagnosis once; its kind and its two addresses point straight at the
		// execution path that owes the CaptureExit.
		f := &c.cur[c.stack[len(c.stack)-1]]
		return nil, nil, fmt.Sprintf("the call stack did not close: %d of %d frames never got a CaptureExit, innermost is %s from %s to %s at depth %d",
			len(c.stack), len(c.cur), vm.OpCode(f.kind), f.from, f.to, f.depth)
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
	return rec, addrs, ""
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

// reannounced reports whether this CaptureEnter is libevm announcing a call it
// has ALREADY announced, in which case the frame is already open and pushing a
// second one would both duplicate a call that happened once and leave the stack
// one deep at the end of the transaction.
//
// THE SHAPE, exactly as pinned (this is what halted mainnet C at 5,456,905, a
// NativeAssetCall transaction): a stateful precompile that calls back into the
// EVM goes through libevm's precompile environment, which fires CaptureEnter
// itself (core/vm/environment.libevm.go:131) and then hands the very same call
// to evm.call, which fires CaptureEnter again (core/vm/evm.go:230). The
// environment then closes ITS announcement with CaptureEnd rather than
// CaptureExit (environment.libevm.go:135), so one of the two enters can never
// be paired. Only coreth reaches this: nativeasset.NativeAssetCall is the only
// caller of PrecompileEnvironment.Call in the pinned dependency set.
//
// THE TEST IS THE FORWARDED GAS and it cannot false-positive on a real call. A
// genuine nested call is always given strictly less gas than the frame that
// makes it, because that frame pays at least the CALL/CREATE opcode's own cost
// before forwarding and EIP-150 then withholds a 64th. Equal gas, equal
// participants, equal input and equal value, with no event in between, is only
// ever the same call named twice.
func (c *frameCapture) reannounced(typ vm.OpCode, from, to common.Address, input []byte, gas uint64, value []byte) bool {
	// The open frame must also be the most recent one: anything nested in
	// between means execution moved on and this is a new call.
	if len(c.stack) == 0 || c.stack[len(c.stack)-1] != len(c.cur)-1 {
		return false
	}
	f := &c.cur[len(c.cur)-1]
	return f.kind == byte(typ) && f.gas == gas && f.from == from && f.to == to &&
		bytes.Equal(c.arena[f.inOff:f.inOff+f.inLen], input) &&
		bytes.Equal(c.arena[f.valOff:f.valOff+f.valLen], value)
}

func (l frameLogger) CaptureEnter(typ vm.OpCode, from, to common.Address, input []byte, gas uint64, value *big.Int) {
	c := l.c
	c.participant(from)
	c.participant(to)
	var valBytes []byte
	if value != nil && value.Sign() != 0 {
		valBytes = value.Bytes()
	}
	if c.reannounced(typ, from, to, input, gas, valBytes) {
		return
	}
	f := openFrame{kind: byte(typ), depth: byte(len(c.stack)), from: from, to: to, gas: gas}
	if len(valBytes) != 0 {
		// DELEGATECALL carries the PARENT's value by design: excluded by a
		// read-time transfer filter, never by capture (DESIGN).
		f.valOff, f.valLen = c.intern(valBytes)
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
