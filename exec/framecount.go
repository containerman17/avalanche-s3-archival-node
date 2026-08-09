package exec

// A COUNT-ONLY frame tracer, no storage and no format: it exists to settle
// the two unmeasured inputs of the stored-call-frames design (DESIGN.md)
// during a real replay. (1) The C-chain frame rate, which sizes the ~+400GB
// estimate. (2) The true cost of a non-nil tracer at this libevm pin, which
// dispatches per OPCODE (~2.6ns each) even when every hook is a no-op, so
// the honest way to measure it is to run exactly this on the real workload.
//
// EPOCHDB_COUNT_FRAMES=1 enables it; anything else and runEVM passes a nil
// tracer, zero overhead. Counters are atomics: the executor is one
// goroutine, but atomics cost nothing here and survive any future caller.
// The executor's status loop prints the totals; nothing is persisted.

import (
	"os"
	"sync/atomic"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/vm"

	"math/big"
)

var frameCountEnabled = os.Getenv("EPOCHDB_COUNT_FRAMES") == "1"

// frameCounts holds cumulative counts since process start.
var frameCounts struct {
	txs      atomic.Uint64
	frames   atomic.Uint64 // all CaptureEnter frames (top-level excluded, it IS the tx)
	call     atomic.Uint64
	callcode atomic.Uint64
	dcall    atomic.Uint64
	scall    atomic.Uint64
	create   atomic.Uint64
	create2  atomic.Uint64
	valued   atomic.Uint64 // frames carrying value != 0 (Glacier's "internal tx" rule)
	inBytes  atomic.Uint64 // sum of input lengths, the size driver of a stored frame
}

// FrameCountLine returns a one-line cumulative summary, empty when disabled.
func FrameCountLine() string {
	if !frameCountEnabled {
		return ""
	}
	c := &frameCounts
	return "frames: tx=" + u(c.txs.Load()) + " frames=" + u(c.frames.Load()) +
		" call=" + u(c.call.Load()) + " dcall=" + u(c.dcall.Load()) +
		" scall=" + u(c.scall.Load()) + " ccode=" + u(c.callcode.Load()) +
		" create=" + u(c.create.Load()) + " create2=" + u(c.create2.Load()) +
		" valued=" + u(c.valued.Load()) + " inMB=" + u(c.inBytes.Load()>>20)
}

func u(v uint64) string {
	// strconv would be the obvious call; this avoids importing it for one
	// hot-path-free helper. Plain and allocation-cheap.
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}

// frameSamplerInst is set once by New (one executor per process); the
// tracer reaches it through this var because vm.Config is built per tx.
var frameSamplerInst *frameSampler

// frameTracer returns the tracer to put in vm.Config, nil when both the
// counter and the sampler are off.
func frameTracer() vm.EVMLogger {
	if !frameCountEnabled && frameSamplerInst == nil {
		return nil
	}
	return frameCounter{}
}

type frameCounter struct{}

func (frameCounter) CaptureTxStart(uint64) { frameCounts.txs.Add(1) }
func (frameCounter) CaptureTxEnd(uint64) {
	if s := frameSamplerInst; s != nil {
		s.txEnd()
	}
}
func (frameCounter) CaptureStart(*vm.EVM, common.Address, common.Address, bool, []byte, uint64, *big.Int) {
}
func (frameCounter) CaptureEnd([]byte, uint64, error) {}

func (frameCounter) CaptureEnter(typ vm.OpCode, from, to common.Address, input []byte, gas uint64, value *big.Int) {
	if s := frameSamplerInst; s != nil {
		s.enter(typ, from, to, input, gas, value)
	}
	if !frameCountEnabled {
		return
	}
	c := &frameCounts
	c.frames.Add(1)
	c.inBytes.Add(uint64(len(input)))
	if value != nil && value.Sign() != 0 {
		c.valued.Add(1)
	}
	switch typ {
	case vm.CALL:
		c.call.Add(1)
	case vm.CALLCODE:
		c.callcode.Add(1)
	case vm.DELEGATECALL:
		c.dcall.Add(1)
	case vm.STATICCALL:
		c.scall.Add(1)
	case vm.CREATE:
		c.create.Add(1)
	case vm.CREATE2:
		c.create2.Add(1)
	}
}

func (frameCounter) CaptureExit(output []byte, gasUsed uint64, err error) {
	if s := frameSamplerInst; s != nil {
		s.exit(output, gasUsed, err)
	}
}
func (frameCounter) CaptureState(uint64, vm.OpCode, uint64, uint64, *vm.ScopeContext, []byte, int, error) {
}
func (frameCounter) CaptureFault(uint64, vm.OpCode, uint64, uint64, *vm.ScopeContext, int, error) {}
