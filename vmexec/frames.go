package vmexec

// STORED CALL FRAMES, THE PRODUCTION CAPTURE (DESIGN, "Principles": traces are
// stored, and capture failure is death). Every transaction is executed under
// libevm's OWN callTracer, and the JSON it answers is the `itx/<txnum>` row,
// VERBATIM (storage v4, user ruling 2026-09-08). `debug_traceTransaction` with
// the callTracer is then a copy of stored bytes: there is no renderer of ours
// between execution and the wire that could be wrong, and the double-render
// check in rpc/callframes.go compares bytes to bytes.
//
// WHY NOT OUR OWN COMPACT RECORD (storage v0..v3 had one): it held every
// subcall exactly but not the top-level frame's output nor a failed frame's
// error string, so a stored trace could not answer what callTracer answers,
// and the missing pieces exist only at execution time. One resync buys the
// whole answer; the tracer that defines the answer is the one that captures.
//
// THE TRACER IS ALWAYS ON. There is no env var: frames are the one thing not
// derivable from stored bytes, so a corpus captured without them is a corpus
// that has to be re-executed.
//
// PARTICIPANTS ride along: the wrapper records every from/to the tracer sees
// (top-level and nested) for the addr/ postings, with no ECDSA and no JSON
// walk at flush.
//
// A CAPTURE THAT CANNOT CLOSE IS DEATH: callTracer refuses to answer when its
// call stack did not fold back to one top-level frame, and take() hands that
// refusal to the executor, which log.Fatalf's naming the transaction. libevm
// db6d70f2748e balanced the precompile call-out that once tripped this on
// mainnet C 5,456,905 (see exec/frames_coreth_test.go).

import (
	"encoding/json"
	"math/big"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/vm"
	"github.com/ava-labs/libevm/eth/tracers"

	// Registers callTracer in tracers.DefaultDirectory.
	_ "github.com/ava-labs/libevm/eth/tracers/native"
)

// frames is the process's capture. One executor per process (DESIGN, one chain
// per process) and one execution goroutine, so this is a plain var with no lock.
var frames = newFrameCapture()

// frameTracer is what goes into vm.Config for every transaction.
func frameTracer() vm.EVMLogger { return frameLogger{frames} }

type frameCapture struct {
	// inner is libevm's callTracer for the transaction in flight; nil says
	// the tracer never ran, which turns "this path forgot the tracer" from a
	// silent hole into the documented death: see Executor.captureTx.
	inner tracers.Tracer
	addrs []common.Address
	seen  map[common.Address]struct{}
}

func newFrameCapture() *frameCapture {
	return &frameCapture{seen: map[common.Address]struct{}{}}
}

func (c *frameCapture) resetTx() {
	c.inner, c.addrs = nil, c.addrs[:0]
	clear(c.seen)
}

// take serialises the transaction in flight and starts the next one.
//
// A NON-EMPTY why IS A HOLE AND THE CALLER TURNS IT INTO DEATH: the tracer
// never ran (the saexec seam at this pin), or its call stack did not close,
// which callTracer itself refuses to serialise.
func (c *frameCapture) take() (rec []byte, addrs [][]byte, why string) {
	if c.inner == nil {
		return nil, nil, "the frame tracer never ran for this transaction"
	}
	res, err := c.inner.GetResult()
	if err != nil {
		c.resetTx()
		return nil, nil, "callTracer refused the transaction: " + err.Error()
	}
	rec = []byte(res)
	for _, a := range c.addrs {
		addrs = append(addrs, a.Bytes())
	}
	c.resetTx()
	return rec, addrs, ""
}

func (c *frameCapture) participant(a common.Address) {
	if _, dup := c.seen[a]; dup {
		return
	}
	c.seen[a] = struct{}{}
	c.addrs = append(c.addrs, a)
}

// frameLogger is the vm.EVMLogger seam: every hook goes to the callTracer,
// and the two address hooks also feed the participants.
type frameLogger struct{ c *frameCapture }

func (l frameLogger) CaptureTxStart(gasLimit uint64) {
	l.c.resetTx()
	t, err := tracers.DefaultDirectory.New("callTracer", &tracers.Context{}, json.RawMessage(nil))
	if err != nil {
		// The name is a constant registered by the import above; this cannot
		// fail, and a nil inner is the documented death if it ever does.
		return
	}
	l.c.inner = t
	t.CaptureTxStart(gasLimit)
}

func (l frameLogger) CaptureTxEnd(restGas uint64) {
	if l.c.inner != nil {
		l.c.inner.CaptureTxEnd(restGas)
	}
}

// CaptureStart is the TOP-LEVEL frame: `from` is the sender the EVM already
// recovered, `to` is the recipient or, for a creation, the created address.
func (l frameLogger) CaptureStart(env *vm.EVM, from, to common.Address, create bool, input []byte, gas uint64, value *big.Int) {
	l.c.participant(from)
	l.c.participant(to)
	if l.c.inner != nil {
		l.c.inner.CaptureStart(env, from, to, create, input, gas, value)
	}
}

func (l frameLogger) CaptureEnd(output []byte, gasUsed uint64, err error) {
	if l.c.inner != nil {
		l.c.inner.CaptureEnd(output, gasUsed, err)
	}
}

func (l frameLogger) CaptureEnter(typ vm.OpCode, from, to common.Address, input []byte, gas uint64, value *big.Int) {
	l.c.participant(from)
	l.c.participant(to)
	if l.c.inner != nil {
		l.c.inner.CaptureEnter(typ, from, to, input, gas, value)
	}
}

func (l frameLogger) CaptureExit(output []byte, gasUsed uint64, err error) {
	if l.c.inner != nil {
		l.c.inner.CaptureExit(output, gasUsed, err)
	}
}

// The per-opcode hooks stay empty: the plain callTracer ignores them (they
// only matter for withLog), so not forwarding them is what keeps capture free.
func (frameLogger) CaptureState(uint64, vm.OpCode, uint64, uint64, *vm.ScopeContext, []byte, int, error) {
}
func (frameLogger) CaptureFault(uint64, vm.OpCode, uint64, uint64, *vm.ScopeContext, int, error) {}
