package jitbench

import (
	"testing"

	"github.com/holiman/uint256"

	"github.com/containerman17/epochdb/exp/jitbench/vmx"
	"github.com/containerman17/epochdb/exp/jitbench/vmx/runtime"
)

// arithCompiled is the "arith" kernel as a PERFECT ahead-of-time compilation
// would emit it: no stack object, no jump table, no program counter, operands
// in locals, and the block's gas charged once per straight-line block exactly
// as the basic-block scheme does. It is what the arithmetic costs when every
// piece of interpreter machinery is gone, so it is the ceiling on any compiler
// for this kernel, in Go, with no cgo boundary.
func arithCompiled(n uint64, gas uint64) (uint64, uint64) {
	var t, seven, three uint256.Int
	seven.SetUint64(7)
	three.SetUint64(3)
	var sink uint64
	for c := n; ; {
		// JUMPDEST + 10x body + PUSH1/SWAP1/SUB/DUP1/PUSH2/JUMPI
		const blockGas = 1 + 10*19 + 3 + 3 + 3 + 3 + 3 + 10
		if gas < blockGas {
			return 0, sink
		}
		gas -= blockGas
		for j := 0; j < 10; j++ {
			t.SetUint64(c)
			t.Add(&t, &seven)
			t.Mul(&t, &three)
			sink += t[0]
		}
		c--
		if c == 0 {
			break
		}
	}
	return gas, sink
}

var sinkU uint64

// BenchmarkCeiling puts the interpreted kernel and the hand-compiled kernel
// side by side. The ratio is the honest upper bound for an AOT compiler on the
// most compiler-friendly workload there is.
func BenchmarkCeiling(b *testing.B) {
	const n = 20000
	code := benchKernels["arith"]
	cfg := newCfg(code)
	b.Run("interpreted", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			cfg.GasLimit = 300_000_000
			runtime.Call(callee, nil, cfg)
		}
	})
	b.Run("blockgas", func(b *testing.B) {
		vmx.Mode = vmx.ModeBlockGas
		defer func() { vmx.Mode = vmx.ModeStock }()
		for i := 0; i < b.N; i++ {
			cfg.GasLimit = 300_000_000
			runtime.Call(callee, nil, cfg)
		}
	})
	b.Run("compiled", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, s := arithCompiled(n, 300_000_000)
			sinkU += s
		}
	})
}

// TestCeilingMatchesInterpreter is the check on the hand-compiled kernel: if it
// did not burn the same gas as the EVM running the same bytecode, it is not
// doing the same work and the ratio above means nothing.
func TestCeilingMatchesInterpreter(t *testing.T) {
	const n = 20000
	const limit = 300_000_000
	_, left, err := func() ([]byte, uint64, error) {
		cfg := newCfg(benchKernels["arith"])
		cfg.GasLimit = limit
		return runtime.Call(callee, nil, cfg)
	}()
	if err != nil {
		t.Fatal(err)
	}
	gotLeft, _ := arithCompiled(n, limit)
	// The interpreter also pays the PUSH2 in the prologue, which the compiled
	// form folds into its loop bound.
	if left != gotLeft-3 {
		t.Fatalf("compiled left %d gas, interpreter left %d (+3 prologue)", gotLeft, left)
	}
}
