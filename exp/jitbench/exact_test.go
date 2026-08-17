package jitbench

import (
	"bytes"
	"testing"

	"github.com/containerman17/epochdb/exp/jitbench/vmx"
	"github.com/containerman17/epochdb/exp/jitbench/vmx/runtime"
)

// run executes one call under mode and reports what a caller can observe:
// gas used, return data, and whether it failed.
func run(mode vmx.RunMode, code, input []byte, gas uint64) (used uint64, ret []byte, failed bool) {
	vmx.Mode = mode
	defer func() { vmx.Mode = vmx.ModeStock }()
	cfg := newCfg(code)
	cfg.GasLimit = gas
	ret, left, err := runtime.Call(callee, input, cfg)
	return gas - left, ret, err != nil
}

// TestBlockGasIsExact is the whole correctness claim: for real hot bytecode, at
// EVERY gas limit from "not enough for the first opcode" up to "plenty", the
// basic-block loop must burn the same gas and return the same bytes as the
// stock per-opcode loop.
//
// The gas sweep is the point. It walks the out-of-gas boundary through every
// basic block in the contract, which is exactly where block accounting is
// suspected of diverging.
func TestBlockGasIsExact(t *testing.T) {
	for _, c := range loadContracts(t) {
		c := c
		t.Run(c.name, func(t *testing.T) {
			full, _, _ := run(vmx.ModeStock, c.code, c.input, 30_000_000)
			// Sweep the whole gas range of the call in 200 steps, plus a dense
			// sweep over the first 3000 gas where the dispatcher lives.
			var limits []uint64
			for g := uint64(0); g <= full+64; g += max(1, (full+64)/200) {
				limits = append(limits, g)
			}
			for g := uint64(0); g < 3000; g += 7 {
				limits = append(limits, g)
			}
			limits = append(limits, full, full+1, full-1, 30_000_000)
			for _, g := range limits {
				wantUsed, wantRet, wantFail := run(vmx.ModeStock, c.code, c.input, g)
				gotUsed, gotRet, gotFail := run(vmx.ModeBlockGas, c.code, c.input, g)
				if wantUsed != gotUsed || !bytes.Equal(wantRet, gotRet) || wantFail != gotFail {
					t.Fatalf("gas limit %d: stock used=%d fail=%v ret=%x; blockgas used=%d fail=%v ret=%x",
						g, wantUsed, wantFail, wantRet, gotUsed, gotFail, gotRet)
				}
			}
			t.Logf("%d gas limits agree (full call = %d gas, %d opcodes)", len(limits), full, c.steps)
		})
	}
}

// TestBlockGasIsExactOnKernels does the same sweep over the synthetic kernels,
// which reach opcodes the real contracts never touch here: GAS, CALL, SSTORE,
// deep stacks.
func TestBlockGasIsExactOnKernels(t *testing.T) {
	for name, code := range kernels {
		code := code
		t.Run(name, func(t *testing.T) {
			full, _, _ := run(vmx.ModeStock, code, nil, 30_000_000)
			for g := uint64(0); g <= full+64; g += max(1, (full+64)/500) {
				wantUsed, wantRet, wantFail := run(vmx.ModeStock, code, nil, g)
				gotUsed, gotRet, gotFail := run(vmx.ModeBlockGas, code, nil, g)
				if wantUsed != gotUsed || !bytes.Equal(wantRet, gotRet) || wantFail != gotFail {
					t.Fatalf("gas limit %d: stock used=%d fail=%v ret=%x; blockgas used=%d fail=%v ret=%x",
						g, wantUsed, wantFail, wantRet, gotUsed, gotFail, gotRet)
				}
			}
		})
	}
}
