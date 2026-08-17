package jitbench

import (
	"fmt"
	"sort"
	"testing"

	"github.com/containerman17/epochdb/exp/jitbench/vmx"
	"github.com/containerman17/epochdb/exp/jitbench/vmx/runtime"
)

var modes = []struct {
	name string
	mode vmx.RunMode
}{
	{"stock", vmx.ModeStock},
	{"blockgas", vmx.ModeBlockGas},
	{"nometer", vmx.ModeNoMeter},
}

// BenchmarkContracts runs a real call into real hot mainnet C-chain bytecode
// under each loop.
func BenchmarkContracts(b *testing.B) {
	cs := loadContracts(b)
	for _, c := range cs {
		cfg := newCfg(c.code)
		for _, m := range modes {
			b.Run(fmt.Sprintf("%s/%s", c.name[:11], m.name), func(b *testing.B) {
				vmx.Mode = m.mode
				defer func() { vmx.Mode = vmx.ModeStock }()
				b.ReportMetric(float64(c.steps), "opcodes/call")
				for i := 0; i < b.N; i++ {
					cfg.GasLimit = 30_000_000
					runtime.Call(callee, c.input, cfg)
				}
			})
		}
	}
}

// BenchmarkKernels runs the synthetic loops, which is where a compiler would
// have its best case.
func BenchmarkKernels(b *testing.B) {
	names := make([]string, 0, len(benchKernels))
	for n := range benchKernels {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, name := range names {
		code := benchKernels[name]
		cfg := newCfg(code)
		for _, m := range modes {
			b.Run(fmt.Sprintf("%s/%s", name, m.name), func(b *testing.B) {
				vmx.Mode = m.mode
				defer func() { vmx.Mode = vmx.ModeStock }()
				for i := 0; i < b.N; i++ {
					cfg.GasLimit = 300_000_000
					runtime.Call(callee, nil, cfg)
				}
			})
		}
	}
}

// BenchmarkAnalysis measures the one-off cost of the basic-block analysis per
// contract, which is what has to amortise over a replay.
func BenchmarkAnalysis(b *testing.B) {
	for _, c := range loadContracts(b) {
		b.Run(c.name[:11], func(b *testing.B) {
			b.SetBytes(int64(len(c.code)))
			for i := 0; i < b.N; i++ {
				vmx.AnalyzeForBench(c.code)
			}
		})
	}
}
