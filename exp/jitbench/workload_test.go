//go:build jitbench

// Package jitbench is an EXPERIMENT, not production code. It measures how much
// of libevm's EVM interpreter time is per-opcode bookkeeping (gas charging and
// stack bounds checking) rather than real work, which is the ceiling on what an
// AOT/JIT compiler or evmone-style basic-block accounting could ever win.
//
// It runs against exp/jitbench/vmx, a verbatim fork of libevm's core/vm with
// two extra run loops bolted on. Nothing here is imported by the node.
package jitbench

import (
	"bytes"
	"encoding/hex"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/rawdb"
	"github.com/ava-labs/libevm/core/state"
	"github.com/ava-labs/libevm/params"
	"github.com/holiman/uint256"

	"github.com/containerman17/epochdb/exp/jitbench/vmx"
	"github.com/containerman17/epochdb/exp/jitbench/vmx/runtime"
)

// contract is one real hot mainnet C-chain contract plus the call that makes it
// do the most work against an empty state.
type contract struct {
	name  string
	code  []byte
	input []byte
	steps int // opcodes executed by that call
}

var callee = common.HexToAddress("0xc0ffee0000000000000000000000000000000001")

// newCfg builds the runtime config every measurement uses: real Cancun rules,
// an in-memory state, and the code installed at a fixed address so CALLs and
// EXTCODE* behave.
func newCfg(code []byte) *runtime.Config {
	sdb, err := state.New(common.Hash{}, state.NewDatabase(rawdb.NewMemoryDatabase()), nil)
	if err != nil {
		panic(err)
	}
	sdb.CreateAccount(callee)
	sdb.SetCode(callee, code)
	sdb.SetBalance(callee, uint256.NewInt(1e18))
	cfg := &runtime.Config{State: sdb, GasLimit: 30_000_000}
	runtime.SetDefaults(cfg)
	cfg.ChainConfig = params.TestChainConfig
	sdb.CreateAccount(cfg.Origin)
	sdb.SetBalance(cfg.Origin, uint256.NewInt(1e18))
	return cfg
}

// countSteps runs one call under the stock loop with a counting tracer and
// reports how many opcodes it executed.
func countSteps(code, input []byte) int {
	cfg := newCfg(code)
	c := &counter{}
	cfg.EVMConfig.Tracer = c
	runtime.Call(callee, input, cfg)
	return c.n
}

// selectors pulls every PUSH4 immediate out of the code. A solidity dispatcher
// compares the calldata selector against exactly these.
func selectors(code []byte) [][]byte {
	var out [][]byte
	seen := map[string]bool{}
	for pc := 0; pc < len(code); pc++ {
		op := code[pc]
		if op == 0x63 && pc+4 < len(code) { // PUSH4
			s := code[pc+1 : pc+5]
			if !seen[string(s)] {
				seen[string(s)] = true
				out = append(out, append([]byte(nil), s...))
			}
		}
		if op >= 0x60 && op <= 0x7f {
			pc += int(op-0x60) + 1
		}
	}
	return out
}

// loadContracts reads testdata and picks, per contract, the selector whose call
// executes the most opcodes against an empty state. Those are real calls into
// real hot bytecode: the dispatcher, the function body, and SLOADs that read
// zero.
func loadContracts(t testing.TB) []contract {
	t.Helper()
	files, err := filepath.Glob("testdata/*.hex")
	if err != nil || len(files) == 0 {
		t.Fatalf("no testdata: %v", err)
	}
	var out []contract
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		code, err := hex.DecodeString(strings.TrimSpace(string(raw)))
		if err != nil {
			t.Fatal(err)
		}
		best := contract{name: strings.SplitN(filepath.Base(f), ".", 2)[0], code: code}
		// Three argument fills, because a lot of hot contracts bail out
		// immediately on a zero amount or an expired deadline.
		fills := [][]byte{
			make([]byte, 160),
			bytes.Repeat([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}, 5),
			bytes.Repeat([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x0f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, 5),
		}
		for _, sel := range selectors(code) {
			for _, fill := range fills {
				in := append(append([]byte(nil), sel...), fill...)
				if n := countSteps(code, in); n > best.steps {
					best.steps, best.input = n, in
				}
			}
		}
		if best.steps < 30 {
			continue // nothing but a revert; it would measure call overhead only
		}
		out = append(out, best)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].steps > out[j].steps })
	return out
}

// counter is an EVMLogger that only counts.
type counter struct{ n int }

func (c *counter) CaptureStart(env *vmx.EVM, from, to common.Address, create bool, input []byte, gas uint64, value *big.Int) {
}
func (c *counter) CaptureState(pc uint64, op vmx.OpCode, gas, cost uint64, scope *vmx.ScopeContext, rData []byte, depth int, err error) {
	c.n++
}
func (c *counter) CaptureFault(pc uint64, op vmx.OpCode, gas, cost uint64, scope *vmx.ScopeContext, depth int, err error) {
}
func (c *counter) CaptureEnd(output []byte, gasUsed uint64, err error) {}
func (c *counter) CaptureEnter(typ vmx.OpCode, from, to common.Address, input []byte, gas uint64, value *big.Int) {
}
func (c *counter) CaptureExit(output []byte, gasUsed uint64, err error) {}
func (c *counter) CaptureTxStart(gasLimit uint64)                       {}
func (c *counter) CaptureTxEnd(restGas uint64)                          {}
