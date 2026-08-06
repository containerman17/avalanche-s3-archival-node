package main

import (
	"strings"
	"testing"
)

// TestProbeVerdict pins the size rule and, more importantly, that an
// unreachable chain is a RESULT and not an error: the whole point of probing
// over p2p is to learn in 90 seconds that nobody will serve this chain.
func TestProbeVerdict(t *testing.T) {
	small := probeOut{Connected: 12, Archival: 9, Validators: 12, TipHeight: 4_000_000, TipDecoded: true, Sampled: 2000, TipLocalTxs: 5_000_000}
	if v, _ := verdict(small); v != "SYNC" {
		t.Fatalf("small chain: %s", v)
	}
	tall := small
	tall.TipHeight = 40_000_000
	if v, why := verdict(tall); v != "TOO BIG" || !strings.Contains(why, "blocks") {
		t.Fatalf("40M blocks: %s / %s", v, why)
	}
	fat := small
	fat.TipLocalTxs = 900_000_000
	if v, why := verdict(fat); v != "TOO BIG" || !strings.Contains(why, "txs") {
		t.Fatalf("900M txs: %s / %s", v, why)
	}
	// The Bnry shape, 2026-08-06: a chain over the block rule whose TIP-LOCAL
	// density extrapolates to a comfortable 66M. It really holds over a
	// billion. The block count is measured and the extrapolation is not, so
	// the block rule must still reject and the verdict must say so.
	bnry := probeOut{Connected: 5, Archival: 5, Validators: 5, TipHeight: 15_273_059, TipDecoded: true, Sampled: 2000, TipLocalTxs: 66_000_000}
	if v, why := verdict(bnry); v != "TOO BIG" || !strings.Contains(why, "blocks") {
		t.Fatalf("a low tip-local extrapolation must not rescue a chain over the block rule: %s / %s", v, why)
	}
	dead := probeOut{Validators: 5}
	if v, _ := verdict(dead); v != "UNREACHABLE" {
		t.Fatalf("no peers connected: %s", v)
	}
	mute := probeOut{Connected: 5, Validators: 5}
	if v, _ := verdict(mute); v != "SILENT" {
		t.Fatalf("connected but no frontier is its own verdict, got %s", v)
	}
	notEVM := small
	notEVM.TipDecoded = false
	if v, _ := verdict(notEVM); v != "UNKNOWN" {
		t.Fatalf("undecodable frontier: %s", v)
	}
}

// TestCoverageWarns pins that a tip-local sample declares how little of the
// chain it saw. The ratio was always derivable from sampledLowHeight and
// nobody derived it, which is how a >1B-tx chain read as ~66M.
func TestCoverageWarns(t *testing.T) {
	c, warn := coverage(2000, 15_273_059)
	if warn == "" {
		t.Fatalf("2000 of 15.3M blocks must warn, got coverage %v", c)
	}
	if c > 0.0002 {
		t.Fatalf("coverage should be ~0.013%%, got %v", c)
	}
	// A chain small enough that the sample really is most of it earns no
	// warning: the extrapolation is then close to a count.
	if _, warn := coverage(2000, 5_000); warn != "" {
		t.Fatalf("2000 of 5000 blocks should not warn: %s", warn)
	}
}

// TestSniffGenesis pins that the VM guess comes from the GENESIS and never
// from the vmID, which is routinely a vanity plugin ID on a stock binary.
func TestSniffGenesis(t *testing.T) {
	stock := []byte(`{"config":{"chainId":43214,"feeConfig":{"gasLimit":8000000}},"alloc":{}}`)
	got := sniffGenesis(stock)
	if !got.SubnetEVM || got.ChainID != 43214 || len(got.Precompiles) != 0 {
		t.Fatalf("stock subnet-evm genesis: %+v", got)
	}
	withPre := []byte(`{"config":{"chainId":7,"subnetEVMTimestamp":0,"txAllowListConfig":{"blockTimestamp":0},"warpConfig":{"blockTimestamp":0}}}`)
	got = sniffGenesis(withPre)
	if !got.SubnetEVM || len(got.Precompiles) != 2 {
		t.Fatalf("precompiles not named: %+v", got)
	}
	if got := sniffGenesis([]byte("not json at all")); got.SubnetEVM || got.Note == "" {
		t.Fatalf("non-JSON genesis: %+v", got)
	}
	if got := sniffGenesis([]byte(`{"config":{"chainId":1}}`)); got.SubnetEVM || !strings.Contains(got.Note, "feeConfig") {
		t.Fatalf("coreth-shaped genesis: %+v", got)
	}
}

// TestWeightShape pins the one distinction that changes an operator's answer.
func TestWeightShape(t *testing.T) {
	if s := weightShape([]uint64{100}); !strings.Contains(s, "SINGLE VALIDATOR") {
		t.Fatalf("single validator not flagged: %s", s)
	}
	if s := weightShape([]uint64{20, 20, 20}); !strings.HasPrefix(s, "uniform") {
		t.Fatalf("uniform set: %s", s)
	}
	if s := weightShape([]uint64{900, 50, 50}); !strings.HasPrefix(s, "skewed") {
		t.Fatalf("skewed set: %s", s)
	}
	if s := weightShape(nil); s != "no validators" {
		t.Fatalf("empty set: %s", s)
	}
}
