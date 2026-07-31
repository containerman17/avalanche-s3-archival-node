package sevm_test

import (
	"testing"

	sevmparams "github.com/ava-labs/avalanchego/graft/subnet-evm/params"
	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/params"
	"github.com/ava-labs/libevm/rlp"

	"github.com/containerman17/epochdb/chain"
	"github.com/containerman17/epochdb/fetch"
)

// TestRegisterSubnetEVM registers the subnet-evm extras in THIS process. It can
// only live in a package that never registers coreth, because libevm's registry
// is process-global and panics on re-registration; `go test` gives every
// package its own process, which is the isolation this relies on.
//
// It then proves the registration actually took, on the two things that make
// the kinds mutually exclusive:
//
//   - the header encoding: a subnet-evm header RLP-encodes with BaseFee right
//     after Nonce, where a coreth header would demand an ExtDataHash field in
//     between, so a round-trip through the wire form is kind-specific.
//   - the chain-config extras: params.GetExtra returns subnet-evm's own
//     ChainConfigExtra, which only exists once params.RegisterExtras ran.
func TestRegisterSubnetEVM(t *testing.T) {
	fetch.RegisterExtras(chain.SubnetEVM)
	if got := fetch.RegisteredKind(); got != chain.SubnetEVM {
		t.Fatalf("RegisteredKind = %q, want %q", got, chain.SubnetEVM)
	}
	// Idempotent for the same kind.
	fetch.RegisterExtras(chain.SubnetEVM)

	h := &types.Header{Number: common.Big1, BaseFee: common.Big32, Difficulty: common.Big1}
	raw, err := rlp.EncodeToBytes(h)
	if err != nil {
		t.Fatalf("encode header: %v", err)
	}
	var back types.Header
	if err := rlp.DecodeBytes(raw, &back); err != nil {
		t.Fatalf("decode header: %v", err)
	}
	if back.BaseFee == nil || back.BaseFee.Cmp(h.BaseFee) != 0 {
		t.Fatalf("BaseFee round-trip = %v, want %v", back.BaseFee, h.BaseFee)
	}
	if back.Hash() != h.Hash() {
		t.Fatalf("header hash round-trip mismatch")
	}

	cfg := &params.ChainConfig{ChainID: common.Big1}
	if extra := sevmparams.GetExtra(cfg); extra == nil {
		t.Fatal("params.GetExtra returned nil: subnet-evm params extras not registered")
	}
}

// TestRegisterMixedPanics pins the one-or-the-other rule: asking for coreth in
// a process that already registered subnet-evm is a hard panic, not a silent
// no-op that would leave every header decoding under the wrong extras.
func TestRegisterMixedPanics(t *testing.T) {
	fetch.RegisterExtras(chain.SubnetEVM)
	defer func() {
		if recover() == nil {
			t.Fatal("registering coreth after subnet-evm did not panic")
		}
	}()
	fetch.RegisterExtras(chain.Coreth)
}
