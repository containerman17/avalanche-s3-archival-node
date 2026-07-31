package exec

import (
	"testing"

	avaconstants "github.com/ava-labs/avalanchego/utils/constants"

	"github.com/containerman17/epochdb/chain"
	"github.com/containerman17/epochdb/fetch"
)

// The Warp precompile returns snow.Context.ChainID straight into EVM output
// (getBlockchainID), so a wrong or empty value silently changes deployed
// bytecode and the state root. Pin both networks' C-Chain blockchain IDs.
func TestSnowContextChainIDs(t *testing.T) {
	fetch.RegisterExtras(chain.Coreth)
	for _, tc := range []struct {
		networkID uint32
		chainID   string
		assetID   string
	}{
		{avaconstants.FujiID, "yH8D7ThNJkxmtkuv2jgBa4P1Rn3Qpr4pPr7QYNfcdoS6k6HWp", "U8iRqJoiJm8xZHAacmvYyZVwqQx6uDNtQeP3CQ6fcgQk3JqnK"},
		{avaconstants.MainnetID, "2q9e4r6Mu3U68nU1fYjgbR6JvwrRx36CohpAX5UQxse55x1Q5", "FvwEAhmxKfeiG8SnEvq42hc6whRyY3EFYAvebMqDNDGCgxN5Z"},
	} {
		ctx, err := snowContextFor(mustCChain(t, tc.networkID))
		if err != nil {
			t.Fatalf("network %d: %v", tc.networkID, err)
		}
		if got := ctx.ChainID.String(); got != tc.chainID {
			t.Errorf("network %d ChainID = %s, want %s", tc.networkID, got, tc.chainID)
		}
		if got := ctx.AVAXAssetID.String(); got != tc.assetID {
			t.Errorf("network %d AVAXAssetID = %s, want %s", tc.networkID, got, tc.assetID)
		}
	}
}

// mustCChain is the primary-network C-chain descriptor, what every test here
// runs against.
func mustCChain(t *testing.T, networkID uint32) *chain.Chain {
	t.Helper()
	c, err := chain.CChain(networkID)
	if err != nil {
		t.Fatal(err)
	}
	return c
}
