package exec

import (
	"fmt"

	"github.com/ava-labs/avalanchego/snow"
	"github.com/ava-labs/avalanchego/upgrade"
	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/rawdb"
	"github.com/ava-labs/libevm/ethdb"
	"github.com/ava-labs/libevm/triedb"

	"github.com/containerman17/avalanche-s3-archival-node/chain"
)

// snowContextFor builds the snow.Context the EVM sees. EVERY field is an
// EXECUTION INPUT, not bookkeeping, so none of them may be left zero:
//
//   - ChainID: the chain's own blockchain ID, which the Warp precompile's
//     getBlockchainID() returns straight into EVM output. Leaving it zero
//     diverged the state root at Fuji 29,955,803, the first block after
//     Durango whose transaction called Warp: a TeleporterRegistry deploy
//     baked the returned value into an immutable, so the deployed code (and
//     the account's code hash) differed from the chain's.
//   - SubnetID: the primary network for the C-chain, the L1's own subnet
//     otherwise. Warp signature verification reads it.
//   - AVAXAssetID: the atomic-tx state transfer credits imported AVAX by it.
//     C-CHAIN ONLY: atomic txs do not exist on subnet-evm, and an L1 on a
//     network with no embedded config has no way to learn it.
//   - CChainID: the primary network's C-chain, filled for any chain on a
//     network whose config is embedded.
//   - NetworkUpgrades: the network's activation schedule, which BOTH VMs read
//     straight off the context when they wire the genesis chain config (coreth
//     GetNetworkUpgrades, subnet-evm SetDefaults). Unknown networks get
//     upgrade.Default, exactly as avalanchego does.
func snowContextFor(c *chain.Chain) (*snow.Context, error) {
	ctx := &snow.Context{
		NetworkID:       c.NetworkID,
		SubnetID:        c.SubnetID,
		ChainID:         c.BlockchainID,
		NetworkUpgrades: upgrade.GetConfig(c.NetworkID),
	}
	cChainID, avaxAssetID, err := chain.PrimaryC(c.NetworkID)
	if err != nil {
		if c.VMKind == chain.Coreth {
			return nil, err
		}
		// A subnet-evm chain on a network avalanchego does not embed (a local
		// network): neither field is reachable and neither is an input for it.
		return ctx, nil
	}
	ctx.CChainID = cChainID
	if c.VMKind == chain.Coreth {
		ctx.AVAXAssetID = avaxAssetID
	}
	return ctx, nil
}

// commitGenesisIfNeeded materialises the genesis state inside the
// Firewood-backed triedb. No-op if the genesis root is already present.
//
// ethdbKV must be the same persistent ethdb the executor uses at runtime:
// genesis Alloc contains pre-deployed contract code whose hash/body get
// routed there via rawdb.WriteCode, and Firewood delegates code storage to
// rawdb entirely.
func commitGenesisIfNeeded(tdb *triedb.Database, g *Genesis, ethdbKV ethdb.KeyValueStore) (common.Hash, error) {
	if tdb.Initialized(g.Root) {
		return g.Root, nil
	}
	if err := g.commit(rawdb.NewDatabase(ethdbKV), tdb); err != nil {
		return common.Hash{}, fmt.Errorf("commit genesis: %w", err)
	}
	if !tdb.Initialized(g.Root) {
		return common.Hash{}, fmt.Errorf("genesis committed but root %x not initialized", g.Root)
	}
	return g.Root, nil
}
