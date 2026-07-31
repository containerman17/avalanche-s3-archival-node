// Package chain is the chain descriptor: the one value every component that
// used to take a bare networkID now takes instead.
//
// A networkID names a NETWORK (mainnet, Fuji), and that was enough only
// because the primary network has exactly one EVM chain whose genesis is
// embedded in avalanchego. An Avalanche L1 has none of that: its genesis lives
// on the P-chain, its blockchainID and subnetID are execution inputs (the Warp
// precompile returns the blockchainID straight into EVM output), and it runs
// subnet-evm rather than coreth, which is a different libevm extras
// registration and a different header encoding.
//
// So the descriptor carries all of it, and the C-chain is just the case where
// every field comes from the embedded config (see CChain). Nothing here reaches
// the network except Resolve's first-start genesis fetch.
//
// A chain is named on the command line by its blockchainID alone (or "C"), and
// Resolve derives the rest: the blockchainID is its CreateChainTx's txID, so
// the P-chain hands over the verbatim genesisData and the subnetID. See
// Resolve.
package chain

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/ava-labs/avalanchego/genesis"
	"github.com/ava-labs/avalanchego/ids"
	avaconstants "github.com/ava-labs/avalanchego/utils/constants"

	"github.com/containerman17/epochdb/dist"
)

// VMKind selects the libevm extras registration set, and with it the header
// encoding: coreth headers carry a mandatory ExtDataHash where subnet-evm goes
// straight to the optional BaseFee, so the two are mutually exclusive and
// libevm's registry is process-global. One kind per process, one kind per data
// directory, for life.
type VMKind string

const (
	Coreth    VMKind = "coreth"
	SubnetEVM VMKind = "subnetevm"
)

// Chain describes one EVM chain end to end.
//
// GenesisJSON is the canonical genesis bytes VERBATIM: for an L1 the P-chain
// CreateChainTx genesisData, for the C-chain avalanchego's embedded
// cfg.CChainGenesis, which is byte-identical to the same tx's genesisData (see
// dist/chainroot.go). UpgradeJSON is the chain's upgrade.json verbatim, or nil.
// Both are hashed unmodified into Root, so nothing here may re-serialize them.
type Chain struct {
	GenesisJSON  []byte
	UpgradeJSON  []byte
	NetworkID    uint32
	SubnetID     ids.ID
	BlockchainID ids.ID
	VMKind       VMKind
}

// Root is the anchor of this chain's epoch hash chain,
// sha256(genesisData || upgradeBytes) per DESIGN rulings 5 and 6.
func (c *Chain) Root() [32]byte { return dist.ChainRootFrom(c.GenesisJSON, c.UpgradeJSON) }

// CChain builds the descriptor for a primary-network C-chain out of
// avalanchego's embedded config, exactly as every path did before the
// descriptor existed. networkID 0 defaults to Fuji.
func CChain(networkID uint32) (*Chain, error) {
	if networkID == 0 {
		networkID = avaconstants.FujiID
	}
	cfg := genesis.GetConfig(networkID)
	if cfg == nil {
		return nil, fmt.Errorf("chain: no embedded genesis config for network %d", networkID)
	}
	cChainID, _, err := PrimaryC(networkID)
	if err != nil {
		return nil, err
	}
	return &Chain{
		GenesisJSON:  []byte(cfg.CChainGenesis),
		NetworkID:    networkID,
		SubnetID:     avaconstants.PrimaryNetworkID,
		BlockchainID: cChainID,
		VMKind:       Coreth,
	}, nil
}

// PrimaryC returns the primary network's C-chain blockchain ID and AVAX asset
// ID for networkID. Both are snow.Context fields, i.e. execution inputs, and
// both are derived from the embedded genesis rather than guessed.
func PrimaryC(networkID uint32) (chainID, avaxAssetID ids.ID, err error) {
	cfg := genesis.GetConfig(networkID)
	if cfg == nil {
		return ids.Empty, ids.Empty, fmt.Errorf("chain: no embedded genesis config for network %d", networkID)
	}
	genesisBytes, avaxAssetID, err := genesis.FromConfig(cfg)
	if err != nil {
		return ids.Empty, ids.Empty, fmt.Errorf("chain: build genesis for network %d: %w", networkID, err)
	}
	cChain, err := genesis.VMGenesis(genesisBytes, avaconstants.EVMID)
	if err != nil {
		return ids.Empty, ids.Empty, fmt.Errorf("chain: locate C-Chain in genesis for network %d: %w", networkID, err)
	}
	return cChain.ID(), avaxAssetID, nil
}

// upgradeFile is avalanchego's own convention, borrowed verbatim: a chain's
// upgrade.json lives in that chain's data directory. It is THE one input that
// is not derivable from the chain ID (nothing on the P-chain records it), and
// its bytes enter the chain root unmodified, so it is read and never parsed.
const upgradeFile = "upgrade.json"

// cacheFile is the resolved descriptor, written into the chain's data dir the
// first time a blockchainID is resolved so later starts need no P-chain call.
// It is a CACHE, not a registry: delete it and the next start refetches. Same
// role as the vmkind stamp next to it, one directory, one chain, for life.
const cacheFile = "chain.json"

type cachedChain struct {
	NetworkID    uint32 `json:"networkID"`
	BlockchainID string `json:"blockchainID"`
	SubnetID     string `json:"subnetID"`
	VMKind       string `json:"vmKind"`
	// GenesisData is the CreateChainTx genesisData verbatim, base64 exactly as
	// platform.getTx emits it.
	GenesisData []byte `json:"genesisData"`
}

// Resolve turns a CLI chain spec into a descriptor. The spec is either "C" (the
// primary network's C-chain for networkID, straight out of avalanchego's
// embedded config) or a blockchainID, which is all an L1 needs: the
// blockchainID IS its CreateChainTx's txID, so platform.getTx hands back the
// verbatim genesisData and the subnetID. dataDir is that chain's data
// directory: the resolved descriptor is cached there, and an upgrade.json
// sitting there is picked up on every start.
//
// vmKind is not a question: the primary network's C-chain is coreth and
// everything else is subnet-evm. A custom vmID means nothing (FIFA ships stock
// subnet-evm under its own plugin ID), and a wrong guess cannot be silent: the
// data dir's vmkind stamp refuses it and the first executed header fails to
// decode.
func Resolve(ctx context.Context, spec string, networkID uint32, dataDir string) (*Chain, error) {
	var (
		c   *Chain
		err error
	)
	if spec == "" || strings.EqualFold(spec, "C") {
		c, err = CChain(networkID)
	} else {
		c, err = resolveL1(ctx, spec, networkID, dataDir)
	}
	if err != nil {
		return nil, err
	}
	// Verbatim or absent: ChainRootFrom hashes these bytes as they are.
	up, err := os.ReadFile(filepath.Join(dataDir, upgradeFile))
	switch {
	case err == nil:
		c.UpgradeJSON = up
	case !errors.Is(err, os.ErrNotExist):
		return nil, fmt.Errorf("chain: %w", err)
	}
	return c, nil
}

func resolveL1(ctx context.Context, spec string, networkID uint32, dataDir string) (*Chain, error) {
	blockchainID, err := ids.FromString(spec)
	if err != nil {
		return nil, fmt.Errorf("chain: %q is neither \"C\" nor a blockchainID: %w", spec, err)
	}
	path := filepath.Join(dataDir, cacheFile)
	raw, err := os.ReadFile(path)
	switch {
	case err == nil:
		return fromCache(raw, path, blockchainID)
	case !errors.Is(err, os.ErrNotExist):
		return nil, fmt.Errorf("chain: %w", err)
	}

	var api string
	switch networkID {
	case avaconstants.MainnetID:
		api = "https://api.avax.network"
	case avaconstants.FujiID:
		api = "https://api.avax-test.network"
	default:
		return nil, fmt.Errorf("chain: %s: no public P-chain endpoint for network %d", spec, networkID)
	}
	genesisData, subnetID, err := dist.CreateChainTx(ctx, api, spec)
	if err != nil {
		return nil, err
	}
	sub, err := ids.FromString(subnetID)
	if err != nil {
		return nil, fmt.Errorf("chain: %s: subnetID from %s: %w", spec, api, err)
	}
	c := &Chain{
		GenesisJSON:  genesisData,
		NetworkID:    networkID,
		SubnetID:     sub,
		BlockchainID: blockchainID,
		VMKind:       SubnetEVM,
	}
	// Best effort is not enough: a dir that cannot hold the cache would refetch
	// forever, and the operator should hear about it now rather than on the day
	// the public endpoint is down.
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("chain: %w", err)
	}
	blob, err := json.MarshalIndent(cachedChain{
		NetworkID:    c.NetworkID,
		BlockchainID: spec,
		SubnetID:     subnetID,
		VMKind:       string(c.VMKind),
		GenesisData:  genesisData,
	}, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("chain: %w", err)
	}
	if err := os.WriteFile(path, blob, 0o644); err != nil {
		return nil, fmt.Errorf("chain: cache %s: %w", path, err)
	}
	return c, nil
}

// fromCache rebuilds the descriptor from a previously resolved chain.json. The
// cache is the authority on networkID and subnetID (it is what the dir was
// built with, exactly like the vmkind stamp), so nothing here defaults: every
// field is an execution input the EVM can read back out, and the recorded Warp
// bug was a zero ChainID diverging Fuji state at the first block that called
// the precompile.
func fromCache(raw []byte, path string, want ids.ID) (*Chain, error) {
	var f cachedChain
	if err := json.Unmarshal(raw, &f); err != nil {
		return nil, fmt.Errorf("chain: %s: %w", path, err)
	}
	blockchainID, err := ids.FromString(f.BlockchainID)
	if err != nil {
		return nil, fmt.Errorf("chain: %s: blockchainID: %w", path, err)
	}
	if blockchainID != want {
		return nil, fmt.Errorf("chain: %s holds %s, refusing to open it as %s: one data dir, one chain", path, blockchainID, want)
	}
	subnetID, err := ids.FromString(f.SubnetID)
	if err != nil {
		return nil, fmt.Errorf("chain: %s: subnetID: %w", path, err)
	}
	if f.NetworkID == 0 {
		return nil, fmt.Errorf("chain: %s: networkID required", path)
	}
	if len(f.GenesisData) == 0 {
		return nil, fmt.Errorf("chain: %s: genesisData required (delete the file to refetch)", path)
	}
	kind := VMKind(f.VMKind)
	if kind != Coreth && kind != SubnetEVM {
		return nil, fmt.Errorf("chain: %s: unknown vmKind %q (coreth|subnetevm)", path, f.VMKind)
	}
	return &Chain{
		GenesisJSON:  f.GenesisData,
		NetworkID:    f.NetworkID,
		SubnetID:     subnetID,
		BlockchainID: blockchainID,
		VMKind:       kind,
	}, nil
}
