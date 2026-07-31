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
// the network except Load's optional genesis fetch.
package chain

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

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

// descriptorFile is the --chain <path.json> format.
//
//	{
//	  "networkID": 1,
//	  "blockchainID": "SUDoK9P89PCcguskyof41fZexw7U3zubDP2DZpGf3HbFWwJ4E",
//	  "subnetID":     "h7egyVb6fKHMDpVaEsTEcy7YaEnXrayxZS4A1AEU4pyBzmwGp",
//	  "vmID":         "XxNJpcFRu85cPDeTZofVHdeD5NCLj1XTgZpJNJABexXJ5e2ho",
//	  "genesisData":  "<base64, optional>",
//	  "upgradeFile":  "<path, optional>",
//	  "api":          "<P-chain endpoint, optional>"
//	}
//
// genesisData is the CreateChainTx bytes verbatim, base64 exactly as
// platform.getTx emits them. Omit it and the bytes are fetched from api (or the
// public endpoint for networkID) by blockchainID, which is the canonical
// source: the txID IS the blockchainID.
type descriptorFile struct {
	NetworkID    uint32 `json:"networkID"`
	BlockchainID string `json:"blockchainID"`
	SubnetID     string `json:"subnetID"`
	VMID         string `json:"vmID"`
	VMKind       string `json:"vmKind"`
	GenesisData  []byte `json:"genesisData"`
	UpgradeFile  string `json:"upgradeFile"`
	API          string `json:"api"`
}

// Load reads a --chain descriptor file, fetching the genesis bytes from the
// P-chain when the file does not carry them inline.
func Load(ctx context.Context, path string) (*Chain, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("chain: %w", err)
	}
	var f descriptorFile
	if err := json.Unmarshal(raw, &f); err != nil {
		return nil, fmt.Errorf("chain: %s: %w", path, err)
	}
	if f.NetworkID == 0 {
		return nil, fmt.Errorf("chain: %s: networkID required", path)
	}
	// Every field below is an execution input the EVM can read back out (the
	// recorded Warp bug: a zero ChainID diverged Fuji state at the first block
	// whose tx called the Warp precompile), so none of them may default.
	blockchainID, err := ids.FromString(f.BlockchainID)
	if err != nil {
		return nil, fmt.Errorf("chain: %s: blockchainID: %w", path, err)
	}
	subnetID, err := ids.FromString(f.SubnetID)
	if err != nil {
		return nil, fmt.Errorf("chain: %s: subnetID: %w", path, err)
	}

	kind := VMKind(f.VMKind)
	switch {
	case kind == "":
		// The vmID cannot decide this on its own: FIFA ships stock subnet-evm
		// under its own plugin ID, which is normal for an L1. So it only ever
		// identifies the primary network's coreth, and everything else defaults
		// to subnet-evm.
		if f.VMID != "" {
			if vmID, err := ids.FromString(f.VMID); err != nil {
				return nil, fmt.Errorf("chain: %s: vmID: %w", path, err)
			} else if vmID == avaconstants.EVMID {
				kind = Coreth
			}
		}
		if kind == "" {
			kind = SubnetEVM
		}
	case kind != Coreth && kind != SubnetEVM:
		return nil, fmt.Errorf("chain: %s: unknown vmKind %q (coreth|subnetevm)", path, f.VMKind)
	}

	c := &Chain{
		GenesisJSON:  f.GenesisData,
		NetworkID:    f.NetworkID,
		SubnetID:     subnetID,
		BlockchainID: blockchainID,
		VMKind:       kind,
	}
	if len(c.GenesisJSON) == 0 {
		api := f.API
		if api == "" {
			switch f.NetworkID {
			case avaconstants.MainnetID:
				api = "https://api.avax.network"
			case avaconstants.FujiID:
				api = "https://api.avax-test.network"
			default:
				return nil, fmt.Errorf("chain: %s: no genesisData and no api for network %d", path, f.NetworkID)
			}
		}
		if c.GenesisJSON, err = dist.GenesisData(ctx, api, f.BlockchainID); err != nil {
			return nil, err
		}
	}
	if f.UpgradeFile != "" {
		if c.UpgradeJSON, err = os.ReadFile(f.UpgradeFile); err != nil {
			return nil, fmt.Errorf("chain: upgradeFile: %w", err)
		}
	}
	return c, nil
}
