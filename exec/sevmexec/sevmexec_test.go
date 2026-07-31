// Package sevmexec exists only to hold this test, for the same reason
// fetch/sevm does: libevm's extras registry is process-global and PANICS on
// re-registration, package exec's own tests register coreth, and `go test`
// gives every package its own process. So a subnet-evm execution test cannot
// live in package exec, and this is the isolation it needs.
package sevmexec

import (
	"context"
	"crypto/ecdsa"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"testing"

	sevmdummy "github.com/ava-labs/avalanchego/graft/subnet-evm/consensus/dummy"
	sevmcore "github.com/ava-labs/avalanchego/graft/subnet-evm/core"
	sevmparams "github.com/ava-labs/avalanchego/graft/subnet-evm/params"
	sevmextras "github.com/ava-labs/avalanchego/graft/subnet-evm/params/extras"
	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/snow"
	"github.com/ava-labs/avalanchego/upgrade"
	avaconstants "github.com/ava-labs/avalanchego/utils/constants"
	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/crypto"
	"github.com/ava-labs/libevm/rlp"

	"github.com/containerman17/epochdb/chain"
	"github.com/containerman17/epochdb/exec"
	"github.com/containerman17/epochdb/state"
)

// The chain under test is shaped like FIFA: a mainnet L1, its own blockchainID
// and subnetID, NativeMinter configured IN GENESIS at time 0, and a genesis
// timestamp late enough that every mainnet upgrade through Granite is already
// active (which is where a real L1 syncing today lives).
const (
	testChainID = 13322
	// genesisTime is 2026-01-01T00:00:00Z: past every scheduled mainnet
	// upgrade in this dep set, so the chain runs under the newest rules.
	genesisTime = 1767225600
	blockGap    = 2 // seconds between generated blocks
)

// funded is a fixed test key; its address holds the whole alloc.
var funded = mustKey("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")

func mustKey(hex string) *ecdsa.PrivateKey {
	k, err := crypto.HexToECDSA(hex)
	if err != nil {
		panic(err)
	}
	return k
}

func fundedAddr() common.Address { return crypto.PubkeyToAddress(funded.PublicKey) }

// genesisJSON is the descriptor's GenesisJSON: the bytes an L1 operator put on
// the P-chain, verbatim, exactly as chain.Load would hand them over.
func genesisJSON(t *testing.T) []byte {
	t.Helper()
	addr := fundedAddr().Hex()
	return []byte(fmt.Sprintf(`{
	  "config": {
	    "chainId": %d,
	    "feeConfig": {
	      "gasLimit": 8000000,
	      "targetBlockRate": 2,
	      "minBaseFee": 25000000000,
	      "targetGas": 15000000,
	      "baseFeeChangeDenominator": 36,
	      "minBlockGasCost": 0,
	      "maxBlockGasCost": 1000000,
	      "blockGasCostStep": 200000
	    },
	    "subnetEVMTimestamp": 0,
	    "contractNativeMinterConfig": {
	      "blockTimestamp": 0,
	      "adminAddresses": ["%s"]
	    }
	  },
	  "alloc": {
	    "%s": {"balance": "0x52B7D2DCC80CD2E4000000"}
	  },
	  "nonce": "0x0",
	  "timestamp": "0x%x",
	  "extraData": "0x",
	  "gasLimit": "0x7A1200",
	  "difficulty": "0x0",
	  "mixHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
	  "coinbase": "0x0000000000000000000000000000000000000000",
	  "number": "0x0",
	  "gasUsed": "0x0",
	  "parentHash": "0x0000000000000000000000000000000000000000000000000000000000000000"
	}`, testChainID, addr, addr[2:], genesisTime))
}

// upgradeJSON is the chain's upgrade.json: a STATE UPGRADE, which is the half
// of ruling 6 that is pure off-chain operator input. It lands during block 3.
// If exec ignored UpgradeJSON, this account would not exist on our side and
// every root from block 3 on would diverge.
func upgradeJSON() []byte {
	return []byte(fmt.Sprintf(`{
	  "stateUpgrades": [
	    {
	      "blockTimestamp": %d,
	      "accounts": {
	        "0x00000000000000000000000000000000000000ff": {
	          "balanceChange": "0x3e8",
	          "storage": {
	            "0x0000000000000000000000000000000000000000000000000000000000000001":
	            "0x00000000000000000000000000000000000000000000000000000000000000aa"
	          }
	        }
	      }
	    }
	  ]
	}`, genesisTime+3*blockGap))
}

func testChain(t *testing.T) *chain.Chain {
	t.Helper()
	return &chain.Chain{
		GenesisJSON:  genesisJSON(t),
		UpgradeJSON:  upgradeJSON(),
		NetworkID:    avaconstants.MainnetID,
		SubnetID:     ids.ID{0xf1, 0xfa},
		BlockchainID: ids.ID{0xf1, 0xfa, 0xb1},
		VMKind:       chain.SubnetEVM,
	}
}

// referenceGenesis parses the SAME bytes the descriptor carries the way
// subnet-evm's own plugin/evm parseGenesis does, INDEPENDENTLY of exec. The
// reference chain is generated against this, so any difference in how exec
// wires the config (fee config, upgrade bytes, eth upgrades, network upgrade
// defaults) shows up as a state root mismatch rather than as agreement.
func referenceGenesis(t *testing.T, c *chain.Chain) *sevmcore.Genesis {
	t.Helper()
	g := new(sevmcore.Genesis)
	if err := json.Unmarshal(c.GenesisJSON, g); err != nil {
		t.Fatalf("reference genesis: %v", err)
	}
	cx := sevmparams.GetExtra(g.Config)
	cx.AvalancheContext = sevmextras.AvalancheContext{SnowCtx: &snow.Context{
		NetworkID:       c.NetworkID,
		SubnetID:        c.SubnetID,
		ChainID:         c.BlockchainID,
		NetworkUpgrades: upgrade.GetConfig(c.NetworkID),
	}}
	cx.SetDefaults(upgrade.GetConfig(c.NetworkID))
	var uc sevmextras.UpgradeConfig
	if err := json.Unmarshal(c.UpgradeJSON, &uc); err != nil {
		t.Fatalf("reference upgrade bytes: %v", err)
	}
	cx.UpgradeConfig = uc
	if err := g.Verify(); err != nil {
		t.Fatalf("reference genesis verify: %v", err)
	}
	if err := sevmparams.SetEthUpgrades(g.Config); err != nil {
		t.Fatalf("reference eth upgrades: %v", err)
	}
	return g
}

// generateChain builds the reference blocks with subnet-evm's OWN block
// builder, which runs its OWN StateProcessor (ApplyUpgrades + ApplyTransaction
// per tx) to fill each header.Root. Those roots are what our executor is
// checked against, block by block.
func generateChain(t *testing.T, g *sevmcore.Genesis) []*types.Block {
	t.Helper()
	engine := sevmdummy.NewFakerWithMode(sevmdummy.Mode{
		ModeSkipHeader: true, ModeSkipBlockFee: true, ModeSkipCoinbase: true,
	})
	chainID := g.Config.ChainID
	to := common.HexToAddress("0x00000000000000000000000000000000000000c0")
	// SSTORE(0, 1) then implicit STOP: a real account creation plus a real
	// storage write, in five bytes of init code.
	initCode := common.FromHex("0x6001600055")

	_, blocks, _, err := sevmcore.GenerateChainWithGenesis(g, engine, 4, blockGap, func(i int, b *sevmcore.BlockGen) {
		sign := func(data types.TxData) *types.Transaction {
			tx, err := types.SignNewTx(funded, b.Signer(), data)
			if err != nil {
				t.Fatalf("sign tx: %v", err)
			}
			return tx
		}
		switch i {
		case 0: // block 1: a plain value transfer
			b.AddTx(sign(&types.DynamicFeeTx{
				ChainID: chainID, Nonce: b.TxNonce(fundedAddr()),
				GasTipCap: common.Big0, GasFeeCap: b.BaseFee(),
				Gas: 21_000, To: &to, Value: big.NewInt(1_000),
			}))
		case 1: // block 2: no transactions (the empty-block fast path)
		case 2: // block 3: a contract creation that writes storage
			b.AddTx(sign(&types.DynamicFeeTx{
				ChainID: chainID, Nonce: b.TxNonce(fundedAddr()),
				GasTipCap: common.Big0, GasFeeCap: b.BaseFee(),
				Gas: 200_000, Data: initCode,
			}))
		case 3: // block 4: two transfers in one block
			for range 2 {
				b.AddTx(sign(&types.DynamicFeeTx{
					ChainID: chainID, Nonce: b.TxNonce(fundedAddr()),
					GasTipCap: common.Big0, GasFeeCap: b.BaseFee(),
					Gas: 21_000, To: &to, Value: big.NewInt(7),
				}))
			}
		}
	})
	if err != nil {
		t.Fatalf("generate reference chain: %v", err)
	}
	return blocks
}

// source is exec.BlockSource over pre-ProposerVM raw block RLP, which is what
// parseEthBlock's second path decodes.
type source map[uint64][]byte

func (s source) GetByHeight(n uint64) ([]byte, bool, error) {
	raw, ok := s[n]
	return raw, ok, nil
}

func containers(t *testing.T, blocks []*types.Block) source {
	t.Helper()
	src := source{}
	for _, blk := range blocks {
		raw, err := rlp.EncodeToBytes(blk)
		if err != nil {
			t.Fatalf("encode block %d: %v", blk.NumberU64(), err)
		}
		src[blk.NumberU64()] = raw
	}
	return src
}

func runExecutor(t *testing.T, c *chain.Chain, src source, stopAt uint64) (*exec.Executor, error) {
	t.Helper()
	dir := t.TempDir()
	store, err := state.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	e, err := exec.New(exec.Config{DataDir: dir, Blocks: src, Store: store, Chain: c, StopAt: stopAt})
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() { e.Close() })
	return e, e.Run(context.Background())
}

// TestSubnetEVMExecution replays a subnet-evm chain built by subnet-evm's own
// block builder through the executor. Every block's computed state root is
// verified against the reference header root INSIDE the executor (a mismatch
// is a hard stop), so a clean run to the last height is the assertion.
func TestSubnetEVMExecution(t *testing.T) {
	c := testChain(t)

	g, err := exec.ChainGenesis(c)
	if err != nil {
		t.Fatalf("ChainGenesis: %v", err)
	}
	// The genesis precompile must have survived the parse. It fails SILENTLY
	// when subnet-evm's precompile registry is not linked in (empty registry =
	// zero precompiles, no error), and a missing NativeMinter admin role is a
	// wrong genesis root and a wrong chain forever after.
	if pc := sevmparams.GetExtra(g.Config).GenesisPrecompiles; len(pc) != 1 {
		t.Fatalf("genesis precompiles = %v, want the one configured in genesis", pc)
	}
	if got := g.Config.ChainID; got == nil || got.Int64() != testChainID {
		t.Fatalf("chain id = %v, want %d", got, testChainID)
	}

	ref := referenceGenesis(t, c)
	if refRoot := ref.ToBlock().Root(); refRoot != g.Root {
		t.Fatalf("genesis root: exec %x, subnet-evm's own parse %x", g.Root, refRoot)
	}

	blocks := generateChain(t, ref)
	if len(blocks) != 4 {
		t.Fatalf("generated %d blocks", len(blocks))
	}
	// Block 3 is where the state upgrade lands; if it did not, its root is the
	// one that diverges.
	if blocks[2].Time() != genesisTime+3*blockGap {
		t.Fatalf("block 3 time %d, want the state upgrade timestamp %d", blocks[2].Time(), genesisTime+3*blockGap)
	}

	e, err := runExecutor(t, c, containers(t, blocks), 4)
	if err != nil {
		t.Fatalf("execute subnet-evm chain: %v", err)
	}
	if e.Head() != 4 || e.LiveHead() != 4 {
		t.Fatalf("head=%d live=%d, want 4/4", e.Head(), e.LiveHead())
	}
	// M4: SAE never engages on subnet-evm, so safe/finalized fall back to the
	// executed head, which is what the rpc labels report.
	if e.SettledHeight() != e.LiveHead() {
		t.Fatalf("SettledHeight=%d, want LiveHead=%d", e.SettledHeight(), e.LiveHead())
	}
	// The block header is post-Helicon-shaped only on coreth; a subnet-evm
	// header can never carry the markers.
	if exec.HasSettledMarkers(blocks[3].Header()) {
		t.Fatal("a subnet-evm header reported ACP-194 settlement markers")
	}
}

// TestSubnetEVMRootMismatchStops proves the run above can actually fail: one
// corrupted header root and the executor must stop instead of accepting it.
func TestSubnetEVMRootMismatchStops(t *testing.T) {
	c := testChain(t)
	blocks := generateChain(t, referenceGenesis(t, c))

	bad := blocks[0].Header()
	bad.Root = common.HexToHash("0xbad")
	src := containers(t, blocks)
	raw, err := rlp.EncodeToBytes(types.NewBlockWithHeader(bad).WithBody(types.Body{
		Transactions: blocks[0].Transactions(),
	}))
	if err != nil {
		t.Fatal(err)
	}
	src[1] = raw

	_, err = runExecutor(t, c, src, 4)
	if err == nil {
		t.Fatal("executor accepted a block whose header root does not match its execution")
	}
	if !strings.Contains(err.Error(), "state root mismatch") {
		t.Fatalf("stopped for the wrong reason: %v", err)
	}
}
