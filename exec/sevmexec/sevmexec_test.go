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
	"github.com/containerman17/epochdb/dist"
	"github.com/containerman17/epochdb/exec"
	"github.com/containerman17/epochdb/fetch"
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
	// nBlocks is the generated corpus, cut into two epochs of four for the
	// frontier test: the state upgrade lands in the first, the precompile
	// disable in the second.
	nBlocks  = 8
	perEpoch = 4
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

// upgradeJSON is the chain's upgrade.json, carrying BOTH halves of ruling 6's
// off-chain operator input:
//
//   - a STATE UPGRADE, landing during block 3. If exec ignored UpgradeJSON,
//     this account would not exist on our side and every root from block 3 on
//     would diverge.
//   - a PRECOMPILE UPGRADE that DISABLES the NativeMinter the genesis enabled,
//     landing during block 5. subnet-evm disables a precompile with
//     statedb.SelfDestruct, so this deletes an account (and its allowlist
//     storage) that GENESIS put in the trie and no epoch row ever created.
//     That is Beam's exact shape (mainnet L1 2tmrrBo1..., which disables the
//     genesis contractDeployerAllowList) and it is what the frontier merge
//     used to get wrong.
func upgradeJSON() []byte {
	return []byte(fmt.Sprintf(`{
	  "precompileUpgrades": [
	    {
	      "contractNativeMinterConfig": {
	        "blockTimestamp": %d,
	        "disable": true
	      }
	    }
	  ],
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
	}`, genesisTime+5*blockGap, genesisTime+3*blockGap))
}

// nativeMinter is the NativeMinter precompile's fixed address: enabled in the
// genesis above, disabled by the upgrade above.
var nativeMinter = common.HexToAddress("0x0200000000000000000000000000000000000001")

func testChain(t *testing.T) *chain.Chain {
	t.Helper()
	// Every test here parses subnet-evm genesis bytes, and the libevm extras
	// registry has to be the subnet-evm one before any of it decodes.
	fetch.RegisterExtras(chain.SubnetEVM)
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

	_, blocks, _, err := sevmcore.GenerateChainWithGenesis(g, engine, nBlocks, blockGap, func(i int, b *sevmcore.BlockGen) {
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
		// Block 5 carries no transactions of its own: the only thing that
		// moves its root is the precompile upgrade disabling NativeMinter.
		// Blocks 6..8 keep writing afterwards, so the merge has to reach the
		// frontier through rows written on BOTH sides of that destruct.
		case 5, 6, 7:
			b.AddTx(sign(&types.DynamicFeeTx{
				ChainID: chainID, Nonce: b.TxNonce(fundedAddr()),
				GasTipCap: common.Big0, GasFeeCap: b.BaseFee(),
				Gas: 21_000, To: &to, Value: big.NewInt(int64(i)),
			}))
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
	e, _, err := runExecutorIn(t, c, src, stopAt)
	return e, err
}

func runExecutorIn(t *testing.T, c *chain.Chain, src source, stopAt uint64) (*exec.Executor, *state.Store, error) {
	t.Helper()
	dir := t.TempDir()
	store, err := state.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	e, err := exec.New(exec.Config{DataDir: dir, Blocks: src, Store: store, Chain: c, StopAt: stopAt})
	if err != nil {
		return nil, nil, err
	}
	t.Cleanup(func() { e.Close() })
	return e, store, e.Run(context.Background())
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
	if len(blocks) != nBlocks {
		t.Fatalf("generated %d blocks", len(blocks))
	}
	// Block 3 is where the state upgrade lands; if it did not, its root is the
	// one that diverges.
	if blocks[2].Time() != genesisTime+3*blockGap {
		t.Fatalf("block 3 time %d, want the state upgrade timestamp %d", blocks[2].Time(), genesisTime+3*blockGap)
	}

	e, err := runExecutor(t, c, containers(t, blocks), nBlocks)
	if err != nil {
		t.Fatalf("execute subnet-evm chain: %v", err)
	}
	if e.Head() != nBlocks || e.LiveHead() != nBlocks {
		t.Fatalf("head=%d live=%d, want %d", e.Head(), e.LiveHead(), nBlocks)
	}
	// M4: SAE never engages on subnet-evm, so safe/finalized fall back to the
	// executed head, which is what the rpc labels report.
	if e.SettledHeight() != e.LiveHead() {
		t.Fatalf("SettledHeight=%d, want LiveHead=%d", e.SettledHeight(), e.LiveHead())
	}
	// The block header is post-Helicon-shaped only on coreth; a subnet-evm
	// header can never carry the markers.
	if exec.HasSettledMarkers(blocks[nBlocks-1].Header()) {
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

	_, err = runExecutor(t, c, src, nBlocks)
	if err == nil {
		t.Fatal("executor accepted a block whose header root does not match its execution")
	}
	if !strings.Contains(err.Error(), "state root mismatch") {
		t.Fatalf("stopped for the wrong reason: %v", err)
	}
}

// TestSubnetEVMFrontierFromEpochs IS THE BEAM JOIN (2026-08-05, mainnet L1
// 2tmrrBo1...): the whole round trip on a subnet-evm chain that ships an
// upgrade.json, executed here, sealed into epochs, and then merged back into a
// state frontier by a node that has NOTHING BUT THOSE EPOCHS.
//
// The chain's precompile upgrade DISABLES a precompile the genesis enabled,
// which subnet-evm implements as statedb.SelfDestruct: the account and its
// allowlist storage were put in the trie by Genesis.Commit, not by any block,
// so the epochs carry a tombstone against state the merge starts on. Dropping
// that tombstone is what made the real join produce root 40e033de... where
// header(6034097) commits to 91851ed9....
func TestSubnetEVMFrontierFromEpochs(t *testing.T) {
	c := testChain(t)
	blocks := generateChain(t, referenceGenesis(t, c))

	// 1. EXECUTE. Every block's root is verified against its header inside the
	//    executor, and the capture writes the post-images the seal will cut.
	e, store, err := runExecutorIn(t, c, containers(t, blocks), nBlocks)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	// 2. SEAL. Two epochs of four blocks, out of the frames the executor just
	//    captured, decoded by the seal's own decoder.
	rows := map[uint64][]state.StateRow{}
	sawDestruct := false
	for n := uint64(1); n <= nBlocks; n++ {
		frame, ok, err := store.Writes(n)
		if err != nil {
			t.Fatalf("writelog %d: %v", n, err)
		}
		if !ok {
			continue
		}
		r, err := state.FrameRows(frame, n)
		if err != nil {
			t.Fatalf("frame %d: %v", n, err)
		}
		rows[n] = r
		for _, row := range r {
			if row.Key[0] == 'a' && common.Address(row.Key[1:21]) == nativeMinter && len(row.Value) == 0 {
				sawDestruct = true
			}
		}
	}
	// The capture side is not the suspect and this pins that down: if the
	// tombstone were missing HERE, the epochs would be incomplete and no
	// frontier build could ever be right.
	if !sawDestruct {
		t.Fatal("no SELFDESTRUCT row for the disabled precompile: the capture missed the upgrade")
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	st, err := dist.Local(dir)
	if err != nil {
		t.Fatal(err)
	}
	for start := uint64(1); start <= nBlocks; start += perEpoch {
		in := &state.EpochInput{Start: start, TxHashes: map[uint64][][32]byte{}}
		for n := start; n < start+perEpoch; n++ {
			hdr, err := rlp.EncodeToBytes(blocks[n-1].Header())
			if err != nil {
				t.Fatal(err)
			}
			raw, err := rlp.EncodeToBytes(blocks[n-1])
			if err != nil {
				t.Fatal(err)
			}
			in.Headers = append(in.Headers, hdr)
			in.Containers = append(in.Containers, raw)
			in.StateRows = append(in.StateRows, rows[n]...)
		}
		if _, err := state.BuildEpoch(st, in); err != nil {
			t.Fatal(err)
		}
	}
	set, err := state.OpenEpochSet(st)
	if err != nil {
		t.Fatal(err)
	}
	defer set.Close()

	// 3. JOIN. A dir holding the epochs and nothing else merges them onto the
	//    genesis its own VM committed, and the merged root has to be the one
	//    header(8) commits to. That check lives inside BuildFrontier, and it
	//    is the assertion: the error prints both roots.
	store2, err := state.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()
	joined, err := exec.New(exec.Config{DataDir: dir, Blocks: set, Store: store2, Chain: c, FrontierBuild: true})
	if err != nil {
		t.Fatal(err)
	}
	defer joined.Close()
	if err := joined.BuildFrontier(set); err != nil {
		t.Fatalf("build frontier from epochs alone: %v", err)
	}
	if joined.Head() != nBlocks {
		t.Fatalf("frontier parked at %d, want %d", joined.Head(), nBlocks)
	}
}
