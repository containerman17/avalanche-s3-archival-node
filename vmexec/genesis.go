package vmexec

// The subnet-evm execution backend, copied from exec/vm_sevm.go, exec/genesis.go
// and exec/serve.go: genesis parsing (upgrades wired exactly as the VM's own
// parseGenesis does), what the genesis materialises in the trie, the snow
// context, and one block's EVM run. coreth, SAE and atomic txs are gone: this
// node is subnet-evm only.

import (
	"encoding/json"
	"fmt"
	"maps"
	"math/big"

	sevmcommontype "github.com/ava-labs/avalanchego/graft/subnet-evm/commontype"
	sevmconsensus "github.com/ava-labs/avalanchego/graft/subnet-evm/consensus"
	sevmdummy "github.com/ava-labs/avalanchego/graft/subnet-evm/consensus/dummy"
	sevmcore "github.com/ava-labs/avalanchego/graft/subnet-evm/core"
	sevmextstate "github.com/ava-labs/avalanchego/graft/subnet-evm/core/extstate"
	sevmparams "github.com/ava-labs/avalanchego/graft/subnet-evm/params"
	sevmextras "github.com/ava-labs/avalanchego/graft/subnet-evm/params/extras"
	sevmmodules "github.com/ava-labs/avalanchego/graft/subnet-evm/precompile/modules"

	// GENESIS PRECOMPILES DO NOT PARSE WITHOUT THIS, AND THEY FAIL SILENTLY
	// (see exec/vm_sevm.go).
	_ "github.com/ava-labs/avalanchego/graft/subnet-evm/precompile/registry"
	"github.com/ava-labs/avalanchego/snow"
	"github.com/ava-labs/avalanchego/upgrade"
	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/rawdb"
	ethstate "github.com/ava-labs/libevm/core/state"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/core/vm"
	"github.com/ava-labs/libevm/params"
	"github.com/ava-labs/libevm/rlp"
	"github.com/ava-labs/libevm/triedb"

	"github.com/containerman17/avalanche-s3-archival-node/chain"
	"github.com/containerman17/avalanche-s3-archival-node/fetch"
	"github.com/containerman17/avalanche-s3-archival-node/store"
)

// Genesis is the VM-neutral view of a chain's genesis (see exec.Genesis).
type Genesis struct {
	Config    *params.ChainConfig
	TrieAlloc types.GenesisAlloc
	Header    *types.Header // block 1's parent, which no container carries
	Timestamp uint64
	Hash      common.Hash
	Root      common.Hash
}

// ChainGenesis returns the fully wired genesis for a subnet-evm chain
// descriptor and performs this process's one-and-only extras registration.
func ChainGenesis(c *chain.Chain) (*Genesis, error) {
	if c == nil || c.VMKind != chain.SubnetEVM {
		return nil, fmt.Errorf("vmexec: subnet-evm chains only")
	}
	fetch.RegisterExtras(c.VMKind)
	snowCtx, err := snowContextFor(c)
	if err != nil {
		return nil, err
	}
	return parseGenesis(c, snowCtx)
}

// snowContextFor builds the snow.Context the EVM sees; every field is an
// execution input (exec/genesis.go has the history of each).
func snowContextFor(c *chain.Chain) (*snow.Context, error) {
	ctx := &snow.Context{
		NetworkID:       c.NetworkID,
		SubnetID:        c.SubnetID,
		ChainID:         c.BlockchainID,
		NetworkUpgrades: upgrade.GetConfig(c.NetworkID),
	}
	cChainID, _, err := chain.PrimaryC(c.NetworkID)
	if err != nil {
		return ctx, nil // a local network: neither field is an input for an L1
	}
	ctx.CChainID = cChainID
	return ctx, nil
}

// parseGenesis mirrors subnet-evm/plugin/evm/vm.go parseGenesis.
func parseGenesis(c *chain.Chain, snowCtx *snow.Context) (*Genesis, error) {
	g := new(sevmcore.Genesis)
	if err := json.Unmarshal(c.GenesisJSON, g); err != nil {
		return nil, fmt.Errorf("unmarshal subnet-evm genesis: %w", err)
	}
	if g.Config == nil {
		g.Config = sevmparams.SubnetEVMDefaultChainConfig
	}
	configExtra := sevmparams.GetExtra(g.Config)
	configExtra.AvalancheContext = sevmextras.AvalancheContext{SnowCtx: snowCtx}
	if configExtra.FeeConfig == sevmcommontype.EmptyFeeConfig {
		configExtra.FeeConfig = sevmparams.DefaultFeeConfig
	}
	configExtra.SetDefaults(snowCtx.NetworkUpgrades)
	if len(c.UpgradeJSON) > 0 {
		var upgradeConfig sevmextras.UpgradeConfig
		if err := json.Unmarshal(c.UpgradeJSON, &upgradeConfig); err != nil {
			return nil, fmt.Errorf("parse upgrade bytes: %w", err)
		}
		configExtra.UpgradeConfig = upgradeConfig
	}
	if overrides := configExtra.UpgradeConfig.NetworkUpgradeOverrides; overrides != nil {
		configExtra.Override(overrides)
	}
	if err := g.Verify(); err != nil {
		return nil, fmt.Errorf("invalid subnet-evm genesis: %w", err)
	}
	if err := sevmparams.SetEthUpgrades(g.Config); err != nil {
		return nil, fmt.Errorf("set eth upgrades: %w", err)
	}
	alloc, err := trieAlloc(g)
	if err != nil {
		return nil, err
	}
	blk := g.ToBlock()
	return &Genesis{Config: g.Config, TrieAlloc: alloc, Timestamp: g.Timestamp, Hash: blk.Hash(), Root: blk.Root(), Header: blk.Header()}, nil
}

// trieAlloc returns what this genesis actually MATERIALISES: the alloc plus
// every precompile enabled at time 0 (see exec/vm_sevm.go for why).
func trieAlloc(g *sevmcore.Genesis) (types.GenesisAlloc, error) {
	db := rawdb.NewMemoryDatabase()
	tdb := triedb.NewDatabase(db, &triedb.Config{Preimages: true})
	sdb, err := ethstate.New(types.EmptyRootHash, sevmextstate.NewDatabaseWithNodeDB(db, tdb), nil)
	if err != nil {
		return nil, err
	}
	blockCtx := sevmcore.NewBlockContext(new(big.Int).SetUint64(g.Number), g.Timestamp)
	if err := sevmcore.ApplyPrecompileActivations(g.Config, nil, blockCtx, sdb); err != nil {
		return nil, fmt.Errorf("configure genesis precompiles: %w", err)
	}
	root, err := sdb.Commit(0, false)
	if err != nil {
		return nil, err
	}
	if root == types.EmptyRootHash {
		return g.Alloc, nil
	}
	if sdb, err = ethstate.New(root, sevmextstate.NewDatabaseWithNodeDB(db, tdb), nil); err != nil {
		return nil, err
	}
	out := make(types.GenesisAlloc, len(g.Alloc)+len(sevmmodules.RegisteredModules()))
	for _, da := range sdb.RawDump(&ethstate.DumpConfig{}).Accounts {
		if da.Address == nil {
			return nil, fmt.Errorf("genesis precompile account %x has no preimage", da.AddressHash)
		}
		bal, ok := new(big.Int).SetString(da.Balance, 10)
		if !ok {
			return nil, fmt.Errorf("genesis precompile %s: undecodable balance %q", da.Address, da.Balance)
		}
		acct := types.Account{Nonce: da.Nonce, Balance: bal, Code: da.Code}
		if len(da.Storage) > 0 {
			acct.Storage = make(map[common.Hash]common.Hash, len(da.Storage))
			for slot, v := range da.Storage {
				acct.Storage[slot] = common.HexToHash(v)
			}
		}
		out[*da.Address] = acct
	}
	for addr, a := range g.Alloc {
		if pre, ok := out[addr]; ok && len(pre.Storage) > 0 {
			st := maps.Clone(pre.Storage)
			maps.Copy(st, a.Storage)
			a.Storage = st
		}
		out[addr] = a
	}
	return out, nil
}

// chainContext is subnet-evm's ChainContext over the store's headers, so
// BLOCKHASH resolves real hashes across its 256-block window. A read or
// decode failure panics: nil is libevm's "no such header" and would become a
// silent zero hash inside a running contract. recent holds the headers of
// executed blocks the checker has not written to the store yet (executor
// goroutine only).
type chainContext struct {
	store  *store.DB
	recent map[uint64]*types.Header
}

func (chainContext) Engine() sevmconsensus.Engine { return sevmdummy.NewFullFaker() }

func (c chainContext) GetHeader(_ common.Hash, num uint64) *types.Header {
	if h := c.recent[num]; h != nil {
		return h
	}
	raw, ok, err := c.store.HeaderRLP(num)
	if err != nil {
		panic(fmt.Sprintf("vmexec: read header %d for BLOCKHASH: %v", num, err))
	}
	if !ok {
		return nil
	}
	var h types.Header
	if err := rlp.DecodeBytes(raw, &h); err != nil {
		panic(fmt.Sprintf("vmexec: decode header %d for BLOCKHASH: %v", num, err))
	}
	return &h
}

// TxHook is called after each transaction is applied, on the executor's own
// goroutine: where the per-tx state capture drains.
type TxHook func(txIndex int, tx *types.Transaction, receipt *types.Receipt) error

// runEVM mirrors subnet-evm core.StateProcessor.Process minus engine.Finalize
// (no StateDB in subnet-evm's: it cannot move a state root). parentTime is
// the PARENT's timestamp, which is what makes a precompile activate once.
func runEVM(cc chainContext, chainCfg *params.ChainConfig, blk *types.Block, parentTime uint64,
	statedb *ethstate.StateDB, onTx TxHook,
) (types.Receipts, error) {
	header := blk.Header()
	upgradeBlockCtx := sevmcore.NewBlockContext(header.Number, header.Time)
	if err := sevmcore.ApplyUpgrades(chainCfg, &parentTime, upgradeBlockCtx, statedb); err != nil {
		return nil, fmt.Errorf("apply upgrades: %w", err)
	}
	blockCtx := sevmcore.NewEVMBlockContext(header, cc, nil)
	gp := new(sevmcore.GasPool).AddGas(header.GasLimit)
	// One EVM and one signer per block, the block hash from the block's
	// cache: what subnet-evm's own Process does (see applyTx).
	signer := types.MakeSigner(chainCfg, header.Number, header.Time)
	vmenv := vm.NewEVM(blockCtx, vm.TxContext{}, statedb, chainCfg, vm.Config{Tracer: frameTracer()})
	blockHash := blk.Hash()
	var (
		usedGas  uint64
		receipts types.Receipts
	)
	for txIndex, tx := range blk.Transactions() {
		msg, err := sevmcore.TransactionToMessage(tx, signer, header.BaseFee)
		if err != nil {
			return nil, fmt.Errorf("tx %d: %w", txIndex, err)
		}
		statedb.SetTxContext(tx.Hash(), txIndex)
		receipt, err := applyTx(msg, chainCfg, gp, statedb, header.Number, blockHash, tx, &usedGas, vmenv)
		if err != nil {
			return nil, fmt.Errorf("tx %d: %w", txIndex, err)
		}
		receipts = append(receipts, receipt)
		if onTx != nil {
			if err := onTx(txIndex, tx, receipt); err != nil {
				return nil, fmt.Errorf("tx %d capture: %w", txIndex, err)
			}
		}
	}
	return receipts, nil
}
