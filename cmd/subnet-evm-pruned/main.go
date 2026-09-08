//go:build subnetbench

// Command subnet-evm-pruned serves subnet-evm with a selectable pruned state backend.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"

	"github.com/ava-labs/avalanchego/database"
	"github.com/ava-labs/avalanchego/graft/subnet-evm/core"
	"github.com/ava-labs/avalanchego/graft/subnet-evm/plugin/evm"
	"github.com/ava-labs/avalanchego/graft/subnet-evm/plugin/evm/config"
	"github.com/ava-labs/avalanchego/graft/subnet-evm/plugin/runner"
	"github.com/ava-labs/avalanchego/snow"
	commonEng "github.com/ava-labs/avalanchego/snow/engine/common"
	"github.com/ava-labs/avalanchego/utils/logging"
	"github.com/ava-labs/avalanchego/utils/ulimit"
	"github.com/ava-labs/avalanchego/version"
	"github.com/ava-labs/avalanchego/vms/evm/sync/customrawdb"
	"github.com/ava-labs/avalanchego/vms/rpcchainvm"
	"github.com/ava-labs/libevm/core/state"
	"github.com/ava-labs/libevm/ethdb"
	"github.com/ava-labs/libevm/log"
	"github.com/ava-labs/libevm/triedb"

	"github.com/containerman17/avalanche-s3-archival-node/pruned"
)

const (
	backendField = "benchmark-state-backend"
	pprofField   = "benchmark-pprof-addr" // optional: serve net/http/pprof on this address
)

var (
	_ core.RevisionTrieDB = (*pruned.DB)(nil)
	_ triedb.HashDB       = (*pruned.DB)(nil)
)

type prunedVM struct {
	evm.VM
	backend string
}

func (vm *prunedVM) Initialize(
	ctx context.Context,
	chainCtx *snow.Context,
	db database.Database,
	genesisBytes, upgradeBytes, configBytes []byte,
	fxs []*commonEng.Fx,
	appSender commonEng.AppSender,
) error {
	backend, stockConfig, err := backendConfig(configBytes, chainCtx.NetworkID)
	if err != nil {
		return err
	}
	vm.backend = backend
	return vm.VM.Initialize(ctx, chainCtx, db, genesisBytes, upgradeBytes, stockConfig, fxs, appSender)
}

func backendConfig(data []byte, networkID uint32) (string, []byte, error) {
	backend := "firewood"
	if len(data) > 0 {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(data, &fields); err != nil {
			return "", nil, fmt.Errorf("decode VM config: %w", err)
		}
		changed := false
		if value, ok := fields[pprofField]; ok {
			var addr string
			if err := json.Unmarshal(value, &addr); err != nil {
				return "", nil, fmt.Errorf("decode %s: %w", pprofField, err)
			}
			delete(fields, pprofField)
			changed = true
			go func() { _ = http.ListenAndServe(addr, nil) }()
		}
		if value, ok := fields[backendField]; ok {
			var selected string
			if err := json.Unmarshal(value, &selected); err != nil {
				return "", nil, fmt.Errorf("decode %s: %w", backendField, err)
			}
			backend = selected
			delete(fields, backendField)
			changed = true
		}
		if changed {
			var err error
			data, err = json.Marshal(fields)
			if err != nil {
				return "", nil, fmt.Errorf("encode VM config: %w", err)
			}
		}
	}
	if backend != "firewood" && backend != "epochdb" {
		return "", nil, fmt.Errorf("unsupported %s %q", backendField, backend)
	}
	cfg, _, err := config.GetConfig(data, networkID)
	if err != nil {
		return "", nil, fmt.Errorf("parse VM config: %w", err)
	}
	if cfg.StateScheme != customrawdb.FirewoodScheme || !cfg.Pruning || cfg.StateSyncEnabled {
		return "", nil, fmt.Errorf("pruned comparison requires state-scheme=firewood, pruning-enabled=true, and state-sync-enabled=false")
	}
	return backend, data, nil
}

func (vm *prunedVM) constructor(c *core.CacheConfig) triedb.DBConstructor {
	if vm.backend != "epochdb" {
		return nil
	}
	return func(ethdb.Database) triedb.DBOverride {
		db, err := pruned.New(pruned.Config{
			Dir:            filepath.Join(c.ChainDataDir, "epochdb"),
			Retain:         c.StateHistory,
			CommitInterval: c.CommitInterval,
		})
		if err != nil {
			log.Crit("creating epochdb state backend", "error", err)
		}
		return db
	}
}

func run() error {
	evm.RegisterAllLibEVMExtras()
	printVersion, err := runner.PrintVersion()
	if err != nil {
		return err
	}
	if printVersion {
		fmt.Printf("Subnet-EVM/%s [rpcchainvm=%d, state-backends=firewood,epochdb]\n", version.Current.SemanticWithCommit(version.GitCommit), version.RPCChainVMProtocol)
		return nil
	}
	if err := ulimit.Set(ulimit.DefaultFDLimit, logging.NoLog{}); err != nil {
		return fmt.Errorf("set fd limit: %w", err)
	}
	vm := &prunedVM{}
	core.RegisterTrieDBConstructor(vm.constructor)
	state.RegisterDatabaseInterceptor(pruned.NewStateAccessor)
	return rpcchainvm.Serve(context.Background(), vm)
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "subnet-evm-pruned: %v\n", err)
		os.Exit(1)
	}
}
