package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/ava-labs/avalanchego/api/metrics"
	avaatomic "github.com/ava-labs/avalanchego/chains/atomic"
	"github.com/ava-labs/avalanchego/database/pebbledb"
	"github.com/ava-labs/avalanchego/database/prefixdb"
	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/snow"
	"github.com/ava-labs/avalanchego/snow/engine/snowman/block"
	"github.com/ava-labs/avalanchego/staking"
	"github.com/ava-labs/avalanchego/upgrade"
	"github.com/ava-labs/avalanchego/utils/crypto/bls/signer/localsigner"
	"github.com/ava-labs/avalanchego/utils/logging"
	"github.com/ava-labs/avalanchego/vms/platformvm/warp"
	"github.com/ava-labs/avalanchego/vms/rpcchainvm"
	"github.com/ava-labs/avalanchego/vms/rpcchainvm/runtime"
	ethtypes "github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/rlp"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/containerman17/avalanche-s3-archival-node/chain"
	"github.com/containerman17/avalanche-s3-archival-node/fetch"
)

const corpusMagic = "EPCORP01"
const maxCorpusContainer = 64 << 20

// readCorpus skips accepted records, checks the accepted anchor, and emits only
// records through stop. Each emitted block has a checked height and parent.
func readCorpus(ctx context.Context, path string, accepted, stop uint64, anchor ids.ID, upgrades *upgrade.Config, emit func(item) error) error {
	if accepted > stop {
		return fmt.Errorf("accepted height %d exceeds stop %d", accepted, stop)
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	var magic [len(corpusMagic)]byte
	if _, err = io.ReadFull(f, magic[:]); err != nil {
		return fmt.Errorf("corpus magic: %w", err)
	}
	if string(magic[:]) != corpusMagic {
		return errors.New("invalid corpus magic")
	}
	parent := anchor
	var parentPCH uint64
	for expected := uint64(1); expected <= stop; expected++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		var frame [12]byte
		if _, err = io.ReadFull(f, frame[:]); err != nil {
			return fmt.Errorf("corpus needs height %d through stop %d: %w", expected, stop, err)
		}
		height := binary.BigEndian.Uint64(frame[:8])
		size := binary.BigEndian.Uint32(frame[8:])
		if height != expected {
			return fmt.Errorf("corpus height %d, expected %d", height, expected)
		}
		if size == 0 || size > maxCorpusContainer {
			return fmt.Errorf("corpus height %d has invalid container size %d", height, size)
		}
		if height < accepted {
			if _, err = f.Seek(int64(size), io.SeekCurrent); err != nil {
				return err
			}
			continue
		}
		raw := make([]byte, size)
		if _, err = io.ReadFull(f, raw); err != nil {
			return fmt.Errorf("corpus height %d payload: %w", height, err)
		}
		inner, pch := unwrap(raw, upgrades, &parentPCH)
		var decoded ethtypes.Block
		if err = rlp.DecodeBytes(inner, &decoded); err != nil {
			return fmt.Errorf("corpus height %d block: %w", height, err)
		}
		if decoded.NumberU64() != height {
			return fmt.Errorf("corpus height %d contains block %d", height, decoded.NumberU64())
		}
		if height == accepted {
			if ids.ID(decoded.Hash()) != anchor {
				return fmt.Errorf("corpus height %d differs from accepted block %s", height, anchor)
			}
			continue
		}
		if ids.ID(decoded.ParentHash()) != parent {
			return fmt.Errorf("corpus height %d parent %s differs from %s", height, decoded.ParentHash(), parent)
		}
		parent = ids.ID(decoded.Hash())
		if err := emit(item{h: height, inner: inner, pch: pch, txs: uint64(len(decoded.Transactions())), gas: decoded.GasUsed()}); err != nil {
			return err
		}
	}
	return nil
}

func runCorpus(ctx context.Context, c *chain.Chain, sources []string, vmPath, dataDir, httpAddr, corpusPath, configPath string, stopHeight uint64, queueAhead, batchSize int) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if c.VMKind != chain.SubnetEVM {
		return errors.New("corpus mode requires subnet-evm")
	}
	configBytes, err := os.ReadFile(configPath)
	if err != nil {
		return err
	}
	fetch.RegisterExtras(c.VMKind)
	if err := staking.InitNodeStakingKeyPair(filepath.Join(dataDir, "staker.key"), filepath.Join(dataDir, "staker.crt")); err != nil {
		return err
	}
	nodeID, err := nodeIDFrom(dataDir)
	if err != nil {
		return err
	}
	blsKey, err := localsigner.FromFileOrPersistNew(filepath.Join(dataDir, "signer.key"))
	if err != nil {
		return err
	}
	xChainID, cChainID, avaxAssetID, err := primaryIDs(c.NetworkID)
	if err != nil {
		return err
	}
	logger := logging.NewLogger("host", logging.NewWrappedCore(logging.Info, os.Stderr, logging.Plain.ConsoleEncoder()))
	tracker := &pidTracker{}
	v, err := rpcchainvm.NewFactory(vmPath, tracker, runtime.NewManager(), metrics.NewPrefixGatherer()).New(logger)
	if err != nil {
		return fmt.Errorf("launch plugin: %w", err)
	}
	vm := v.(block.ChainVM)
	db, err := pebbledb.New(filepath.Join(dataDir, "db"), nil, logger, prometheus.NewRegistry())
	if err != nil {
		return err
	}
	defer db.Close()
	chainDataDir := filepath.Join(dataDir, "chainData")
	if err := os.MkdirAll(chainDataDir, 0o755); err != nil {
		return err
	}
	snowCtx := &snow.Context{
		NetworkID: c.NetworkID, SubnetID: c.SubnetID, ChainID: c.BlockchainID,
		NodeID: nodeID, PublicKey: blsKey.PublicKey(), NetworkUpgrades: upgrade.GetConfig(c.NetworkID),
		XChainID: xChainID, CChainID: cChainID, AVAXAssetID: avaxAssetID, Log: logger,
		SharedMemory: avaatomic.NewMemory(prefixdb.New([]byte("atomic"), db)).NewSharedMemory(c.BlockchainID),
		BCLookup:     ids.NewAliaser(), Metrics: metrics.NewPrefixGatherer(),
		WarpSigner:     warp.NewSigner(blsKey, c.NetworkID, c.BlockchainID),
		ValidatorState: newRPCValidatorState(sources, c.BlockchainID, c.SubnetID), ChainDataDir: chainDataDir,
	}
	defer func() {
		shutdownCtx, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		if err := vm.Shutdown(shutdownCtx); err != nil {
			log.Printf("epochdb-host: plugin shutdown: %v", err)
		}
	}()
	if err := vm.Initialize(ctx, snowCtx, prefixdb.New([]byte("vm"), db), c.GenesisJSON, c.UpgradeJSON, configBytes, nil, noopSender{}); err != nil {
		return fmt.Errorf("initialize: %w", err)
	}
	if err := vm.SetState(ctx, snow.Bootstrapping); err != nil {
		return err
	}
	handlers, err := vm.CreateHandlers(ctx)
	if err != nil {
		return err
	}
	rpcHandler := handlers["/rpc"]
	if rpcHandler == nil {
		return errors.New("plugin has no /rpc handler")
	}
	mux := http.NewServeMux()
	mux.Handle("/debug/pprof/", http.DefaultServeMux)
	for ext, handler := range handlers {
		mux.Handle("/ext/bc/"+c.BlockchainID.String()+ext, handler)
	}
	listener, err := net.Listen("tcp", httpAddr)
	if err != nil {
		return err
	}
	server := &http.Server{Handler: mux}
	defer server.Close()
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("epochdb-host: corpus HTTP: %v", err)
			cancel()
		}
	}()
	lastID, err := vm.LastAccepted(ctx)
	if err != nil {
		return err
	}
	last, err := vm.GetBlock(ctx, lastID)
	if err != nil {
		return err
	}
	log.Printf("epochdb-host: corpus=%s accepted=%d stop=%d batch=%d ring=%d vm_pid=%d", corpusPath, last.Height(), stopHeight, batchSize, queueAhead, tracker.pid.Load())
	p := &pipe{from: last.Height() + 1, batch: batchSize, ring: make(chan item, queueAhead), batches: make(chan []parsed, 2), stop: cancel}
	b := &bench{tracker: tracker, ring: p.ring}
	p.b = b
	b.height.Store(last.Height())
	benchCtx, stopBench := context.WithCancel(ctx)
	defer stopBench()
	go b.loop(benchCtx)
	go func() {
		defer close(p.ring)
		if err := readCorpus(ctx, corpusPath, last.Height(), stopHeight, lastID, &snowCtx.NetworkUpgrades, func(it item) error {
			select {
			case p.ring <- it:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}); err != nil {
			p.fail(err)
		}
	}()
	go p.parse(ctx, vm)
	if err := p.drive(ctx, vm, b); err != nil {
		return err
	}
	lastID, err = vm.LastAccepted(ctx)
	if err != nil {
		return err
	}
	last, err = vm.GetBlock(ctx, lastID)
	if err != nil {
		return err
	}
	if last.Height() != stopHeight {
		return fmt.Errorf("stopped at %d, expected %d", last.Height(), stopHeight)
	}
	if err := vm.SetPreference(ctx, lastID); err != nil {
		return err
	}
	if err := vm.SetState(ctx, snow.NormalOp); err != nil {
		return err
	}
	if err := waitCorpusRPC(ctx, rpcHandler, stopHeight); err != nil {
		return err
	}
	stopBench()
	b.exit()
	log.Printf("corpus ready height=%d host_pid=%d id=%s vm_pid=%d rpc=http://%s/ext/bc/%s/rpc", stopHeight, os.Getpid(), lastID, tracker.pid.Load(), listener.Addr(), c.BlockchainID)
	<-ctx.Done()
	return ctx.Err()
}

// waitCorpusRPC waits for the stock handler to observe the completed acceptor.
func waitCorpusRPC(ctx context.Context, handler http.Handler, height uint64) error {
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		request := httptest.NewRequest(http.MethodPost, "/rpc", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`)).WithContext(ctx)
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		var result struct {
			Result string          `json:"result"`
			Error  json.RawMessage `json:"error"`
		}
		if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
			return fmt.Errorf("readiness RPC response: %w", err)
		}
		if len(result.Error) != 0 {
			return fmt.Errorf("readiness RPC error: %s", result.Error)
		}
		n, err := strconv.ParseUint(strings.TrimPrefix(result.Result, "0x"), 16, 64)
		if err != nil {
			return fmt.Errorf("readiness RPC height %q: %w", result.Result, err)
		}
		if n == height {
			return nil
		}
		if n > height {
			return fmt.Errorf("RPC advanced beyond stop: %d > %d", n, height)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}
