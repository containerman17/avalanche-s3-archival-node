// Package rpc is a minimal JSON-RPC HTTP server over the historical
// overlay: eth_chainId, eth_blockNumber, eth_getBalance,
// eth_getTransactionCount, eth_getCode, eth_getStorageAt, eth_call. No
// websockets, no subscriptions, single requests only (a JSON array gets a
// polite error).
package rpc

import (
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"strings"
	"sync"

	"github.com/gorilla/websocket"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/common/hexutil"
	ethstate "github.com/ava-labs/libevm/core/state"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/core/vm"
	"github.com/ava-labs/libevm/params"
	"github.com/ava-labs/libevm/rlp"

	"github.com/containerman17/epochdb/state"
)

// GasCap bounds eth_call execution.
const GasCap = 50_000_000

// Server serves historical reads at heights [0, hist.Head()]. Requests
// run fully concurrent: every shared reader underneath is immutable
// (mmaps, RO maps) or internally locked (bucketLog handle LRU); zstd
// DecodeAll is concurrent-safe.
type Server struct {
	hist     *state.History
	chainCtx ChainContext
	chainCfg *params.ChainConfig

	// tx APIs, wired by EnableTxAPIs (nil = methods unavailable).
	txidx  TxCandidateSource
	blocks BlockSource
	parse  ContainerParser

	// filter registry (rpc/filters.go), created on first use.
	filtersOnce sync.Once
	filters     *filterReg

	// recent is the BOUNDED live-tail block-hash map (see recentHashes):
	// everything older resolves through the tx index.
	recent *recentHashes

	// live is the in-process executor (serve); nil before EnableLive
	// serve, where every read is historical.
	live Live
}

// Live is the in-process executor view that serve wires in: the height
// labels, and nothing else. Latest STATE does not come through here, it comes
// from History's uncooked-tail overlay (ruling 2026-07-29: Firewood is
// verify-only and has no readers).
type Live interface {
	// LiveHead is the last EXECUTED height: the `latest` label.
	LiveHead() uint64
	// AcceptedHead is the last height the follower accepted (>= LiveHead):
	// the `pending` label.
	AcceptedHead() uint64
	// SettledHeight is the last SAE-settled height: the `safe`/`finalized`
	// labels. Below the Helicon boundary it equals LiveHead.
	SettledHeight() uint64
}

// EnableLive wires the executor frontier into the server (serve).
func (s *Server) EnableLive(l Live) { s.live = l }

// recentBlockHashes bounds the live-tail block-hash map. Every block hash is
// in the tx index (epoch format v6 for sealed epochs, the cooked staging
// index for the raw tail), so the only heights this has to answer for are the
// ones ACCEPTED SINCE THE LAST COOK, which at chain pace is a couple of
// hundred: 128Ki entries is over a day of cook outage at Fuji/mainnet block
// rates, for ~4MB of ring plus a map of the same order.
//
// ponytail: fixed-size FIFO, not a cook-watermark prune. During a multi-
// thousand-block/s catch-up the ring can wrap before the next cook, and a
// block-by-hash for a height in the wrapped window answers null until that
// cook lands (a node in that state already reports eth_syncing). Swap the
// FIFO for an explicit "drop below the cooked height" prune if that window
// ever has to be zero.
const recentBlockHashes = 1 << 17

// recentHashes is the eth block hash -> height map for the LIVE TAIL only:
// serve/sdk push each accepted block in, the oldest entry falls out, and the
// whole of history is answered by the fingerprint index instead. The
// floor-to-head version of this map was the largest resident item in the
// process (measured 100.7 B/entry, 5.78GB at Fuji's 57.4M blocks).
type recentHashes struct {
	mu   sync.RWMutex
	m    map[common.Hash]uint64
	ring []common.Hash
	next int
}

func newRecentHashes(n int) *recentHashes {
	return &recentHashes{m: make(map[common.Hash]uint64), ring: make([]common.Hash, n)}
}

func (h *recentHashes) get(k common.Hash) (uint64, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	n, ok := h.m[k]
	return n, ok
}

func (h *recentHashes) add(k common.Hash, n uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, dup := h.m[k]; dup {
		return
	}
	if old := h.ring[h.next]; old != (common.Hash{}) {
		delete(h.m, old)
	}
	h.ring[h.next] = k
	h.next = (h.next + 1) % len(h.ring)
	h.m[k] = n
}

// HistoryChainContext serves BLOCKHASH headers through the epochs-aware
// History (raw store first, sealed epochs once the raws are gone). It is
// VM-neutral: the consensus engine the two VMs disagree about is added by
// whichever backend the seam picks (rpc/vm.go).
func HistoryChainContext(hist *state.History) ChainContext {
	return histChainCtx{hist}
}

type histChainCtx struct{ hist *state.History }

func (c histChainCtx) GetHeader(_ common.Hash, n uint64) *types.Header {
	raw, ok, err := c.hist.HeaderRLP(n)
	if err != nil || !ok {
		return nil
	}
	var h types.Header
	if err := rlp.DecodeBytes(raw, &h); err != nil {
		return nil
	}
	return &h
}

func NewServer(hist *state.History, chainCtx ChainContext, chainCfg *params.ChainConfig) *Server {
	return &Server{hist: hist, chainCtx: chainCtx, chainCfg: chainCfg, recent: newRecentHashes(recentBlockHashes)}
}

type rpcRequest struct {
	ID     json.RawMessage   `json:"id"`
	Method string            `json:"method"`
	Params []json.RawMessage `json:"params"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

func (e *rpcError) Error() string { return e.Message }

func errInvalid(format string, args ...any) *rpcError {
	return &rpcError{Code: -32602, Message: fmt.Sprintf(format, args...)}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if websocket.IsWebSocketUpgrade(r) {
		s.serveWS(w, r)
		return
	}
	var req rpcRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeReply(w, nil, nil, &rpcError{Code: -32700, Message: "parse error (no batching)"})
		return
	}
	result, rerr := s.dispatch(&req)
	writeReply(w, req.ID, result, rerr)
}

func writeReply(w http.ResponseWriter, id json.RawMessage, result any, rerr *rpcError) {
	w.Header().Set("Content-Type", "application/json")
	if id == nil {
		id = json.RawMessage("null")
	}
	reply := map[string]any{"jsonrpc": "2.0", "id": id}
	if rerr != nil {
		reply["error"] = rerr
	} else {
		reply["result"] = result
	}
	json.NewEncoder(w).Encode(reply)
}

func (s *Server) dispatch(req *rpcRequest) (any, *rpcError) {
	switch req.Method {
	case "eth_chainId":
		return hexutil.EncodeBig(s.chainCfg.ChainID), nil
	case "eth_blockNumber":
		return hexutil.EncodeUint64(s.hist.Head()), nil
	case "eth_getBalance":
		return s.accountField(req.Params, func(st *ethstate.StateDB, a common.Address) any {
			return hexutil.EncodeBig(st.GetBalance(a).ToBig())
		})
	case "eth_getTransactionCount":
		return s.accountField(req.Params, func(st *ethstate.StateDB, a common.Address) any {
			return hexutil.EncodeUint64(st.GetNonce(a))
		})
	case "eth_getCode":
		return s.accountField(req.Params, func(st *ethstate.StateDB, a common.Address) any {
			return hexutil.Encode(st.GetCode(a))
		})
	case "eth_getStorageAt":
		return s.getStorageAt(req.Params)
	case "eth_call":
		return s.ethCall(req.Params)
	case "eth_getTransactionByHash", "eth_getTransactionReceipt":
		if s.txidx == nil {
			return nil, &rpcError{Code: -32000, Message: "tx index not available yet (the node cooks it on its own cadence)"}
		}
		if req.Method == "eth_getTransactionByHash" {
			return s.getTransactionByHash(req.Params)
		}
		return s.getTransactionReceipt(req.Params)
	case "eth_getLogs":
		if s.blocks == nil {
			return nil, &rpcError{Code: -32000, Message: "log queries need the container source (tx APIs enabled)"}
		}
		return s.getLogs(req.Params)
	// block, fee, and trivia methods live in block.go / misc.go.
	case "eth_getBlockByNumber":
		return s.getBlockByNumber(req.Params)
	case "eth_getBlockByHash":
		return s.getBlockByHash(req.Params)
	case "eth_getBlockTransactionCountByNumber":
		return s.blockTxCount(req.Params, false)
	case "eth_getBlockTransactionCountByHash":
		return s.blockTxCount(req.Params, true)
	case "eth_getTransactionByBlockNumberAndIndex":
		return s.txByBlockAndIndex(req.Params, false)
	case "eth_getTransactionByBlockHashAndIndex":
		return s.txByBlockAndIndex(req.Params, true)
	case "eth_getBlockReceipts":
		return s.getBlockReceipts(req.Params)
	case "eth_estimateGas":
		return s.estimateGas(req.Params)
	case "eth_gasPrice":
		return hexutil.EncodeBig(s.gasOracle()), nil
	case "eth_maxPriorityFeePerGas":
		// Pre-London corpus: the whole gas price is the tip.
		return hexutil.EncodeBig(s.gasOracle()), nil
	case "eth_feeHistory":
		return s.feeHistory(req.Params)
	case "net_version":
		return s.chainCfg.ChainID.String(), nil
	case "web3_clientVersion":
		return ClientVersion, nil
	case "eth_syncing":
		// Following: report the geth-shaped object whenever execution trails
		// what the follower already accepted. A node 60M blocks behind the tip
		// answering `false` is the lie that hides a broken pipeline.
		if s.live != nil {
			cur, acc := s.live.LiveHead(), s.live.AcceptedHead()
			if acc > cur+1 {
				return map[string]string{
					"startingBlock": hexutil.EncodeUint64(0),
					"currentBlock":  hexutil.EncodeUint64(cur),
					"highestBlock":  hexutil.EncodeUint64(acc),
				}, nil
			}
		}
		return false, nil // caught up (or a fixed complete corpus)
	case "net_listening":
		return false, nil // no p2p listener on the read server
	case "eth_accounts":
		return []common.Address{}, nil
	case "eth_coinbase", "eth_etherbase":
		// The fixed blackhole coinbase, matching the public API
		// (eth_etherbase is its coreth-served alias). It is the SAME address on
		// both VM kinds: graft/evm's constants.BlackholeAddr, which subnet-evm
		// also uses unless the chain sets allowFeeRecipients, in which case the
		// real recipient is per block and lives in each header's Coinbase (this
		// method has no block, so it cannot report it).
		return common.HexToAddress("0x0100000000000000000000000000000000000000"), nil
	case "eth_getUncleCountByBlockNumber", "eth_getUncleCountByBlockHash":
		return hexutil.Uint(0), nil // no uncles on Avalanche
	case "eth_getUncleByBlockNumberAndIndex", "eth_getUncleByBlockHashAndIndex":
		return nil, nil
	case "eth_getProof":
		return nil, &rpcError{Code: -32000, Message: "eth_getProof unsupported by design: epochdb stores no tries"}
	case "eth_callDetailed":
		return s.callDetailed(req.Params)
	case "eth_suggestPriceOptions":
		return s.suggestPriceOptions()
	case "eth_getChainConfig", "debug_chainConfig":
		return s.chainConfig()
	// debug/trace namespace (debug.go).
	case "debug_traceTransaction":
		return s.debugTraceTransaction(req.Params)
	case "debug_traceBlockByNumber":
		return s.debugTraceBlock(req.Params, false)
	case "debug_traceBlockByHash":
		return s.debugTraceBlock(req.Params, true)
	case "debug_getRawBlock":
		return s.debugGetRawBlock(req.Params)
	case "debug_getRawHeader":
		return s.debugGetRawHeader(req.Params)
	case "debug_getRawTransaction":
		return s.debugGetRawTransaction(req.Params)
	case "debug_getRawReceipts":
		return s.debugGetRawReceipts(req.Params)
	case "eth_createAccessList":
		return s.createAccessList(req.Params)
	case "debug_traceCall":
		return s.debugTraceCall(req.Params)
	case "debug_getModifiedAccountsByNumber":
		return s.debugGetModifiedAccounts(req.Params, false)
	case "debug_getModifiedAccountsByHash":
		return s.debugGetModifiedAccounts(req.Params, true)
	case "debug_getBadBlocks":
		return []any{}, nil // root-verified replay retains no bad blocks
	case "debug_printBlock":
		return s.printBlock(req.Params)
	case "debug_getAccessibleState":
		return s.getAccessibleState(req.Params)
	case "debug_dumpBlock", "debug_accountRange", "debug_storageRangeAt", "debug_intermediateRoots":
		// intermediateRoots is the same refusal as getProof for the same
		// reason: a per-tx state root only exists if the node hashes a trie
		// per transaction, and epochdb stores no tries at all.
		return nil, &rpcError{Code: -32000, Message: req.Method + " unsupported by design: epochdb stores no tries"}
	case "debug_preimage":
		// The state key space is ALREADY hashed (wiki: firewood key space is
		// already hashed, no preimages needed), so there is no preimage table
		// to look a hash up in.
		return nil, &rpcError{Code: -32000, Message: "debug_preimage unsupported by design: epochdb keeps no preimage table"}
	case "debug_traceBadBlock":
		// Consistent with debug_getBadBlocks returning []: replay is
		// root-verified block by block, so a bad block halts the executor
		// instead of being retained for later inspection.
		return nil, &rpcError{Code: -32000, Message: "debug_traceBadBlock: no bad blocks are retained (replay is root-verified and halts instead)"}
	case "debug_traceChain":
		// geth streams this one over a notification channel, unbounded, for a
		// whole range. Refused rather than half-implemented with a different
		// wire shape: debug_traceBlockByNumber is the same work, per block,
		// with a shape clients already parse.
		return nil, &rpcError{Code: -32000, Message: "debug_traceChain unsupported: trace a range with debug_traceBlockByNumber per block"}
	// filters, subscriptions bookkeeping, and audit trivia (filters.go).
	case "eth_newFilter":
		return s.newFilter(req.Params)
	case "eth_newBlockFilter":
		return s.newBlockFilter()
	case "eth_newPendingTransactionFilter":
		return s.newPendingTxFilter()
	case "eth_getFilterChanges":
		return s.getFilterChanges(req.Params)
	case "eth_getFilterLogs":
		return s.getFilterLogs(req.Params)
	case "eth_uninstallFilter":
		return s.uninstallFilter(req.Params)
	case "web3_sha3":
		return web3Sha3(req.Params)
	case "net_peerCount":
		return "0x0", nil // read server, no p2p peers
	case "txpool_status", "txpool_content", "txpool_contentFrom", "txpool_inspect":
		return emptyTxPool(req.Method), nil
	case "eth_pendingTransactions":
		return []any{}, nil // no mempool
	case "eth_baseFee":
		return s.baseFee()
	case "eth_getHeaderByNumber":
		return s.getHeaderBy(req.Params, false)
	case "eth_getHeaderByHash":
		return s.getHeaderBy(req.Params, true)
	case "eth_getRawTransactionByHash":
		return s.rawTxByHash(req.Params)
	case "eth_getRawTransactionByBlockNumberAndIndex":
		return s.rawTxByBlockAndIndex(req.Params, false)
	case "eth_getRawTransactionByBlockHashAndIndex":
		return s.rawTxByBlockAndIndex(req.Params, true)
	case "eth_sendRawTransaction", "eth_sendTransaction", "eth_fillTransaction", "eth_resend":
		return nil, errTxSubmission(req.Method)
	case "eth_sign", "eth_signTransaction":
		return nil, errNoKeystore(req.Method)
	case "eth_subscribe", "eth_unsubscribe":
		return nil, &rpcError{Code: -32000, Message: req.Method + " requires the WebSocket transport"}
	default:
		if ns, _, ok := strings.Cut(req.Method, "_"); ok {
			switch ns {
			case "personal", "miner", "admin", "les", "clique", "ethash":
				return nil, errNotArchivable(req.Method)
			}
		}
		return nil, &rpcError{Code: -32601, Message: "method not found: " + req.Method}
	}
}

// blockNumber resolves a block tag parameter to a height within coverage.
//
// SAE label mapping (ACP-194), which collapses to one height below the Helicon
// boundary because settlement does not exist there:
//
//	pending         = last accepted (staged, not executed here)
//	latest          = last executed
//	safe, finalized = last settled
//
// `pending` deliberately resolves to the LAST EXECUTED height for state reads
// (there is no mempool, so pending state is latest state); the pending BLOCK
// itself is served by getBlockByNumber, which handles the tag before this.
func (s *Server) blockNumber(raw json.RawMessage) (uint64, *rpcError) {
	head := s.hist.Head()
	if len(raw) == 0 {
		return head, nil
	}
	// geth's BlockNumberOrHash object form, accepted at EVERY position whose
	// geth signature is a blockNrOrHash (getBalance, getCode, getStorageAt,
	// getTransactionCount, call, estimateGas, createAccessList, traceCall,
	// the filter criteria). Resolved here, once, so no method can forget it.
	if raw[0] == '{' {
		var o struct {
			BlockNumber      json.RawMessage `json:"blockNumber"`
			BlockHash        *common.Hash    `json:"blockHash"`
			RequireCanonical bool            `json:"requireCanonical"`
		}
		if err := json.Unmarshal(raw, &o); err != nil {
			return 0, errInvalid("bad block tag: %v", err)
		}
		switch {
		case o.BlockHash != nil:
			// Every height this node serves is canonical (there is no side
			// chain to confuse it with), so requireCanonical is a no-op.
			n, ok, err := s.HeightByHash(*o.BlockHash)
			if err != nil {
				return 0, &rpcError{Code: -32000, Message: err.Error()}
			}
			if !ok {
				return 0, &rpcError{Code: -32000, Message: "header for hash not found"}
			}
			return n, nil
		case len(o.BlockNumber) > 0:
			return s.blockNumber(o.BlockNumber)
		}
		return 0, errInvalid("block tag object needs blockNumber or blockHash")
	}
	var tag string
	if err := json.Unmarshal(raw, &tag); err != nil {
		return 0, errInvalid("bad block tag: %v", err)
	}
	switch tag {
	case "latest", "pending", "":
		return head, nil
	case "safe", "finalized":
		if s.live != nil {
			return min(s.live.SettledHeight(), head), nil
		}
		return head, nil
	case "earliest":
		return 0, nil
	}
	n, err := hexutil.DecodeUint64(tag)
	if err != nil {
		return 0, errInvalid("bad block number %q: %v", tag, err)
	}
	if n > s.acceptedHead() {
		return 0, errInvalid("block %d beyond head %d", n, head)
	}
	return n, nil
}

// acceptedHead is the highest height any tail source can answer for: the
// follower's accepted height when following, the serving head otherwise.
func (s *Server) acceptedHead() uint64 {
	head := s.hist.Head()
	if s.live == nil {
		return head
	}
	return max(head, s.live.AcceptedHead())
}

// stateAt opens the state at height n. Three bands when following:
//
//	n > executed        not executed here yet (staged, or a backfill hole): error.
//	n in [head, exec]   the executed head: the uncooked-tail overlay over the
//	                    descent, so latest/pending answer at chain pace with no
//	                    cook wait and no Firewood. The band is one advance tick
//	                    wide, which is the same head race every node has.
//	n < head            the historical descent, which refuses heights the cook
//	                    has not reached rather than answering with a pre-gap value.
func (s *Server) stateAt(n uint64) (*ethstate.StateDB, *rpcError) {
	if s.live != nil {
		executed := s.live.LiveHead()
		switch {
		case n > executed:
			return nil, &rpcError{Code: -32000, Message: fmt.Sprintf(
				"state at block %d is not executed yet (executed head %d)", n, executed)}
		case n >= s.hist.Head():
			st, err := ethstate.New(common.Hash{}, s.hist.StateLatest(), nil)
			if err != nil {
				return nil, &rpcError{Code: -32000, Message: err.Error()}
			}
			return st, nil
		}
	}
	st, err := ethstate.New(common.Hash{}, s.hist.StateAt(n), nil)
	if err != nil {
		return nil, &rpcError{Code: -32000, Message: err.Error()}
	}
	return st, nil
}

func (s *Server) accountField(params []json.RawMessage, f func(*ethstate.StateDB, common.Address) any) (any, *rpcError) {
	if len(params) < 1 {
		return nil, errInvalid("need [address, blockTag]")
	}
	var addr common.Address
	if err := json.Unmarshal(params[0], &addr); err != nil {
		return nil, errInvalid("bad address: %v", err)
	}
	var tag json.RawMessage
	if len(params) > 1 {
		tag = params[1]
	}
	n, rerr := s.blockNumber(tag)
	if rerr != nil {
		return nil, rerr
	}
	st, rerr := s.stateAt(n)
	if rerr != nil {
		return nil, rerr
	}
	res := f(st, addr)
	if err := st.Error(); err != nil {
		return nil, &rpcError{Code: -32000, Message: err.Error()}
	}
	return res, nil
}

func (s *Server) getStorageAt(params []json.RawMessage) (any, *rpcError) {
	if len(params) < 2 {
		return nil, errInvalid("need [address, slot, blockTag]")
	}
	var addr common.Address
	if err := json.Unmarshal(params[0], &addr); err != nil {
		return nil, errInvalid("bad address: %v", err)
	}
	var slotHex string
	if err := json.Unmarshal(params[1], &slotHex); err != nil {
		return nil, errInvalid("bad slot: %v", err)
	}
	slot := common.HexToHash(slotHex)
	var tag json.RawMessage
	if len(params) > 2 {
		tag = params[2]
	}
	n, rerr := s.blockNumber(tag)
	if rerr != nil {
		return nil, rerr
	}
	st, rerr := s.stateAt(n)
	if rerr != nil {
		return nil, rerr
	}
	val := st.GetState(addr, slot)
	if err := st.Error(); err != nil {
		return nil, &rpcError{Code: -32000, Message: err.Error()}
	}
	return val.Hex(), nil
}

type callArgs struct {
	From     *common.Address `json:"from"`
	To       *common.Address `json:"to"`
	Gas      *hexutil.Uint64 `json:"gas"`
	GasPrice *hexutil.Big    `json:"gasPrice"`
	Value    *hexutil.Big    `json:"value"`
	Data     *hexutil.Bytes  `json:"data"`
	Input    *hexutil.Bytes  `json:"input"`
}

func (s *Server) ethCall(params []json.RawMessage) (any, *rpcError) {
	if len(params) < 1 {
		return nil, errInvalid("need [callArgs, blockTag]")
	}
	var args callArgs
	if err := json.Unmarshal(params[0], &args); err != nil {
		return nil, errInvalid("bad call args: %v", err)
	}
	var tag json.RawMessage
	if len(params) > 1 {
		tag = params[1]
	}
	n, rerr := s.blockNumber(tag)
	if rerr != nil {
		return nil, rerr
	}
	if n == 0 {
		return nil, errInvalid("eth_call needs an executed block (>=1)")
	}
	// geth's signature is [args, blockNrOrHash, stateOverride, blockOverrides].
	ov, rerr := parseOverrides(params, 2, 3)
	if rerr != nil {
		return nil, rerr
	}
	raw, ok, err := s.hist.HeaderRLP(n)
	if err != nil || !ok {
		return nil, &rpcError{Code: -32000, Message: fmt.Sprintf("header %d unavailable: ok=%v err=%v", n, ok, err)}
	}
	var header types.Header
	if err := rlp.DecodeBytes(raw, &header); err != nil {
		return nil, &rpcError{Code: -32000, Message: err.Error()}
	}

	st, rerr := s.stateAt(n)
	if rerr != nil {
		return nil, rerr
	}
	if err := ov.stateDiff().apply(st); err != nil {
		return nil, errInvalid("%v", err)
	}

	gas := uint64(GasCap)
	if args.Gas != nil && uint64(*args.Gas) < gas {
		gas = uint64(*args.Gas)
	}
	msg := &callMsg{
		To:        args.To,
		Value:     new(big.Int),
		GasLimit:  gas,
		GasPrice:  new(big.Int),
		GasFeeCap: new(big.Int),
		GasTipCap: new(big.Int),
	}
	if args.From != nil {
		msg.From = *args.From
	}
	if args.Value != nil {
		msg.Value = (*big.Int)(args.Value)
	}
	if args.GasPrice != nil {
		msg.GasPrice = (*big.Int)(args.GasPrice)
	}
	if args.Input != nil {
		msg.Data = *args.Input
	} else if args.Data != nil {
		msg.Data = *args.Data
	}

	backend := registeredVM()
	blockCtx := backend.blockContext(&header, s.chainCtx)
	ov.blockDiff().apply(&blockCtx)
	res, err := backend.applyMsg(s.chainCfg, blockCtx, st, msg, vm.Config{NoBaseFee: true})
	if err != nil {
		return nil, &rpcError{Code: -32000, Message: err.Error()}
	}
	if err := st.Error(); err != nil {
		return nil, &rpcError{Code: -32000, Message: err.Error()}
	}
	if res.Err != nil {
		return nil, revertError(res)
	}
	return hexutil.Encode(res.ReturnData), nil
}

// revertError is the geth-shaped failure for a call that executed and failed:
// code 3 with the revert payload when there is one, the raw EVM error
// otherwise.
func revertError(res *callResult) *rpcError {
	out := &rpcError{Code: 3, Message: "execution reverted"}
	if len(res.Revert) > 0 {
		out.Data = hexutil.Encode(res.Revert)
	} else {
		out.Message = res.Err.Error()
	}
	return out
}
