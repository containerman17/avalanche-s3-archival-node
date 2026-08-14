package rpc

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/big"

	"github.com/ava-labs/libevm/common"
	ethstate "github.com/ava-labs/libevm/core/state"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/eth/tracers"

	"github.com/containerman17/epochdb/store"
)

// THE CORE QUERY LAYER in Go (DESIGN "Entry points and adapters"): the reads
// every wire format is a thin PEER adapter over. Every one of these is the SAME
// call the JSON-RPC handler makes, minus the JSON, so there is no second read
// path, no HTTP hop for a consumer in the same process, and no adapter stacked
// on another.

func (e *rpcError) error() error {
	if e == nil {
		return nil
	}
	return errors.New(e.Message)
}

// StateAt opens the state at height n with the three serving bands of
// stateAt: above the executed head is an error, at or above the stored head is
// the latest view (re-read per access), below it is the fixed-height view. The
// returned StateDB is single-use and NOT goroutine-safe.
func (s *Server) StateAt(n uint64) (*ethstate.StateDB, error) {
	st, rerr := s.stateAt(n)
	return st, rerr.error()
}

// BlockAt returns the parsed block at height n.
func (s *Server) BlockAt(n uint64) (*types.Block, error) {
	blk, rerr := s.blockAt(n)
	return blk, rerr.error()
}

// HeightByHash resolves a block hash through the blkh/ lookup rows. found=false
// is a clean "not on this chain"; a read failure is the error.
func (s *Server) HeightByHash(h common.Hash) (uint64, bool, error) {
	n, ok, rerr := s.heightByHash(h)
	return n, ok, rerr.error()
}

// FindTx resolves a tx hash to its block and index. found=false is a clean
// "unknown tx".
func (s *Server) FindTx(hash common.Hash) (blk *types.Block, txIndex int, found bool, err error) {
	return s.findTx(hash)
}

// BlockReceipts reconstructs every receipt of blk from its stored per-tx
// receipt rows (no re-execution).
func (s *Server) BlockReceipts(blk *types.Block) (types.Receipts, error) {
	rs, rerr := s.storedBlockReceipts(blk)
	return rs, rerr.error()
}

// GetLogs runs a log query over [from, to] through the posting lists and the
// stored log sections. Range bounds are the caller's to keep sane; the JSON
// path caps them at GetLogsMaxRange.
func (s *Server) GetLogs(from, to uint64, addrs []common.Address, topics [][]common.Hash) ([]*types.Log, error) {
	logs, rerr := s.runGetLogs(from, to, addrs, topics)
	return logs, rerr.error()
}

// Head is the serving frontier, the three labels at once.
type Head struct {
	Number    uint64 // last block this node serves: the `latest` label
	Hash      common.Hash
	Timestamp uint64
	Accepted  uint64 // the follower's accepted head: the `pending` label
	Settled   uint64 // SAE-settled: `safe`/`finalized`. == Number below Helicon
}

// Head reads it. An empty store answers Number 0 with a zero hash.
func (s *Server) Head() (Head, error) {
	n := s.head()
	h := Head{Number: n, Accepted: s.acceptedHead(), Settled: n}
	if s.live != nil {
		h.Settled = min(s.live.SettledHeight(), n)
	}
	header, rerr := s.headerAt(n)
	if rerr != nil {
		return Head{}, rerr.error()
	}
	h.Hash, h.Timestamp = header.Hash(), header.Time
	return h, nil
}

// CallMsg is the message an in-process call executes: the VM-neutral
// core.Message the two backends share, with no JSON shape anywhere near it.
type CallMsg = callMsg

// Call runs msg at height n and returns its return data. THE FAST PATH: this
// is eth_call with the JSON removed on both sides, and it is the same
// s.call the JSON handler runs. A reverted call is an error whose return data
// is still handed back.
func (s *Server) Call(msg *CallMsg, n uint64) ([]byte, error) {
	res, rerr := s.call(msg, n, nil, nil)
	if rerr != nil {
		return nil, rerr.error()
	}
	if res.Err != nil {
		return res.ReturnData, revertError(res).error()
	}
	return res.ReturnData, nil
}

// Frames returns a transaction's STORED call frames (DESIGN: traces are
// stored, so this is a read and never a re-execution). found=false is a clean
// "unknown transaction"; a known transaction with no nested call returns no
// frames and found=true.
func (s *Server) Frames(hash common.Hash) (frames []store.Frame, found bool, err error) {
	num, ok, err := s.db.TxNumByHash(hash[:])
	if err != nil || !ok {
		return nil, false, err
	}
	rec, ok, err := s.db.Frames(num)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, errors.New("transaction " + hash.Hex() + " has no stored call frames: this node never executed it")
	}
	frames, err = store.DecodeFrames(rec)
	return frames, err == nil, err
}

// AddrHit is one `addr/` posting row: a transaction the address took part in,
// and the roles it played (store.Role* bits).
type AddrHit struct {
	TxNum  uint64
	Height uint64
	Hash   common.Hash
	Roles  byte
}

// SearchByAddress walks an address's transaction history, KEYSET-paged over
// the posting rows exactly as the Otterscan methods are: cursor is the TxNum to
// continue from (inclusive), 0 meaning "from the end this walk starts at", and
// more reports whether a further page exists, read one row past the page so it
// is exact rather than guessed.
func (s *Server) SearchByAddress(addr common.Address, cursor uint64, limit int, desc bool) (hits []AddrHit, more bool, err error) {
	head, ok := s.otsHeadTx()
	if !ok || limit <= 0 {
		return nil, false, nil
	}
	lo, hi := uint64(0), head
	if desc {
		if cursor > 0 && cursor < hi {
			hi = cursor
		}
	} else {
		lo = cursor
	}
	nums, more, rerr := s.otsWalk(addr, lo, hi, limit, desc)
	if rerr != nil {
		return nil, false, rerr.error()
	}
	// The role bits are the posting VALUE, so they come from a second pass over
	// the same rows rather than from otsWalk, whose shape the ots_ methods own.
	roles := make(map[uint64]byte, len(nums))
	if len(nums) > 0 {
		first, last := nums[0], nums[len(nums)-1]
		if first > last {
			first, last = last, first
		}
		if err := s.db.Postings(store.AddrPrefix(addr[:]), first, last, func(n uint64, v byte) bool {
			roles[n] = v
			return true
		}); err != nil {
			return nil, false, err
		}
	}
	for _, num := range nums {
		height, ok, err := s.db.HeightOfTx(num)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, false, errors.New("TxNum is in no block")
		}
		tx, rerr := s.txAt(num)
		if rerr != nil {
			return nil, false, rerr.error()
		}
		hits = append(hits, AddrHit{TxNum: num, Height: height, Hash: tx.Hash(), Roles: roles[num]})
	}
	return hits, more, nil
}

// HeaderAt returns the header at height n, decoded from its stored bytes.
func (s *Server) HeaderAt(n uint64) (*types.Header, error) {
	h, rerr := s.headerAt(n)
	return h, rerr.error()
}

// HeadPollInterval is how often an adapter should look for a new head. The
// WebSocket subscriptions poll on it too, so every push surface here shares
// one number (see wsPollInterval for why polling).
const HeadPollInterval = wsPollInterval

// --- execution: the call shapes beyond a plain Call ---------------------------

// CallResult is the VM-neutral execution result: the shape eth_callDetailed
// reports and the one a caller wants when a REVERT is an answer rather than a
// failure (res.Err set, ReturnData and Revert still filled in).
type CallResult = callResult

// CallDetailed runs msg at height n and hands back the whole result, so a
// revert is data instead of an error. The transport half of a failure (an
// unreadable block, a height this node cannot serve) is still an error.
func (s *Server) CallDetailed(msg *CallMsg, n uint64) (*CallResult, error) {
	res, rerr := s.call(msg, n, nil, nil)
	if rerr != nil {
		return nil, rerr.error()
	}
	return res, nil
}

// EstimateGas binary-searches the smallest executable gas limit for msg at
// height n. msg.GasLimit is the CEILING (0 takes the server's cap).
func (s *Server) EstimateGas(msg *CallMsg, n uint64) (uint64, error) {
	m := *msg
	if m.GasLimit == 0 || m.GasLimit > GasCap {
		m.GasLimit = GasCap
	}
	gas, rerr := s.estimateGasAt(&m, n, nil)
	return gas, rerr.error()
}

// AccessList runs eth_createAccessList's fixed-point loop for msg at height n:
// the tuples, the gas the converged run used, and the EVM's own error string
// where the call itself failed (which is a result, not a transport failure).
// prev seeds the loop with a caller-supplied list; nonce, when set, is the
// creation nonce the deployed address is derived from.
func (s *Server) AccessList(msg *CallMsg, n uint64, prev types.AccessList, nonce *uint64) (types.AccessList, uint64, string, error) {
	al, gas, execErr, rerr := s.accessListAt(msg, n, prev, nonce)
	return al, gas, execErr, rerr.error()
}

// --- tracing ------------------------------------------------------------------

// A TRACER'S CONFIG AND ITS RESULT ARE JSON EVERYWHERE, including here: geth's
// tracer directory takes a JSON config and every tracer marshals its own
// result, so a byte-for-byte passthrough is the only shape that does not
// re-encode a tracer's output into something it did not say. tracer is the
// name ("callTracer", "prestateTracer", ...); empty means the struct logger.

// TraceTransaction re-executes one transaction under a tracer. Unlike Frames
// (the STORED call frames), this is a real re-execution, which is what a
// caller asking for a specific tracer is asking for.
func (s *Server) TraceTransaction(hash common.Hash, tracer string, tracerCfg json.RawMessage) (json.RawMessage, error) {
	blk, i, found, err := s.findTx(hash)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("transaction %s is not on this chain", hash)
	}
	res, rerr := s.traceTxsInBlock(blk, i, traceCfgOf(tracer, tracerCfg))
	if rerr != nil {
		return nil, rerr.error()
	}
	return res[len(res)-1], nil
}

// TraceBlock traces every transaction of block n, in block order.
func (s *Server) TraceBlock(n uint64, tracer string, tracerCfg json.RawMessage) ([]json.RawMessage, error) {
	blk, rerr := s.blockAt(n)
	if rerr != nil {
		return nil, rerr.error()
	}
	res, rerr := s.traceTxsInBlock(blk, -1, traceCfgOf(tracer, tracerCfg))
	return res, rerr.error()
}

// TraceCall runs msg at height n under a tracer (debug_traceCall).
func (s *Server) TraceCall(msg *CallMsg, n uint64, tracer string, tracerCfg json.RawMessage) (json.RawMessage, error) {
	t, err := newTracer(traceCfgOf(tracer, tracerCfg), &tracers.Context{BlockNumber: new(big.Int).SetUint64(n)})
	if err != nil {
		return nil, err
	}
	if _, rerr := s.call(msg, n, t, nil); rerr != nil {
		return nil, rerr.error()
	}
	return t.GetResult()
}

func traceCfgOf(tracer string, cfg json.RawMessage) *traceConfig {
	out := &traceConfig{TracerConfig: cfg}
	if tracer != "" {
		out.Tracer = &tracer
	}
	return out
}

// --- fees ---------------------------------------------------------------------

// FeeHistory is eth_feeHistory in numbers: BaseFee holds one entry per block
// PLUS a trailing entry for the block after the range (read below the head,
// projected at it), and Reward is one row per block when percentiles were
// asked for.
type FeeHistory struct {
	OldestBlock  uint64
	BaseFee      []*big.Int
	GasUsedRatio []float64
	Reward       [][]*big.Int
}

// FeeHistory answers over the count blocks ending at newest.
func (s *Server) FeeHistory(count, newest uint64, percentiles []float64) (*FeeHistory, error) {
	fh, rerr := s.feeHistoryAt(count, newest, percentiles)
	return fh, rerr.error()
}

// PriceOption is one of coreth's three suggested speeds.
type PriceOption struct{ MaxPriorityFee, MaxFee *big.Int }

// GasPrices is the whole gas-price surface out of one sample: eth_gasPrice,
// eth_maxPriorityFeePerGas, eth_baseFee and eth_suggestPriceOptions. BaseFee
// and NextBaseFee are nil on a chain with no base fee at that height, which is
// an answer; the three options are nil with them.
type GasPrices struct {
	GasPrice       *big.Int
	MaxPriorityFee *big.Int
	BaseFee        *big.Int
	NextBaseFee    *big.Int
	Slow           *PriceOption
	Normal         *PriceOption
	Fast           *PriceOption
}

// GasPrices reads it.
func (s *Server) GasPrices() (*GasPrices, error) {
	p, rerr := s.gasPrices()
	return p, rerr.error()
}

// --- raw stored forms ----------------------------------------------------------

// RawBlock is the VERBATIM consensus container at height n, reassembled from
// the stored header, proposervm extras and transaction rows.
func (s *Server) RawBlock(n uint64) ([]byte, error) {
	raw, rerr := s.rawBlock(n)
	return raw, rerr.error()
}

// --- Otterscan reads -----------------------------------------------------------

// ContractCreator names the transaction that deployed addr and the account
// that sent it, covering a top-level creation and a factory CREATE frame
// alike. found=false is a clean "no contract at this address".
func (s *Server) ContractCreator(addr common.Address) (txHash common.Hash, creator common.Address, found bool, err error) {
	out, rerr := s.contractCreator(addr)
	if rerr != nil {
		return common.Hash{}, common.Address{}, false, rerr.error()
	}
	if out == nil {
		return common.Hash{}, common.Address{}, false, nil
	}
	return out.Hash, out.Creator, true, nil
}

// TxBySenderAndNonce resolves "which transaction did this account send with
// this nonce". found=false is a clean "never sent".
func (s *Server) TxBySenderAndNonce(sender common.Address, nonce uint64) (common.Hash, bool, error) {
	hash, found, rerr := s.txBySenderAndNonce(sender, nonce)
	return hash, found, rerr.error()
}

// --- node identity, and what this node will never answer ------------------------

// NodeInfo is what a client needs to know about the node itself before it asks
// anything: which chain, which build, whether it is still catching up, and the
// list of capabilities it refuses BY DESIGN.
type NodeInfo struct {
	ChainID       *big.Int
	ClientVersion string
	Syncing       bool
	CurrentBlock  uint64 // the executed head
	HighestBlock  uint64 // the height this node is syncing toward
	ChainConfig   json.RawMessage
	OtsAPILevel   uint32
	Refusals      []Refusal
}

// Refusal is one permanently refused capability and the reason it is refused.
// A REFUSAL IS NEVER AN EMPTY ANSWER (DESIGN: "null means not on this chain,
// never I could not read it"), so every adapter publishes this list and every
// adapter errors when one of these is asked for.
type Refusal struct {
	Capability string
	Reason     string
}

// Refusals is the permanent refusal set, shared by every adapter. It is
// PERMANENT: none of these is a phase gap, each is a consequence of what
// storage v0 keeps and of this node being a read-only archive.
func Refusals() []Refusal {
	return []Refusal{
		{"eth_getProof", "epochdb stores no tries, so there is no merkle proof to build"},
		{"debug_dumpBlock, debug_accountRange, debug_storageRangeAt, debug_intermediateRoots",
			"epochdb stores no tries: there is no trie to walk and no per-tx root to report"},
		{"debug_preimage", "the state key space is already hashed, so there is no preimage table"},
		{"debug_getBadBlocks, debug_traceBadBlock",
			"replay is root-verified and halts on a bad block, so none is ever retained"},
		{"debug_traceChain", "trace a range one block at a time (Trace with a block target)"},
		{"debug_getModifiedAccountsByNumber, debug_getModifiedAccountsByHash",
			"storage v0 keys state by account, not by block, so there is no touched-account index"},
		{"eth_sendRawTransaction, eth_sendTransaction, eth_fillTransaction, eth_resend",
			"read-only archive node: it has no mempool and gossips nothing"},
		{"eth_sign, eth_signTransaction", "no keystore and no keys"},
		{"txpool_*, eth_pendingTransactions, pending-transaction subscriptions",
			"no mempool exists, so a pending transaction is not a thing this node can see"},
		{"personal_*, miner_*, admin_*, les_*, clique_*, ethash_*",
			"not archivable: these manage a validating node, and this node validates nothing"},
	}
}

// NodeInfo reads it.
func (s *Server) NodeInfo() (*NodeInfo, error) {
	cfg, err := json.Marshal(s.chainCfg)
	if err != nil {
		return nil, err
	}
	out := &NodeInfo{
		ChainID:       s.chainCfg.ChainID,
		ClientVersion: ClientVersion,
		CurrentBlock:  s.head(),
		HighestBlock:  s.head(),
		ChainConfig:   cfg,
		OtsAPILevel:   OtsAPILevel,
		Refusals:      Refusals(),
	}
	// The same rule eth_syncing follows: a node whose execution trails the
	// height it is syncing toward says so, and one that is caught up (or is a
	// fixed complete corpus) does not.
	if s.live != nil {
		out.CurrentBlock, out.HighestBlock = s.live.LiveHead(), s.live.SyncTarget()
		out.Syncing = out.HighestBlock > out.CurrentBlock+1
	}
	return out, nil
}
