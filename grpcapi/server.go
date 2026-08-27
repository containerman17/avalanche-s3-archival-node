// Package grpcapi is the gRPC adapter, THE PRIMARY REMOTE API (user ruling
// 2026-08-12: "gRPC should be the best way to access it").
//
// It is a THIN PEER over the core query layer: every method here is one call
// into rpc.Server's direct methods or the store's own readers, plus protobuf.
// Nothing routes through JSON-RPC. For a same-box separate process it costs
// 20-50us CPU per unary call, which is why the library exists (DESIGN "Entry
// points and adapters"); gRPC is for everything slower than in-process.
package grpcapi

import (
	"context"
	"fmt"
	"math/big"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"

	"github.com/containerman17/epochdb"
	"github.com/containerman17/epochdb/rpc"
)

// THE TWO FAILURE CODES, and they are not interchangeable (DESIGN: "null means
// not on this chain, never I could not read it"). gone is NotFound: the chain
// does not have it. readErr is Internal: this node could not read it, and a
// caller must never mistake that for an empty answer.
func gone(format string, a ...any) error { return status.Errorf(codes.NotFound, format, a...) }
func readErr(err error) error            { return status.Error(codes.Internal, err.Error()) }

// bad is a caller error: the request itself does not name anything.
func bad(format string, a ...any) error { return status.Errorf(codes.InvalidArgument, format, a...) }

// Server answers the EpochDB service out of one node.
type Server struct {
	UnimplementedEpochDBServer
	n *epochdb.Node
}

// New wraps a node. Serve is the usual entry point; this exists for a caller
// that runs its own grpc.Server (its own interceptors, its own listener).
func New(n *epochdb.Node) *Server { return &Server{n: n} }

// Serve starts the gRPC listener on port and returns the running server, which
// the caller stops. Errors from the listener bind are returned rather than
// logged: a taken port must fail the start, like every other listener here.
func Serve(port int, n *epochdb.Node) (*grpc.Server, error) {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, err
	}
	g := grpc.NewServer()
	RegisterEpochDBServer(g, New(n))
	go g.Serve(ln)
	return g, nil
}

// height resolves a request height: 0 means the serving head.
func (s *Server) height(h uint64) (uint64, error) {
	if h > 0 {
		return h, nil
	}
	head, err := s.n.Head()
	return head.Number, err
}

func headPB(h rpc.Head) *HeadResponse {
	return &HeadResponse{
		Number: h.Number, Hash: h.Hash[:], Timestamp: h.Timestamp,
		Accepted: h.Accepted, Settled: h.Settled,
	}
}

func (s *Server) GetHead(context.Context, *HeadRequest) (*HeadResponse, error) {
	h, err := s.n.Head()
	if err != nil {
		return nil, readErr(err)
	}
	return headPB(h), nil
}

// StreamHeads polls the serving head, exactly as the WebSocket subscriptions
// do and off the same interval: one read per tick, and the latency it adds is
// well under one Avalanche block time.
func (s *Server) StreamHeads(_ *HeadRequest, out grpc.ServerStreamingServer[HeadResponse]) error {
	// The FIRST message goes out whatever the head is, so a subscriber knows
	// where the stream starts; height 0 is a real head on a fresh chain.
	var last uint64
	first := true
	for {
		h, err := s.n.Head()
		if err != nil {
			return readErr(err)
		}
		if first || h.Number != last {
			if err := out.Send(headPB(h)); err != nil {
				return err
			}
			last, first = h.Number, false
		}
		select {
		case <-out.Context().Done():
			return out.Context().Err()
		case <-time.After(rpc.HeadPollInterval):
		}
	}
}

// blockHeight resolves the BlockRequest addressing rule, which every method
// that names a block shares: a hash wins, a number stands, and 0 with no hash
// is the serving head.
func (s *Server) blockHeight(number uint64, hash []byte) (uint64, error) {
	if len(hash) > 0 {
		h, ok, err := s.n.Core().HeightByHash(common.BytesToHash(hash))
		if err != nil {
			return 0, readErr(err)
		}
		if !ok {
			return 0, gone("block %#x is not on this chain", hash)
		}
		return h, nil
	}
	if number > 0 {
		return number, nil
	}
	n, err := s.height(0)
	if err != nil {
		return 0, readErr(err)
	}
	return n, nil
}

func (s *Server) GetBlock(_ context.Context, req *BlockRequest) (*BlockResponse, error) {
	core := s.n.Core()
	n, err := s.blockHeight(req.Number, req.Hash)
	if err != nil {
		return nil, err
	}
	// The header goes out as the STORED bytes: nothing re-serializes what the
	// block hash is computed over.
	raw, ok, err := s.n.Store().HeaderRLP(n)
	if err != nil {
		return nil, readErr(err)
	}
	if !ok {
		return nil, gone("block %d is not on this chain", n)
	}
	hdr, err := core.HeaderAt(n)
	if err != nil {
		return nil, readErr(err)
	}
	out := &BlockResponse{Number: n, Hash: hdr.Hash().Bytes(), HeaderRlp: raw}
	first, count, ok, err := s.n.Store().BlockTxRange(n)
	if err != nil {
		return nil, readErr(err)
	}
	if ok {
		out.TxCount = count
	}
	if req.Full && out.TxCount > 0 {
		for i := uint64(0); i < uint64(count); i++ {
			txRLP, ok, err := s.n.Store().TxRLP(first + i)
			if err != nil {
				return nil, readErr(err)
			}
			if !ok {
				return nil, readErr(fmt.Errorf("tx/%d of block %d is not stored", first+i, n))
			}
			out.TxRlp = append(out.TxRlp, txRLP)
		}
	}
	if req.Raw {
		if out.BlockRlp, err = core.RawBlock(n); err != nil {
			return nil, readErr(err)
		}
	}
	if !req.TxHashes && !req.Receipts {
		return out, nil
	}
	// Hashes and receipts both want the parsed block; the receipts come from
	// the STORED rows, never from a re-execution.
	blk, err := core.BlockAt(n)
	if err != nil {
		return nil, readErr(err)
	}
	if req.TxHashes {
		for _, tx := range blk.Transactions() {
			out.TxHash = append(out.TxHash, tx.Hash().Bytes())
		}
	}
	if req.Receipts && out.TxCount > 0 {
		receipts, err := core.BlockReceipts(blk)
		if err != nil {
			return nil, readErr(err)
		}
		total := new(big.Int)
		for _, r := range receipts {
			out.Receipts = append(out.Receipts, receiptPB(r))
			rlpBytes, err := r.MarshalBinary()
			if err != nil {
				return nil, readErr(err)
			}
			out.ReceiptRlp = append(out.ReceiptRlp, rlpBytes)
			total.Add(total, new(big.Int).Mul(r.EffectiveGasPrice, new(big.Int).SetUint64(r.GasUsed)))
		}
		out.TotalFees = total.Bytes()
	}
	return out, nil
}

// txHash resolves the three ways a transaction is named: by hash, by sender
// and nonce, or by its position in a block. found=false is a clean "not on
// this chain" and NEVER an error.
func (s *Server) txHash(req *TransactionRequest) (common.Hash, bool, error) {
	core := s.n.Core()
	switch {
	case len(req.Hash) > 0:
		return common.BytesToHash(req.Hash), true, nil
	case len(req.Sender) > 0 && req.Nonce != nil:
		h, found, err := core.TxBySenderAndNonce(common.BytesToAddress(req.Sender), *req.Nonce)
		if err != nil {
			return common.Hash{}, false, readErr(err)
		}
		return h, found, nil
	case req.Index != nil:
		n, err := s.blockHeight(req.BlockNumber, req.BlockHash)
		if err != nil {
			return common.Hash{}, false, err
		}
		blk, err := core.BlockAt(n)
		if err != nil {
			return common.Hash{}, false, readErr(err)
		}
		txs := blk.Transactions()
		if int(*req.Index) >= len(txs) {
			return common.Hash{}, false, nil // the block has no such position
		}
		return txs[*req.Index].Hash(), true, nil
	}
	return common.Hash{}, false, bad("name a transaction by hash, by sender and nonce, or by block and index")
}

func (s *Server) GetTransaction(_ context.Context, req *TransactionRequest) (*TransactionResponse, error) {
	hash, ok, err := s.txHash(req)
	if err != nil {
		return nil, err
	}
	if !ok {
		return &TransactionResponse{}, nil
	}
	blk, idx, found, err := s.n.Core().FindTx(hash)
	if err != nil {
		return nil, readErr(err)
	}
	if !found {
		return &TransactionResponse{}, nil
	}
	tx := blk.Transactions()[idx]
	raw, err := tx.MarshalBinary()
	if err != nil {
		return nil, readErr(err)
	}
	out := &TransactionResponse{
		Found: true, TxRlp: raw, BlockNumber: blk.NumberU64(),
		BlockHash: blk.Hash().Bytes(), Index: uint32(idx), Hash: hash.Bytes(),
	}
	receipts, err := s.n.Core().BlockReceipts(blk)
	if err != nil {
		return nil, readErr(err)
	}
	if idx < len(receipts) {
		out.Receipt = receiptPB(receipts[idx])
	}
	frames, _, err := s.n.Core().Frames(hash)
	if err != nil {
		return nil, readErr(err)
	}
	for _, f := range frames {
		out.Frames = append(out.Frames, &Frame{
			Kind: uint32(f.Kind), Depth: uint32(f.Depth),
			From: f.From.Bytes(), To: f.To.Bytes(), Value: bigBytes(f.Value),
			Gas: f.Gas, GasUsed: f.GasUsed, Failed: f.Failed,
			Input: f.Input, Output: f.Output,
		})
	}
	return out, nil
}

func receiptPB(r *types.Receipt) *Receipt {
	out := &Receipt{
		Status: r.Status, GasUsed: r.GasUsed, CumulativeGasUsed: r.CumulativeGasUsed,
		EffectiveGasPrice: bigBytes(r.EffectiveGasPrice), Type: uint32(r.Type),
		TxHash: r.TxHash.Bytes(),
	}
	if r.ContractAddress != (common.Address{}) {
		out.ContractAddress = r.ContractAddress.Bytes()
	}
	for _, l := range r.Logs {
		out.Logs = append(out.Logs, logPB(l))
	}
	return out
}

func logPB(l *types.Log) *Log {
	out := &Log{
		Address: l.Address.Bytes(), Data: l.Data, BlockNumber: l.BlockNumber,
		TxHash: l.TxHash.Bytes(), TxIndex: uint32(l.TxIndex), Index: uint32(l.Index),
	}
	for _, t := range l.Topics {
		out.Topics = append(out.Topics, t.Bytes())
	}
	return out
}

// bigBytes is a big-endian big.Int on the wire; nil and zero are both empty.
func bigBytes(v *big.Int) []byte {
	if v == nil {
		return nil
	}
	return v.Bytes()
}

// callMsg builds the core message a CallRequest describes, with the server's
// gas cap applied. It is shared by Call, EstimateGas and a call-target Trace,
// so those three cannot drift apart in what they execute.
func callMsg(req *CallRequest) *rpc.CallMsg {
	msg := &rpc.CallMsg{
		From:      common.BytesToAddress(req.From),
		Value:     new(big.Int).SetBytes(req.Value),
		GasLimit:  req.Gas,
		GasPrice:  new(big.Int).SetBytes(req.GasPrice),
		GasFeeCap: new(big.Int),
		GasTipCap: new(big.Int),
		Data:      req.Data,
	}
	if msg.GasLimit == 0 || msg.GasLimit > rpc.GasCap {
		msg.GasLimit = rpc.GasCap
	}
	if len(req.To) > 0 {
		to := common.BytesToAddress(req.To)
		msg.To = &to
	}
	return msg
}

func (s *Server) Call(_ context.Context, req *CallRequest) (*CallResponse, error) {
	n, err := s.height(req.Height)
	if err != nil {
		return nil, readErr(err)
	}
	msg := callMsg(req)
	if req.AccessList {
		al, gas, execErr, err := s.n.Core().AccessList(msg, n, nil, req.Nonce)
		if err != nil {
			return nil, readErr(err)
		}
		out := &CallResponse{GasUsed: gas, Error: execErr}
		for _, t := range al {
			tuple := &AccessTuple{Address: t.Address.Bytes()}
			for _, k := range t.StorageKeys {
				tuple.StorageKeys = append(tuple.StorageKeys, k.Bytes())
			}
			out.AccessList = append(out.AccessList, tuple)
		}
		return out, nil
	}
	if req.Detailed {
		// A REVERT IS THE ANSWER HERE, not a failure: gas and return data are
		// what the caller asked for (eth_callDetailed's contract).
		res, err := s.n.Core().CallDetailed(msg, n)
		if err != nil {
			return nil, readErr(err)
		}
		out := &CallResponse{Output: res.ReturnData, GasUsed: res.UsedGas, Revert: res.Revert}
		if res.Err != nil {
			out.Error = res.Err.Error()
		}
		return out, nil
	}
	// The plain form runs the SAME core call the library's fast path runs, so
	// a revert is the same error here as everywhere else.
	out, err := s.n.CallAt(msg, n)
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return &CallResponse{Output: out}, nil
}

func (s *Server) EstimateGas(_ context.Context, req *CallRequest) (*EstimateGasResponse, error) {
	n, err := s.height(req.Height)
	if err != nil {
		return nil, readErr(err)
	}
	gas, err := s.n.Core().EstimateGas(callMsg(req), n)
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return &EstimateGasResponse{Gas: gas}, nil
}

// Trace is the whole trace surface: a transaction, a block, or a call, under
// any tracer in geth's directory. The result is the tracer's OWN JSON,
// verbatim, because re-encoding a tracer's output is how a trace stops being
// the trace that tracer produced.
func (s *Server) Trace(_ context.Context, req *TraceRequest) (*TraceResponse, error) {
	core := s.n.Core()
	switch t := req.Target.(type) {
	case *TraceRequest_TxHash:
		res, err := core.TraceTransaction(common.BytesToHash(t.TxHash), req.Tracer, req.TracerConfig)
		if err != nil {
			return nil, readErr(err)
		}
		return &TraceResponse{ResultJson: [][]byte{res}, TxHash: [][]byte{t.TxHash}}, nil
	case *TraceRequest_BlockNumber, *TraceRequest_BlockHash:
		var (
			n   uint64
			err error
		)
		if h, ok := req.Target.(*TraceRequest_BlockHash); ok {
			n, err = s.blockHeight(0, h.BlockHash)
		} else {
			n, err = s.blockHeight(req.GetBlockNumber(), nil)
		}
		if err != nil {
			return nil, err
		}
		blk, err := core.BlockAt(n)
		if err != nil {
			return nil, readErr(err)
		}
		res, err := core.TraceBlock(n, req.Tracer, req.TracerConfig)
		if err != nil {
			return nil, readErr(err)
		}
		out := &TraceResponse{}
		for i, r := range res {
			out.ResultJson = append(out.ResultJson, r)
			out.TxHash = append(out.TxHash, blk.Transactions()[i].Hash().Bytes())
		}
		return out, nil
	case *TraceRequest_Call:
		n, err := s.height(t.Call.Height)
		if err != nil {
			return nil, readErr(err)
		}
		res, err := core.TraceCall(callMsg(t.Call), n, req.Tracer, req.TracerConfig)
		if err != nil {
			return nil, readErr(err)
		}
		return &TraceResponse{ResultJson: [][]byte{res}}, nil
	}
	return nil, bad("trace needs a target: tx_hash, block_number, block_hash or call")
}

func (s *Server) GetNodeInfo(_ context.Context, _ *NodeInfoRequest) (*NodeInfoResponse, error) {
	info, err := s.n.Core().NodeInfo()
	if err != nil {
		return nil, readErr(err)
	}
	out := &NodeInfoResponse{
		ChainId: bigBytes(info.ChainID), ClientVersion: info.ClientVersion,
		Syncing: info.Syncing, CurrentBlock: info.CurrentBlock, HighestBlock: info.HighestBlock,
		ChainConfigJson: info.ChainConfig, OtsApiLevel: info.OtsAPILevel,
	}
	for _, r := range info.Refusals {
		out.Refusals = append(out.Refusals, &Refusal{Capability: r.Capability, Reason: r.Reason})
	}
	return out, nil
}

func (s *Server) GetFeeHistory(_ context.Context, req *FeeHistoryRequest) (*FeeHistoryResponse, error) {
	newest, err := s.height(req.NewestBlock)
	if err != nil {
		return nil, readErr(err)
	}
	count := req.BlockCount
	if count == 0 {
		count = 1
	}
	fh, err := s.n.Core().FeeHistory(count, newest, req.RewardPercentiles)
	if err != nil {
		return nil, readErr(err)
	}
	out := &FeeHistoryResponse{OldestBlock: fh.OldestBlock, GasUsedRatio: fh.GasUsedRatio}
	for _, f := range fh.BaseFee {
		out.BaseFeePerGas = append(out.BaseFeePerGas, bigBytes(f))
	}
	for _, row := range fh.Reward {
		r := &RewardRow{}
		for _, v := range row {
			r.Values = append(r.Values, bigBytes(v))
		}
		out.Reward = append(out.Reward, r)
	}
	return out, nil
}

func (s *Server) GetGasPrice(_ context.Context, _ *GasPriceRequest) (*GasPriceResponse, error) {
	p, err := s.n.Core().GasPrices()
	if err != nil {
		return nil, readErr(err)
	}
	opt := func(o *rpc.PriceOption) *PriceOption {
		if o == nil {
			return nil
		}
		return &PriceOption{
			MaxPriorityFeePerGas: bigBytes(o.MaxPriorityFee), MaxFeePerGas: bigBytes(o.MaxFee),
		}
	}
	return &GasPriceResponse{
		GasPrice: bigBytes(p.GasPrice), MaxPriorityFeePerGas: bigBytes(p.MaxPriorityFee),
		BaseFee: bigBytes(p.BaseFee), NextBaseFee: bigBytes(p.NextBaseFee),
		Slow: opt(p.Slow), Normal: opt(p.Normal), Fast: opt(p.Fast),
	}, nil
}

func (s *Server) GetContractCreator(_ context.Context, req *ContractCreatorRequest) (*ContractCreatorResponse, error) {
	hash, creator, found, err := s.n.Core().ContractCreator(common.BytesToAddress(req.Address))
	if err != nil {
		return nil, readErr(err)
	}
	if !found {
		return &ContractCreatorResponse{}, nil
	}
	return &ContractCreatorResponse{Found: true, TxHash: hash.Bytes(), Creator: creator.Bytes()}, nil
}

func (s *Server) GetState(_ context.Context, req *StateRequest) (*StateResponse, error) {
	n, err := s.height(req.Height)
	if err != nil {
		return nil, readErr(err)
	}
	st, err := s.n.StateAt(n)
	if err != nil {
		return nil, readErr(err)
	}
	addr := common.BytesToAddress(req.Address)
	out := &StateResponse{
		Nonce:    st.GetNonce(addr),
		Balance:  bigBytes(st.GetBalance(addr).ToBig()),
		CodeHash: st.GetCodeHash(addr).Bytes(),
	}
	if req.WithCode {
		out.Code = st.GetCode(addr)
	}
	if len(req.Slot) > 0 {
		v := st.GetState(addr, common.BytesToHash(req.Slot))
		out.SlotValue = v.Bytes()
	}
	if err := st.Error(); err != nil {
		return nil, readErr(err)
	}
	return out, nil
}

// searchMaxPage bounds one page, so a caller cannot ask for a whole address
// history in one response. Otterscan's own page sizes are far below it.
const searchMaxPage = 1000

func (s *Server) SearchTransactionsByAddress(_ context.Context, req *SearchRequest) (*SearchResponse, error) {
	limit := int(req.Limit)
	if limit <= 0 || limit > searchMaxPage {
		limit = searchMaxPage
	}
	hits, more, err := s.n.Core().SearchByAddress(
		common.BytesToAddress(req.Address), req.Cursor, limit, !req.Ascending)
	if err != nil {
		return nil, readErr(err)
	}
	out := &SearchResponse{More: more}
	for _, h := range hits {
		out.Hits = append(out.Hits, &AddrHit{
			TxNum: h.TxNum, Height: h.Height, Hash: h.Hash.Bytes(), Roles: uint32(h.Roles),
		})
	}
	return out, nil
}

// --- the posting-list log reads ---------------------------------------------

func pageOf(p *Page) (cursor uint64, limit int, desc bool) {
	if p == nil {
		return 0, 0, true
	}
	return p.Cursor, int(p.Limit), !p.Ascending
}

func optHash(b []byte) *common.Hash {
	if len(b) == 0 {
		return nil
	}
	h := common.BytesToHash(b)
	return &h
}

func pagedPB(p *rpc.PagedLogs, err error) (*PagedLogsResponse, error) {
	if err != nil {
		return nil, readErr(err)
	}
	out := &PagedLogsResponse{More: p.More, NextCursor: p.NextCursor}
	for _, l := range p.Logs {
		out.Logs = append(out.Logs, logPB(l))
	}
	return out, nil
}

func (s *Server) GetLogsByEmitter(_ context.Context, req *LogsByEmitterRequest) (*PagedLogsResponse, error) {
	cursor, limit, desc := pageOf(req.Page)
	return pagedPB(s.n.Core().LogsByEmitter(common.BytesToAddress(req.Emitter), optHash(req.Topic0), cursor, limit, desc))
}

func (s *Server) GetLogsByTopicValue(_ context.Context, req *LogsByTopicValueRequest) (*PagedLogsResponse, error) {
	cursor, limit, desc := pageOf(req.Page)
	return pagedPB(s.n.Core().LogsByTopicValue(common.BytesToHash(req.Value), optHash(req.Topic0), byte(req.Positions), cursor, limit, desc))
}

func (s *Server) GetTopicGroups(_ context.Context, req *TopicGroupsRequest) (*TopicGroupsResponse, error) {
	groups, err := s.n.Core().TopicGroups(common.BytesToHash(req.Value), optHash(req.Topic0))
	if err != nil {
		return nil, readErr(err)
	}
	out := &TopicGroupsResponse{}
	for _, g := range groups {
		out.Groups = append(out.Groups, &TopicGroup{Topic0: g.Topic0.Bytes(), Emitter: g.Emitter.Bytes(), FirstTxNum: g.First, LastTxNum: g.Last})
	}
	return out, nil
}

func (s *Server) GetTokenTransfersByHolder(_ context.Context, req *TokenTransfersRequest) (*PagedLogsResponse, error) {
	cursor, limit, desc := pageOf(req.Page)
	return pagedPB(s.n.Core().TokenTransfersByHolder(common.BytesToAddress(req.Address), req.Standard, cursor, limit, desc))
}

func (s *Server) GetTokenTransfersByContract(_ context.Context, req *TokenTransfersRequest) (*PagedLogsResponse, error) {
	cursor, limit, desc := pageOf(req.Page)
	return pagedPB(s.n.Core().TokenTransfersByContract(common.BytesToAddress(req.Address), req.Standard, cursor, limit, desc))
}

func (s *Server) GetTokenContracts(_ context.Context, req *TokenContractsRequest) (*TokenContractsResponse, error) {
	cs, err := s.n.Core().TokenContracts(common.BytesToAddress(req.Address))
	if err != nil {
		return nil, readErr(err)
	}
	out := &TokenContractsResponse{}
	for _, c := range cs {
		out.Contracts = append(out.Contracts, &TokenContract{Standard: c.Standard, Token: c.Token.Bytes(), FirstTxNum: c.First, LastTxNum: c.Last})
	}
	return out, nil
}

// logMatchers is the request's filter in core terms, shared by the one-shot
// query and the stream so the two cannot match differently.
func logMatchers(req *LogsRequest) ([]common.Address, [][]common.Hash) {
	var addrs []common.Address
	for _, a := range req.Addresses {
		addrs = append(addrs, common.BytesToAddress(a))
	}
	topics := make([][]common.Hash, len(req.Topics))
	for i, set := range req.Topics {
		for _, v := range set.Values {
			topics[i] = append(topics[i], common.BytesToHash(v))
		}
	}
	return addrs, topics
}

func (s *Server) GetLogs(_ context.Context, req *LogsRequest) (*LogsResponse, error) {
	to := req.ToBlock
	if to == 0 {
		var err error
		if to, err = s.height(0); err != nil {
			return nil, readErr(err)
		}
	}
	addrs, topics := logMatchers(req)
	logs, err := s.n.Core().GetLogs(req.FromBlock, to, addrs, topics)
	if err != nil {
		return nil, readErr(err)
	}
	out := &LogsResponse{ToBlock: to}
	for _, l := range logs {
		out.Logs = append(out.Logs, logPB(l))
	}
	return out, nil
}

// StreamLogs REPLACES the filter-id model: nothing is installed, so nothing
// can leak or expire, and cancelling the stream is uninstalling the filter. A
// bounded request (to_block set) ends the stream cleanly at that block; an
// unbounded one follows the head until the caller goes away.
//
// A READ FAILURE ENDS THE STREAM, it never skips a block: a consumer that
// keeps receiving after a gap cannot tell a missing log from an absent one
// (DESIGN's subscription rule, "end the connection rather than skip a promised
// notification").
func (s *Server) StreamLogs(req *LogsRequest, out grpc.ServerStreamingServer[LogsResponse]) error {
	addrs, topics := logMatchers(req)
	from := req.FromBlock
	for {
		head, err := s.n.Head()
		if err != nil {
			return readErr(err)
		}
		to := head.Number
		if req.ToBlock > 0 && to > req.ToBlock {
			to = req.ToBlock
		}
		if to >= from {
			logs, err := s.n.Core().GetLogs(from, to, addrs, topics)
			if err != nil {
				return readErr(err)
			}
			if len(logs) > 0 {
				batch := &LogsResponse{ToBlock: to}
				for _, l := range logs {
					batch.Logs = append(batch.Logs, logPB(l))
				}
				if err := out.Send(batch); err != nil {
					return err
				}
			}
			from = to + 1
		}
		if req.ToBlock > 0 && from > req.ToBlock {
			return nil // the whole range went out: a clean end of stream
		}
		select {
		case <-out.Context().Done():
			return out.Context().Err()
		case <-time.After(rpc.HeadPollInterval):
		}
	}
}

// StreamTransactions pushes one batch per block that carries transactions.
// THERE IS NO MEMPOOL on this node, so these are ACCEPTED transactions: the
// pending-transaction feed a real node has does not exist here and is in the
// refusal list rather than being faked with an empty stream.
func (s *Server) StreamTransactions(req *StreamTransactionsRequest, out grpc.ServerStreamingServer[TransactionBatch]) error {
	next := req.FromBlock
	if next == 0 {
		head, err := s.n.Head()
		if err != nil {
			return readErr(err)
		}
		next = head.Number + 1
	}
	for {
		head, err := s.n.Head()
		if err != nil {
			return readErr(err)
		}
		to := head.Number
		if req.ToBlock > 0 && to > req.ToBlock {
			to = req.ToBlock
		}
		for ; next <= to; next++ {
			blk, err := s.n.Core().BlockAt(next)
			if err != nil {
				return readErr(err)
			}
			txs := blk.Transactions()
			if len(txs) == 0 {
				continue
			}
			batch := &TransactionBatch{BlockNumber: next}
			for _, tx := range txs {
				batch.TxHash = append(batch.TxHash, tx.Hash().Bytes())
				if req.Full {
					raw, err := tx.MarshalBinary()
					if err != nil {
						return readErr(err)
					}
					batch.TxRlp = append(batch.TxRlp, raw)
				}
			}
			if err := out.Send(batch); err != nil {
				return err
			}
		}
		if req.ToBlock > 0 && next > req.ToBlock {
			return nil // a clean end of stream
		}
		select {
		case <-out.Context().Done():
			return out.Context().Err()
		case <-time.After(rpc.HeadPollInterval):
		}
	}
}
