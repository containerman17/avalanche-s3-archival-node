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

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"

	"github.com/containerman17/epochdb"
	"github.com/containerman17/epochdb/rpc"
)

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
		return nil, err
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
			return err
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

func (s *Server) GetBlock(_ context.Context, req *BlockRequest) (*BlockResponse, error) {
	core := s.n.Core()
	n := req.Number
	if len(req.Hash) > 0 {
		h, ok, err := core.HeightByHash(common.BytesToHash(req.Hash))
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("block %x is not on this chain", req.Hash)
		}
		n = h
	} else if req.Number == 0 {
		var err error
		if n, err = s.height(0); err != nil {
			return nil, err
		}
	}
	// The header goes out as the STORED bytes: nothing re-serializes what the
	// block hash is computed over.
	raw, ok, err := s.n.Store().HeaderRLP(n)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("block %d is not stored", n)
	}
	hdr, err := core.HeaderAt(n)
	if err != nil {
		return nil, err
	}
	out := &BlockResponse{Number: n, Hash: hdr.Hash().Bytes(), HeaderRlp: raw}
	first, count, ok, err := s.n.Store().BlockTxRange(n)
	if err != nil {
		return nil, err
	}
	if ok {
		out.TxCount = count
	}
	if !req.Full || out.TxCount == 0 {
		return out, nil
	}
	for i := uint64(0); i < uint64(count); i++ {
		txRLP, ok, err := s.n.Store().TxRLP(first + i)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("tx/%d of block %d is not stored", first+i, n)
		}
		out.TxRlp = append(out.TxRlp, txRLP)
	}
	return out, nil
}

func (s *Server) GetTransaction(_ context.Context, req *TransactionRequest) (*TransactionResponse, error) {
	hash := common.BytesToHash(req.Hash)
	blk, idx, found, err := s.n.Core().FindTx(hash)
	if err != nil {
		return nil, err
	}
	if !found {
		return &TransactionResponse{}, nil
	}
	tx := blk.Transactions()[idx]
	raw, err := tx.MarshalBinary()
	if err != nil {
		return nil, err
	}
	out := &TransactionResponse{
		Found: true, TxRlp: raw, BlockNumber: blk.NumberU64(),
		BlockHash: blk.Hash().Bytes(), Index: uint32(idx),
	}
	receipts, err := s.n.Core().BlockReceipts(blk)
	if err != nil {
		return nil, err
	}
	if idx < len(receipts) {
		out.Receipt = receiptPB(receipts[idx])
	}
	frames, _, err := s.n.Core().Frames(hash)
	if err != nil {
		return nil, err
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

func (s *Server) Call(_ context.Context, req *CallRequest) (*CallResponse, error) {
	n, err := s.height(req.Height)
	if err != nil {
		return nil, err
	}
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
	out, err := s.n.CallAt(msg, n)
	if err != nil {
		return nil, err
	}
	return &CallResponse{Output: out}, nil
}

func (s *Server) GetState(_ context.Context, req *StateRequest) (*StateResponse, error) {
	n, err := s.height(req.Height)
	if err != nil {
		return nil, err
	}
	st, err := s.n.StateAt(n)
	if err != nil {
		return nil, err
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
		return nil, err
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
		return nil, err
	}
	out := &SearchResponse{More: more}
	for _, h := range hits {
		out.Hits = append(out.Hits, &AddrHit{
			TxNum: h.TxNum, Height: h.Height, Hash: h.Hash.Bytes(), Roles: uint32(h.Roles),
		})
	}
	return out, nil
}

func (s *Server) GetLogs(_ context.Context, req *LogsRequest) (*LogsResponse, error) {
	to := req.ToBlock
	if to == 0 {
		var err error
		if to, err = s.height(0); err != nil {
			return nil, err
		}
	}
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
	logs, err := s.n.Core().GetLogs(req.FromBlock, to, addrs, topics)
	if err != nil {
		return nil, err
	}
	out := &LogsResponse{}
	for _, l := range logs {
		out.Logs = append(out.Logs, logPB(l))
	}
	return out, nil
}
