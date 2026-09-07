package store

import (
	"encoding/json"
	"fmt"
	"math/big"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/common/hexutil"
	"github.com/ava-labs/libevm/core/vm"
)

// THE `itx/<txnum>` RECORD IS libevm's callTracer JSON, VERBATIM (storage v4):
// exec/frames.go captures it, rpc serves it as bytes, and this is the reader
// for everything that wants frames as values (Otterscan's internal
// operations, gRPC Trace, verify). It lives in store for the reason the
// receipt codecs do: the executor writes, rpc and verify read, and exec cannot
// import rpc.
//
// DecodeFrames flattens the tree to ENTER order with depth (a pre-order DFS),
// EXCLUDING the top-level frame, which is the transaction itself: that is the
// shape every reader had under v0..v3, so nothing downstream moved. A
// transaction that made no nested call decodes to no frames, a real answer
// and not a missing one.

// Frame is one decoded call frame.
type Frame struct {
	Kind    byte // the vm.OpCode of the call that opened it
	Depth   byte
	From    common.Address
	To      common.Address
	Value   *big.Int // nil when the tracer printed none (STATICCALL)
	Gas     uint64
	GasUsed uint64
	Failed  bool
	Error   string // callTracer's error string, "" on success
	Input   []byte
	Output  []byte
}

// callFrameJSON is the stored shape, field for field (libevm native.callFrame).
type callFrameJSON struct {
	Type         string          `json:"type"`
	From         common.Address  `json:"from"`
	To           *common.Address `json:"to"`
	Value        *hexutil.Big    `json:"value"`
	Gas          hexutil.Uint64  `json:"gas"`
	GasUsed      hexutil.Uint64  `json:"gasUsed"`
	Input        hexutil.Bytes   `json:"input"`
	Output       hexutil.Bytes   `json:"output"`
	Error        string          `json:"error"`
	RevertReason string          `json:"revertReason"`
	Calls        []callFrameJSON `json:"calls"`
}

// DecodeFrames decodes an itx/ row. A nil/empty record decodes to no frames.
func DecodeFrames(rec []byte) ([]Frame, error) {
	if len(rec) == 0 {
		return nil, nil
	}
	var top callFrameJSON
	if err := json.Unmarshal(rec, &top); err != nil {
		return nil, fmt.Errorf("frames: %w", err)
	}
	var out []Frame
	var walk func(f *callFrameJSON, depth byte)
	walk = func(f *callFrameJSON, depth byte) {
		for i := range f.Calls {
			c := &f.Calls[i]
			fr := Frame{
				Kind: byte(vm.StringToOp(c.Type)), Depth: depth, From: c.From,
				Gas: uint64(c.Gas), GasUsed: uint64(c.GasUsed),
				Failed: c.Error != "", Error: c.Error, Input: c.Input, Output: c.Output,
			}
			if c.To != nil {
				fr.To = *c.To
			}
			if c.Value != nil {
				fr.Value = (*big.Int)(c.Value)
			}
			out = append(out, fr)
			walk(c, depth+1)
		}
	}
	walk(&top, 0)
	return out, nil
}
