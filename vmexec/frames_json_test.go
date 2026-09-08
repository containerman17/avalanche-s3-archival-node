package vmexec

// A hand-written encoder for callTracer's JSON, for the benchmark only: it
// measures what a zero-reflection renderer would save against
// encoding/json. It is NOT wired into capture (the stored bytes stay
// libevm's own, see frames.go); TestFrameJSONParity keeps it honest.

import (
	"encoding/hex"
	"encoding/json"
	"math/big"
	"strconv"
	"testing"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/common/hexutil"
	"github.com/ava-labs/libevm/core/vm"
	"github.com/ava-labs/libevm/eth/tracers"
)

type callFrameJSON struct {
	From         common.Address  `json:"from"`
	Gas          hexutil.Uint64  `json:"gas"`
	GasUsed      hexutil.Uint64  `json:"gasUsed"`
	To           *common.Address `json:"to,omitempty"`
	Input        hexutil.Bytes   `json:"input"`
	Output       hexutil.Bytes   `json:"output,omitempty"`
	Error        string          `json:"error,omitempty"`
	RevertReason string          `json:"revertReason,omitempty"`
	Calls        []callFrameJSON `json:"calls,omitempty"`
	Value        *hexutil.Big    `json:"value,omitempty"`
	Type         string          `json:"type"`
}

func appendHexBytes(b, v []byte) []byte {
	b = append(b, '"', '0', 'x')
	n := len(b)
	b = append(b, make([]byte, 2*len(v))...)
	hex.Encode(b[n:], v)
	return append(b, '"')
}

func appendHexU64(b []byte, v uint64) []byte {
	b = append(b, '"', '0', 'x')
	b = strconv.AppendUint(b, v, 16)
	return append(b, '"')
}

// appendString escapes as encoding/json does with HTML escaping on.
func appendString(b []byte, s string) []byte {
	enc, _ := json.Marshal(s)
	return append(b, enc...)
}

func (f *callFrameJSON) append(b []byte) []byte {
	b = append(b, `{"from":`...)
	b = appendHexBytes(b, f.From[:])
	b = append(b, `,"gas":`...)
	b = appendHexU64(b, uint64(f.Gas))
	b = append(b, `,"gasUsed":`...)
	b = appendHexU64(b, uint64(f.GasUsed))
	if f.To != nil {
		b = append(b, `,"to":`...)
		b = appendHexBytes(b, f.To[:])
	}
	b = append(b, `,"input":`...)
	b = appendHexBytes(b, f.Input)
	if len(f.Output) > 0 {
		b = append(b, `,"output":`...)
		b = appendHexBytes(b, f.Output)
	}
	if f.Error != "" {
		b = append(b, `,"error":`...)
		b = appendString(b, f.Error)
	}
	if f.RevertReason != "" {
		b = append(b, `,"revertReason":`...)
		b = appendString(b, f.RevertReason)
	}
	if len(f.Calls) > 0 {
		b = append(b, `,"calls":[`...)
		for i := range f.Calls {
			if i > 0 {
				b = append(b, ',')
			}
			b = f.Calls[i].append(b)
		}
		b = append(b, ']')
	}
	if f.Value != nil {
		b = append(b, `,"value":"0x`...)
		b = append(b, (*big.Int)(f.Value).Text(16)...)
		b = append(b, '"')
	}
	b = append(b, `,"type":`...)
	b = appendString(b, f.Type)
	return append(b, '}')
}

func TestFrameJSONParity(t *testing.T) {
	mk := func(fail bool) tracers.Tracer {
		tr, err := tracers.DefaultDirectory.New("callTracer", &tracers.Context{}, json.RawMessage(nil))
		if err != nil {
			t.Fatal(err)
		}
		from, to, to2 := common.Address{1}, common.Address{0xab, 2}, common.Address{3}
		in := []byte{0xa9, 0x05, 0x9c, 0xbb, 1, 2, 3}
		tr.CaptureTxStart(150_000)
		tr.CaptureStart(nil, from, to, false, in, 150_000, big.NewInt(1e18))
		tr.CaptureEnter(vm.CALL, to, to2, in[:4], 100_000, big.NewInt(0))
		tr.CaptureExit([]byte{9}, 21_000, nil)
		tr.CaptureEnter(vm.STATICCALL, to, to2, nil, 70_000, nil)
		if fail {
			tr.CaptureExit(nil, 2_600, vm.ErrExecutionReverted)
			tr.CaptureEnd(append([]byte{0x08, 0xc3, 0x79, 0xa0}, make([]byte, 64)...), 120_000, vm.ErrExecutionReverted)
		} else {
			tr.CaptureExit([]byte{1, 2}, 2_600, nil)
			tr.CaptureEnd(nil, 120_000, nil)
		}
		tr.CaptureTxEnd(30_000)
		return tr
	}
	for _, fail := range []bool{false, true} {
		want, err := mk(fail).GetResult()
		if err != nil {
			t.Fatal(err)
		}
		var f callFrameJSON
		if err := json.Unmarshal(want, &f); err != nil {
			t.Fatal(err)
		}
		if got := f.append(nil); string(got) != string(want) {
			t.Fatalf("fail=%v\n got %s\nwant %s", fail, got, want)
		}
	}
}
