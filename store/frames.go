package store

import (
	"encoding/binary"
	"fmt"
	"math/big"

	"github.com/ava-labs/libevm/common"
)

// THE `itx/<txnum>` RECORD, read side. exec/frames.go is the writer and owns
// the format comment; this is its inverse, and it lives in store for the reason
// the receipt codecs do: the executor writes, rpc and verify read, and exec
// cannot import rpc.
//
// Layout: uvarint frameCount, then the frames in ENTER order (a pre-order DFS
// when read with depth). The TOP-LEVEL frame is excluded because it IS the
// transaction. An empty record means the transaction made no nested call,
// which is a real answer and not a missing one.

// Frame is one decoded call frame.
type Frame struct {
	Kind    byte // the vm.OpCode of the call that opened it
	Depth   byte
	From    common.Address
	To      common.Address
	Value   *big.Int // nil when the frame moved no value
	Gas     uint64
	GasUsed uint64
	Failed  bool
	Input   []byte
	Output  []byte
}

// DecodeFrames decodes an itx/ row. A nil/empty record decodes to no frames.
func DecodeFrames(rec []byte) ([]Frame, error) {
	if len(rec) == 0 {
		return nil, nil
	}
	pos := 0
	next := func(what string) (uint64, error) {
		v, k := binary.Uvarint(rec[pos:])
		if k <= 0 {
			return 0, fmt.Errorf("frames: bad %s", what)
		}
		pos += k
		return v, nil
	}
	take := func(n int, what string) ([]byte, error) {
		if pos+n > len(rec) {
			return nil, fmt.Errorf("frames: truncated %s", what)
		}
		b := rec[pos : pos+n]
		pos += n
		return b, nil
	}
	n, err := next("frame count")
	if err != nil {
		return nil, err
	}
	out := make([]Frame, 0, n)
	for i := uint64(0); i < n; i++ {
		var f Frame
		hdr, err := take(2+20+20, "frame header")
		if err != nil {
			return nil, err
		}
		f.Kind, f.Depth = hdr[0], hdr[1]
		copy(f.From[:], hdr[2:22])
		copy(f.To[:], hdr[22:42])
		vl, err := next("value len")
		if err != nil {
			return nil, err
		}
		val, err := take(int(vl), "value")
		if err != nil {
			return nil, err
		}
		if vl > 0 {
			f.Value = new(big.Int).SetBytes(val)
		}
		if f.Gas, err = next("gas"); err != nil {
			return nil, err
		}
		if f.GasUsed, err = next("gas used"); err != nil {
			return nil, err
		}
		flag, err := take(1, "error flag")
		if err != nil {
			return nil, err
		}
		f.Failed = flag[0] != 0
		il, err := next("input len")
		if err != nil {
			return nil, err
		}
		if f.Input, err = take(int(il), "input"); err != nil {
			return nil, err
		}
		ol, err := next("output len")
		if err != nil {
			return nil, err
		}
		if f.Output, err = take(int(ol), "output"); err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, nil
}
