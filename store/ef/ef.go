// Package ef codes one posting chunk: a sorted list of txnums as a plain
// Elias-Fano sequence, followed by a fixed-width payload per entry.
//
// Layout: uvarint n | uvarint lowBits | uvarint payloadBits | upper bits
// (unary, n ones, ceil((max>>low)+n)/8 bytes) | lower bits (n*low bits) |
// payload (n*payloadBits bits). All bit vectors are LSB-first. Chunks are
// small (<=4096 entries) and always decoded whole, so there is no select
// structure: a chunk is one SST value read once.
package ef

import (
	"encoding/binary"
	"errors"
	"math/bits"
)

// MaxEntries is the chunk size the store cuts at: about one lookup block.
const MaxEntries = 4096

// Encode packs txnums (sorted ascending, may repeat) relative to base with
// one payloadBits-wide payload per entry. payloadBits 0 means no payload.
func Encode(base uint64, txnums []uint64, payload []byte, payloadBits int) []byte {
	n := len(txnums)
	var span uint64
	if n > 0 {
		span = txnums[n-1] - base
	}
	low := 0
	if n > 0 && span/uint64(n) > 0 {
		low = bits.Len64(span / uint64(n))
	}
	out := binary.AppendUvarint(nil, uint64(n))
	out = binary.AppendUvarint(out, uint64(low))
	out = binary.AppendUvarint(out, uint64(payloadBits))
	upperLen := int(span>>low) + n
	bw := writer{buf: out, bits: 0}
	for _, t := range txnums {
		hi := (t - base) >> low
		bw.set(int(hi) + bw.ones) // one bit per entry at position hi+i
		bw.ones++
	}
	bw.pad(upperLen)
	for _, t := range txnums {
		bw.put((t-base)&(1<<low-1), low)
	}
	for i := 0; i < n && payloadBits > 0; i++ {
		bw.put(uint64(payload[i]), payloadBits)
	}
	return bw.done()
}

var errCorrupt = errors.New("ef: corrupt chunk")

// Decode is the inverse of Encode. txnums are absolute (base added back).
func Decode(base uint64, v []byte) (txnums []uint64, payload []byte, err error) {
	pos := 0
	next := func() (uint64, error) {
		x, k := binary.Uvarint(v[pos:])
		if k <= 0 {
			return 0, errCorrupt
		}
		pos += k
		return x, nil
	}
	n64, err := next()
	if err != nil {
		return nil, nil, err
	}
	low64, err := next()
	if err != nil {
		return nil, nil, err
	}
	pb64, err := next()
	if err != nil {
		return nil, nil, err
	}
	n, low, pb := int(n64), int(low64), int(pb64)
	if n > MaxEntries || low > 64 || pb > 8 {
		return nil, nil, errCorrupt
	}
	r := reader{buf: v, bit: pos * 8}
	txnums = make([]uint64, n)
	var hi uint64
	for i := 0; i < n; i++ {
		for {
			b, ok := r.next()
			if !ok {
				return nil, nil, errCorrupt
			}
			if b {
				break
			}
			hi++
		}
		txnums[i] = hi
	}
	r.bit = (r.bit + 7) &^ 7 // upper vector is byte padded
	for i := range txnums {
		lo, ok := r.get(low)
		if !ok {
			return nil, nil, errCorrupt
		}
		txnums[i] = base + txnums[i]<<low + lo
	}
	if pb > 0 {
		payload = make([]byte, n)
		for i := range payload {
			p, ok := r.get(pb)
			if !ok {
				return nil, nil, errCorrupt
			}
			payload[i] = byte(p)
		}
	}
	return txnums, payload, nil
}

type writer struct {
	buf  []byte
	bits int // bits written past buf's uvarint header
	base int
	ones int
}

func (w *writer) set(i int) {
	if w.base == 0 {
		w.base = len(w.buf)
	}
	for len(w.buf) <= w.base+i/8 {
		w.buf = append(w.buf, 0)
	}
	w.buf[w.base+i/8] |= 1 << (i % 8)
	if i+1 > w.bits {
		w.bits = i + 1
	}
}

// pad closes the byte-aligned upper vector at exactly n bits.
func (w *writer) pad(n int) {
	if w.base == 0 {
		w.base = len(w.buf)
	}
	for len(w.buf) < w.base+(n+7)/8 {
		w.buf = append(w.buf, 0)
	}
	w.bits = len(w.buf) * 8
}

func (w *writer) put(x uint64, nbits int) {
	for k := 0; k < nbits; k++ {
		if w.bits%8 == 0 {
			w.buf = append(w.buf, 0)
		}
		if x>>k&1 == 1 {
			w.buf[w.bits/8] |= 1 << (w.bits % 8)
		}
		w.bits++
	}
}

func (w *writer) done() []byte { return w.buf }

type reader struct {
	buf []byte
	bit int
}

func (r *reader) next() (bool, bool) {
	if r.bit/8 >= len(r.buf) {
		return false, false
	}
	b := r.buf[r.bit/8]>>(r.bit%8)&1 == 1
	r.bit++
	return b, true
}

func (r *reader) get(nbits int) (uint64, bool) {
	var x uint64
	for k := 0; k < nbits; k++ {
		b, ok := r.next()
		if !ok {
			return 0, false
		}
		if b {
			x |= 1 << k
		}
	}
	return x, true
}
