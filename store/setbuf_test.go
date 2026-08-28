package store

import (
	"bytes"
	"math/rand"
	"slices"
	"sort"
	"testing"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
)

// synthSets is a mainnet-C-shaped buffer of fixed-width set/ records: topic0
// is dominated by one signature, the topic value is a zero-padded address out
// of a skewed pool, and the emitter comes out of a small pool of contracts.
// That shape is what makes the sort hard: the first ~50 bytes of two records
// are usually equal.
func synthSets(n int) []byte {
	rng := rand.New(rand.NewSource(1))
	sigs := make([]common.Hash, 200)
	for i := range sigs {
		rng.Read(sigs[i][:])
	}
	addrs := make([]common.Address, 200_000)
	for i := range addrs {
		rng.Read(addrs[i][:])
	}
	emitters := make([]common.Address, 5000)
	for i := range emitters {
		rng.Read(emitters[i][:])
	}
	pick := func(n int) int { // skewed: half the draws hit the first 1%
		if rng.Intn(2) == 0 {
			return rng.Intn(n/100 + 1)
		}
		return rng.Intn(n)
	}
	buf := make([]byte, 0, n*setKeyLen)
	var val common.Hash
	for i := 0; i < n; i++ {
		t0 := sigs[0]
		if rng.Intn(10) != 0 {
			t0 = sigs[pick(len(sigs))]
		}
		val = common.Hash{}
		copy(val[12:], addrs[pick(len(addrs))][:])
		emitter := emitters[pick(len(emitters))]
		buf = append(buf, SetKey(t0[:], byte(1+rng.Intn(3)), val[:], emitter[:])...)
	}
	return buf
}

func benchSort(b *testing.B, sortFn func([]byte)) {
	const n = 4_000_000
	src := synthSets(n)
	b.SetBytes(int64(len(src)))
	buf := make([]byte, len(src))
	for b.Loop() {
		b.StopTimer()
		copy(buf, src)
		b.StartTimer()
		sortFn(buf)
	}
}

// BenchmarkSetSortInterface is the pre-change sort: sort.Interface over the
// flat buffer, one 92-byte swap per swap.
func BenchmarkSetSortInterface(b *testing.B) {
	benchSort(b, func(buf []byte) { sort.Sort(setRecs(buf)) })
}

// BenchmarkSetSortIndex is the alternative that was MEASURED AND REJECTED: a
// []uint32 record index sorted with slices.SortFunc, so no interface dispatch
// and a 4-byte swap instead of a 92-byte one. It is ~60% SLOWER than
// sort.Interface over the flat buffer, because the indirection turns every
// comparison into a random read of a multi-hundred-megabyte buffer where the
// in-place sort reads and writes its own neighbourhood. Kept as a benchmark so
// the next person does not re-derive it from first principles.
func BenchmarkSetSortIndex(b *testing.B) {
	benchSort(b, func(buf []byte) {
		idx := make([]uint32, len(buf)/setKeyLen)
		for i := range idx {
			idx[i] = uint32(i)
		}
		slices.SortFunc(idx, func(a, c uint32) int {
			return bytes.Compare(buf[int(a)*setKeyLen:][:setKeyLen], buf[int(c)*setKeyLen:][:setKeyLen])
		})
	})
}

// TestAppendSetKeys: the migration's fixed-width writer emits exactly what the
// flush path's logSets emits, which is what keeps a migrated run byte-equal to
// a from-scratch one.
func TestAppendSetKeys(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	for i := 0; i < 200; i++ {
		emitter := make([]byte, 20)
		rng.Read(emitter)
		topics := make([][]byte, rng.Intn(5))
		for j := range topics {
			topics[j] = make([]byte, 32)
			rng.Read(topics[j])
		}
		var want []byte
		for _, k := range logSets(emitter, topics) {
			if len(k) != setKeyLen {
				t.Fatalf("logSets key is %d bytes, not %d", len(k), setKeyLen)
			}
			want = append(want, k...)
		}
		if got := appendSetKeys(nil, emitter, topics); !bytes.Equal(got, want) {
			t.Fatalf("%d topics: %x != %x", len(topics), got, want)
		}
	}
}

// BenchmarkSetKeysAlloc / BenchmarkSetKeysAppend: per-log key building, the
// [][]byte-per-log form against the append-into-a-buffer form.
func benchKeys(b *testing.B, build func(dst []byte, emitter []byte, topics [][]byte) []byte) {
	emitter := make([]byte, 20)
	topics := [][]byte{make([]byte, 32), make([]byte, 32), make([]byte, 32)}
	buf := make([]byte, 0, 3*setKeyLen)
	b.ReportAllocs()
	for b.Loop() {
		buf = build(buf[:0], emitter, topics)
	}
	_ = buf
}

func BenchmarkSetKeysAlloc(b *testing.B) {
	benchKeys(b, func(dst []byte, emitter []byte, topics [][]byte) []byte {
		for _, k := range logSets(emitter, topics) {
			dst = append(dst, k...)
		}
		return dst
	})
}

func BenchmarkSetKeysAppend(b *testing.B) { benchKeys(b, appendSetKeys) }

func rcptRec(nlogs int) []byte {
	rng := rand.New(rand.NewSource(4))
	var logs []*types.Log
	for i := 0; i < nlogs; i++ {
		l := &types.Log{Data: make([]byte, 64)}
		rng.Read(l.Address[:])
		for j := 0; j < 3; j++ {
			var h common.Hash
			rng.Read(h[:])
			l.Topics = append(l.Topics, h)
		}
		logs = append(logs, l)
	}
	return EncodeTxReceipt(&types.Receipt{Status: 1, GasUsed: 21000, Logs: logs}, 21000)
}

// BenchmarkRcptScanLog: one log's worth of the migration's rcpt phase, decode
// plus set-key building.
func BenchmarkRcptScanLog(b *testing.B) {
	const nlogs = 8
	rec := rcptRec(nlogs)
	topics := make([][]byte, 0, 4)
	buf := make([]byte, 0, 1<<16)
	b.ReportAllocs()
	n := 0
	for b.Loop() {
		_, _, _, logs, err := DecodeTxReceipt(rec)
		if err != nil {
			b.Fatal(err)
		}
		buf = buf[:0]
		for _, l := range logs {
			topics = topics[:0]
			for i := range l.Topics {
				topics = append(topics, l.Topics[i][:])
			}
			buf = appendSetKeys(buf, l.Address[:], topics)
		}
		n += len(logs)
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(n), "ns/log")
}
