package ef

import (
	"bytes"
	"math/rand"
	"testing"
)

func roundTrip(t *testing.T, base uint64, nums []uint64, pb int) []byte {
	t.Helper()
	var payload []byte
	if pb > 0 {
		payload = make([]byte, len(nums))
		for i := range payload {
			payload[i] = byte(i*7) & (1<<pb - 1)
		}
	}
	v := Encode(base, nums, payload, pb)
	gotN, gotP, err := Decode(base, v)
	if err != nil {
		t.Fatal(err)
	}
	if len(gotN) != len(nums) {
		t.Fatalf("len %d want %d", len(gotN), len(nums))
	}
	for i := range nums {
		if gotN[i] != nums[i] {
			t.Fatalf("[%d] %d want %d", i, gotN[i], nums[i])
		}
	}
	if !bytes.Equal(gotP, payload) {
		t.Fatalf("payload %x want %x", gotP, payload)
	}
	if !bytes.Equal(Encode(base, nums, payload, pb), v) {
		t.Fatal("not deterministic")
	}
	return v
}

func TestRoundTrip(t *testing.T) {
	roundTrip(t, 0, nil, 0)
	roundTrip(t, 5, []uint64{5}, 0)
	roundTrip(t, 5, []uint64{5, 5, 5}, 3) // repeats
	dense := make([]uint64, MaxEntries)
	for i := range dense {
		dense[i] = 1000 + uint64(i)
	}
	if v := roundTrip(t, 1000, dense, 0); len(v) > MaxEntries/4+16 {
		t.Fatalf("dense chunk %dB", len(v))
	}
	for _, pb := range []int{0, 3, 5} {
		r := rand.New(rand.NewSource(int64(pb)))
		nums := make([]uint64, MaxEntries)
		x := uint64(1 << 40)
		for i := range nums {
			x += uint64(r.Intn(2000))
			nums[i] = x
		}
		v := roundTrip(t, nums[0], nums, pb)
		t.Logf("pb=%d: %.2f bits/entry", pb, float64(len(v)*8)/float64(len(nums)))
	}
}

func TestCorrupt(t *testing.T) {
	v := Encode(0, []uint64{1, 2, 3}, nil, 0)
	if _, _, err := Decode(0, v[:len(v)-1]); err == nil {
		t.Fatal("truncated chunk decoded")
	}
}
