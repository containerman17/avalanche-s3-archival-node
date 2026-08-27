package store

import (
	"bytes"
	"sort"

	"github.com/containerman17/epochdb/store/ef"
)

// chunkPostings turns the window's ungrouped postings into lookup rows: sorted
// by (group, TxNum), one TxNum per group (payloads OR-ed), cut into chunks of
// ef.MaxEntries keyed by the chunk's first TxNum.
type kv struct{ key, val []byte }

func chunkPostings(post []posting) []kv {
	sort.Slice(post, func(i, j int) bool {
		if c := bytes.Compare(post[i].group, post[j].group); c != 0 {
			return c < 0
		}
		return post[i].txnum < post[j].txnum
	})
	var rows []kv
	var nums []uint64
	var pay []byte
	flush := func(group []byte) {
		if len(nums) == 0 {
			return
		}
		rows = append(rows, kv{
			Suffixed(group, nums[0]),
			ef.Encode(nums[0], nums, pay, PayloadBits(group)),
		})
		nums, pay = nums[:0], pay[:0]
	}
	for i := 0; i < len(post); i++ {
		p := post[i]
		if i > 0 && !bytes.Equal(post[i-1].group, p.group) {
			flush(post[i-1].group)
		}
		if n := len(nums); n > 0 && nums[n-1] == p.txnum {
			pay[n-1] |= p.payload
			continue
		}
		if len(nums) == ef.MaxEntries {
			flush(p.group)
		}
		nums = append(nums, p.txnum)
		pay = append(pay, p.payload)
	}
	if len(post) > 0 {
		flush(post[len(post)-1].group)
	}
	return rows
}

// ScanChunks returns every posting entry under prefix with a TxNum in
// [lo, hi], in key order (grouped, ascending inside a group). prefix is a
// group key or a one-component prefix. A chunk is decoded only if it can hold
// an entry of the range: its first TxNum is <= hi and the group's next chunk
// (if any) starts past lo.
func (r *Run) ScanChunks(prefix []byte, lo, hi uint64) ([]posting, error) {
	if !r.MayHave(SecLookup, Suffixed(prefix, 0)) {
		return nil, nil
	}
	it, err := r.newIter(SecLookup)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	var out []posting
	var pendKey, pendVal []byte
	decode := func() error {
		if pendKey == nil {
			return nil
		}
		group := append([]byte(nil), pendKey[:len(pendKey)-8]...)
		nums, pay, err := ef.Decode(TxNumOf(pendKey), pendVal)
		if err != nil {
			return err
		}
		for i, n := range nums {
			if n < lo || n > hi {
				continue
			}
			var p byte
			if pay != nil {
				p = pay[i]
			}
			out = append(out, posting{group: group, txnum: n, payload: p})
		}
		pendKey = nil
		return nil
	}
	for kv := it.SeekGE(prefix, 0); kv != nil; kv = it.Next() {
		k := kv.K.UserKey
		if !bytes.HasPrefix(k, prefix) || len(k) < len(prefix)+8 {
			break
		}
		first := TxNumOf(k)
		sameGroup := pendKey != nil && bytes.Equal(pendKey[:len(pendKey)-8], k[:len(k)-8])
		if sameGroup && first <= lo {
			pendKey = nil // the pending chunk ends before lo
		} else if err := decode(); err != nil {
			return nil, err
		}
		if first > hi {
			continue
		}
		raw, _, err := kv.Value(nil)
		if err != nil {
			return nil, err
		}
		pendKey, pendVal = append(pendKey[:0], k...), append(pendVal[:0], raw...)
	}
	if err := decode(); err != nil {
		return nil, err
	}
	return out, nil
}

// ScanGroups calls fn once per distinct group key under prefix, in key order.
func (r *Run) ScanGroups(prefix []byte, fn func(group []byte) bool) error {
	if !r.MayHave(SecLookup, Suffixed(prefix, 0)) {
		return nil
	}
	it, err := r.newIter(SecLookup)
	if err != nil {
		return err
	}
	defer it.Close()
	var last []byte
	for kv := it.SeekGE(prefix, 0); kv != nil; kv = it.Next() {
		k := kv.K.UserKey
		if !bytes.HasPrefix(k, prefix) || len(k) < len(prefix)+8 {
			return nil
		}
		g := k[:len(k)-8]
		if bytes.Equal(g, last) {
			continue
		}
		last = append(last[:0], g...)
		if !fn(append([]byte(nil), g...)) {
			return nil
		}
	}
	return nil
}
