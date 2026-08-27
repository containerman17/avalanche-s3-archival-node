package store

import (
	"bytes"
	"sort"

	"github.com/containerman17/epochdb/store/ef"
)

type kv struct{ key, val []byte }

func sortKV(rows []kv) {
	sort.Slice(rows, func(i, j int) bool { return bytes.Compare(rows[i].key, rows[j].key) < 0 })
}

// postSet is a flush's or a migration's worth of posting entries with the
// group keys INTERNED: a terminal run holds ~60M entries over ~9M groups, and
// an entry that carried its own key copy was ~100 bytes where this is 16.
type postSet struct {
	groups []string
	ids    map[string]int32
	ents   []postEnt
}

type postEnt struct {
	gid     int32
	payload byte
	txnum   uint64
}

func (p *postSet) add(group []byte, txnum uint64, payload byte) {
	if p.ids == nil {
		p.ids = map[string]int32{}
	}
	id, ok := p.ids[string(group)]
	if !ok {
		id = int32(len(p.groups))
		p.groups = append(p.groups, string(group))
		p.ids[string(group)] = id
	}
	p.ents = append(p.ents, postEnt{gid: id, txnum: txnum, payload: payload})
}

// chunks turns the set into lookup rows: sorted by (group, TxNum), one TxNum
// per group (payloads OR-ed), cut into chunks of ef.MaxEntries keyed by the
// chunk's first TxNum.
func (p *postSet) chunks() []kv {
	rank := make([]int32, len(p.groups))
	order := make([]int32, len(p.groups))
	for i := range order {
		order[i] = int32(i)
	}
	sort.Slice(order, func(i, j int) bool { return p.groups[order[i]] < p.groups[order[j]] })
	for r, id := range order {
		rank[id] = int32(r)
	}
	sort.Slice(p.ents, func(i, j int) bool {
		if a, b := rank[p.ents[i].gid], rank[p.ents[j].gid]; a != b {
			return a < b
		}
		return p.ents[i].txnum < p.ents[j].txnum
	})
	var rows []kv
	var nums []uint64
	var pay []byte
	flush := func(gid int32) {
		if len(nums) == 0 {
			return
		}
		group := []byte(p.groups[gid])
		rows = append(rows, kv{Suffixed(group, nums[0]), ef.Encode(nums[0], nums, pay, PayloadBits(group))})
		nums, pay = nums[:0], pay[:0]
	}
	for i, e := range p.ents {
		if i > 0 && p.ents[i-1].gid != e.gid {
			flush(p.ents[i-1].gid)
		}
		if n := len(nums); n > 0 && nums[n-1] == e.txnum {
			pay[n-1] |= e.payload
			continue
		}
		if len(nums) == ef.MaxEntries {
			flush(e.gid)
		}
		nums = append(nums, e.txnum)
		pay = append(pay, e.payload)
	}
	if len(p.ents) > 0 {
		flush(p.ents[len(p.ents)-1].gid)
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
