// rscompat is the Go side of the rs/state cross-language oracle. Rows files
// are [klen u8][vlen u8][key][value] (exp/statedump's framing).
//
//	runwrite -rows f -out run     write a latest run from sorted rows
//	runcheck -rows f -run run     open a run and check every Get and Iter
//	roll -rows f -out nodes       Roll contract-form rows, print root and stats
//	dirty -rows f -updates u      Roll rows, apply updates through Dirty, print the root
//	noderoot -file nodes          print a node file's footer root
package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"os"
	"slices"
	"sort"

	"github.com/containerman17/avalanche-s3-archival-node/commit"
	"github.com/containerman17/avalanche-s3-archival-node/latest"
)

type row struct{ k, v []byte }
type rows []row

func (r rows) Seek(prefix []byte) ([]byte, []byte) {
	i := sort.Search(len(r), func(i int) bool { return bytes.Compare(r[i].k, prefix) >= 0 })
	if i == len(r) {
		return nil, nil
	}
	return r[i].k, r[i].v
}

type rowIter struct {
	r rows
	i int
}

func (it *rowIter) Next() bool    { it.i++; return it.i <= len(it.r) }
func (it *rowIter) Key() []byte   { return it.r[it.i-1].k }
func (it *rowIter) Value() []byte { return it.r[it.i-1].v }
func (it *rowIter) Err() error    { return nil }

func load(path string) rows {
	raw, err := os.ReadFile(path)
	check(err)
	var rs rows
	for i := 0; i+2 <= len(raw); {
		kl, vl := int(raw[i]), int(raw[i+1])
		i += 2
		rs = append(rs, row{raw[i : i+kl], raw[i+kl : i+kl+vl]})
		i += kl + vl
	}
	return rs
}

func main() {
	mode := flag.String("mode", "", "runwrite|runcheck|roll|dirty|noderoot")
	rowsPath := flag.String("rows", "", "rows file")
	out := flag.String("out", "", "output file")
	run := flag.String("run", "", "run file")
	updates := flag.String("updates", "", "updates rows file (empty value = delete)")
	file := flag.String("file", "", "node file")
	flag.Parse()
	switch *mode {
	case "runwrite":
		rs := load(*rowsPath)
		slices.SortFunc(rs, func(a, b row) int { return bytes.Compare(a.k, b.k) })
		w, err := latest.NewWriter(*out)
		check(err)
		var user [32]byte
		for i := range user {
			user[i] = byte(i)
		}
		w.SetUserData(user)
		for _, r := range rs {
			check(w.Add(r.k, r.v))
		}
		check(w.Close())
		fmt.Printf("wrote %d entries\n", len(rs))
	case "runcheck":
		rs := load(*rowsPath)
		slices.SortFunc(rs, func(a, b row) int { return bytes.Compare(a.k, b.k) })
		r, err := latest.Open(*run)
		check(err)
		defer r.Close()
		if r.Len() != len(rs) {
			log.Fatalf("Len %d, want %d", r.Len(), len(rs))
		}
		for _, e := range rs {
			v, ok := r.Get(e.k)
			if !ok || !bytes.Equal(v, e.v) {
				log.Fatalf("Get %x: %x %v, want %x", e.k, v, ok, e.v)
			}
			k := bytes.Clone(e.k)
			k[len(k)-1] ^= 1
			if _, found := slices.BinarySearchFunc(rs, k, func(e row, k []byte) int { return bytes.Compare(e.k, k) }); !found {
				if _, ok := r.Get(k); ok {
					log.Fatalf("Get absent %x", k)
				}
			}
		}
		it := r.Iter(nil, nil)
		n := 0
		for it.Next() {
			if n >= len(rs) || !bytes.Equal(it.Key(), rs[n].k) || !bytes.Equal(it.Value(), rs[n].v) {
				log.Fatalf("Iter entry %d mismatch", n)
			}
			n++
		}
		if n != len(rs) {
			log.Fatalf("Iter %d entries, want %d", n, len(rs))
		}
		if len(rs) > 10 {
			lo, hi := rs[len(rs)/4].k, rs[len(rs)/2].k
			it := r.Iter(lo, hi)
			n := 0
			for it.Next() {
				n++
			}
			if n != len(rs)/2-len(rs)/4 {
				log.Fatalf("bounded Iter %d entries", n)
			}
		}
		fmt.Printf("OK %d entries, user %x\n", r.Len(), r.UserData())
	case "roll":
		rs := load(*rowsPath)
		root, st, err := commit.Roll(&rowIter{r: rs}, *out, [32]byte{7})
		check(err)
		fmt.Printf("root %x keys %d nodes %d bytes %d\n", root, st.Keys, st.Nodes, st.Bytes)
	case "dirty":
		rs := load(*rowsPath)
		tmp, err := os.CreateTemp("", "rscompat-nodes")
		check(err)
		tmp.Close()
		defer os.Remove(tmp.Name())
		root, _, err := commit.Roll(&rowIter{r: rs}, tmp.Name(), [32]byte{})
		check(err)
		f, err := commit.Open(tmp.Name())
		check(err)
		defer f.Close()
		d := commit.NewDirty(f, rs.Seek)
		for _, u := range load(*updates) {
			check(d.Apply(u.k, u.v))
		}
		got, err := d.Root()
		check(err)
		fmt.Printf("rolled %x\ndirty %x\nretained %d\n", root, got, d.Bytes())
	case "noderoot":
		f, err := commit.Open(*file)
		check(err)
		defer f.Close()
		fmt.Printf("root %x nodes %d keys %d size %d\n", f.Root(), f.NodeCount(), f.KeyCount(), f.Size())
	default:
		log.Fatal("unknown -mode")
	}
}

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
