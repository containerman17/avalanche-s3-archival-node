//go:build exp

package main

import (
	"bytes"
	"fmt"
	"os"

	"github.com/containerman17/epochdb/dist"
	"github.com/containerman17/epochdb/store"
)

func main() {
	cas, err := dist.Local(os.Args[1])
	if err != nil {
		panic(err)
	}
	r, err := store.OpenRun(cas, os.Args[2])
	if err != nil {
		panic(err)
	}
	type stat struct{ rows, keyB, valB, firsts int }
	stats := map[string]*stat{}
	var lastFirst []byte
	err = r.ScanRange(store.SecLookup, []byte{0}, []byte{0xff}, func(k, v []byte) bool {
		i := bytes.IndexByte(k, '/')
		fam := string(k[:i+1])
		s := stats[fam]
		if s == nil {
			s = &stat{}
			stats[fam] = s
		}
		s.rows++
		s.keyB += len(k)
		s.valB += len(v)
		// first component
		var first []byte
		switch fam {
		case "tval/":
			first = k[:5+32]
		case "elog/":
			first = k[:5+20]
		case "addr/":
			first = k[:5+20]
		case "sig/":
			first = k[:4+32]
		}
		if first != nil && !bytes.Equal(first, lastFirst) {
			s.firsts++
			lastFirst = append(lastFirst[:0], first...)
		}
		return true
	})
	if err != nil {
		panic(err)
	}
	for fam, s := range stats {
		fmt.Printf("%-8s rows %9d  keyMB %7.1f  valMB %7.1f  distinct-first %9d\n", fam, s.rows, float64(s.keyB)/1e6, float64(s.valB)/1e6, s.firsts)
	}
}
