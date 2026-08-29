//go:build exp

// idbook: rewrite the sorted tlog4 spill with address-book ids and measure SST bytes.
package main

import (
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/containerman17/avalanche-s3-archival-node/dist"
	"github.com/containerman17/avalanche-s3-archival-node/store"
)

func main() {
	dir := os.Args[1]
	scratch := filepath.Join(dir, "scratch")
	local, err := dist.Local(dir)
	if err != nil {
		panic(err)
	}
	sf, err := os.Open(filepath.Join(scratch, "tlog4.sorted"))
	if err != nil {
		panic(err)
	}
	sc := bufio.NewScanner(sf)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	addrs, sigs, emits := map[[20]byte]uint32{}, map[[32]byte]uint16{}, map[[20]byte]uint32{}
	for _, variant := range []string{"ids", "ids-no-emitter"} {
		w, err := store.NewRunWriter(filepath.Join(scratch, "y"), [32]byte{}, 0)
		if err != nil {
			panic(err)
		}
		var rows uint64
		sf.Seek(0, 0)
		sc = bufio.NewScanner(sf)
		sc.Buffer(make([]byte, 1<<20), 1<<20)
		out, _ := os.Create(filepath.Join(scratch, "ids.hex"))
		bw := bufio.NewWriterSize(out, 4<<20)
		for sc.Scan() {
			r, _ := hex.DecodeString(sc.Text())
			var a, e [20]byte
			var t [32]byte
			copy(a[:], r[5:25])
			copy(t[:], r[25:57])
			copy(e[:], r[57:77])
			txn := r[77:85]
			// ids assigned in sorted first-seen order keep the sort order
			if _, ok := addrs[a]; !ok {
				addrs[a] = uint32(len(addrs))
			}
			if _, ok := sigs[t]; !ok {
				sigs[t] = uint16(len(sigs))
			}
			if _, ok := emits[e]; !ok {
				emits[e] = uint32(len(emits))
			}
			k := []byte("tl/")
			k = binary.BigEndian.AppendUint32(k, addrs[a])
			k = binary.BigEndian.AppendUint16(k, sigs[t])
			if variant == "ids" {
				k = binary.BigEndian.AppendUint32(k, emits[e])
			}
			k = append(k, txn...)
			bw.WriteString(hex.EncodeToString(k))
			bw.WriteByte('\n')
		}
		bw.Flush()
		out.Close()
		c := exec.Command("sort", "-u", "-S", "6G", "--parallel", "8", "-T", scratch, "-o", filepath.Join(scratch, "ids.sorted"), filepath.Join(scratch, "ids.hex"))
		c.Env = append(os.Environ(), "LC_ALL=C")
		if err := c.Run(); err != nil {
			panic(err)
		}
		in, _ := os.Open(filepath.Join(scratch, "ids.sorted"))
		is := bufio.NewScanner(in)
		is.Buffer(make([]byte, 1<<20), 1<<20)
		for s := store.SecChain; s <= store.SecLookup; s++ {
			w.Begin(s)
			if s == store.SecLookup {
				for is.Scan() {
					k, _ := hex.DecodeString(is.Text())
					rows++
					if err := w.Set(k, []byte{1}); err != nil {
						panic(err)
					}
				}
			}
			w.End()
		}
		in.Close()
		_, ft, err := w.Finish(local, 0, 1, 0, 1)
		if err != nil {
			panic(err)
		}
		fmt.Printf("%-16s rows=%d sst=%.1fMB %.2f B/row (book: %d addrs, %d sigs, %d emitters)\n", variant, rows, float64(ft.Len[store.SecLookup])/1e6, float64(ft.Len[store.SecLookup])/float64(rows), len(addrs), len(sigs), len(emits))
	}
}
