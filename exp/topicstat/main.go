//go:build exp

// topicstat: one pass over a sealed run's rcpt/ rows, counting address-shaped
// topics per position and topic0, and measuring real SST bytes of the existing
// topic/ family against candidate composite tlog/ families.
package main

import (
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/crypto"
	"github.com/containerman17/avalanche-s3-archival-node/dist"
	"github.com/containerman17/avalanche-s3-archival-node/store"
)

var names = map[common.Hash]string{}

func init() {
	for _, s := range []string{
		"Transfer(address,address,uint256)", "Approval(address,address,uint256)",
		"TransferSingle(address,address,address,uint256,uint256)", "TransferBatch(address,address,address,uint256[],uint256[])",
		"ApprovalForAll(address,address,bool)", "Swap(address,uint256,uint256,uint256,uint256,address)",
		"Swap(address,address,int256,int256,uint160,uint128,int24)", "Sync(uint112,uint112)", "Deposit(address,uint256)",
		"Withdrawal(address,uint256)", "Mint(address,uint256,uint256)", "Burn(address,uint256,uint256,address)",
		"OwnershipTransferred(address,address)", "Deposit(address,address,uint256,uint256)", "Withdraw(address,address,address,uint256,uint256)",
	} {
		names[crypto.Keccak256Hash([]byte(s))] = s
	}
}

var (
	sigTransfer = crypto.Keccak256Hash([]byte("Transfer(address,address,uint256)"))
	sigSingle   = crypto.Keccak256Hash([]byte("TransferSingle(address,address,address,uint256,uint256)"))
	sigBatch    = crypto.Keccak256Hash([]byte("TransferBatch(address,address,address,uint256[],uint256[])"))
)

func addrShaped(h common.Hash) bool {
	for _, b := range h[:12] {
		if b != 0 {
			return false
		}
	}
	return true
}

func main() {
	dir, name := os.Args[1], os.Args[2]
	local, err := dist.Local(dir)
	if err != nil {
		panic(err)
	}
	run, err := store.OpenRun(local, name)
	if err != nil {
		panic(err)
	}
	f := run.Footer
	fmt.Printf("run tx [%d,%d) heights [%d,%d) chain=%dMB state=%dMB lookup=%dMB\n", f.FromTx, f.ToTx, f.FromHeight, f.ToHeight,
		f.Len[store.SecChain]>>20, f.Len[store.SecState]>>20, f.Len[store.SecLookup]>>20)

	var txs, logs, topics, addrTopics uint64
	var posAddr [4]uint64
	var posAll [4]uint64
	byTopic0 := map[common.Hash][2]uint64{} // [logs, addr-shaped topics]
	scratch := filepath.Join(dir, "scratch")
	os.MkdirAll(scratch, 0o755)
	spill := map[string]*bufio.Writer{}
	for _, n := range []string{"topic", "tlog3", "tlog4", "hold", "nftid"} {
		fh, err := os.Create(filepath.Join(scratch, n+".hex"))
		if err != nil {
			panic(err)
		}
		defer fh.Close()
		spill[n] = bufio.NewWriterSize(fh, 4<<20)
		defer spill[n].Flush()
	}
	emit := func(n string, k []byte) {
		spill[n].WriteString(hex.EncodeToString(k))
		spill[n].WriteByte('\n')
	}
	holderPosts := map[common.Address]uint64{}
	nftHolder := map[common.Address][2]uint64{} // in, out
	var transferPosts uint64

	lo, hi := []byte(store.PrefixRcpt), []byte("rcpt0")
	err = run.ScanRange(store.SecChain, lo, hi, func(k, v []byte) bool {
		txnum := store.TxNumOf(k)
		txs++
		_, _, _, ls, err := store.DecodeTxReceipt(v)
		if err != nil {
			panic(err)
		}
		for _, l := range ls {
			logs++
			if len(l.Topics) == 0 {
				continue
			}
			t0 := l.Topics[0]
			c := byTopic0[t0]
			c[0]++
			for i, t := range l.Topics {
				topics++
				posAll[i]++
				emit("topic", suffix(append([]byte(store.PrefixTopic), t[:]...), txnum))
				if i == 0 || !addrShaped(t) {
					continue
				}
				addrTopics++
				posAddr[i]++
				c[1]++
				a := common.BytesToAddress(t[12:])
				k3 := append(append([]byte("tlog/"), a[:]...), t0[:]...)
				emit("tlog3", suffix(k3, txnum))
				emit("tlog4", suffix(append(append([]byte(nil), k3...), l.Address[:]...), txnum))
				transfer := (t0 == sigTransfer && (i == 1 || i == 2)) || ((t0 == sigSingle || t0 == sigBatch) && (i == 2 || i == 3))
				if !transfer {
					continue
				}
				transferPosts++
				emit("hold", append(append([]byte(nil), a[:]...), l.Address[:]...))
				holderPosts[a]++
				if t0 == sigTransfer && len(l.Topics) == 4 { // ERC721
					n := nftHolder[a]
					if i == 2 {
						n[0]++
					} else {
						n[1]++
					}
					nftHolder[a] = n
					emit("nftid", append(append([]byte(nil), l.Address[:]...), l.Topics[3][:]...))
				}
			}
			byTopic0[t0] = c
		}
		return true
	})
	if err != nil {
		panic(err)
	}
	fmt.Printf("txs=%d logs=%d topics=%d addr-shaped(pos1-3)=%d\n", txs, logs, topics, addrTopics)
	for i := 1; i < 4; i++ {
		fmt.Printf("  pos%d: %d topics, %d addr-shaped (%.1f%%)\n", i, posAll[i], posAddr[i], pct(posAddr[i], posAll[i]))
	}
	type kv struct {
		h common.Hash
		c [2]uint64
	}
	var top []kv
	for h, c := range byTopic0 {
		top = append(top, kv{h, c})
	}
	sort.Slice(top, func(i, j int) bool { return top[i].c[1] > top[j].c[1] })
	fmt.Println("top topic0 by addr-shaped postings (logs, addr-topics, share of all addr-topics):")
	var cum uint64
	for i := 0; i < 20 && i < len(top); i++ {
		cum += top[i].c[1]
		n := names[top[i].h]
		if n == "" {
			n = hex.EncodeToString(top[i].h[:4])
		}
		fmt.Printf("  %-60s %10d %10d %5.1f%% cum %5.1f%%\n", n, top[i].c[0], top[i].c[1], pct(top[i].c[1], addrTopics), pct(cum, addrTopics))
	}
	for _, w := range spill {
		w.Flush()
	}
	sorted := func(n string, uniq bool) string {
		args := []string{"-S", "6G", "--parallel", "8", "-T", scratch, "-o", filepath.Join(scratch, n+".sorted")}
		if uniq {
			args = append(args, "-u")
		}
		c := exec.Command("sort", append(args, filepath.Join(scratch, n+".hex"))...)
		c.Env = append(os.Environ(), "LC_ALL=C")
		c.Stderr = os.Stderr
		if err := c.Run(); err != nil {
			panic(err)
		}
		return filepath.Join(scratch, n+".sorted")
	}
	lines := func(p string) uint64 {
		out, err := exec.Command("wc", "-l", p).Output()
		if err != nil {
			panic(err)
		}
		var n uint64
		fmt.Sscan(string(out), &n)
		return n
	}
	fmt.Printf("transfer-family postings=%d (%.1f%% of addr-shaped) distinct (holder,emitter) pairs=%d\n", transferPosts, pct(transferPosts, addrTopics), lines(sorted("hold", true)))
	type hp struct {
		a common.Address
		n uint64
	}
	var hs []hp
	for a, n := range holderPosts {
		hs = append(hs, hp{a, n})
	}
	sort.Slice(hs, func(i, j int) bool { return hs[i].n > hs[j].n })
	fmt.Println("top holders by transfer postings (postings, distinct emitters):")
	for i := 0; i < 8 && i < len(hs); i++ {
		out, _ := exec.Command("bash", "-c", "grep -c ^"+hex.EncodeToString(hs[i].a[:])+" "+filepath.Join(scratch, "hold.sorted")).Output()
		fmt.Printf("  %s %9d %6s\n", hs[i].a, hs[i].n, strings.TrimSpace(string(out)))
	}
	fmt.Printf("holders=%d p50/p99/max postings per holder: ", len(hs))
	if len(hs) > 0 {
		fmt.Printf("%d/%d/%d\n", hs[len(hs)/2].n, hs[len(hs)/100].n, hs[0].n)
	}
	var nh []hp
	for a, n := range nftHolder {
		nh = append(nh, hp{a, n[0] + n[1]})
	}
	sort.Slice(nh, func(i, j int) bool { return nh[i].n > nh[j].n })
	fmt.Printf("ERC721: distinct (contract,id)=%d holders=%d; top holders (in+out, net in-out):\n", lines(sorted("nftid", true)), len(nh))
	for i := 0; i < 8 && i < len(nh); i++ {
		n := nftHolder[nh[i].a]
		fmt.Printf("  %s %9d %9d\n", nh[i].a, nh[i].n, int64(n[0])-int64(n[1]))
	}

	for _, c := range []struct {
		name, file string
	}{{"topic/ (existing, rebuilt)", "topic"}, {"tlog/addr/topic0/txnum", "tlog3"}, {"tlog/addr/topic0/emitter/txnum", "tlog4"}} {
		sf, err := os.Open(sorted(c.file, true))
		if err != nil {
			panic(err)
		}
		sc := bufio.NewScanner(sf)
		sc.Buffer(make([]byte, 1<<20), 1<<20)
		var rows uint64
		w, err := store.NewRunWriter(filepath.Join(scratch, "x"), [32]byte{}, 0)
		if err != nil {
			panic(err)
		}
		for s := store.SecChain; s <= store.SecLookup; s++ {
			if err := w.Begin(s); err != nil {
				panic(err)
			}
			if s == store.SecLookup {
				for sc.Scan() {
					r, _ := hex.DecodeString(sc.Text())
					rows++
					if err := w.Set(r, []byte{1}); err != nil {
						panic(err)
					}
				}
			}
			if err := w.End(); err != nil {
				panic(err)
			}
		}
		_, ft, err := w.Finish(local, f.FromTx, f.ToTx, f.FromHeight, f.ToHeight)
		if err != nil {
			panic(err)
		}
		sf.Close()
		fmt.Printf("%-36s rows=%9d sst=%6.1fMB  %.1f B/row\n", c.name, rows, float64(ft.Len[store.SecLookup])/1e6, float64(ft.Len[store.SecLookup])/float64(rows))
	}
}

func suffix(k []byte, n uint64) []byte { return binary.BigEndian.AppendUint64(k, n) }
func pct(a, b uint64) float64 {
	if b == 0 {
		return 0
	}
	return 100 * float64(a) / float64(b)
}
