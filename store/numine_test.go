package store

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/containerman17/epochdb/dist"
)

// TestNumineReadThrough is the read-through measurement on a REAL corpus
// (DESIGN: "the target machine cannot hold 700GB"). It publishes an existing
// numine data dir's runs into MinIO, opens a FRESH data dir against the bucket
// with nothing local but the manifest, and reports what serving costs: resident
// bytes against corpus bytes, and chunks fetched per cold query shape.
//
// Skipped unless both EPOCHDB_MINIO_ENDPOINT and EPOCHDB_CORPUS_DIR are set:
//
//	EPOCHDB_CORPUS_DIR=$PWD/data-numine2 go test -run TestNumineReadThrough -v ./store/
func TestNumineReadThrough(t *testing.T) {
	endpoint := requireMinio(t)
	corpus := os.Getenv("EPOCHDB_CORPUS_DIR")
	if corpus == "" {
		t.Skip("set EPOCHDB_CORPUS_DIR to a data dir holding storage v0 runs")
	}
	man, err := loadManifest(corpus)
	if err != nil {
		t.Fatal(err)
	}
	if len(man.Runs) == 0 {
		t.Fatalf("%s holds no runs", corpus)
	}
	root, err := decodeRoot(man.ChainRoot)
	if err != nil {
		t.Fatal(err)
	}

	// SAMPLE REAL KEYS out of the corpus locally, before anything is published:
	// a cold read is only a measurement if it asks for something that exists.
	local, err := dist.Local(corpus)
	if err != nil {
		t.Fatal(err)
	}
	src, err := Open(corpus, local, root)
	if err != nil {
		t.Fatal(err)
	}
	srcRuns, srcDone := src.snapshot()
	defer srcDone()
	oldest := srcRuns[0]
	var slotKey, txhKey []byte
	if err := oldest.ScanRange(SecState, []byte(PrefixState), []byte("state0"), func(k, v []byte) bool {
		if len(k) > 27 && k[27] == 's' && len(v) > 0 {
			slotKey = append([]byte(nil), k...)
			return false
		}
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if err := oldest.ScanRange(SecLookup, []byte(PrefixTxHash), []byte("txi"), func(k, v []byte) bool {
		txhKey = append([]byte(nil), k...)
		return false
	}); err != nil {
		t.Fatal(err)
	}
	if slotKey == nil || txhKey == nil {
		t.Fatalf("could not sample keys out of run %s", oldest.Name)
	}
	firstHeight, lastTx := oldest.Footer.FromHeight, oldest.Footer.ToTx-1

	// THE LOCAL ANSWERS, taken before anything is published. Every one of them
	// is asked again below out of a cold bucket and must come back identical:
	// read-through is a transport, never a different answer.
	type answer struct {
		what string
		val  []byte
	}
	want := []answer{}
	add := func(what string, v []byte, ok bool, err error) {
		if err != nil || !ok {
			t.Fatalf("local %s: %v %v", what, ok, err)
		}
		want = append(want, answer{what, append([]byte(nil), v...)})
	}
	heights := []uint64{firstHeight, man.Runs[len(man.Runs)/2].FromHeight, man.Runs[len(man.Runs)-1].ToHeight}
	for _, h := range heights {
		v, ok, err := src.HeaderRLP(h)
		add(fmt.Sprintf("header %d", h), v, ok, err)
		p, ok, err := src.Pvm(h)
		add(fmt.Sprintf("pvm %d", h), p, ok, err)
	}
	for _, n := range []uint64{0, lastTx, man.Runs[len(man.Runs)-1].ToTx - 1} {
		v, ok, err := src.TxRLP(n)
		add(fmt.Sprintf("tx %d", n), v, ok, err)
		r, ok, err := src.Receipt(n)
		add(fmt.Sprintf("receipt %d", n), r, ok, err)
	}
	sv, _, ok, err := oldest.Latest(SecState, slotKey[:len(slotKey)-8], TxNumOf(slotKey))
	if err != nil || !ok {
		t.Fatalf("local slot: %v %v", ok, err)
	}
	want = append(want, answer{"state slot", sv})

	src.Close()
	local.Close()

	// PUBLISH. Sync unlinks the spool copy it uploads, so the corpus is
	// hardlinked into a throwaway dir first (wiki:
	// how_to_run_the_minio_s3_roundtrip_test.md).
	c := newCounter(t, endpoint)
	prefix := fmt.Sprintf("numine/%d/", time.Now().UnixNano())
	pub := t.TempDir()
	if err := os.MkdirAll(filepath.Join(pub, "cas"), 0o755); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("cp", "-al", filepath.Join(corpus, "cas")+"/.", filepath.Join(pub, "cas")+"/").CombinedOutput(); err != nil {
		t.Fatalf("hardlink the corpus into %s: %v: %s", pub, err, out)
	}
	minioEnv(t, c.srv.URL, prefix)
	pubStore, err := dist.Open(pub)
	if err != nil {
		t.Fatal(err)
	}
	t0 := time.Now()
	if _, err := pubStore.Sync(); err != nil {
		t.Fatal(err)
	}
	pubStore.Close()
	t.Logf("published %d runs to the bucket in %s", len(man.Runs), time.Since(t0).Round(time.Millisecond))

	// A FRESH DATA DIR: manifest only, no spool, no cache.
	cold := t.TempDir()
	if err := man.save(cold); err != nil {
		t.Fatal(err)
	}
	cas, err := dist.Open(cold)
	if err != nil {
		t.Fatal(err)
	}
	defer cas.Close()

	since := c.reset()
	t0 = time.Now()
	db, err := Open(cold, cas, root)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	openGets, openChunks := since()
	openDur := time.Since(t0)

	var resident uint64
	runs, done := db.snapshot()
	defer done()
	for _, r := range runs {
		resident += r.ResidentBytes()
	}
	sizes := db.SectionSizes()
	total := sizes[SecChain] + sizes[SecState] + sizes[SecLookup]
	t.Logf("corpus %.1f MB (chain %.1f, state %.1f, lookup %.1f) over %d runs",
		mb(total), mb(sizes[SecChain]), mb(sizes[SecState]), mb(sizes[SecLookup]), len(runs))
	t.Logf("COLD OPEN: %s, %d ranged GETs / %d chunks; RESIDENT %.2f MB = %.3f%% of the corpus",
		openDur.Round(time.Millisecond), openGets, openChunks, mb(resident), 100*float64(resident)/float64(total))

	probes := []struct {
		name string
		run  func() error
	}{
		{"state slot", func() error {
			pfx := slotKey[:len(slotKey)-8]
			v, ok, err := db.latest(SecState, pfx, TxNumOf(slotKey))
			if err != nil || !ok {
				return fmt.Errorf("%x %v %v", v, ok, err)
			}
			return nil
		}},
		{"tx by hash", func() error {
			n, ok, err := db.TxNumByHash(txhKey[len(PrefixTxHash):])
			if err != nil || !ok {
				return fmt.Errorf("%d %v %v", n, ok, err)
			}
			return nil
		}},
		{"tx body", func() error {
			v, ok, err := db.TxRLP(lastTx)
			if err != nil || !ok || len(v) == 0 {
				return fmt.Errorf("%x %v %v", v, ok, err)
			}
			return nil
		}},
		{"receipt", func() error {
			v, ok, err := db.Receipt(lastTx)
			if err != nil || !ok || len(v) == 0 {
				return fmt.Errorf("%x %v %v", v, ok, err)
			}
			return nil
		}},
		{"header", func() error {
			v, ok, err := db.HeaderRLP(firstHeight)
			if err != nil || !ok || len(v) == 0 {
				return fmt.Errorf("%x %v %v", v, ok, err)
			}
			return nil
		}},
		{"absent tx hash", func() error {
			_, ok, err := db.TxNumByHash(hash32(0xfe))
			if err != nil || ok {
				return fmt.Errorf("a hash nobody wrote resolved: %v %v", ok, err)
			}
			return nil
		}},
	}
	for _, p := range probes {
		since := c.reset()
		t0 := time.Now()
		if err := p.run(); err != nil {
			t.Fatalf("cold %s: %v", p.name, err)
		}
		gets, chunks := since()
		t.Logf("cold %-14s: %d chunks (%d ranged GETs) in %s", p.name, chunks, gets, time.Since(t0).Round(time.Millisecond))
		if chunks > 1 {
			t.Fatalf("cold %s cost %d chunks, DESIGN says at most one", p.name, chunks)
		}
		since = c.reset()
		if err := p.run(); err != nil {
			t.Fatalf("warm %s: %v", p.name, err)
		}
		if gets, chunks := since(); gets != 0 || chunks != 0 {
			t.Fatalf("warm %s went back to the bucket: %d GETs / %d chunks", p.name, gets, chunks)
		}
	}

	// READ-THROUGH CORRECTNESS: cold answers equal local answers.
	i := 0
	same := func(what string, v []byte, ok bool, err error) {
		if err != nil || !ok {
			t.Fatalf("cold %s: %v %v", what, ok, err)
		}
		if want[i].what != what {
			t.Fatalf("probe order drifted: %s vs %s", want[i].what, what)
		}
		if !bytes.Equal(want[i].val, v) {
			t.Fatalf("cold %s differs from the local answer: %d vs %d bytes", what, len(v), len(want[i].val))
		}
		i++
	}
	for _, h := range heights {
		v, ok, err := db.HeaderRLP(h)
		same(fmt.Sprintf("header %d", h), v, ok, err)
		p, ok, err := db.Pvm(h)
		same(fmt.Sprintf("pvm %d", h), p, ok, err)
	}
	for _, n := range []uint64{0, lastTx, man.Runs[len(man.Runs)-1].ToTx - 1} {
		v, ok, err := db.TxRLP(n)
		same(fmt.Sprintf("tx %d", n), v, ok, err)
		r, ok, err := db.Receipt(n)
		same(fmt.Sprintf("receipt %d", n), r, ok, err)
	}
	cv, ok, err := db.latest(SecState, slotKey[:len(slotKey)-8], TxNumOf(slotKey))
	same("state slot", cv, ok, err)
	t.Logf("%d answers served out of the cold bucket are byte-identical to the local corpus", len(want))

}

func mb(n uint64) float64 { return float64(n) / (1 << 20) }
