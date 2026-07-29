package main

// Distribution: one publisher, few consumers, plain HTTP (user ruling
// 2026-07-29, DESIGN.md "Distribution"). The torrent stack (dist/, anacrolix,
// `epochdb seed`, DHT/trackers, infohashes) is deleted: a swarm was a worse
// HTTP server for this shape. Publishing = upload the files to object storage
// and ship the manifest; consuming = GET what is missing, sha256-verify,
// tmp+rename. Epochs are bit-identical across builders, so the sha256s stay
// stable facts anyone can reproduce.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/containerman17/epochdb/state"
)

// ManifestFile is one published file and its identity.
type ManifestFile struct {
	Name   string `json:"name"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"` // hex
}

// Manifest is all epochs plus the ONE newest snapshot (null when the
// publisher has none). No ladder: a client takes the pair it is given.
type Manifest struct {
	Epochs   []ManifestFile `json:"epochs"`
	Snapshot *ManifestFile  `json:"snapshot"`
}

// Files returns the snapshot (if any) first, then the epochs: the snapshot is
// the floor a limited-history node needs before anything else is useful.
func (m *Manifest) Files() []ManifestFile {
	var out []ManifestFile
	if m.Snapshot != nil {
		out = append(out, *m.Snapshot)
	}
	return append(out, m.Epochs...)
}

func loadManifest(path string) (*Manifest, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var m Manifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("manifest %s: %w", path, err)
	}
	return &m, nil
}

func writeManifest(path string, m *Manifest) error {
	raw, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(raw, '\n'), 0o644)
}

// hashFile returns the manifest entry for path.
func hashFile(path string) (ManifestFile, error) {
	f, err := os.Open(path)
	if err != nil {
		return ManifestFile{}, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return ManifestFile{}, err
	}
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return ManifestFile{}, err
	}
	return ManifestFile{Name: filepath.Base(path), Size: st.Size(), SHA256: hex.EncodeToString(h.Sum(nil))}, nil
}

// buildManifest hashes every epoch in dir (parallel) plus the base file if
// one is there.
func buildManifest(dir string) (*Manifest, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "epoch_*.epoch"))
	if err != nil {
		return nil, err
	}
	sort.Strings(paths)
	m := &Manifest{Epochs: make([]ManifestFile, len(paths))}
	errs := make([]error, len(paths))
	sem := make(chan struct{}, 8)
	var wg sync.WaitGroup
	for i, p := range paths {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			m.Epochs[i], errs[i] = hashFile(p)
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			return nil, fmt.Errorf("%s: %w", paths[i], err)
		}
	}
	if b, ok, err := state.PeekBase(dir); err != nil {
		return nil, err
	} else if ok {
		snap, err := hashFile(filepath.Join(dir, state.BaseFileName(b)))
		if err != nil {
			return nil, err
		}
		m.Snapshot = &snap
	}
	return m, nil
}

// fetchFile GETs baseURL/name into dir via a temp file, checking size and
// sha256 before the rename. A mismatch leaves nothing behind.
func fetchFile(baseURL, dir, tmpDir string, e ManifestFile) error {
	u, err := url.JoinPath(baseURL, e.Name)
	if err != nil {
		return err
	}
	resp, err := http.Get(u)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: %s", u, resp.Status)
	}
	tmp, err := os.CreateTemp(tmpDir, e.Name+".*")
	if err != nil {
		return err
	}
	defer func() {
		tmp.Close()
		os.Remove(tmp.Name()) // no-op once renamed
	}()
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(tmp, h), resp.Body)
	if err != nil {
		return err
	}
	if n != e.Size {
		return fmt.Errorf("%s: got %d bytes, manifest says %d", e.Name, n, e.Size)
	}
	if sum := hex.EncodeToString(h.Sum(nil)); sum != e.SHA256 {
		return fmt.Errorf("%s: sha256 %s, manifest says %s", e.Name, sum, e.SHA256)
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), filepath.Join(dir, e.Name))
}

// fetchMissing downloads every manifest file absent from dataDir, retrying
// each up to attempts times. onFile fires for each file that becomes locally
// present, already-there ones included (the --verify cursor's feed).
func fetchMissing(dataDir, baseURL string, m *Manifest, attempts int, onFile func(name string)) error {
	tmpDir := filepath.Join(dataDir, ".epochfetch")
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		return err
	}
	defer os.RemoveAll(tmpDir)

	var want []ManifestFile
	for _, e := range m.Files() {
		if _, err := os.Stat(filepath.Join(dataDir, e.Name)); err == nil {
			if onFile != nil {
				onFile(e.Name)
			}
			continue
		}
		want = append(want, e)
	}
	log.Printf("bootstrap: %d files present, %d to fetch from %s", len(m.Files())-len(want), len(want), baseURL)
	if len(want) == 0 {
		return nil
	}

	sem := make(chan struct{}, 4) // ponytail: fixed fan-out; flag it if it matters
	var wg sync.WaitGroup
	var mu sync.Mutex
	var failed []string
	for _, e := range want {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			var err error
			for try := 1; try <= attempts; try++ {
				if err = fetchFile(baseURL, dataDir, tmpDir, e); err == nil {
					break
				}
				log.Printf("bootstrap: %s: attempt %d/%d: %v", e.Name, try, attempts, err)
				time.Sleep(time.Duration(try) * time.Second)
			}
			if err != nil {
				mu.Lock()
				failed = append(failed, e.Name)
				mu.Unlock()
				return
			}
			log.Printf("bootstrap: %s fetched and verified (%.1f GB)", e.Name, float64(e.Size)/1e9)
			if onFile != nil {
				onFile(e.Name)
			}
		}()
	}
	wg.Wait()
	if len(failed) > 0 {
		sort.Strings(failed)
		return fmt.Errorf("bootstrap incomplete: %d/%d files still missing: %v", len(failed), len(m.Files()), failed)
	}
	return nil
}

// defaultManifestPath keeps the historical file name (JSON content since
// 2026-07-29, when the infohash column died).
func defaultManifestPath(dataDir, given string) string {
	if given != "" {
		return given
	}
	return filepath.Join(dataDir, "epochs.manifest")
}

func trimURL(s string) string { return strings.TrimRight(s, "/") }
