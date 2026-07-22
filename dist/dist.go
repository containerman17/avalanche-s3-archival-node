// Package dist distributes sealed epoch files over BitTorrent. Epochs are
// deterministic and bit-identical across builders, so their infohashes are
// stable facts shipped in a committed manifest: seeders serve whatever
// epochs they have locally, leechers fetch expected-but-missing epochs by
// magnet (DHT + a couple of public trackers) at startup.
package dist

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/storage"
)

// PieceLength suits 10-15MB epochs now and multi-GB epochs later.
const PieceLength = 4 << 20

// Trackers boost peer discovery on top of DHT.
var Trackers = []string{
	"udp://tracker.opentrackr.org:1337/announce",
	"udp://tracker.openbittorrent.com:6969/announce",
}

// Entry is one manifest line: an epoch file and its stable identities.
type Entry struct {
	Name     string
	Size     int64
	SHA256   string // hex
	Infohash string // hex btih (v1 single-file torrent, PieceLength pieces)
}

// LoadManifest reads "name size sha256 btih" lines ('#' comments allowed).
func LoadManifest(path string) ([]Entry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []Entry
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fld := strings.Fields(line)
		if len(fld) != 4 {
			return nil, fmt.Errorf("manifest: bad line %q", line)
		}
		size, err := strconv.ParseInt(fld[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("manifest: bad size in %q", line)
		}
		out = append(out, Entry{Name: fld[0], Size: size, SHA256: fld[2], Infohash: fld[3]})
	}
	return out, sc.Err()
}

// WriteManifest writes entries sorted by name.
func WriteManifest(path string, entries []Entry) error {
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
	var b strings.Builder
	b.WriteString("# epochdb sealed-epoch manifest: name size sha256 btih\n")
	b.WriteString(fmt.Sprintf("# piece length %d, v1 single-file torrents; epochs are bit-identical, hashes are stable\n", PieceLength))
	for _, e := range entries {
		fmt.Fprintf(&b, "%s %d %s %s\n", e.Name, e.Size, e.SHA256, e.Infohash)
	}
	return os.WriteFile(path, []byte(b.String()), 0o644)
}

// fileEntry hashes one epoch file into a manifest Entry (sha256 + v1
// torrent infohash in a single read pass via TeeReader).
func fileEntry(path string) (Entry, error) {
	name := filepath.Base(path)
	f, err := os.Open(path)
	if err != nil {
		return Entry{}, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return Entry{}, err
	}
	whole := sha256.New()
	info := metainfo.Info{Name: name, PieceLength: PieceLength, Length: st.Size()}
	if err := info.GeneratePieces(func(metainfo.FileInfo) (io.ReadCloser, error) {
		return io.NopCloser(io.TeeReader(f, whole)), nil
	}); err != nil {
		return Entry{}, err
	}
	ib, err := bencode.Marshal(info)
	if err != nil {
		return Entry{}, err
	}
	return Entry{
		Name:     name,
		Size:     st.Size(),
		SHA256:   hex.EncodeToString(whole.Sum(nil)),
		Infohash: metainfo.HashBytes(ib).HexString(),
	}, nil
}

// BuildManifest hashes every epoch_*.epoch in dir (parallel).
func BuildManifest(dir string) ([]Entry, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "epoch_*.epoch"))
	if err != nil {
		return nil, err
	}
	entries := make([]Entry, len(paths))
	errs := make([]error, len(paths))
	sem := make(chan struct{}, 8)
	var wg sync.WaitGroup
	for i, p := range paths {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			entries[i], errs[i] = fileEntry(p)
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			return nil, fmt.Errorf("%s: %w", paths[i], err)
		}
	}
	return entries, nil
}

func sha256File(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// ClientOpts tune the torrent client for tests (localhost two-node runs).
type ClientOpts struct {
	ListenPort  int  // 0 = random
	NoDHT       bool // tests: explicit peers only
	NoTrackers  bool // tests
	NoUpnp      bool
	DisableIPv6 bool
}

func newClient(dataDir string, o ClientOpts) (*torrent.Client, error) {
	cfg := torrent.NewDefaultClientConfig()
	cfg.DataDir = dataDir
	cfg.Seed = true
	cfg.ListenPort = o.ListenPort
	cfg.NoDHT = o.NoDHT
	cfg.DisableTrackers = o.NoTrackers
	cfg.NoDefaultPortForwarding = o.NoUpnp
	cfg.DisableIPv6 = o.DisableIPv6
	return torrent.NewClient(cfg)
}

// Seed verifies (sha256) and seeds every manifest epoch present in dataDir,
// logging peer/upload stats every 60s until the process is killed.
func Seed(dataDir, manifestPath string, opts ClientOpts) error {
	entries, err := LoadManifest(manifestPath)
	if err != nil {
		return err
	}
	cl, err := newClient(dataDir, opts)
	if err != nil {
		return err
	}
	defer cl.Close()

	var torrents []*torrent.Torrent
	for _, e := range entries {
		path := filepath.Join(dataDir, e.Name)
		if _, err := os.Stat(path); err != nil {
			continue // not local: nothing to seed
		}
		sum, err := sha256File(path)
		if err != nil {
			return err
		}
		if sum != e.SHA256 {
			log.Printf("seed: SKIP %s: sha256 mismatch (local %s, manifest %s)", e.Name, sum[:12], e.SHA256[:12])
			continue
		}
		mi, ih, err := metainfoFor(path)
		if err != nil {
			return fmt.Errorf("%s: %w", e.Name, err)
		}
		if ih != e.Infohash {
			log.Printf("seed: SKIP %s: infohash mismatch (built %s, manifest %s)", e.Name, ih, e.Infohash)
			continue
		}
		t, err := cl.AddTorrent(mi)
		if err != nil {
			return fmt.Errorf("%s: %w", e.Name, err)
		}
		torrents = append(torrents, t)
	}
	log.Printf("seed: seeding %d/%d manifest epochs from %s (listen port %d)",
		len(torrents), len(entries), dataDir, cl.LocalPort())
	for {
		time.Sleep(60 * time.Second)
		var up int64
		peers := map[string]bool{}
		for _, t := range torrents {
			st := t.Stats()
			up += st.BytesWrittenData.Int64()
			for _, pc := range t.PeerConns() {
				peers[pc.RemoteAddr.String()] = true
			}
		}
		log.Printf("seed: %d torrents, %d unique peers, %.1f MB uploaded total", len(torrents), len(peers), float64(up)/1e6)
	}
}

// metainfoFor builds the deterministic v1 single-file metainfo for path and
// returns it with its hex infohash.
func metainfoFor(path string) (*metainfo.MetaInfo, string, error) {
	info := metainfo.Info{PieceLength: PieceLength}
	if err := info.BuildFromFilePath(path); err != nil {
		return nil, "", err
	}
	ib, err := bencode.Marshal(info)
	if err != nil {
		return nil, "", err
	}
	mi := &metainfo.MetaInfo{InfoBytes: ib}
	for _, tr := range Trackers {
		mi.AnnounceList = append(mi.AnnounceList, []string{tr})
	}
	return mi, metainfo.HashBytes(ib).HexString(), nil
}

// FetchOpts control the startup leech pass.
type FetchOpts struct {
	Timeout time.Duration // per-epoch give-up
	Peer    string        // optional explicit host:port seeder (tests, boost)
	Max     int           // fetch at most N missing epochs (0 = all; tests)
	Client  ClientOpts
	// OnEpoch, when set, is called with each manifest epoch's file name as
	// it becomes locally present during Bootstrap: immediately for epochs
	// already on disk, after the sha256-verified rename for fetched ones.
	// Called from multiple goroutines; must not block for long.
	OnEpoch func(name string)
}

// FetchMissing leeches manifest epochs absent from dataDir into place
// (tmp dir + sha256 verify + rename). Returns the names still missing
// after the pass. Failures are per-epoch, never fatal.
func FetchMissing(dataDir, manifestPath string, o FetchOpts) (missing []string, err error) {
	entries, err := LoadManifest(manifestPath)
	if err != nil {
		return nil, err
	}
	var want []Entry
	for _, e := range entries {
		if _, err := os.Stat(filepath.Join(dataDir, e.Name)); err != nil {
			want = append(want, e)
		}
	}
	if len(want) == 0 {
		return nil, nil
	}
	if o.Max > 0 && len(want) > o.Max {
		want = want[:o.Max]
	}
	if o.Timeout <= 0 {
		o.Timeout = 5 * time.Minute
	}
	tmp := filepath.Join(dataDir, ".epochfetch")
	if err := os.MkdirAll(tmp, 0o755); err != nil {
		return nil, err
	}
	cl, err := newClient(tmp, o.Client)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	log.Printf("fetch-epochs: %d missing epochs, per-epoch timeout %s", len(want), o.Timeout)

	sem := make(chan struct{}, 4) // ponytail: fixed fan-out; flag it if it matters
	var wg sync.WaitGroup
	var mu sync.Mutex
	for _, e := range want {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			if err := fetchOne(cl, dataDir, tmp, e, o); err != nil {
				log.Printf("fetch-epochs: %s: %v", e.Name, err)
				mu.Lock()
				missing = append(missing, e.Name)
				mu.Unlock()
				return
			}
			log.Printf("fetch-epochs: %s fetched and verified", e.Name)
		}()
	}
	wg.Wait()
	os.RemoveAll(tmp)
	sort.Strings(missing)
	return missing, nil
}

// Bootstrap leeches until the local set exactly matches the manifest,
// retrying failed epochs in rounds (this mode's whole job is
// completeness), and seeds every verified epoch while at it. Returns nil
// only when nothing is missing; errors with a summary after maxRounds.
func Bootstrap(dataDir, manifestPath string, maxRounds int, o FetchOpts) error {
	entries, err := LoadManifest(manifestPath)
	if err != nil {
		return err
	}
	if o.Timeout <= 0 {
		o.Timeout = 5 * time.Minute
	}
	tmp := filepath.Join(dataDir, ".epochfetch")
	if err := os.MkdirAll(tmp, 0o755); err != nil {
		return err
	}
	defer os.RemoveAll(tmp)
	cl, err := newClient(tmp, o.Client)
	if err != nil {
		return err
	}
	defer cl.Close()
	seedStore := storage.NewFile(dataDir) // seed torrents read the real files
	defer seedStore.Close()
	seed := func(e Entry) {
		mi, ih, err := metainfoFor(filepath.Join(dataDir, e.Name))
		if err != nil || ih != e.Infohash {
			log.Printf("bootstrap: not seeding %s: err=%v infohash=%s want %s", e.Name, err, ih, e.Infohash)
			return
		}
		spec := torrent.TorrentSpecFromMetaInfo(mi)
		spec.Storage = seedStore
		if _, _, err := cl.AddTorrentSpec(spec); err != nil {
			log.Printf("bootstrap: not seeding %s: %v", e.Name, err)
		}
	}

	var missing []Entry
	for _, e := range entries {
		if _, err := os.Stat(filepath.Join(dataDir, e.Name)); err != nil {
			missing = append(missing, e)
		} else {
			seed(e) // be a good citizen: present epochs seed immediately
			if o.OnEpoch != nil {
				o.OnEpoch(e.Name)
			}
		}
	}
	log.Printf("bootstrap: %d/%d epochs present, %d to fetch (listen port %d)",
		len(entries)-len(missing), len(entries), len(missing), cl.LocalPort())

	for round := 1; len(missing) > 0 && round <= maxRounds; round++ {
		sem := make(chan struct{}, 4)
		var wg sync.WaitGroup
		var mu sync.Mutex
		var failed []Entry
		for _, e := range missing {
			wg.Add(1)
			go func() {
				defer wg.Done()
				sem <- struct{}{}
				defer func() { <-sem }()
				if err := fetchOne(cl, dataDir, tmp, e, o); err != nil {
					log.Printf("bootstrap: round %d: %s: %v", round, e.Name, err)
					mu.Lock()
					failed = append(failed, e)
					mu.Unlock()
					return
				}
				log.Printf("bootstrap: %s fetched and verified", e.Name)
				seed(e)
				if o.OnEpoch != nil {
					o.OnEpoch(e.Name)
				}
			}()
		}
		wg.Wait()
		log.Printf("bootstrap: round %d done: %d fetched, %d still missing", round, len(missing)-len(failed), len(failed))
		missing = failed
	}
	if len(missing) > 0 {
		names := make([]string, len(missing))
		for i, e := range missing {
			names[i] = e.Name
		}
		sort.Strings(names)
		return fmt.Errorf("bootstrap incomplete after %d rounds: %d/%d epochs still missing: %v", maxRounds, len(missing), len(entries), names)
	}
	log.Printf("bootstrap: complete, local set matches the manifest (%d epochs)", len(entries))
	return nil
}

func fetchOne(cl *torrent.Client, dataDir, tmp string, e Entry, o FetchOpts) error {
	var ih metainfo.Hash
	if err := ih.FromHexString(e.Infohash); err != nil {
		return err
	}
	m := metainfo.Magnet{InfoHash: ih, DisplayName: e.Name, Trackers: Trackers}
	t, err := cl.AddMagnet(m.String())
	if err != nil {
		return err
	}
	defer t.Drop()
	if o.Peer != "" {
		host, port, err := net.SplitHostPort(o.Peer)
		if err != nil {
			return err
		}
		p, _ := strconv.Atoi(port)
		t.AddPeers([]torrent.PeerInfo{{Addr: ipPortAddr{net.ParseIP(host), p}}})
	}
	deadline := time.After(o.Timeout)
	select {
	case <-t.GotInfo():
	case <-deadline:
		return fmt.Errorf("no metadata within %s", o.Timeout)
	}
	t.DownloadAll()
	for t.BytesCompleted() < e.Size {
		select {
		case <-deadline:
			return fmt.Errorf("incomplete after %s: %d/%d bytes", o.Timeout, t.BytesCompleted(), e.Size)
		case <-time.After(time.Second):
		}
	}
	got := filepath.Join(tmp, e.Name)
	sum, err := sha256File(got)
	if err != nil {
		return err
	}
	if sum != e.SHA256 {
		os.Remove(got)
		return fmt.Errorf("sha256 mismatch after download")
	}
	return os.Rename(got, filepath.Join(dataDir, e.Name))
}

type ipPortAddr struct {
	ip   net.IP
	port int
}

func (a ipPortAddr) Network() string { return "" }
func (a ipPortAddr) String() string  { return net.JoinHostPort(a.ip.String(), strconv.Itoa(a.port)) }
