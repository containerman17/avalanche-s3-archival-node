// Package dist is epochdb's artifact layer (DESIGN.md "Distribution").
//
// A SEALED EPOCH IS NAMED BY SHA256 OF ITS OWN PER-CHUNK HASH LIST, which
// casfs stores in a tail past the content and never shows anyone (see
// casfs/chunked.go). A producer streams the epoch, appends that tail, and
// renames the file onto the casfs spool path for the resulting name; the
// rename is the entire registration. A ticker uploads whatever the spool holds
// and unlinks the local copy once the bucket confirms it. Reads go through one
// call, Open(hash), and every 4MB chunk they touch is verified against the
// list before it is served.
//
// THE EPOCH FORMAT IS UNCHANGED BY ANY OF THAT. The tail is casfs's metadata,
// so an epoch is still L bytes with its footer LAST: Blob.Size() is L on both
// roads, and a reader seeking the footer seeks from L rather than from the end
// of the file on disk.
//
// S3 CREDENTIALS ARE OPTIONAL and that is not a special case anywhere above
// this file: with no endpoint configured there is no casfs store at all, the
// spool directory IS the node's storage, Sync is a no-op, and reads mmap the
// spool file. With credentials, reads go through casfs (chunk cache, ranged
// GETs for whatever is no longer local). Both roads hand back a *Blob.
//
// Configuration is environment only (no config system, user directive):
//
//	EPOCHDB_S3_ENDPOINT    scheme://host[:port]; empty = fully local, no S3
//	EPOCHDB_S3_BUCKET      required when the endpoint is set
//	EPOCHDB_S3_ACCESS_KEY  } static keys, the R2 and MinIO path. UNSET means the
//	EPOCHDB_S3_SECRET_KEY  } AWS default chain: env, shared config, SSO, instance
//	                       } role, refresh and session tokens included.
//	EPOCHDB_S3_PREFIX      optional key prefix, used verbatim
//	EPOCHDB_S3_REGION      optional; default the chain's region, else "auto"
//	                       (R2 wants "auto"; MinIO ignores it)
//	EPOCHDB_CACHE_DIR      the chunk cache's root, default <data>/cache. THE ONE
//	                       THING SEVERAL CHAIN PROCESSES SHARE (RULING
//	                       2026-08-04): point every process on a box at one
//	                       directory and they share one elastic LRU.
//	EPOCHDB_CACHE_MIN_FREE bytes of free space the chunk cache stops filling at,
//	                       default 5% of the filesystem
//	EPOCHDB_CACHE_MAX_AGE  a chunk's ceiling age (Go duration), default 720h
package dist

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/containerman17/casfs"
)

const (
	// ChunkSize is the granularity of the casfs chunk cache and of the hash
	// list an artifact's name commits to. One number, so the list describes
	// exactly the ranges a downloader fetches.
	ChunkSize = casfs.DefaultChunkSize

	spoolName = "cas" // <data>/cas/<hash>: durable until uploaded
	// cacheName is the casfs chunk cache, disposable, laid out as
	// <data>/cache/<window>/<chain>/<hash>.<index>. It must NOT collide with a
	// pointer name: casfs owns its cache directory and deletes everything in it
	// that is not one of its own window directories, so a cache dir sharing a
	// name with a published pointer prefix eats that pointer the moment a
	// credentialed store opens. Nothing is published under "cache".
	cacheName = "cache"
	// legacyCacheName was the same thing until 2026-08-03. It is deleted on
	// sight rather than left to rot, because the cache is disposable by
	// construction and a stale one is pure wasted disk.
	legacyCacheName = "chunkcache"
)

// View is a long-lived window onto an artifact, one mapping per 4MB chunk.
// Held for the life of an epoch by the sections every query starts from.
type View = casfs.View

// ViewOf wraps bytes that are already in hand as a View, so a caller that
// parses a structure does not care whether it came from a mapping or a pread.
func ViewOf(b []byte) *View { return casfs.ViewOf(b) }

// LatestPointer names a chain's one mutable object. It is a HINT, never an
// authority: every artifact self-verifies by hash and the epoch footers chain
// back to the chain root.
//
// THE NAME CARRIES THE CHAIN ROOT, and that is not cosmetic: N chains publish
// into one bucket prefix, so an unqualified `latest` meant every chain
// published its tip over the others'. Content is safe there because artifacts
// are named by their
// hash; the pointer is the only mutable name, so it is the only one that needs
// the qualifier. The chain root is the right qualifier because every side
// already holds it: the producer to link epoch 1's footer, the consumer to
// refuse the wrong chain.
func LatestPointer(chainRoot [32]byte) string { return "latest-" + hex.EncodeToString(chainRoot[:]) }

// Store is the node's artifact store. cas nil means no credentials were
// configured: everything stays in the spool and nothing is ever uploaded.
type Store struct {
	dir   string
	spool string
	cas   *sharedCas
}

// ONE CASFS PER CACHE DIRECTORY inside a process, refcounted. Since the window
// cache went in this is a cost question rather than a correctness one: two
// stores over one directory would each run an eviction worker and each keep a
// chunk map, which is duplicated work, but the cache is shared by design and a
// wrong map entry costs an ENOENT and a refetch. Across PROCESSES it is
// explicitly fine, and since RULING 2026-08-04 it is the POINT: one process per
// chain, N processes sharing one EPOCHDB_CACHE_DIR, no coordination at all.
// Both bootstrap paths need the sharing:
// `bootstrap --frontier` opens the artifact store and then a whole state layer
// over the same directory.
var (
	casMu     sync.Mutex
	casShared = map[string]*sharedCas{}
)

type sharedCas struct {
	*casfs.Store
	key  string
	refs int
}

// openCas keys on the cache dir AND the spool, not the cache dir alone: since
// 2026-08-04 two data dirs share one cache root, and they still have their own
// spool, their own namespace and their own not-yet-uploaded artifacts. Two
// opens of the SAME data dir (bootstrap --frontier opens the artifact store and
// then a state layer over it) still land on one store, which is all the
// refcount was ever for.
func openCas(cacheDir, spool string, cfg casfs.Config) (*sharedCas, error) {
	cd, err := filepath.Abs(cacheDir)
	if err != nil {
		return nil, err
	}
	sp, err := filepath.Abs(spool)
	if err != nil {
		return nil, err
	}
	key := cd + "\x00" + sp
	casMu.Lock()
	defer casMu.Unlock()
	if s := casShared[key]; s != nil {
		s.refs++
		return s, nil
	}
	st, err := casfs.New(cfg)
	if err != nil {
		return nil, err
	}
	s := &sharedCas{Store: st, key: key, refs: 1}
	casShared[key] = s
	return s, nil
}

// close drops one reference and stops the eviction worker at the last one.
func (s *sharedCas) close() error {
	casMu.Lock()
	defer casMu.Unlock()
	if s.refs--; s.refs > 0 {
		return nil
	}
	delete(casShared, s.key)
	return s.Store.Close()
}

// CacheRoot is where the chunk cache lives, and it is THE ONLY THING SEVERAL
// CHAIN PROCESSES SHARE (RULING 2026-08-04: one process = one chain). Default
// <data>/cache, i.e. self-contained; EPOCHDB_CACHE_DIR points N processes at
// one directory and they then share one elastic LRU, with zero coordination:
// casfs evicts whole windows and tolerates a chunk vanishing under it, so a
// sibling's eviction costs a refetch and never a wrong answer.
//
// The SPOOL is deliberately NOT shared: it is one chain's durable, not-yet-
// uploaded artifacts, and it stays at <data>/cas with the epoch markers and the
// local `latest` that name them.
// It is exported because it is also the fleet's one RENDEZVOUS POINT: the box
// gate that keeps two frontier builds from running at once locks a file here,
// for the same reason the cache is here, i.e. it is the only directory every
// chain process on a box is already pointed at.
func CacheRoot(dataDir string) string {
	if dir := os.Getenv("EPOCHDB_CACHE_DIR"); dir != "" {
		return dir
	}
	return filepath.Join(dataDir, cacheName)
}

// chainKey names this chain inside the chunk cache
// (<CacheRoot>/<window>/<chainKey>/...). It is THE DATA DIRECTORY'S OWN NAME,
// which needs no plumbing at all and is already the right string: a chain's dir
// is the one thing an operator names it by, so `du -sh <cache>/*/<dir>` and
// `rm -r <cache>/*/<dir>` name a chain the way an operator already does. With
// the default cache root the level is pure legibility, since that root is
// inside the one chain's dir anyway, and the layout stays identical either way.
//
// The chain root hex would be the other candidate, since it qualifies the tip
// pointer, but dist does not have it until SetLatest: it comes from a P-chain
// resolve that happens long after the store is open.
func chainKey(dataDir string) string {
	abs, err := filepath.Abs(dataDir)
	if err != nil {
		abs = dataDir
	}
	if base := filepath.Base(abs); base != "" && base != "." && base != string(filepath.Separator) {
		return base
	}
	return "default"
}

// Open builds the store for a data directory from the environment.
func Open(dataDir string) (*Store, error) {
	s, err := Local(dataDir)
	if err != nil {
		return nil, err
	}
	endpoint := os.Getenv("EPOCHDB_S3_ENDPOINT")
	if endpoint == "" {
		return s, nil
	}
	// Zero means casfs's own defaults: 5% of the filesystem free, 30 days.
	var minFree int64
	if v := os.Getenv("EPOCHDB_CACHE_MIN_FREE"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n <= 0 {
			return nil, fmt.Errorf("dist: EPOCHDB_CACHE_MIN_FREE=%q is not a positive byte count", v)
		}
		minFree = n
	}
	var maxAge time.Duration
	if v := os.Getenv("EPOCHDB_CACHE_MAX_AGE"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			return nil, fmt.Errorf("dist: EPOCHDB_CACHE_MAX_AGE=%q is not a positive duration", v)
		}
		maxAge = d
	}
	// A cold 4MB chunk comes down as parallel sub-ranges, and casfs bounds them
	// store-wide. The knob is here because the right number is a property of
	// the BOX (its NIC and its distance from the bucket), which is measured on
	// the box rather than guessed at build time.
	var fetchConc int
	if v := os.Getenv("EPOCHDB_FETCH_CONCURRENCY"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return nil, fmt.Errorf("dist: EPOCHDB_FETCH_CONCURRENCY=%q is not a positive request count", v)
		}
		fetchConc = n
	}
	cacheDir := CacheRoot(dataDir)
	os.RemoveAll(filepath.Join(dataDir, legacyCacheName))
	// Empty keys are not an error: casfs falls back to the AWS default chain,
	// which is what makes an SSO session or an instance role work with nothing
	// but an endpoint and a bucket set.
	cas, err := openCas(cacheDir, s.spool, casfs.Config{
		Endpoint:     endpoint,
		Region:       os.Getenv("EPOCHDB_S3_REGION"),
		Bucket:       os.Getenv("EPOCHDB_S3_BUCKET"),
		Prefix:       os.Getenv("EPOCHDB_S3_PREFIX"),
		AccessKey:    os.Getenv("EPOCHDB_S3_ACCESS_KEY"),
		SecretKey:    os.Getenv("EPOCHDB_S3_SECRET_KEY"),
		SpoolDir:     s.spool,
		CacheDir:     cacheDir,
		Namespace:    chainKey(dataDir),
		CacheMinFree: minFree,
		CacheMaxAge:  maxAge,

		FetchConcurrency: fetchConc,
	})
	if err != nil {
		return nil, err
	}
	s.cas = cas
	return s, nil
}

// Local builds a store that never talks to S3 whatever the environment says
// (tests, tools, and any node run without credentials).
func Local(dataDir string) (*Store, error) {
	s := &Store{dir: dataDir, spool: filepath.Join(dataDir, spoolName)}
	if err := os.MkdirAll(s.spool, 0o755); err != nil {
		return nil, err
	}
	return s, nil
}

// Dir is the data directory the store hangs off.
func (s *Store) Dir() string { return s.dir }

// Remote reports whether S3 credentials are configured.
func (s *Store) Remote() bool { return s.cas != nil }

// CacheStats is what the chunk cache has actually been doing. ok=false without
// credentials, where there is no cache at all: the spool IS the storage.
//
// VictimAge is the number worth watching. It is the age of the window the
// eviction worker last deleted from, i.e. how long a chunk really survives on
// this node, which a configured byte cap could never have told anyone.
func (s *Store) CacheStats() (casfs.Stats, bool) {
	if s.cas == nil {
		return casfs.Stats{}, false
	}
	return s.cas.Stats(), true
}

// SpoolPath is where the file for hash lives while it is still local.
func (s *Store) SpoolPath(hash string) string { return filepath.Join(s.spool, hash) }

// SpoolDir is the spool itself, for a writer that builds its artifact in place
// and then Adopts it: a file created here is on the spool's own filesystem, so
// the adopting rename cannot fail with EXDEV, and casfs ignores whatever is
// not a hash-named file (a half-built epoch is `epoch-*.tmp`).
func (s *Store) SpoolDir() string { return s.spool }

// ---------- publishing ----------

// Hasher builds an artifact's chunk hash list as its content streams past, and
// with it the artifact's NAME. A producer writes content, calls Finish, and
// appends the tail Finish returns; nothing about it is proportional to the
// artifact (32 bytes per 4MB, 76KB for the largest epoch anyone here builds).
type Hasher = casfs.Hasher

// NewHasher starts a list at epochdb's one chunk size.
func NewHasher() *Hasher { return casfs.NewHasher(ChunkSize) }

// Put stores b as an artifact and returns its name. The write is
// tmp+fsync+rename onto the spool path, so a kill at any instant leaves either
// nothing or a complete, correctly named file.
func (s *Store) Put(b []byte) (string, error) {
	h := NewHasher()
	h.Write(b)
	name, tail, err := h.Finish()
	if err != nil {
		return "", err
	}
	return name, writeDurable(s.SpoolPath(name), b, tail)
}

// Adopt seals an already-written file and renames it into the spool under the
// name its Hasher accumulated while the file was being written: the hash list
// and trailer are appended to what is on disk, and sha256 of that list is the
// name. path must be on the same filesystem as the spool (both live inside the
// data dir).
//
// NOTHING IS HELD IN MEMORY, which is the sealer's hard-won property: the tail
// is kilobytes and the multi-GB content was never in RAM at all.
func (s *Store) Adopt(path string, h *Hasher) (string, error) {
	name, tail, err := h.Finish()
	if err != nil {
		return "", err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		return "", err
	}
	_, err = f.Write(tail)
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return "", err
	}
	if err := os.Rename(path, s.SpoolPath(name)); err != nil {
		return "", err
	}
	return name, nil
}

// writeDurable lands parts at path by tmp+FSYNC+rename. The fsync is the whole
// durability claim: without it power loss can leave a renamed but zero-length
// file, and neither an empty artifact nor an empty pointer value announces
// itself as damaged (decodeLatest used to read an empty pointer as a valid
// "this chain has no epochs").
func writeDurable(path string, parts ...[]byte) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	for _, p := range parts {
		if _, err = f.Write(p); err != nil {
			break
		}
	}
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

// Sync uploads everything in the spool the bucket does not have yet and then
// unlinks the local copy of whatever it now confirms. No-op without
// credentials, where the spool is the only durable copy there is.
func (s *Store) Sync() error {
	if s.cas == nil {
		return nil
	}
	done, err := s.cas.Sync()
	for _, h := range done {
		if rerr := s.cas.Release(h); rerr != nil {
			err = errors.Join(err, rerr)
		}
	}
	return err
}

// ---------- reading ----------

// Close stops casfs's eviction worker. The cache is a tree of finished files,
// so skipping this costs nothing at all: the next start reads whatever is
// there.
func (s *Store) Close() error {
	if s.cas == nil {
		return nil
	}
	return s.cas.close()
}

// Open returns the bytes of one artifact. Without credentials that is an mmap
// of the spool file; with them it is casfs, reading the window chunk cache and
// filling it from the spool or a ranged GET.
func (s *Store) Open(hash string) (*Blob, error) {
	if s.cas == nil {
		b, err := mmapArtifact(s.SpoolPath(hash))
		if err != nil {
			return nil, fmt.Errorf("dist: open %s: %w", hash, err)
		}
		return b, nil
	}
	f, err := s.cas.Open(hash)
	if err != nil {
		return nil, err
	}
	return &Blob{size: uint64(f.Size()), f: f}, nil
}

// Blob is one artifact's bytes: a whole-file mapping of the spool file without
// credentials, casfs's window chunk cache with them.
//
// TWO WAYS TO READ IT, and the choice is the whole cost model. Read copies and
// is for everything transient (a query's binary search, one decompression
// frame). View maps, one mapping per 4MB chunk, and is ONLY for the ranges an
// epoch holds for the process's life. Nothing is pinned any more: casfs evicts
// by unlinking, and unlinking a mapped file cannot turn live bytes into zeros
// the way the old hole punch could.
type Blob struct {
	size uint64
	mm   []byte      // no credentials: whole-file mapping of the spool file
	f    *casfs.File // credentials: the chunk cache
}

// mmapArtifact maps a spool-resident artifact's CONTENT, which is the
// no-credentials read path in full.
//
// IT MAPS L BYTES, NOT THE FILE. The hash list and trailer sit past the
// content, so mapping the file whole would hand the epoch reader a "file" whose
// last bytes are casfs metadata and whose footer is nowhere near the end. The
// logical length comes from casfs, and nothing above here knows the difference
// between this road and the credentialed one.
func mmapArtifact(path string) (*Blob, error) {
	size, _, err := casfs.ReadTail(path, ChunkSize)
	if err != nil {
		return nil, err
	}
	if size == 0 {
		return &Blob{}, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	mm, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	return &Blob{size: uint64(size), mm: mm}, nil
}

func (b *Blob) Size() uint64 { return b.size }

func (b *Blob) bounds(off, n uint64) error {
	if off > b.size || n > b.size-off {
		return fmt.Errorf("dist: range [%d,%d) outside a %d byte artifact", off, off+n, b.size)
	}
	return nil
}

// Read copies bytes [off, off+n) onto the heap. This is the query path: the
// bytes are the caller's, they cannot change underneath, and a chunk evicted
// the instant afterwards is somebody else's problem.
func (b *Blob) Read(off, n uint64) ([]byte, error) {
	if err := b.bounds(off, n); err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, nil
	}
	if b.f == nil {
		return append([]byte(nil), b.mm[off:off+n]...), nil
	}
	p := make([]byte, n)
	if _, err := b.f.ReadAt(p, int64(off)); err != nil {
		return nil, err
	}
	return p, nil
}

// View maps [off, off+n) for as long as the caller holds it. Only the sections
// an epoch keeps resident should use this: each one pins its chunk files'
// blocks on disk until Close, so the ghost disk a node can hold is the size of
// its resident set.
func (b *Blob) View(off, n uint64) (*View, error) {
	if err := b.bounds(off, n); err != nil {
		return nil, err
	}
	if n == 0 {
		return ViewOf(nil), nil
	}
	if b.f == nil {
		return ViewOf(b.mm[off : off+n]), nil
	}
	return b.f.View(int64(off), int64(n))
}

func (b *Blob) Close() error {
	if b.mm != nil {
		err := syscall.Munmap(b.mm)
		b.mm = nil
		return err
	}
	if b.f != nil {
		err := b.f.Close()
		b.f = nil
		return err
	}
	return nil
}

// ---------- pointers ----------

// Latest is the bucket's one mutable object: the name of the chain's newest
// MANIFEST artifact, and nothing else. A HINT, never an authority: the manifest
// is content-addressed and the runs it lists chain back to the chain root by
// their footers, so a consumer verifies everything the pointer claims.
type Latest struct {
	Manifest string
}

func (l Latest) encode() string {
	var b strings.Builder
	if l.Manifest != "" {
		fmt.Fprintf(&b, "manifest %s\n", l.Manifest)
	}
	return b.String()
}

func decodeLatest(s string) (Latest, error) {
	var l Latest
	// An EMPTY VALUE IS NOT A POINTER. A torn write (or a bucket object
	// truncated to nothing) would otherwise decode to a perfectly valid
	// Latest{Manifest: ""}, i.e. "this chain has no runs", which is the most
	// destructive thing this file can say.
	if strings.TrimSpace(s) == "" {
		return l, fmt.Errorf("dist: latest pointer is empty")
	}
	for _, line := range strings.Split(strings.TrimSpace(s), "\n") {
		f := strings.Fields(line)
		if len(f) == 0 {
			continue
		}
		if len(f) != 2 || !ValidHash(f[1]) {
			return l, fmt.Errorf("dist: bad latest pointer line %q", line)
		}
		switch f[0] {
		case "manifest":
			l.Manifest = f[1]
		default:
			return l, fmt.Errorf("dist: unknown latest pointer field %q", f[0])
		}
	}
	return l, nil
}

// Latest reads chainRoot's pointer. A missing pointer wraps fs.ErrNotExist.
func (s *Store) Latest(chainRoot [32]byte) (Latest, error) {
	v, err := s.GetPointer(LatestPointer(chainRoot))
	if err != nil {
		return Latest{}, err
	}
	return decodeLatest(v)
}

// SetLatest publishes chainRoot's pointer. Call it only after the artifacts it
// names are durable (uploaded when there is a bucket).
func (s *Store) SetLatest(chainRoot [32]byte, l Latest) error {
	return s.SetPointer(LatestPointer(chainRoot), l.encode())
}

// SetPointer writes a small mutable value locally and hands it to casfs, which
// is also local and also synchronous (it lands under the spool) and which
// uploads it on a later Sync, after that pass's content. NOTHING HERE TOUCHES
// THE NETWORK, which is what lets a seal publish an epoch on a node whose
// credentials expired hours ago.
//
// THE SPOOL COPY IS WRITTEN WHETHER OR NOT THERE ARE CREDENTIALS, and that is
// the whole reason this is not two lines. Publishing a pointer used to depend on
// there being a casfs at the moment of the write, and the one caller
// (setLatestEpoch, inside the seal loop) runs once per epoch: a dir that sealed
// everything offline and restarted WITH keys uploaded its artifacts and no
// pointer, and the pointer then waited for the next seal, which is weeks away on
// a slow chain. casfs's own constructor rebuilds its dirty set by scanning the
// spool's pointer directory, so a value left there while offline is picked up by
// the next credentialed process and uploaded on its first Sync, still last and
// still only after that pass's content went up cleanly.
func (s *Store) SetPointer(name, value string) error {
	p := s.pointerPath(name)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	if err := writeDurable(p, []byte(value)); err != nil {
		return err
	}
	if s.cas != nil {
		return s.cas.SetPointer(name, value) // also marks it dirty for this process
	}
	return casfs.WriteSpoolPointer(s.spool, name, value)
}

// PrefixHasObjects reports whether the configured bucket prefix holds ANY
// object. False without credentials, where there is no bucket to ask.
//
// It answers one question and it is the join's: a `latest` pointer that is
// missing over a prefix somebody has already published content to is a BROKEN
// PUBLISH, while the same missing pointer over an empty prefix is a chain nobody
// has started yet. Nothing else can tell those apart, because content is named
// by hash and carries no chain.
func (s *Store) PrefixHasObjects() (bool, error) {
	if s.cas == nil {
		return false, nil
	}
	return s.cas.PrefixHasObjects()
}

// GetPointer reads a pointer, local copy first: a producer's own pointer is
// always at least as fresh as the bucket's, and a fresh consumer has no local
// copy at all. A missing pointer wraps fs.ErrNotExist.
func (s *Store) GetPointer(name string) (string, error) {
	b, err := os.ReadFile(s.pointerPath(name))
	if err == nil {
		return string(b), nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return "", err
	}
	if s.cas == nil {
		return "", fmt.Errorf("dist: pointer %s: %w", name, fs.ErrNotExist)
	}
	return s.cas.GetPointer(name)
}

func (s *Store) pointerPath(name string) string {
	return filepath.Join(s.dir, filepath.FromSlash(name))
}

// ValidHash reports whether s is a lowercase hex sha256, the only name shape
// the content-address space has.
func ValidHash(s string) bool {
	if len(s) != 2*sha256.Size {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
