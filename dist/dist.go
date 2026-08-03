// Package dist is epochdb's artifact layer (DESIGN.md "Distribution").
//
// Sealed epochs are WHOLE FILES NAMED BY THEIR HEX SHA256. A
// producer renames a finished file onto the casfs spool path for its own hash
// and that rename is the entire registration; a ticker uploads whatever the
// spool holds and unlinks the local copy once the bucket confirms it. Reads go
// through one call, Open(hash).
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
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
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
	// ChunkSize is the granularity of both the casfs chunk cache and the
	// published per-chunk hash lists. One number, so a hash list describes
	// exactly the ranges a downloader fetches.
	ChunkSize = casfs.DefaultChunkSize

	spoolName = "cas" // <data>/cas/<hash>: durable until uploaded
	// cacheName is the casfs chunk cache, disposable, laid out as
	// <data>/cache/<window>/<chain>/<hash>.<index>. It must NOT collide with a
	// pointer name: casfs owns its cache directory and deletes everything in
	// it that is not one of its own window directories, so a cache dir named
	// "chunks" silently ate the chunk-list pointers at <data>/chunks/<hash>
	// the moment a credentialed store opened. Nothing is published under
	// "cache".
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

// cacheRoot is where the chunk cache lives, and it is THE ONLY THING SEVERAL
// CHAIN PROCESSES SHARE (RULING 2026-08-04: one process = one chain). Default
// <data>/cache, i.e. self-contained; EPOCHDB_CACHE_DIR points N processes at
// one directory and they then share one elastic LRU, with zero coordination:
// casfs evicts whole windows and tolerates a chunk vanishing under it, so a
// sibling's eviction costs a refetch and never a wrong answer.
//
// The SPOOL is deliberately NOT shared: it is one chain's durable, not-yet-
// uploaded artifacts, and it stays at <data>/cas with the epoch markers and the
// local `latest` that name them.
func cacheRoot(dataDir string) string {
	if dir := os.Getenv("EPOCHDB_CACHE_DIR"); dir != "" {
		return dir
	}
	return filepath.Join(dataDir, cacheName)
}

// chainKey names this chain inside the chunk cache
// (<cacheRoot>/<window>/<chainKey>/...). It is THE DATA DIRECTORY'S OWN NAME,
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
	cacheDir := cacheRoot(dataDir)
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

// Put stores b as an artifact and returns its hex sha256. The write is
// tmp+rename onto the spool path, so a kill at any instant leaves either
// nothing or a complete, correctly named file.
func (s *Store) Put(b []byte) (string, error) {
	d := NewDigest()
	d.Write(b)
	hash, chunks := d.Sum()
	if err := s.writeSpool(hash, b); err != nil {
		return "", err
	}
	return hash, s.putChunks(hash, chunks)
}

// Adopt renames an already-written file into the spool under the hash its
// digest accumulated while the file was being written. path must be on the
// same filesystem as the spool (both live inside the data dir).
func (s *Store) Adopt(path string, d *Digest) (string, error) {
	hash, chunks := d.Sum()
	if err := os.Rename(path, s.SpoolPath(hash)); err != nil {
		return "", err
	}
	return hash, s.putChunks(hash, chunks)
}

func (s *Store) writeSpool(hash string, b []byte) error {
	tmp := s.SpoolPath(hash) + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.SpoolPath(hash))
}

// putChunks publishes the artifact's per-chunk hash list, itself an ordinary
// content-addressed artifact, and points at it from chunks/<hash>.
//
// WHY A POINTER AND NOT A FOOTER FIELD (resolved 2026-07-30): a list that
// covers a file's own chunks cannot be referenced from inside that file, since
// writing the reference changes the chunks the list describes. The list's
// authority is therefore the whole-file sha256 the epoch chain already
// carries: a lying list makes a downloader waste bytes, never accept wrong
// ones.
func (s *Store) putChunks(hash string, chunks []byte) error {
	d := NewDigest()
	d.Write(chunks)
	lh, _ := d.Sum()
	if err := s.writeSpool(lh, chunks); err != nil {
		return err
	}
	return s.SetPointer(ChunkPointer(hash), lh)
}

// ChunkPointer names the pointer holding an artifact's chunk-list hash.
func ChunkPointer(hash string) string { return "chunks/" + hash }

// Chunks returns the per-chunk sha256 list published for hash (32 bytes per
// ChunkSize chunk, in order).
func (s *Store) Chunks(hash string) ([]byte, error) {
	lh, err := s.GetPointer(ChunkPointer(hash))
	if err != nil {
		return nil, err
	}
	b, err := s.Open(lh)
	if err != nil {
		return nil, err
	}
	defer b.Close()
	list, err := b.Read(0, b.Size())
	if err != nil {
		return nil, err
	}
	if len(list)%sha256.Size != 0 {
		return nil, fmt.Errorf("dist: chunk list %s is %d bytes, not a multiple of %d", lh, len(list), sha256.Size)
	}
	return list, nil
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
		b, err := MmapBlob(s.SpoolPath(hash))
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

// MmapBlob maps a plain file. Used for spool-resident artifacts and by tools
// that hold a path rather than a hash.
func MmapBlob(path string) (*Blob, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if st.Size() == 0 {
		return &Blob{}, nil
	}
	mm, err := syscall.Mmap(int(f.Fd()), 0, int(st.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	return &Blob{size: uint64(st.Size()), mm: mm}, nil
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

// Latest is the bucket's one mutable object: the newest epoch, and nothing
// else since snapshots died (DESIGN.md ruling 1 of 2026-07-31). A HINT, never
// an authority.
type Latest struct {
	Epoch string
}

func (l Latest) encode() string {
	var b strings.Builder
	if l.Epoch != "" {
		fmt.Fprintf(&b, "epoch %s\n", l.Epoch)
	}
	return b.String()
}

func decodeLatest(s string) (Latest, error) {
	var l Latest
	for _, line := range strings.Split(strings.TrimSpace(s), "\n") {
		f := strings.Fields(line)
		if len(f) == 0 {
			continue
		}
		if len(f) != 2 || !ValidHash(f[1]) {
			return l, fmt.Errorf("dist: bad latest pointer line %q", line)
		}
		switch f[0] {
		case "epoch":
			l.Epoch = f[1]
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
func (s *Store) SetPointer(name, value string) error {
	p := s.pointerPath(name)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(p+".tmp", []byte(value), 0o644); err != nil {
		return err
	}
	if err := os.Rename(p+".tmp", p); err != nil {
		return err
	}
	if s.cas != nil {
		return s.cas.SetPointer(name, value)
	}
	return nil
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

// ---------- hashing ----------

// Digest computes an artifact's whole-file sha256 and, in the same pass, the
// per-chunk hash list that lets a downloader reject a bad ChunkSize range
// without waiting for the whole file.
type Digest struct {
	full    hash.Hash
	chunk   hash.Hash
	inChunk int64
	list    []byte
}

func NewDigest() *Digest {
	return &Digest{full: sha256.New(), chunk: sha256.New()}
}

func (d *Digest) Write(p []byte) (int, error) {
	n := len(p)
	for len(p) > 0 {
		room := int64(ChunkSize) - d.inChunk
		take := int64(len(p))
		if take > room {
			take = room
		}
		d.full.Write(p[:take])
		d.chunk.Write(p[:take])
		d.inChunk += take
		p = p[take:]
		if d.inChunk == ChunkSize {
			d.closeChunk()
		}
	}
	return n, nil
}

func (d *Digest) closeChunk() {
	d.list = d.chunk.Sum(d.list)
	d.chunk.Reset()
	d.inChunk = 0
}

// Sum finalizes the digest: the hex whole-file sha256 and the chunk list
// (32 bytes per chunk, in order). Call it once.
func (d *Digest) Sum() (string, []byte) {
	if d.inChunk > 0 {
		d.closeChunk()
	}
	return hex.EncodeToString(d.full.Sum(nil)), d.list
}

// VerifyChunk checks one downloaded chunk against a published list.
func VerifyChunk(list []byte, idx int, data []byte) error {
	if (idx+1)*sha256.Size > len(list) {
		return fmt.Errorf("dist: chunk %d beyond a %d-chunk list", idx, len(list)/sha256.Size)
	}
	sum := sha256.Sum256(data)
	if want := list[idx*sha256.Size : (idx+1)*sha256.Size]; !bytes.Equal(sum[:], want) {
		return fmt.Errorf("dist: chunk %d hashes to %x, list says %x", idx, sum, want)
	}
	return nil
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
