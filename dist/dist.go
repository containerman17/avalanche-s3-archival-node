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
//	EPOCHDB_S3_ACCESS_KEY  }
//	EPOCHDB_S3_SECRET_KEY  } required when the endpoint is set
//	EPOCHDB_S3_PREFIX      optional key prefix, used verbatim
//	EPOCHDB_S3_REGION      optional, default "auto" (R2; MinIO ignores it)
//	EPOCHDB_CACHE_BYTES    chunk cache cap, default 8GiB
package dist

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/ava-labs/avalanchego/genesis"
	"github.com/containerman17/casfs"
)

const (
	// ChunkSize is the granularity of both the casfs chunk cache and the
	// published per-chunk hash lists. One number, so a hash list describes
	// exactly the ranges a downloader fetches.
	ChunkSize = casfs.DefaultChunkSize

	spoolName = "cas"    // <data>/cas/<hash>: durable until uploaded
	cacheName = "chunks" // <data>/chunks: disposable
	// LatestPointer names the one mutable object in the bucket. It is a HINT,
	// never an authority: every artifact self-verifies by hash and the epoch
	// footers chain back to the genesis config.
	LatestPointer = "latest"

	defaultCacheBytes = 8 << 30
)

// Store is the node's artifact store. cas nil means no credentials were
// configured: everything stays in the spool and nothing is ever uploaded.
type Store struct {
	dir   string
	spool string
	cas   *casfs.Store
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
	cacheBytes := int64(defaultCacheBytes)
	if v := os.Getenv("EPOCHDB_CACHE_BYTES"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n <= 0 {
			return nil, fmt.Errorf("dist: EPOCHDB_CACHE_BYTES=%q is not a positive byte count", v)
		}
		cacheBytes = n
	}
	cas, err := casfs.New(casfs.Config{
		Endpoint:   endpoint,
		Region:     os.Getenv("EPOCHDB_S3_REGION"),
		Bucket:     os.Getenv("EPOCHDB_S3_BUCKET"),
		Prefix:     os.Getenv("EPOCHDB_S3_PREFIX"),
		AccessKey:  os.Getenv("EPOCHDB_S3_ACCESS_KEY"),
		SecretKey:  os.Getenv("EPOCHDB_S3_SECRET_KEY"),
		SpoolDir:   s.spool,
		CacheDir:   filepath.Join(dataDir, cacheName),
		CacheBytes: cacheBytes,
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

// SpoolPath is where the file for hash lives while it is still local.
func (s *Store) SpoolPath(hash string) string { return filepath.Join(s.spool, hash) }

// SpoolTemp is a scratch path in the spool directory for a writer that will
// Adopt its output: same filesystem, and the name cannot be mistaken for
// content (casfs only ever looks at hash-named entries).
func (s *Store) SpoolTemp(name string) string { return filepath.Join(s.spool, name+".tmp") }

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
	list, err := b.Slice(0, b.Size())
	if err != nil {
		return nil, err
	}
	if len(list)%sha256.Size != 0 {
		return nil, fmt.Errorf("dist: chunk list %s is %d bytes, not a multiple of %d", lh, len(list), sha256.Size)
	}
	return append([]byte(nil), list...), nil
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

// Open returns the bytes of one artifact. Without credentials that is an mmap
// of the spool file; with them it is casfs, which reads through its chunk
// cache and fills misses from the spool file or a ranged GET.
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
	return &Blob{size: uint64(f.Size()), ra: f, closer: f}, nil
}

// Blob is one artifact's bytes. Slice hands back a view into the mapping when
// the file is local and a fresh copy when it is not, which is the only
// difference between the two states above this package.
type Blob struct {
	size   uint64
	mm     []byte
	ra     io.ReaderAt
	closer io.Closer
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

// ReaderBlob wraps any ReaderAt as a blob (the copy path, exercised by tests
// without an S3 endpoint).
func ReaderBlob(ra io.ReaderAt, size uint64, closer io.Closer) *Blob {
	return &Blob{size: size, ra: ra, closer: closer}
}

func (b *Blob) Size() uint64 { return b.size }

// Slice returns bytes [off, off+n). The result MAY alias the mapping, so
// callers must treat it as read-only and must not retain it past Close.
func (b *Blob) Slice(off, n uint64) ([]byte, error) {
	if off > b.size || n > b.size-off {
		return nil, fmt.Errorf("dist: range [%d,%d) outside a %d byte artifact", off, off+n, b.size)
	}
	if n == 0 {
		return nil, nil
	}
	if b.mm != nil {
		return b.mm[off : off+n], nil
	}
	p := make([]byte, n)
	if _, err := b.ra.ReadAt(p, int64(off)); err != nil {
		return nil, err
	}
	return p, nil
}

func (b *Blob) Close() error {
	if b.mm != nil {
		err := syscall.Munmap(b.mm)
		b.mm = nil
		return err
	}
	if b.closer != nil {
		err := b.closer.Close()
		b.closer = nil
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

// Latest reads the pointer. A missing pointer wraps fs.ErrNotExist.
func (s *Store) Latest() (Latest, error) {
	v, err := s.GetPointer(LatestPointer)
	if err != nil {
		return Latest{}, err
	}
	return decodeLatest(v)
}

// SetLatest publishes the pointer. Call it only after the artifacts it names
// are durable (uploaded when there is a bucket).
func (s *Store) SetLatest(l Latest) error { return s.SetPointer(LatestPointer, l.encode()) }

// SetPointer writes a small mutable value locally and, when there is a bucket,
// to the bucket too. The local copy is what makes a producer restart without
// asking S3 anything.
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

// ChainRoot is the epoch chain's anchor: sha256 of the chain's own genesis
// config, which every client already ships. Epoch 1's footer carries it as its
// previous-file hash, so one head hash authenticates all of that chain's
// history and there is no global registry anywhere (DESIGN.md "Multi-chain").
func ChainRoot(networkID uint32) ([32]byte, error) {
	cfg := genesis.GetConfig(networkID)
	if cfg == nil {
		return [32]byte{}, fmt.Errorf("dist: no embedded genesis config for network %d", networkID)
	}
	return sha256.Sum256([]byte(cfg.CChainGenesis)), nil
}
