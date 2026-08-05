package state

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ava-labs/libevm/common"
)

// eioPath is a file whose reads fail with EIO rather than EOF: /proc/self/mem
// read at offset 0 is an unmapped address. It is the cheapest real read error
// there is, and the whole point of the fix is that a read error is NOT a torn
// tail. Returns "" where the trick does not work.
func eioPath(t *testing.T, link string) string {
	t.Helper()
	if err := os.Symlink("/proc/self/mem", link); err != nil {
		return ""
	}
	f, err := os.Open(link)
	if err != nil {
		return ""
	}
	defer f.Close()
	var probe [1]byte
	if _, err := f.Read(probe[:]); err == nil || err == io.EOF {
		return ""
	}
	return link
}

// TestRebuildRefusesUnreadableSidecar is the structural regression: the
// startup scan read index records "until ANY error, then stop AND TRUNCATE
// the data file to what was read". On a torn tail that is the recovery rule;
// on a bad sector it silently DELETES every write frame past the bad byte,
// exec's walk-back never regenerates them, cook builds a sorted index missing
// them, and the descent answers those keys with the previous value forever.
func TestRebuildRefusesUnreadableSidecar(t *testing.T) {
	dir := t.TempDir()
	payload := bytes.Repeat([]byte{0xab}, 100)
	if err := os.WriteFile(filepath.Join(dir, "writelog_00000.log"), payload, 0o644); err != nil {
		t.Fatal(err)
	}
	if eioPath(t, filepath.Join(dir, "writelog_idx_00000.log")) == "" {
		t.Skip("no EIO-on-read file available here")
	}

	if _, err := openBucketLog(dir, "writelog"); err == nil {
		t.Fatal("openBucketLog accepted an unreadable sidecar")
	}
	// And, above all, it did not truncate the data away on the way out.
	st, err := os.Stat(filepath.Join(dir, "writelog_00000.log"))
	if err != nil {
		t.Fatal(err)
	}
	if st.Size() != int64(len(payload)) {
		t.Fatalf("writelog data truncated to %d bytes, want %d: the read error was treated as a torn tail", st.Size(), len(payload))
	}
}

// TestScanRORefusesUnreadableSidecar: the read-only scan truncates nothing,
// but stopping on a read error froze the bucket's offset with no log line at
// all, and an SDK follower just stopped advancing.
func TestScanRORefusesUnreadableSidecar(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "headers_00000.log"), []byte("payload"), 0o644); err != nil {
		t.Fatal(err)
	}
	if eioPath(t, filepath.Join(dir, "headers_idx_00000.log")) == "" {
		t.Skip("no EIO-on-read file available here")
	}
	if _, err := openBucketLogRO(dir, "headers"); err == nil {
		t.Fatal("openBucketLogRO accepted an unreadable sidecar")
	}
}

// TestOpenCodeStoreRefusesCorruptRecord: same idiom on code.log, where the
// truncation discarded every blob after the bad record and the loss then
// surfaced as the unrelated "code %x not in code.log or the sealed epochs".
func TestOpenCodeStoreRefusesCorruptRecord(t *testing.T) {
	dir := t.TempDir()
	var log []byte
	good := common.Hash{0x11}
	log = append(log, good[:]...)
	log = binary.AppendUvarint(log, 3)
	log = append(log, 1, 2, 3)
	// A length field of 10 continuation bytes: binary.ReadUvarint returns an
	// overflow error, which is neither EOF nor a torn tail.
	bad := common.Hash{0x22}
	log = append(log, bad[:]...)
	log = append(log, bytes.Repeat([]byte{0xff}, 10)...)
	log = append(log, bytes.Repeat([]byte{0x5a}, 64)...)
	path := filepath.Join(dir, "code.log")
	if err := os.WriteFile(path, log, 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := openCodeStore(dir); err == nil {
		t.Fatal("openCodeStore accepted a corrupt record")
	}
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if st.Size() != int64(len(log)) {
		t.Fatalf("code.log truncated to %d bytes, want %d", st.Size(), len(log))
	}
	if _, err := openCodeStoreRO(dir); err == nil {
		t.Fatal("openCodeStoreRO accepted a corrupt record")
	}
}

// TestLogsStartMustBeEightBytes: a present-but-malformed capture floor used to
// fall through both branches and read as "capture never started", after which
// exec re-stamped it at the current head and declared every block below it
// log-free.
func TestLogsStartMustBeEightBytes(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, logsStartFile), []byte{1, 2, 3}, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir); err == nil {
		t.Fatal("Open accepted a 3-byte logs.start")
	}
	if _, err := OpenReadOnly(dir); err == nil {
		t.Fatal("OpenReadOnly accepted a 3-byte logs.start")
	}
}

// TestMissingFrameNamesTheRealCondition pins the message quality of the
// checkpoint incident: `ok=%v err=%w` rendered `%!w(<nil>)` on the not-found
// branch, which is the branch that actually fires.
func TestMissingFrameNamesTheRealCondition(t *testing.T) {
	dir := t.TempDir()
	// An index entry whose bucket files do not exist: Get returns ok=false,
	// err=nil, which is exactly the shape the old line could not print.
	wl := &bucketLog{
		dir:    dir,
		prefix: "writelog",
		idx:    map[uint64]recLoc{5: {off: 0, ln: 4}},
		pairs:  map[uint64]*blPair{},
	}
	_, err := cookBucket(dir, wl, 0, []uint64{5}, 5, 0)
	if err == nil {
		t.Fatal("cookBucket accepted a missing frame")
	}
	msg := err.Error()
	for _, bad := range []string{"%!w", "err=<nil>", "ok=false"} {
		if strings.Contains(msg, bad) {
			t.Fatalf("error message still prints %q: %s", bad, msg)
		}
	}
	if !strings.Contains(msg, "no write frame") {
		t.Fatalf("error message does not name the condition: %s", msg)
	}
}

// TestNoOkErrFormatSurvives is the cheap guard against the idiom coming back:
// "ok=%v err=%v" prints a nil error as <nil> and tells an operator nothing.
func TestNoOkErrFormatSurvives(t *testing.T) {
	for _, pkg := range []string{".", "../exec"} {
		entries, err := os.ReadDir(pkg)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range entries {
			if !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
				continue
			}
			src, err := os.ReadFile(filepath.Join(pkg, e.Name()))
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(src, []byte("ok=%v err=%")) {
				t.Errorf("%s/%s prints a not-found and an error as one message", pkg, e.Name())
			}
		}
	}
}
