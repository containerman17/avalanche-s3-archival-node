package exec

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	"github.com/ava-labs/libevm/common"
)

// The SAE root ring: a fixed-size, slot-addressed file recording, per
// executed post-Helicon block, THIS executor's own post-execution state
// root and the gas clock after the block.
//
// Both are needed and neither is in the header. Under ACP-194 a header's
// Root is the post-execution root of the block it SETTLES, so (a) a block's
// own root is only attested a few blocks later, which is what the ring is
// consulted for, and (b) the crash-recovery walk-back can no longer read
// "the root at height h" off header h. The gas clock is not derivable at
// an arbitrary height at all: consensus only publishes it at settlement
// points (hook.SettledGasTime), so it is persisted per block.
//
// Fixed size on purpose: the window only has to cover the crash walk-back
// budget plus the settlement lag (a handful of blocks), and a growing
// per-block file would be a second writelog for no reason.
//
// Measured on live Fuji over the first 60 SAE blocks (2026-07-29): lag
// 1..5 blocks, mean 2.6, and the settled height advances in jumps of up to
// 5, so most heights are NEVER named by any header's markers. That is why
// every executed block goes in the ring rather than only settlement
// points.
const (
	saeRingSlots    = 8192
	saeRingSlotSize = 160
	saeRingFile     = "sae.ring"
)

// Slot layout: crc32(rest) | height BE | root | uint16 clock length | clock
// canoto. A slot whose CRC does not match is absent (torn write, or never
// written), so a truncated tail degrades to "unknown height" and the
// executor refuses to verify against it rather than trusting garbage.
const (
	saeRingOffHeight = 4
	saeRingOffRoot   = 12
	saeRingOffLen    = 44
	saeRingOffClock  = 46
	saeRingMaxClock  = saeRingSlotSize - saeRingOffClock
)

type saeRing struct {
	f     *os.File
	buf   []byte // whole file, mirrored in RAM (1.3MB)
	dirty bool
}

func openSAERing(dir string) (*saeRing, error) {
	f, err := os.OpenFile(filepath.Join(dir, saeRingFile), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, saeRingSlots*saeRingSlotSize)
	if _, err := f.ReadAt(buf, 0); err != nil {
		// A short (or absent) file just means unwritten slots; they fail
		// the CRC check and read as absent.
		if !isShortRead(err) {
			f.Close()
			return nil, fmt.Errorf("read %s: %w", saeRingFile, err)
		}
	}
	return &saeRing{f: f, buf: buf}, nil
}

func isShortRead(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}

func (r *saeRing) slot(height uint64) []byte {
	off := int(height%saeRingSlots) * saeRingSlotSize
	return r.buf[off : off+saeRingSlotSize]
}

// put records height's post-execution root and gas clock.
func (r *saeRing) put(height uint64, root common.Hash, clock []byte) error {
	if len(clock) > saeRingMaxClock {
		return fmt.Errorf("sae ring: gas clock is %d bytes, slot holds %d", len(clock), saeRingMaxClock)
	}
	s := r.slot(height)
	clear(s)
	binary.BigEndian.PutUint64(s[saeRingOffHeight:], height)
	copy(s[saeRingOffRoot:], root[:])
	binary.BigEndian.PutUint16(s[saeRingOffLen:], uint16(len(clock)))
	copy(s[saeRingOffClock:], clock)
	binary.BigEndian.PutUint32(s, crc32.ChecksumIEEE(s[saeRingOffHeight:]))
	off := int64(height%saeRingSlots) * saeRingSlotSize
	if _, err := r.f.WriteAt(s, off); err != nil {
		return fmt.Errorf("sae ring: write height %d: %w", height, err)
	}
	r.dirty = true
	return nil
}

// get returns height's recorded root and gas clock bytes. ok=false means
// the height has aged out of the ring, was never executed by this node, or
// its slot is torn.
func (r *saeRing) get(height uint64) (common.Hash, []byte, bool) {
	s := r.slot(height)
	if binary.BigEndian.Uint32(s) != crc32.ChecksumIEEE(s[saeRingOffHeight:]) {
		return common.Hash{}, nil, false
	}
	if binary.BigEndian.Uint64(s[saeRingOffHeight:]) != height {
		return common.Hash{}, nil, false
	}
	n := int(binary.BigEndian.Uint16(s[saeRingOffLen:]))
	if n > saeRingMaxClock {
		return common.Hash{}, nil, false
	}
	return common.BytesToHash(s[saeRingOffRoot:saeRingOffLen]), s[saeRingOffClock : saeRingOffClock+n], true
}

func (r *saeRing) sync() error {
	if !r.dirty {
		return nil
	}
	if err := r.f.Sync(); err != nil {
		return err
	}
	r.dirty = false
	return nil
}

func (r *saeRing) close() error {
	if r.f == nil {
		return nil
	}
	if err := r.sync(); err != nil {
		r.f.Close()
		return err
	}
	err := r.f.Close()
	r.f = nil
	return err
}
