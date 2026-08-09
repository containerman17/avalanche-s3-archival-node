package exec

// A BOUNDED frame-bytes sampler, measurement only: it writes a plausible
// binary encoding of complete call frames to one file so the compression
// ratio of a stored-frames section can be measured OFFLINE at different
// zstd groupings (DESIGN.md, stored call frames). It is NOT the format:
// nothing reads this file, seal never sees it, and the encoding here binds
// nothing. It exists because the count-only tracer proves the RATE
// (~11 frames/tx in the DeFi era) but ratio needs real bytes.
//
// EPOCHDB_FRAME_SAMPLE_MB=<n> enables it (n = byte budget in MB); the file
// is <DataDir>/frame-sample.bin, truncated at start. At the budget the
// sampler logs one line and goes inert. Pairing Enter/Exit needs a stack;
// the executor is one goroutine, so a plain slice is enough.
//
// Record, all integers uvarint unless fixed:
//   kind(1) depth(1) from(20) to(20) valueLen|valueBytes gas gasUsed
//   errByte(1) inputLen|input outputLen|output

import (
	"encoding/binary"
	"log"
	"math/big"
	"os"
	"path/filepath"
	"strconv"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/vm"
)

var frameSampleBudget = func() int64 {
	v := os.Getenv("EPOCHDB_FRAME_SAMPLE_MB")
	if v == "" {
		return 0
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n <= 0 {
		return 0
	}
	return n << 20
}()

// initFrameSampler is called by New with the data dir. Returns nil when
// disabled or on any setup error (measurement must never stop the node).
func initFrameSampler(dataDir string) *frameSampler {
	if frameSampleBudget == 0 {
		return nil
	}
	path := filepath.Join(dataDir, "frame-sample.bin")
	f, err := os.Create(path)
	if err != nil {
		log.Printf("exec: frame sample disabled: %v", err)
		return nil
	}
	log.Printf("exec: frame sample: writing up to %d MB to %s", frameSampleBudget>>20, path)
	return &frameSampler{f: f, budget: frameSampleBudget}
}

type frameSampler struct {
	f       *os.File
	budget  int64
	written int64
	done    bool
	frames  uint64
	buf     []byte
	// stack holds the Enter half of each open frame; Exit pops and emits.
	stack []pendingFrame
}

type pendingFrame struct {
	kind  byte
	depth byte
	from  common.Address
	to    common.Address
	value []byte // big.Int bytes, nil for zero
	gas   uint64
	input []byte // copied: the VM reuses its buffers
}

func (s *frameSampler) enter(typ vm.OpCode, from, to common.Address, input []byte, gas uint64, value *big.Int) {
	if s.done {
		return
	}
	p := pendingFrame{kind: byte(typ), depth: byte(len(s.stack)), from: from, to: to, gas: gas}
	if value != nil && value.Sign() != 0 {
		p.value = value.Bytes()
	}
	if len(input) > 0 {
		p.input = append([]byte(nil), input...)
	}
	s.stack = append(s.stack, p)
}

func (s *frameSampler) exit(output []byte, gasUsed uint64, err error) {
	if s.done || len(s.stack) == 0 {
		return
	}
	p := s.stack[len(s.stack)-1]
	s.stack = s.stack[:len(s.stack)-1]

	b := s.buf[:0]
	b = append(b, p.kind, p.depth)
	b = append(b, p.from[:]...)
	b = append(b, p.to[:]...)
	b = binary.AppendUvarint(b, uint64(len(p.value)))
	b = append(b, p.value...)
	b = binary.AppendUvarint(b, p.gas)
	b = binary.AppendUvarint(b, gasUsed)
	if err != nil {
		b = append(b, 1)
	} else {
		b = append(b, 0)
	}
	b = binary.AppendUvarint(b, uint64(len(p.input)))
	b = append(b, p.input...)
	b = binary.AppendUvarint(b, uint64(len(output)))
	b = append(b, output...)
	s.buf = b

	if _, werr := s.f.Write(b); werr != nil {
		log.Printf("exec: frame sample write failed, stopping: %v", werr)
		s.finish()
		return
	}
	s.frames++
	s.written += int64(len(b))
	if s.written >= s.budget {
		s.finish()
	}
}

// txEnd drops any frames left open by an unusual unwind so depth never lies.
func (s *frameSampler) txEnd() { s.stack = s.stack[:0] }

func (s *frameSampler) finish() {
	s.done = true
	s.f.Close()
	log.Printf("exec: frame sample COMPLETE: %d frames, %d MB", s.frames, s.written>>20)
}
