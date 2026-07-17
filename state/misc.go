package state

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// miscStore holds the handful of non-code rawdb keys (genesis block rows,
// chain config, head markers) as a full RAM map backed by an append-only
// change log, replayed at startup.
//
//	misc.log: records [op 1B: 0=put 1=del][uvarint klen][key]
//	          [uvarint vlen][value]   (del records carry no value fields)
//
// Puts of an identical value are skipped, so replayed blocks and repeated
// genesis writes don't grow the file. Torn tails are truncated at startup.
type miscStore struct {
	f     *os.File
	m     map[string][]byte
	dirty bool
}

func openMiscStore(dir string) (*miscStore, error) {
	f, err := os.OpenFile(filepath.Join(dir, "misc.log"), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	s := &miscStore{f: f, m: make(map[string][]byte)}
	r := bufio.NewReaderSize(f, 1<<20)
	var pos uint64
	for {
		end, op, k, v, err := readMiscRecord(r, pos)
		if err != nil {
			break // clean EOF or torn record: truncate at pos
		}
		if op == 0 {
			s.m[string(k)] = v
		} else {
			delete(s.m, string(k))
		}
		pos = end
	}
	if err := f.Truncate(int64(pos)); err != nil {
		f.Close()
		return nil, err
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return nil, err
	}
	return s, nil
}

func readMiscRecord(r *bufio.Reader, pos uint64) (end uint64, op byte, k, v []byte, err error) {
	op, err = r.ReadByte()
	if err != nil {
		return 0, 0, nil, nil, err
	}
	klen, err := binary.ReadUvarint(r)
	if err != nil {
		return 0, 0, nil, nil, err
	}
	k = make([]byte, klen)
	if _, err = io.ReadFull(r, k); err != nil {
		return 0, 0, nil, nil, err
	}
	end = pos + 1 + uint64(uvarintLen(klen)) + klen
	if op == 1 {
		return end, op, k, nil, nil
	}
	vlen, err := binary.ReadUvarint(r)
	if err != nil {
		return 0, 0, nil, nil, err
	}
	v = make([]byte, vlen)
	if _, err = io.ReadFull(r, v); err != nil {
		return 0, 0, nil, nil, err
	}
	end += uint64(uvarintLen(vlen)) + vlen
	return end, op, k, v, nil
}

func (s *miscStore) Get(key []byte) ([]byte, bool) {
	v, ok := s.m[string(key)]
	if !ok {
		return nil, false
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out, true
}

func (s *miscStore) Put(key, value []byte) error {
	if cur, ok := s.m[string(key)]; ok && bytes.Equal(cur, value) {
		return nil
	}
	var buf []byte
	buf = append(buf, 0)
	buf = binary.AppendUvarint(buf, uint64(len(key)))
	buf = append(buf, key...)
	buf = binary.AppendUvarint(buf, uint64(len(value)))
	buf = append(buf, value...)
	if _, err := s.f.Write(buf); err != nil {
		return fmt.Errorf("misc.log write: %w", err)
	}
	v := make([]byte, len(value))
	copy(v, value)
	s.m[string(key)] = v
	s.dirty = true
	return nil
}

func (s *miscStore) Delete(key []byte) error {
	if _, ok := s.m[string(key)]; !ok {
		return nil
	}
	var buf []byte
	buf = append(buf, 1)
	buf = binary.AppendUvarint(buf, uint64(len(key)))
	buf = append(buf, key...)
	if _, err := s.f.Write(buf); err != nil {
		return fmt.Errorf("misc.log write: %w", err)
	}
	delete(s.m, string(key))
	s.dirty = true
	return nil
}

func (s *miscStore) Sync() error {
	if !s.dirty {
		return nil
	}
	if err := s.f.Sync(); err != nil {
		return err
	}
	s.dirty = false
	return nil
}

func (s *miscStore) Close() error {
	if err := s.Sync(); err != nil {
		s.f.Close()
		return err
	}
	return s.f.Close()
}
