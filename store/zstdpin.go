//go:build pebblegozstd

package store

// CGO IS A PINNED FORMAT CONSTANT (DESIGN's determinism pins). pebble reaches
// zstd through github.com/DataDog/zstd under `cgo && !pebblegozstd` and through
// github.com/klauspost/compress/zstd otherwise, and the two write DIFFERENT
// BYTES from the same rows (klauspost also collapses zstd levels onto four
// speed classes, so the pinned level stops meaning anything). A build with this
// tag would produce runs that are readable but not byte-identical to every
// other builder's, which is exactly what the core promise forbids.
//
// So this build does not exist. The negative array length below is the refusal.
var _ [-1]byte
