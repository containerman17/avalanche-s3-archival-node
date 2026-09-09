//go:build epochdb_ffi_link

// The link proof for libepochdb_engine.a: one Go binary with avalanchego's
// BLS package (its own cgo blst) and the epochdb engine (the Rust blst crate
// inside), linked without --allow-multiple-definition. Build after
// rs/ffi/localize.sh:
//
//	go build -tags epochdb_ffi_link -o /tmp/golink ./rs/ffi/golink && /tmp/golink
package main

/*
#cgo CFLAGS: -I${SRCDIR}/..
#cgo LDFLAGS: ${SRCDIR}/../../target/release/libepochdb_engine.a -lpthread -ldl -lm
#include "epochdb_engine.h"
*/
import "C"

import (
	"fmt"

	"github.com/ava-labs/avalanchego/utils/crypto/bls/signer/localsigner"
)

func main() {
	s, err := localsigner.New()
	if err != nil {
		panic(err)
	}
	sig, err := s.Sign([]byte("epochdb"))
	if err != nil {
		panic(err)
	}
	var b C.epochdb_buf
	C.epochdb_buf_free(&b)
	var ids [32]C.uint8_t
	var e *C.epochdb_engine
	rc := C.epochdb_verify(e, &ids[0], 0, nil)
	fmt.Printf("golink: bls signature %d bytes, epochdb_verify(NULL) = %d (EINVAL), both libraries linked\n", len(sig.Compress()), int(rc))
}
