//go:build epochdb_stub

package validator

/*
#include <stddef.h>
#include <stdint.h>
void epochdb_stub_set(int kind, const uint8_t *p, size_t n);
*/
import "C"

import (
	"unsafe"

	// Link the canned C engine instead of libepochdb_engine.a.
	_ "github.com/containerman17/avalanche-s3-archival-node/cmd/epochdb-validator/stub"
)

// stubSet preloads the stub's canned head header (kind 0) or rpc reply (1).
func stubSet(kind int, raw []byte) {
	C.epochdb_stub_set(C.int(kind), (*C.uint8_t)(unsafe.Pointer(&raw[0])), C.size_t(len(raw)))
}
