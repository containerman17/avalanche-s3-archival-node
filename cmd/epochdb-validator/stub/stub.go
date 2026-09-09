// Package stub is a canned C implementation of epochdb_engine.h, linked
// instead of libepochdb_engine.a when the validator package is built with
// -tags epochdb_stub. See stub.c.
package stub

import "C"
