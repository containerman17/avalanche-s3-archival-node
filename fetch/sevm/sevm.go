// Package sevm holds the subnet-evm half of the libevm extras registration.
//
// Its own package for one reason: libevm's registry is process-global and
// panics on re-registration, package fetch's own tests register the coreth set,
// and `go test` runs one process per package. So the only way to exercise this
// path is from a package that never touches coreth, which is this one.
package sevm

import (
	sevmcore "github.com/ava-labs/avalanchego/graft/subnet-evm/core"
	sevmparams "github.com/ava-labs/avalanchego/graft/subnet-evm/params"
	sevmcustomtypes "github.com/ava-labs/avalanchego/graft/subnet-evm/plugin/evm/customtypes"
)

// Register installs the three subnet-evm libevm extras. This is subnet-evm's
// own aggregate registrar, plugin/evm.RegisterAllLibEVMExtras, inlined: calling
// it directly would link the whole VM plugin (gRPC servers, state sync, the
// snowman engine glue) into the binary for three function calls. If upstream
// ever adds a fourth, it goes there and must be mirrored here.
//
// NOTE the asymmetry with coreth, which is expected and not an omission:
// subnet-evm has NO extstate.RegisterExtras equivalent, because it has no
// multi-coin storage-key normalization to install.
func Register() {
	sevmcore.RegisterExtras()
	sevmcustomtypes.Register()
	sevmparams.RegisterExtras()
}
