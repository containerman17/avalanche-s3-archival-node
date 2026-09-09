// Command epochdb-validator serves validator.VM over avalanchego's
// rpcchainvm: install it in a stock avalanchego's plugin dir under the VM's
// ID (srEXiWaHuhNyGwPUi444Tu47ZEDwxTWrbQiuD7FmgSAQ6X7Dy for subnet-evm
// chains). It links the Rust engine, rs/target/release/libepochdb_engine.a
// (cd rs && cargo build --release -p epochdb-ffi).
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/ava-labs/avalanchego/vms/rpcchainvm"

	"github.com/containerman17/avalanche-s3-archival-node/validator"
)

func main() {
	if len(os.Args) > 1 && (os.Args[1] == "--version" || os.Args[1] == "version") {
		fmt.Println(validator.Version)
		return
	}
	if err := rpcchainvm.Serve(context.Background(), &validator.VM{}); err != nil {
		log.Fatalf("epochdb-validator: %v", err)
	}
}
