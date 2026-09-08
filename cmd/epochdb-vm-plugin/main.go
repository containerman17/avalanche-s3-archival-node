// Command epochdb-vm-plugin serves vmchain.VM over avalanchego's rpcchainvm:
// launch it from cmd/epochdb-host (--vm <this binary>) or install it in a
// stock avalanchego's plugin dir under the VM's ID.
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/ava-labs/avalanchego/vms/rpcchainvm"

	"github.com/containerman17/avalanche-s3-archival-node/vmchain"
)

func main() {
	if len(os.Args) > 1 && (os.Args[1] == "--version" || os.Args[1] == "version") {
		fmt.Println(vmchain.Version)
		return
	}
	// GOMEMLIMIT and GOGC are the executor's (vmexec/budget.go).
	if err := rpcchainvm.Serve(context.Background(), &vmchain.VM{}); err != nil {
		log.Fatalf("epochdb-vm-plugin: %v", err)
	}
}
