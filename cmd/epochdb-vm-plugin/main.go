// Command epochdb-vm-plugin serves vmchain.VM over avalanchego's rpcchainvm:
// launch it from cmd/epochdb-host (--vm <this binary>) or install it in a
// stock avalanchego's plugin dir under the VM's ID.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"runtime/debug"

	"github.com/ava-labs/avalanchego/vms/rpcchainvm"

	"github.com/containerman17/avalanche-s3-archival-node/vmchain"
	"github.com/containerman17/avalanche-s3-archival-node/vmexec"
)

func main() {
	if len(os.Args) > 1 && (os.Args[1] == "--version" || os.Args[1] == "version") {
		fmt.Println(vmchain.Version)
		return
	}
	// As cmd/epochdb-vm: 7/10 of the cgroup ceiling as the soft limit, GOGC
	// 50 unless the environment says otherwise.
	if os.Getenv("GOMEMLIMIT") == "" {
		if limit, ok := vmexec.CgroupMemoryLimit(); ok {
			debug.SetMemoryLimit(int64(limit / 10 * 7))
		}
	}
	if os.Getenv("GOGC") == "" {
		debug.SetGCPercent(50)
	}
	if err := rpcchainvm.Serve(context.Background(), &vmchain.VM{}); err != nil {
		log.Fatalf("epochdb-vm-plugin: %v", err)
	}
}
