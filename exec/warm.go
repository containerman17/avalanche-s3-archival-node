package exec

import (
	"os"
	"strconv"

	"github.com/ava-labs/libevm/common"
	ethstate "github.com/ava-labs/libevm/core/state"
	"github.com/ava-labs/libevm/core/types"
)

// warmEnabled gates the speculative pre-execution (EPOCHDB_WARM=0 turns it
// off), so the A/B is one env flip.
var warmEnabled = os.Getenv("EPOCHDB_WARM") != "0"

// warmWorkers is the size of the warm pool (EPOCHDB_WARM_WORKERS). Four:
// the executor is one core, the warm run of a block costs about the same
// as its real execution, and the box has eight cores that were idle.
var warmWorkers = func() int {
	if n, err := strconv.Atoi(os.Getenv("EPOCHDB_WARM_WORKERS")); err == nil && n > 0 {
		return n
	}
	return 4
}()

// warmBlock runs blk on a THROWAWAY statedb opened at the last committed
// root, off the executor goroutine, purely for the reads: every account,
// slot and code blob it touches lands in Firewood's node cache and the page
// cache before the executor asks for it. The state is stale by up to one
// batch, so account checks are skipped and per-tx failures are ignored; the
// result is discarded. Reads go through the inner database (a read-only
// Firewood revision), never through wrapDB's unlocked Go cache.
func (e *Executor) warmBlock(blk *types.Block) {
	if !warmEnabled || len(blk.Transactions()) == 0 {
		return
	}
	root, ok := e.committedRoot.Load().(common.Hash)
	if !ok {
		return
	}
	statedb, err := ethstate.New(root, e.innerDB, nil)
	if err != nil {
		return
	}
	e.vm.warmEVM(chainContext{store: e.cfg.Store}, e.chainCfg, blk, statedb)
}
