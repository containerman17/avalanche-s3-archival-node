package main

// The fleet: `epochdb serve` hosting SEVERAL chains in one process (--chains,
// DESIGN.md "THE FLEET"). It is not a second node, it is N of serve's own stack
// (startNode, follow.go) side by side, and the only genuinely multi-chain parts
// are here:
//
//   - ONE VM KIND. libevm's extras registry is process-global and panics on
//     re-registration, so every chain must be the same kind; a chain that is not
//     refuses to start and its siblings carry on (serveMain's loop).
//   - ONE CASFS. dist.SetRoot puts the spool and the chunk cache at the --data
//     root instead of inside each chain's dir, which makes the SSD-tier LRU
//     global across chains. Safe because artifacts are named by content hash.
//     One syncLoop, not N: the stores share the underlying casfs.
//   - ONE LISTENER, path-routed per chain, and a per-chain failure boundary
//     (fleetChain): a chain's executor hard-stop cancels that chain's goroutines
//     and nothing else, and /status is where an operator reads the cause.
//
// Anon memory stays bounded by construction: each chain's Firewood cache takes
// the clamp formula off its own dir size and the page cache is the read cache.
import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/ava-labs/avalanchego/ids"
)

// parseTipOverrides parses --tip-override. With SEVERAL chains it is PER CHAIN
// (RULING 2026-08-01): one process hosts N chains, so one bare value cannot say
// which one it caps. Syntax: <chain>=<containerID>[,...], keyed exactly as in
// --chains (C for the primary C-chain). A chain with no entry keeps following.
// With ONE chain there is nothing to disambiguate, so the bare value that
// single-chain serve always took is what it still takes.
func parseTipOverrides(v string, specs []string) (map[string]ids.ID, error) {
	if v == "" {
		return nil, nil
	}
	if len(specs) == 1 && !strings.Contains(v, "=") {
		v = specs[0] + "=" + v
	}
	// A blockchainID is cb58, which IS case-sensitive, so only the C alias
	// folds.
	canon := func(k string) (string, bool) {
		for _, s := range specs {
			if k == s || (strings.EqualFold(s, "C") && strings.EqualFold(k, "C")) {
				return s, true
			}
		}
		return "", false
	}
	out := map[string]ids.ID{}
	for _, kv := range strings.Split(v, ",") {
		kv = strings.TrimSpace(kv)
		if kv == "" {
			continue
		}
		key, val, ok := strings.Cut(kv, "=")
		if !ok {
			return nil, fmt.Errorf("%q is not <chain>=<containerID>: this process hosts several chains, so the override names one of --chains (%s)", kv, strings.Join(specs, ","))
		}
		key, val = strings.TrimSpace(key), strings.TrimSpace(val)
		spec, ok := canon(key)
		if !ok {
			return nil, fmt.Errorf("%q is not in --chains (%s)", key, strings.Join(specs, ","))
		}
		key = spec
		if _, dup := out[key]; dup {
			return nil, fmt.Errorf("%q listed twice", key)
		}
		id, err := parseTipOverride(val)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", key, err)
		}
		out[key] = id
	}
	return out, nil
}

// fleet is the supervisor: the chains and the failure boundary between them.
type fleet struct {
	mu     sync.Mutex
	chains []*fleetChain
	// onFail is set ONLY when there is a single chain, where the process has
	// nothing left to serve once it dies; with siblings it stays nil and a dead
	// chain is a /status entry, never a dead process (RULING 2026-08-03).
	onFail func(id string)
}

// fleetChain is ONE chain's failure domain. Everything about isolation lives
// here: the chain's own cancellable context, the flush that runs when it dies,
// and the recorded cause. A sibling's fleetChain is untouched by any of it.
type fleetChain struct {
	id     string
	node   *chainNode
	cancel context.CancelFunc
	// stop flushes this chain's writers. Called at most once, on the first
	// failure (or by closeAll at shutdown).
	stop   func()
	once   sync.Once
	onFail func(id string)

	mu  sync.Mutex
	err error // non-nil once the chain has stopped
}

func (f *fleet) add(ctx context.Context, id string, cfg nodeConfig) (*fleetChain, error) {
	// The per-chain context is the isolation mechanism: cancelling it stops
	// this chain's follower, executor and cook loop and reaches nothing else.
	cctx, cancel := context.WithCancel(ctx)
	// stop reaches the node through a pointer because a component can fail
	// between startNode launching its goroutines and returning, and report
	// must never call a nil hook. A failure that lands in that window flushes
	// at the recheck below instead.
	var node atomic.Pointer[chainNode]
	fc := &fleetChain{id: id, cancel: cancel, onFail: f.onFail, stop: func() {
		if n := node.Load(); n != nil {
			n.stop()
		}
	}}
	n, err := startNode(cctx, cfg, fc.report)
	if err != nil {
		cancel()
		return nil, err
	}
	node.Store(n)
	fc.node = n
	if fc.stopped() != nil {
		fc.once.Do(fc.stop)
	}
	f.mu.Lock()
	f.chains = append(f.chains, fc)
	f.mu.Unlock()
	return fc, nil
}

// report routes one component goroutine's exit for one chain: report's
// per-chain twin (follow.go). A clean finish and a cancellation are not
// failures; anything else stops THIS chain and leaves the process and its
// siblings serving.
func (c *fleetChain) report(what string, err error) {
	switch {
	case err == nil:
		log.Printf("epochdb: serve: %s: %s finished; the chain keeps serving", c.id, what)
		return
	case errors.Is(err, context.Canceled):
		log.Printf("epochdb: serve: %s: %s stopped: %v", c.id, what, err)
		return
	}
	c.mu.Lock()
	first := c.err == nil
	if first {
		c.err = fmt.Errorf("%s: %w", what, err)
	}
	c.mu.Unlock()
	if !first {
		return
	}
	log.Printf("epochdb: serve: %s STOPPED: %s: %v (the process and its sibling chains keep serving)", c.id, what, err)
	c.cancel()
	c.once.Do(c.stop)
	if c.onFail != nil {
		c.onFail(c.id)
	}
}

// fail records a chain that never started at all: same failure state as one
// that died later, same place an operator reads it (/status), and NOT a reason
// for the process to die. Everything that can refuse a chain routes here, so a
// bad descriptor, a wrong VM kind, an unmakeable directory and a broken epoch
// chain all read the same way.
func (f *fleet) fail(id string, err error) {
	// onFail set means this is the only chain, so there are no siblings to
	// reassure anyone about.
	note := " (siblings unaffected; /status carries the reason)"
	if f.onFail != nil {
		note = ""
	}
	log.Printf("epochdb: serve: %s NOT STARTED: %v%s", id, err, note)
	f.mu.Lock()
	f.chains = append(f.chains, &fleetChain{id: id, err: err})
	f.mu.Unlock()
	if f.onFail != nil {
		f.onFail(id)
	}
}

// stopped returns the cause, nil while the chain is running.
func (c *fleetChain) stopped() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

// statusHandler is /status: what every chain in the process is doing, and which
// ones have stopped and why. The one place an operator learns that chain 3 is
// dead while 1, 2 and 4 are at tip.
func (f *fleet) statusHandler(w http.ResponseWriter, r *http.Request) {
	type chainStatus struct {
		Chain    string `json:"chain"`
		Stopped  bool   `json:"stopped"`
		Error    string `json:"error,omitempty"`
		Accepted uint64 `json:"accepted"`
		Executed uint64 `json:"executed"`
		Cooked   uint64 `json:"cooked"`
	}
	f.mu.Lock()
	chains := append([]*fleetChain(nil), f.chains...)
	f.mu.Unlock()
	out := make([]chainStatus, 0, len(chains))
	for _, c := range chains {
		s := chainStatus{Chain: c.id}
		if err := c.stopped(); err != nil {
			s.Stopped, s.Error = true, err.Error()
		}
		if c.node != nil {
			s.Accepted, s.Executed, s.Cooked = c.node.accepted(), c.node.e.LiveHead(), c.node.hist.StateHead()
		}
		out = append(out, s)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(out)
}

// closeAll is the clean shutdown: flush and release every chain. Call it only
// after the listener is down.
func (f *fleet) closeAll() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.chains {
		if c.node == nil {
			continue
		}
		c.node.closeAll() // chainNode.stopOnce keeps a dead chain from double-closing
		log.Printf("epochdb: serve: %s stopped at executed=%d", c.id, c.node.e.LiveHead())
	}
}
