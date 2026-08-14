package dist

// Talking to a public Avalanche API node, which is the one thing a node does
// that it does not control.
//
// WHY THIS EXISTS (mainnet C, 2026-08-13): 37 chains on one box each resolved
// their network ID, their chain and their validator set against the same
// public endpoint, the endpoint rate-limited the box's IP, and a 429 was a
// FATAL startup error. The orchestrator restarted the chain, the restart hit
// the same 429, and the loop hammered the endpoint that was already refusing
// it. A transient upstream condition may cost a node minutes of startup; it
// may NEVER cost it the process, and a node may never answer a rate limit by
// asking faster.

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"strings"
	"time"
)

// Vars rather than consts ONLY so a test can spend the whole budget in
// milliseconds. Nothing at runtime writes them.
var (
	// upstreamRounds is how many times every source is tried before Try gives
	// up. Bounded on purpose: a node that cannot reach any P-chain endpoint
	// must say so loudly rather than hang, and the operator hears it after
	// ~2 minutes instead of never.
	upstreamRounds = 9
	// upstreamBackoff is the wait after the first failed round, doubling per
	// round up to upstreamMaxWait: 0.25s, 0.5s, 1s ... 60s, ~124s in all. The
	// first retry is fast because most upstream failures are a blip; the tail
	// is slow because a rate limit is not.
	upstreamBackoff = 250 * time.Millisecond
	upstreamMaxWait = 60 * time.Second
	// upstreamTimeout bounds ONE call to ONE source.
	upstreamTimeout = 30 * time.Second
)

// Try calls fn against each source in turn and returns as soon as one
// succeeds, so a rate-limited or broken endpoint costs a request rather than a
// node. When every source fails it waits (exponential backoff with jitter, so
// a fleet restarting together does not retry in lockstep) and goes round
// again, and when the budget is spent it fails with what every source said.
//
// what names the call for the log, e.g. "info.getNetworkID".
func Try(ctx context.Context, what string, sources []string, fn func(ctx context.Context, source string) error) error {
	if len(sources) == 0 {
		return fmt.Errorf("%s: no upstream source configured", what)
	}
	var errs []error
	for round := 0; ; round++ {
		errs = errs[:0]
		for _, src := range sources {
			cctx, cancel := context.WithTimeout(ctx, upstreamTimeout)
			err := fn(cctx, src)
			cancel()
			if err == nil {
				return nil
			}
			errs = append(errs, fmt.Errorf("%s: %w", src, err))
		}
		if round >= upstreamRounds-1 {
			return fmt.Errorf("%s: gave up after %d attempts over %d source(s): %w",
				what, upstreamRounds, len(sources), errors.Join(errs...))
		}
		wait := Jitter(min(upstreamBackoff<<round, upstreamMaxWait))
		// The operator has to be able to see why a startup is slow, and this
		// line is the whole answer.
		log.Printf("dist: %s: every source failed (%v), retrying in %s (round %d of %d)",
			what, errors.Join(errs...), wait.Round(time.Millisecond), round+2, upstreamRounds)
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s: %w (last: %w)", what, ctx.Err(), errors.Join(errs...))
		case <-time.After(wait):
		}
	}
}

// Sources splits a comma-separated endpoint list and drops the blanks. One
// flag, several hosts: that is the whole failover configuration.
func Sources(list string) []string {
	var out []string
	for _, s := range strings.Split(list, ",") {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// Jitter spreads d over [0.75d, 1.25d]. Every wait in this file is jittered:
// dozens of containers on one box start, fail and refresh together, and a
// synchronised retry is the burst that caused the rate limit in the first
// place.
func Jitter(d time.Duration) time.Duration {
	return d - d/4 + rand.N(d/2)
}
