package state

import (
	"log"
	"time"
)

// A SEAL IS CPU-BOUND AND USED TO BE SILENT: it logged when it finished and
// not before, so mainnet's epoch 9 spent over twenty minutes at 100% CPU and
// 5.8 GB resident without a single line (2026-08-06). From outside, an
// in-progress seal was indistinguishable from a hang, and an 8M-tx epoch takes
// 40 minutes to over an hour. The frontier merge has the same shape.
//
// The cadence is TIME-BASED, never per item: an 8M-tx epoch would drown the
// log and slow the seal down if every row said something. It is a var only so
// a test can turn it down; there is no knob in production.
var progressEvery = 30 * time.Second

// LogProgress prints "<phase>: <line()> (<elapsed>)" every progressEvery until
// the returned stop is called. stop WAITS for the ticker goroutine, so no
// progress line can land after the phase's own completion line.
//
// line runs on that goroutine: it may read only atomics and immutable values,
// and its state must be O(1). The seal's bounded peak memory was hard-won
// (2M-block epochs went 11.07 GB -> 2.17 GB) and progress state that grows
// with the epoch is the same bug in a smaller font.
//
//	defer LogProgress("seal: sst", func() string { return "..." })()
func LogProgress(phase string, line func() string) (stop func()) {
	done, stopped := make(chan struct{}), make(chan struct{})
	t0 := time.Now()
	go func() {
		defer close(stopped)
		t := time.NewTicker(progressEvery)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				log.Printf("%s: %s (%s)", phase, line(), time.Since(t0).Round(time.Second))
			}
		}
	}()
	return func() { close(done); <-stopped }
}
