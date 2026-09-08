package vmexec

import (
	"log"
	"math"
	"os"
	"runtime/debug"
	"strconv"
)

// Budget is the pair of memory profiles Run switches between by distance to
// the tip: catching up may use a lot of memory, at the tip the node calms
// down (a small overlay budget, a normal GOGC, one roll to give the ground
// back). Zero fields take the defaults in New.
type Budget struct {
	// Accepted is the fetcher's accepted head, 0 while unknown. nil means
	// the tip is never reached: the catch-up profile for the whole run.
	Accepted func() uint64
	// TipLag is how many blocks behind Accepted still count as the tip;
	// catch-up resumes past twice that (hysteresis).
	TipLag            uint64
	SyncGOGC, TipGOGC int
	SyncRoll, TipRoll int // overlay bytes that trigger a roll
}

func (b *Budget) defaults() {
	if b.TipLag == 0 {
		b.TipLag = 5000
	}
	if b.SyncGOGC == 0 {
		b.SyncGOGC = 400
	}
	if b.TipGOGC == 0 {
		b.TipGOGC = 100
	}
	if b.SyncRoll <= 0 {
		b.SyncRoll = 2 << 30
	}
	if b.TipRoll <= 0 {
		b.TipRoll = 128 << 20
	}
}

// tipGate is the hysteresis: TIP at lag <= limit, back to catch-up only past
// 2*limit, so a node hovering around the threshold does not flap.
type tipGate struct {
	lag   func() uint64
	limit uint64
	tip   bool
}

// update reads the lag and reports whether the profile changed.
func (g *tipGate) update() (lag uint64, changed bool) {
	lag = g.lag()
	switch {
	case !g.tip && lag <= g.limit:
		g.tip = true
		return lag, true
	case g.tip && lag > 2*g.limit:
		g.tip = false
		return lag, true
	}
	return lag, false
}

// lag is the executor's distance to the accepted head; unknown = infinite.
func (e *Executor) lag() uint64 {
	if e.cfg.Budget.Accepted == nil {
		return math.MaxUint64
	}
	a := e.cfg.Budget.Accepted()
	if a == 0 {
		return math.MaxUint64
	}
	if a <= e.headNum {
		return 0
	}
	return a - e.headNum
}

// applyProfile sets GOGC (unless the environment pins it) and the roll
// budget for the gate's current side.
func (e *Executor) applyProfile(why string) {
	b := e.cfg.Budget
	name, gogc, roll := "catch-up", b.SyncGOGC, b.SyncRoll
	if e.gate.tip {
		name, gogc, roll = "tip", b.TipGOGC, b.TipRoll
	}
	e.rollBudget = roll
	if os.Getenv("GOGC") == "" {
		debug.SetGCPercent(gogc)
	}
	log.Printf("vmexec: budget %s (%s): gogc=%d roll-budget=%dMB rss=%dMB", name, why, gogc, roll>>20, RSSMB())
}

// tickBudget runs on the executor goroutine every few seconds. On the way
// to the tip it rolls whatever the overlay holds and returns freed heap to
// the OS once that roll is swapped in (right away when there is nothing to
// roll), so RSS actually drops.
func (e *Executor) tickBudget() error {
	lag, changed := e.gate.update()
	if !changed {
		return e.swapRoll()
	}
	e.applyProfile("lag=" + strconv.FormatUint(lag, 10))
	if e.gate.tip {
		if e.eng.overlay.Len() > 0 {
			e.eng.maybeRoll(0, e.headNum, e.headRoot)
		}
		if e.eng.frozen != nil {
			e.freeOS = true
		} else {
			go debug.FreeOSMemory()
		}
	}
	return e.swapRoll()
}
