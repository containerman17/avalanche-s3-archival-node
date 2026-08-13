package store

// A TEST-ONLY SCALE FOR THE TRIGGERS, and it exists because of one number: a
// terminal run is EIGHT MILLION TRANSACTIONS, and no test is going to execute
// eight million transactions to watch a pointer move. Everything the publish
// path does is a function of the boundaries, not of their size, so a test that
// cuts terminals every few dozen txs exercises the same code the mainnet
// cadence does.
//
// It lives in a _test.go file ON PURPOSE: no production build contains it,
// there is no environment variable anywhere near it, and the pinned format
// constants (FlushTxs, FlushBlocks, TerminalTxs) are untouched. Determinism is
// unharmed because the boundaries stay a pure function of chain content: two
// builders under the same scale cut in the same places, which is exactly what
// the byte-identity tests check.
func (d *DB) scaleTriggers(flushTxs, flushBlocks, terminalTxs uint64) {
	d.flushTxs, d.flushBlocks, d.terminalTxs = flushTxs, flushBlocks, terminalTxs
}
