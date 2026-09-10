//go:build !epochdb_stub

package validator

const realEngine = true

func prepareEngine([]byte) {}

// stubSet is the canned stub's hook; nothing to preload on the real engine.
func stubSet(int, []byte) {}
