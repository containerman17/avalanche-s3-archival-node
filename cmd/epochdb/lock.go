package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"syscall"
)

// dataDirLockFile is the one file that says who owns a data dir. flock is
// advisory, per open file description, and released by the kernel on exit, so
// a killed process never leaves a stale lock behind.
const dataDirLockFile = ".epochdb.lock"

// lockDataDir takes the dir's exclusive writer lock for the life of the
// process. The returned closer releases it; dropping it on the floor is fine
// too, since exiting does the same thing.
//
// READ-ONLY OPENERS DO NOT CALL THIS and must not: a read-only opener, the SDK
// and `dev probe` read a live chain's dir beside its writer on purpose.
func lockDataDir(dir string) (func(), error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	name := filepath.Join(dir, dataDirLockFile)
	f, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("data dir %s is already held by another epochdb process (%s is flocked): one process writes one data dir."+
			" Stop that process (or point this one at another --data dir) and start again;"+
			" read-only users (the SDK, `epochdb dev probe`) need no lock and work beside it", dir, name)
	}
	return func() { f.Close() }, nil
}

// mustLockDataDir is lockDataDir for the dev stages, which write the dir and so
// take the same lock `serve` does: DESIGN warns that no dev stage may run
// beside a live serve, and this is that warning with teeth. One line per stage,
// `defer mustLockDataDir("seal", *dataDir)()`.
func mustLockDataDir(stage, dir string) func() {
	release, err := lockDataDir(dir)
	if err != nil {
		log.Fatalf("epochdb: %s: %v", stage, err)
	}
	return release
}
