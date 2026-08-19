//go:build !linux

package dist

import "errors"

// The page-cache controls are Linux syscalls. Everywhere else dropping a file
// from the cache is a no-op and measuring residency is unsupported, which is
// honest: the node runs on Linux.
func coldFile(string) error { return nil }

func residentPages(string) (int, int, error) {
	return 0, 0, errors.New("dist: page residency is only measurable on linux")
}
