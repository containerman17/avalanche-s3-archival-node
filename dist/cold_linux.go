//go:build linux

package dist

import (
	"os"
	"unsafe"

	"golang.org/x/sys/unix"
)

// coldFile drops a file's clean page-cache pages. POSIX_FADV_DONTNEED skips
// pages that are dirty or MAPPED INTO SOME PROCESS'S PAGE TABLE, which is why
// Blob.Cold madvises its own mapping away first: a page the merge faulted in
// through the mmap is mapped, and fadvise alone would leave every one of them
// resident.
func coldFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_DONTNEED)
}

// residentPages counts how many of a file's pages are in the page cache. It is
// mincore over a throwaway mapping, and it exists so the page-cache rule is
// MEASURED rather than assumed (store's merge tests read it).
func residentPages(path string) (resident, total int, err error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil || st.Size() == 0 {
		return 0, 0, err
	}
	mm, err := unix.Mmap(int(f.Fd()), 0, int(st.Size()), unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		return 0, 0, err
	}
	defer unix.Munmap(mm)
	page := os.Getpagesize()
	total = (len(mm) + page - 1) / page
	// mincore(2) has no wrapper in x/sys/unix, and the raw call is three
	// arguments: it fills one byte per page whose low bit says "resident".
	vec := make([]byte, total)
	if _, _, e := unix.Syscall(unix.SYS_MINCORE,
		uintptr(unsafe.Pointer(&mm[0])), uintptr(len(mm)), uintptr(unsafe.Pointer(&vec[0]))); e != 0 {
		return 0, total, e
	}
	for _, v := range vec {
		if v&1 != 0 {
			resident++
		}
	}
	return resident, total, nil
}
