//go:build linux

package hostview

import (
	"bufio"
	"os"
	"runtime"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

// ReadCapacity reads the host's CPUs, its memory from /proc/meminfo and
// the size of the file system that holds diskPath. It returns nil when
// any of them cannot be read.
func ReadCapacity(diskPath string) *Size {
	mem, ok := memTotalKiB("/proc/meminfo")
	if !ok {
		return nil
	}
	var fs unix.Statfs_t
	if diskPath == "" || unix.Statfs(diskPath, &fs) != nil {
		return nil
	}
	const gib = 1 << 30
	return &Size{
		CPU:     float64(runtime.NumCPU()),
		MemGiB:  float64(mem) / (1 << 20),
		DiskGiB: float64(fs.Blocks) * float64(fs.Bsize) / gib,
	}
}

// memTotalKiB reads MemTotal from a meminfo file.
func memTotalKiB(path string) (uint64, bool) {
	f, err := os.Open(path) //nolint:gosec // G304: a fixed kernel file
	if err != nil {
		return 0, false
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) >= 2 && fields[0] == "MemTotal:" {
			kib, err := strconv.ParseUint(fields[1], 10, 64)
			return kib, err == nil && kib > 0
		}
	}
	if err := sc.Err(); err != nil {
		return 0, false // a read error: no MemTotal to report
	}
	return 0, false
}
