//go:build linux

package hostview

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestMemTotal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "meminfo")
	if err := os.WriteFile(path, []byte("MemFree:  1024 kB\nMemTotal: 131072 kB\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if kib, ok := memTotalKiB(path); !ok || kib != 131072 {
		t.Fatalf("MemTotal %d %v", kib, ok)
	}
	if _, ok := memTotalKiB(filepath.Join(t.TempDir(), "absent")); ok {
		t.Fatal("read an absent file")
	}
}

func TestReadCapacity(t *testing.T) {
	c := ReadCapacity(t.TempDir())
	if c == nil || c.CPU != float64(runtime.NumCPU()) || c.MemGiB <= 0 || c.DiskGiB <= 0 {
		t.Fatalf("capacity %+v", c)
	}
	if c := ReadCapacity(filepath.Join(t.TempDir(), "absent")); c != nil {
		t.Fatalf("capacity of an absent path %+v", c)
	}
}
