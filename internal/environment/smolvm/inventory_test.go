package smolvm

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestParseInventory_RecordedListingMinimumVersion(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile(filepath.Join("testdata", "ls-1.32.4.json"))
	if err != nil {
		t.Fatal(err)
	}
	listed, err := ParseInventory(data)
	if err != nil {
		t.Fatalf("ParseInventory() error = %v", err)
	}
	if len(listed) != 2 {
		t.Fatalf("ParseInventory() = %d machines, want 2", len(listed))
	}
	m := listed[0]
	created := time.Unix(1793451172, 0).UTC()
	if m.Name != "ls-fixture-running" || m.State != StateRunning || m.CPUs != 4 || m.MemoryMiB != 8192 ||
		m.StorageGiB != 0 || !m.Created.Equal(created) || m.Labels["sortie.workspace"] != "VM-1" {
		t.Errorf("machine = %+v", m)
	}
	if listed[1].State != StateCreated {
		t.Errorf("second machine state = %q, want created", listed[1].State)
	}
}

func TestParseInventory_SizesAndTimes(t *testing.T) {
	t.Parallel()

	listed, err := ParseInventory([]byte(`[
		{"name": "a", "state": "stopped", "storage_gb": 20, "overlay_gb": 2, "created_at": "2026-11-02T07:00:00+01:00"},
		{"name": "", "state": "running"},
		{"name": "b", "created_at": null}
	]`))
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 2 {
		t.Fatalf("listed %+v", listed)
	}
	if a := listed[0]; a.StorageGiB != 22 || !a.Created.Equal(time.Date(2026, 11, 2, 6, 0, 0, 0, time.UTC)) {
		t.Errorf("a = %+v", a)
	}
	if b := listed[1]; !b.Created.IsZero() || b.Labels == nil {
		t.Errorf("b = %+v", b)
	}
	if _, err := ParseInventory([]byte(`{}`)); err == nil {
		t.Error("an object parsed as a listing")
	}
}
