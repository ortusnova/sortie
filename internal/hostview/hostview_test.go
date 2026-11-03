package hostview

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"
)

type fakeList struct {
	machines []Listed
	err      error
	calls    int
}

func (f *fakeList) list(context.Context) ([]Listed, error) {
	f.calls++
	return f.machines, f.err
}

type clock struct{ t time.Time }

func (c *clock) now() time.Time { return c.t }

var t0 = time.Date(2026, 11, 2, 9, 0, 0, 0, time.UTC)

func source(mode Mode, f *fakeList, c *clock) *Source {
	return New(Options{
		Name:     "taichi",
		Mode:     func() Mode { return mode },
		Capacity: func() *Size { return &Size{CPU: 32, MemGiB: 125, DiskGiB: 1800} },
		List:     f.list,
		Now:      c.now,
	})
}

func managed(name, state string, cpus, mem int, disk float64) Listed {
	return Listed{Name: name, State: state, Labels: map[string]string{LabelSortieManaged: "1"},
		CPUs: cpus, MemoryMiB: mem, StorageGiB: disk}
}

// The machines Sortie started count in used: CPUs and memory of the
// running ones, disk of all. Every other machine is unmanaged, aged by
// its creation time.
func TestHostUsedAndUnmanaged(t *testing.T) {
	created := t0.Add(-3 * time.Hour)
	f := &fakeList{machines: []Listed{
		managed("sortie-ONS-34", "running", 4, 8192, 20),
		managed("sortie-ONS-68", "stopped", 4, 8192, 20),
		{Name: "job-ONS-9", State: "running", Labels: map[string]string{LabelWorkerJob: "ONS-9"}, CPUs: 2, MemoryMiB: 4096},
		{Name: "sandbox-brian", State: "running", Labels: map[string]string{}, CPUs: 8, MemoryMiB: 16384, Created: created},
	}}
	h := source(ModeMachines, f, &clock{t0}).Host(t.Context())
	want := &Host{
		Name:      "taichi",
		Capacity:  &Size{CPU: 32, MemGiB: 125, DiskGiB: 1800},
		Used:      &Size{CPU: 6, MemGiB: 12, DiskGiB: 40},
		Unmanaged: []Machine{{Name: "sandbox-brian", Since: created, State: "running"}},
	}
	if !reflect.DeepEqual(h, want) {
		t.Fatalf("host %+v, want %+v", h, want)
	}
}

// A machine smolvm gives no creation time is aged from when Sortie
// first listed it, and keeps that age across listings.
func TestHostUnmanagedSinceFirstListed(t *testing.T) {
	c := &clock{t0}
	f := &fakeList{machines: []Listed{{Name: "sandbox", State: "stopped"}}}
	s := source(ModeMachines, f, c)
	s.Host(t.Context())
	c.t = t0.Add(ListEvery)
	h := s.Host(t.Context())
	if f.calls != 2 || len(h.Unmanaged) != 1 || !h.Unmanaged[0].Since.Equal(t0) {
		t.Fatalf("calls %d, host %+v", f.calls, h)
	}
}

// The listing runs at most every ListEvery, and a failed one keeps the
// last; before any succeeds, used and unmanaged are absent.
func TestHostListingIsCachedAndKeptOnFailure(t *testing.T) {
	c := &clock{t0}
	f := &fakeList{err: errors.New("smolvm: no such binary")}
	s := source(ModeMachines, f, c)
	if h := s.Host(t.Context()); h.Used != nil || h.Unmanaged != nil {
		t.Fatalf("host before a listing %+v", h)
	}
	f.err, f.machines = nil, []Listed{{Name: "sandbox", State: "running"}}
	c.t = t0.Add(time.Second)
	if h := s.Host(t.Context()); h.Unmanaged != nil || f.calls != 1 {
		t.Fatalf("listed again within ListEvery: calls %d, host %+v", f.calls, h)
	}
	c.t = t0.Add(ListEvery)
	if h := s.Host(t.Context()); len(h.Unmanaged) != 1 || f.calls != 2 {
		t.Fatalf("calls %d, host %+v", f.calls, h)
	}
	f.err = errors.New("smolvm: timed out")
	c.t = t0.Add(2 * ListEvery)
	if h := s.Host(t.Context()); len(h.Unmanaged) != 1 || f.calls != 3 {
		t.Fatalf("a failed listing dropped the last: calls %d, host %+v", f.calls, h)
	}
}

// Runs on workers elsewhere serve no host; runs as processes on the
// host serve its name and capacity alone.
func TestHostByMode(t *testing.T) {
	f := &fakeList{}
	if h := source(ModeElsewhere, f, &clock{t0}).Host(t.Context()); h != nil {
		t.Fatalf("host under ModeElsewhere %+v", h)
	}
	h := source(ModeHost, f, &clock{t0}).Host(t.Context())
	if h == nil || h.Used != nil || h.Unmanaged != nil || h.Capacity == nil || f.calls != 0 {
		t.Fatalf("host under ModeHost %+v, listed %d times", h, f.calls)
	}
	b, _ := json.Marshal(h)
	if string(b) != `{"name":"taichi","capacity":{"cpu":32,"mem_gib":125,"disk_gib":1800}}` {
		t.Fatalf("JSON %s", b)
	}
	b, _ = json.Marshal(source(ModeMachines, f, &clock{t0}).Host(t.Context()))
	if string(b) != `{"name":"taichi","capacity":{"cpu":32,"mem_gib":125,"disk_gib":1800},"used":{"cpu":0,"mem_gib":0,"disk_gib":0},"unmanaged":[]}` {
		t.Fatalf("JSON with no machines %s", b)
	}
}
