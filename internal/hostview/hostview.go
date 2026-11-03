// Package hostview describes the host Sortie runs its agents on, for the
// operator's view of it: its name, its capacity, what Sortie's machines
// on it were given, and the machines on it that no Sortie started.
package hostview

import (
	"context"
	"log/slog"
	"sort"
	"sync"
	"time"
)

// Labels that say who started a machine: a Sortie's smolvm environment,
// or a worker daemon for a job.
const (
	LabelSortieManaged = "sortie.managed"
	LabelWorkerJob     = "worker-daemon.job"
)

// ListEvery is how often the host's machines are listed at most; a
// request in between serves the last listing.
const ListEvery = 30 * time.Second

// listTimeout bounds one listing.
const listTimeout = 10 * time.Second

// Size is an amount of CPUs, memory in GiB and disk in GiB.
type Size struct {
	CPU     float64 `json:"cpu"`
	MemGiB  float64 `json:"mem_gib"`
	DiskGiB float64 `json:"disk_gib"`
}

// Machine is a machine on the host that no Sortie started.
type Machine struct {
	Name string `json:"name"`
	// Since is when the machine was created, or else when Sortie first
	// listed it.
	Since time.Time `json:"since"`
	// State is the backend's word for it, such as "running"; empty when
	// unknown.
	State string `json:"state,omitempty"`
}

// Host is the host as GET /api/v1/state serves it.
type Host struct {
	Name string `json:"name"`
	// Capacity is the host's CPUs, memory and disk; nil when they cannot
	// be read.
	Capacity *Size `json:"capacity,omitempty"`
	// Used is what the machines Sortie started on the host were given:
	// CPUs and memory of the running ones, disk of all. Nil when the
	// environment lists no machines.
	Used *Size `json:"used,omitempty"`
	// Unmanaged lists the machines no Sortie started; nil when the
	// environment lists no machines.
	Unmanaged []Machine `json:"unmanaged,omitzero"`
}

// Listed is one machine of a backend's listing.
type Listed struct {
	Name       string
	State      string
	Labels     map[string]string
	CPUs       int
	MemoryMiB  int
	StorageGiB float64
	// Created is zero when the backend does not say.
	Created time.Time
}

func (l Listed) managed() bool {
	_, sortie := l.Labels[LabelSortieManaged]
	_, job := l.Labels[LabelWorkerJob]
	return sortie || job
}

// Mode says what the environment in force runs on this host.
type Mode int

const (
	// ModeElsewhere: runs go to workers on other hosts; no host is
	// served.
	ModeElsewhere Mode = iota
	// ModeHost: runs are processes on this host.
	ModeHost
	// ModeMachines: runs are machines on this host, which List lists.
	ModeMachines
)

// Options configures a [Source].
type Options struct {
	Name string
	// Mode reports the environment in force, read on every request.
	Mode func() Mode
	// Capacity reads the host's capacity; nil reads none.
	Capacity func() *Size
	// List lists the host's machines under [ModeMachines].
	List   func(ctx context.Context) ([]Listed, error)
	Now    func() time.Time
	Logger *slog.Logger
}

// Source serves the host. Safe for concurrent use.
type Source struct {
	opts Options

	mu        sync.Mutex
	listing   bool
	listedAt  time.Time
	listed    []Listed
	ok        bool
	firstSeen map[string]time.Time
}

// New returns a source for opts.
func New(opts Options) *Source {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &Source{opts: opts, firstSeen: map[string]time.Time{}}
}

// Host returns the host, or nil under [ModeElsewhere].
func (s *Source) Host(ctx context.Context) *Host {
	mode := ModeHost
	if s.opts.Mode != nil {
		mode = s.opts.Mode()
	}
	if mode == ModeElsewhere {
		return nil
	}
	h := &Host{Name: s.opts.Name}
	if s.opts.Capacity != nil {
		h.Capacity = s.opts.Capacity()
	}
	if mode != ModeMachines || s.opts.List == nil {
		return h
	}
	listed, firstSeen, ok := s.machines(ctx)
	if !ok {
		return h
	}
	used := Size{}
	h.Unmanaged = []Machine{}
	for _, l := range listed {
		if !l.managed() {
			since := l.Created
			if since.IsZero() {
				since = firstSeen[l.Name]
			}
			h.Unmanaged = append(h.Unmanaged, Machine{Name: l.Name, Since: since.UTC(), State: l.State})
			continue
		}
		if l.State == "running" {
			used.CPU += float64(l.CPUs)
			used.MemGiB += float64(l.MemoryMiB) / 1024
		}
		used.DiskGiB += l.StorageGiB
	}
	h.Used = &used
	sort.Slice(h.Unmanaged, func(a, b int) bool { return h.Unmanaged[a].Name < h.Unmanaged[b].Name })
	return h
}

// machines returns the last listing, listing again first when it is
// older than [ListEvery] and no other request is listing. A failed
// listing keeps the last one and waits as long; ok is false until one
// succeeds.
func (s *Source) machines(ctx context.Context) (listed []Listed, firstSeen map[string]time.Time, ok bool) {
	s.mu.Lock()
	due := !s.listing && (s.listedAt.IsZero() || s.opts.Now().Sub(s.listedAt) >= ListEvery)
	if due {
		s.listing = true
	}
	s.mu.Unlock()
	if due {
		lctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), listTimeout)
		got, err := s.opts.List(lctx)
		cancel()
		s.mu.Lock()
		s.listing = false
		now := s.opts.Now()
		s.listedAt = now
		if err != nil {
			s.opts.Logger.Warn("listing the host's machines failed", slog.Any("error", err))
		} else {
			seen := make(map[string]time.Time, len(got))
			for _, l := range got {
				first, known := s.firstSeen[l.Name]
				if !known {
					first = now
				}
				seen[l.Name] = first
			}
			s.listed, s.firstSeen, s.ok = got, seen, true
		}
		s.mu.Unlock()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listed, s.firstSeen, s.ok
}
