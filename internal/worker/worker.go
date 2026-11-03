// Package worker matches jobs to workers that dial out to Sortie. A
// worker checks in with its capabilities and free resources, polls for
// a job, and holds a lease on it that it renews by heartbeat until it
// reports the attempt's outcome. The rule that decides who holds a
// lease sits behind [Leaser].
package worker

import (
	"errors"
	"fmt"
	"math"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Protocol is the worker API protocol version this package serves.
const Protocol = 1

// Errors reported by the matcher and the lease rule.
var (
	ErrUnknownWorker = errors.New("worker: unknown worker")
	ErrUnknownLease  = errors.New("worker: unknown lease")
	ErrLeaseHeld     = errors.New("worker: job already leased")
	ErrLeaseLost     = errors.New("worker: lease expired or held by another worker")
)

// Free is what a worker has free after its own reserve and floor: CPUs,
// memory in GiB and disk in GiB.
type Free struct {
	CPU     float64 `json:"cpu"`
	MemGiB  float64 `json:"mem_gib"`
	DiskGiB float64 `json:"disk_gib"`
}

// Declaration is what a worker states at check-in: its capabilities,
// its run slots and how many machines it runs, and what it has free. A
// running machine counts against the run slots; a stopped one only
// against the free disk.
type Declaration struct {
	OS       string   `json:"os"`
	Arch     string   `json:"arch"`
	KVM      bool     `json:"kvm"`
	Nested   bool     `json:"nested"`
	Backends []string `json:"backends"`
	RunSlots int      `json:"run_slots"`
	Running  int      `json:"running"`
	Free     Free     `json:"free"`
	// Used is what the worker's running jobs were promised; nil when the
	// worker does not say.
	Used *Free `json:"used,omitempty"`
	// Unmanaged lists the machines on the worker's host that no Sortie
	// started; nil when the worker does not say.
	Unmanaged []Unmanaged `json:"unmanaged,omitzero"`
}

// has reports whether the declaration includes capability c: its OS,
// its architecture, "kvm", "nested", or one of its backends.
func (d Declaration) has(c string) bool {
	switch c {
	case "kvm":
		return d.KVM
	case "nested":
		return d.Nested
	}
	return c == d.OS || c == d.Arch || slices.Contains(d.Backends, c)
}

// spare counts the capabilities the declaration includes that r does
// not need.
func (d Declaration) spare(r Requires) int {
	caps := append([]string{d.OS, d.Arch}, d.Backends...)
	if d.KVM {
		caps = append(caps, "kvm")
	}
	if d.Nested {
		caps = append(caps, "nested")
	}
	n := 0
	for _, c := range caps {
		if !slices.Contains(r.Capabilities, c) {
			n++
		}
	}
	return n
}

// Capacity is what the matcher counts a worker as having free now: the
// latest declaration, less the jobs offered since, plus what reports
// since returned.
type Capacity struct {
	RunSlots int     `json:"run_slots"`
	CPU      float64 `json:"cpu"`
	MemGiB   float64 `json:"mem_gib"`
	DiskGiB  float64 `json:"disk_gib"`
}

// Worker is a checked-in worker.
type Worker struct {
	ID          string      `json:"id"`
	Declaration Declaration `json:"declaration"`
	LastCheckIn time.Time   `json:"last_check_in"`
	Available   Capacity    `json:"available"`
	// Gone reports that the worker has not checked in for more than
	// [GoneHeartbeats] heartbeat intervals. A gone worker takes no new
	// job until it checks in again; the jobs it holds keep their leases.
	Gone bool `json:"gone"`
}

// GoneHeartbeats is how many heartbeat intervals may pass without a
// check-in before a worker counts as gone.
const GoneHeartbeats = 3

// Requires is what a job needs of a worker: every capability listed,
// and at least the CPUs, memory and disk free, besides a run slot.
type Requires struct {
	Capabilities []string `json:"capabilities"`
	CPU          float64  `json:"cpu"`
	MemGiB       float64  `json:"mem_gib"`
	DiskGiB      float64  `json:"disk_gib"`
}

// Issue names the issue a job runs.
type Issue struct {
	ID         string `json:"id"`
	Identifier string `json:"identifier"`
	Title      string `json:"title"`
}

// Run is the run descriptor a worker receives with an offer: what to
// start, never a credential. A credential the agent needs appears in
// Env only as a placeholder the egress proxy replaces; a flow's token
// travels apart, in [Offer.Flows].
type Run struct {
	Backend        string            `json:"backend"`
	Image          string            `json:"image"`
	Command        string            `json:"command"`
	Args           []string          `json:"args"`
	Env            map[string]string `json:"env"`
	StallTimeoutMS int               `json:"stall_timeout_ms"`
	// SeedPath is where the workspace seed is fetched, a path on the
	// bundles flow.
	SeedPath    string          `json:"seed_path"`
	Credentials []RunCredential `json:"credentials"`
}

// RunCredential tells the worker which variable stands for a
// credential, the placeholder it holds, and the hosts the egress proxy
// replaces it on. It never holds the value.
type RunCredential struct {
	Name        string   `json:"name"`
	Env         string   `json:"env"`
	Placeholder string   `json:"placeholder"`
	Hosts       []string `json:"hosts"`
	Header      string   `json:"header,omitempty"`
}

// SeedPath returns the path, on the bundles flow, of the workspace seed
// of a job's attempt.
func SeedPath(jobID string, attempt int) string {
	return "/jobs/" + url.PathEscape(jobID) + "/attempts/" + strconv.Itoa(attempt) + "/seed"
}

// Job is one attempt of a run handed to the matcher by the remote
// environment kind. Its ID is the issue identifier.
type Job struct {
	ID      string `json:"id"`
	Attempt int    `json:"attempt"`
	// Priority is the plan priority; lower runs first and nil runs
	// after every set priority.
	Priority *int     `json:"priority"`
	Issue    Issue    `json:"issue"`
	Requires Requires `json:"requires"`
	// Flows maps each data-plane flow to its URL; the offer adds the
	// flow's token.
	Flows    map[string]string `json:"-"`
	Run      Run               `json:"-"`
	QueuedAt time.Time         `json:"-"`
}

// Lease is the right of one worker to run one job until Expires.
type Lease struct {
	JobID    string    `json:"job"`
	WorkerID string    `json:"worker"`
	Expires  time.Time `json:"expires"`
}

// Expiry is a lease as the worker sees it: when it ends unless renewed.
type Expiry struct {
	Expires time.Time `json:"expires"`
}

// FlowAccess is where a run reaches a flow and the token that admits it.
type FlowAccess struct {
	URL   string `json:"url"`
	Token string `json:"token"`
}

// Offer is what a poll hands a worker.
type Offer struct {
	Job   Job                   `json:"job"`
	Lease Expiry                `json:"lease"`
	Run   Run                   `json:"run"`
	Flows map[string]FlowAccess `json:"flows"`
}

// Tokens issues the flow tokens of an attempt; it is the one place they
// come from. The matcher calls Mint once when it grants the attempt's
// lease, for a token per flow of the job, and Void as soon as the
// attempt ends, by report, lease expiry or cancel, after which those
// tokens must be refused. Implementations must be safe for concurrent
// use; the matcher calls them under its lock.
type Tokens interface {
	Mint(job Job) (map[string]string, error)
	Void(jobID string)
}

// NoTokens is the issuer used when job tokens are off: every token is
// empty.
type NoTokens struct{}

// Mint implements [Tokens] with no tokens.
func (NoTokens) Mint(Job) (map[string]string, error) { return nil, nil }

// Void implements [Tokens].
func (NoTokens) Void(string) {}

// Machine is the state of a job's machine when the worker reports.
type Machine string

// Machine states a report names.
const (
	MachineRunning Machine = "running"
	MachineStopped Machine = "stopped"
	MachineDeleted Machine = "deleted"
)

// Outcome is how a job's attempt ended.
type Outcome struct {
	// Succeeded is the worker's report of success.
	Succeeded bool
	// Reason is the worker's free-text reason, or why Sortie ended it.
	Reason string
	// Machine is the state the worker left the job's machine in.
	Machine Machine
	// Expired reports that the lease expired before a report.
	Expired bool
	// Cancelled reports that Sortie withdrew the job.
	Cancelled bool
}

// Leaser is the lease rule: who holds a job and until when. Only one
// worker holds a job at a time. Implementations must be safe for
// concurrent use.
type Leaser interface {
	// Acquire grants jobID to workerID, or returns [ErrLeaseHeld] while
	// an unexpired lease on it exists.
	Acquire(jobID, workerID string, now time.Time) (Lease, error)
	// Renew extends the lease, or returns [ErrLeaseLost].
	Renew(jobID, workerID string, now time.Time) (Lease, error)
	// Release drops the lease.
	Release(jobID, workerID string) error
	// Expired removes and returns the leases that ended before now.
	Expired(now time.Time) []Lease
	// Leases returns the current leases ordered by job.
	Leases() []Lease
	// TTL is how long a lease lasts without renewal.
	TTL() time.Duration
}

// TTLLeaser is the fixed-duration rule: a lease lasts TTL from its
// grant or last renewal, and a missed renewal ends it.
type TTLLeaser struct {
	mu     sync.Mutex
	ttl    time.Duration
	leases map[string]Lease
}

// NewTTLLeaser returns a leaser whose leases last ttl.
func NewTTLLeaser(ttl time.Duration) *TTLLeaser {
	return &TTLLeaser{ttl: ttl, leases: map[string]Lease{}}
}

// SetTTL changes the duration of leases granted or renewed from now on.
func (l *TTLLeaser) SetTTL(ttl time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.ttl = ttl
}

// TTL implements [Leaser].
func (l *TTLLeaser) TTL() time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.ttl
}

// Acquire implements [Leaser].
func (l *TTLLeaser) Acquire(jobID, workerID string, now time.Time) (Lease, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if cur, ok := l.leases[jobID]; ok && now.Before(cur.Expires) {
		return Lease{}, ErrLeaseHeld
	}
	ls := Lease{JobID: jobID, WorkerID: workerID, Expires: now.Add(l.ttl)}
	l.leases[jobID] = ls
	return ls, nil
}

// Renew implements [Leaser].
func (l *TTLLeaser) Renew(jobID, workerID string, now time.Time) (Lease, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	cur, ok := l.leases[jobID]
	if !ok || cur.WorkerID != workerID || !now.Before(cur.Expires) {
		return Lease{}, ErrLeaseLost
	}
	cur.Expires = now.Add(l.ttl)
	l.leases[jobID] = cur
	return cur, nil
}

// Release implements [Leaser].
func (l *TTLLeaser) Release(jobID, workerID string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	cur, ok := l.leases[jobID]
	if !ok || cur.WorkerID != workerID {
		return ErrUnknownLease
	}
	delete(l.leases, jobID)
	return nil
}

// Expired implements [Leaser].
func (l *TTLLeaser) Expired(now time.Time) []Lease {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []Lease
	for id, ls := range l.leases {
		if !now.Before(ls.Expires) {
			out = append(out, ls)
			delete(l.leases, id)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].JobID < out[j].JobID })
	return out
}

// Leases implements [Leaser].
func (l *TTLLeaser) Leases() []Lease {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]Lease, 0, len(l.leases))
	for _, ls := range l.leases {
		out = append(out, ls)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].JobID < out[j].JobID })
	return out
}

// LeaseView is a live lease as the state API shows it.
type LeaseView struct {
	Job     string    `json:"job"`
	Attempt int       `json:"attempt"`
	Worker  string    `json:"worker"`
	Expires time.Time `json:"expires"`
	Step
}

// Queued is a job waiting for a worker and why it waits.
type Queued struct {
	Job      Job       `json:"job"`
	QueuedAt time.Time `json:"queued_at"`
	Waiting  []string  `json:"waiting"`
}

// State is a snapshot of workers, queued jobs and leases. It holds no
// token.
type State struct {
	Workers []Worker    `json:"workers"`
	Queue   []Queued    `json:"queue"`
	Leases  []LeaseView `json:"leases"`
}

// Matcher holds queued jobs and hands each to a worker that polls and
// can run it.
type Matcher struct {
	leaser Leaser
	now    func() time.Time
	tokens Tokens

	mu      sync.Mutex
	workers map[string]*Worker
	pending []Job
	leased  map[string]Job
	ends    map[string]chan Outcome
	// granted holds, per queued job, the channel its worker's ID
	// arrives on when it is leased.
	granted map[string]chan string
	// steps holds the step each leased job last reported.
	steps map[string]Step
}

// NewMatcher returns a matcher using leaser, clock now and token issuer
// tokens; a nil tokens selects [NoTokens].
func NewMatcher(leaser Leaser, now func() time.Time, tokens Tokens) *Matcher {
	if tokens == nil {
		tokens = NoTokens{}
	}
	return &Matcher{leaser: leaser, now: now, tokens: tokens,
		workers: map[string]*Worker{}, leased: map[string]Job{}, ends: map[string]chan Outcome{},
		granted: map[string]chan string{}, steps: map[string]Step{}}
}

// Timings returns how often a worker should check in and renew, and how
// long a lease lasts without renewal. The heartbeat is a third of the
// lease, so two may be lost before it expires.
func (m *Matcher) Timings() (heartbeat, lease time.Duration) {
	lease = m.leaser.TTL()
	return lease / 3, lease
}

// gone reports whether w last checked in more than [GoneHeartbeats]
// heartbeat intervals ago.
func (m *Matcher) gone(w *Worker) bool {
	heartbeat, _ := m.Timings()
	return m.now().Sub(w.LastCheckIn) > GoneHeartbeats*heartbeat
}

// workersLocked returns a copy of each worker ordered by ID, with Gone
// set as of now.
func (m *Matcher) workersLocked() []Worker {
	out := make([]Worker, 0, len(m.workers))
	for _, w := range m.workers {
		c := *w
		c.Gone = m.gone(w)
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Submit queues a job and returns the channel its outcome arrives on,
// once.
func (m *Matcher) Submit(j Job) <-chan Outcome {
	m.mu.Lock()
	defer m.mu.Unlock()
	if j.QueuedAt.IsZero() {
		j.QueuedAt = m.now()
	}
	ch := make(chan Outcome, 1)
	m.ends[j.ID] = ch
	m.granted[j.ID] = make(chan string, 1)
	m.pending = append(m.pending, j)
	sort.SliceStable(m.pending, func(a, b int) bool { return before(m.pending[a], m.pending[b]) })
	return ch
}

// Granted returns the channel the ID of the worker that leases the
// submitted job arrives on, once; nil for a job not queued.
func (m *Matcher) Granted(jobID string) <-chan string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.granted[jobID]
}

// Adopt holds j as leased by workerID from now on, as if a poll had
// granted it, and returns the channel its outcome arrives on, once. A
// process that restarts adopts each job a worker still runs for it: the
// lease lasts its TTL, so the worker's next heartbeat keeps it and a
// worker that is gone lets it expire. It returns [ErrLeaseHeld] while
// another lease on j exists.
func (m *Matcher) Adopt(j Job, workerID string) (<-chan Outcome, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, err := m.leaser.Acquire(j.ID, workerID, m.now()); err != nil {
		return nil, err
	}
	m.leased[j.ID] = j
	delete(m.steps, j.ID)
	ch := make(chan Outcome, 1)
	m.ends[j.ID] = ch
	return ch, nil
}

// before orders jobs by priority, a set priority ahead of none, then
// by age.
func before(a, b Job) bool {
	switch {
	case a.Priority != nil && b.Priority == nil:
		return true
	case a.Priority == nil && b.Priority != nil:
		return false
	case a.Priority != nil && *a.Priority != *b.Priority:
		return *a.Priority < *b.Priority
	}
	return a.QueuedAt.Before(b.QueuedAt)
}

// CheckIn records a worker's declaration, which replaces the previous
// one and what the matcher counted since: a worker declares what it has
// free after every machine it holds, the ones of jobs taken here
// included.
func (m *Matcher) CheckIn(id string, d Declaration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.workers[id] = &Worker{ID: id, Declaration: d, LastCheckIn: m.now(), Available: Capacity{
		RunSlots: d.RunSlots - d.Running, CPU: d.Free.CPU, MemGiB: d.Free.MemGiB, DiskGiB: d.Free.DiskGiB}}
}

// take charges a worker for a job's machine, running.
func (w *Worker) take(r Requires) {
	w.Available.RunSlots--
	w.Available.CPU -= r.CPU
	w.Available.MemGiB -= r.MemGiB
	w.Available.DiskGiB -= r.DiskGiB
}

// release returns what a job's machine in state machine no longer
// holds: a stopped machine keeps its disk, a deleted one nothing, and a
// running one everything.
func (w *Worker) release(r Requires, machine Machine) {
	if machine == MachineRunning {
		return
	}
	w.Available.RunSlots++
	w.Available.CPU += r.CPU
	w.Available.MemGiB += r.MemGiB
	if machine == MachineDeleted {
		w.Available.DiskGiB += r.DiskGiB
	}
}

// misfits returns why w cannot run j now; none means it can. A gone
// worker can run nothing, whatever it last declared.
func misfits(w *Worker, j Job) []string {
	if w.Gone {
		return []string{"no check-in since " + w.LastCheckIn.UTC().Format(time.RFC3339)}
	}
	var out []string
	for _, c := range j.Requires.Capabilities {
		if w.Declaration.has(c) {
			continue
		}
		switch c {
		case "nested":
			out = append(out, "no nested virtualization")
		case "kvm":
			out = append(out, "no KVM")
		default:
			out = append(out, "lacks capability "+c)
		}
	}
	if w.Available.RunSlots < 1 {
		out = append(out, fmt.Sprintf("all %d run slots in use", w.Declaration.RunSlots))
	}
	a, r := w.Available, j.Requires
	if a.CPU < r.CPU {
		out = append(out, fmt.Sprintf("%g of %g CPUs free", hundredths(a.CPU), r.CPU))
	}
	if a.MemGiB < r.MemGiB {
		out = append(out, fmt.Sprintf("%g of %g GiB memory free", hundredths(a.MemGiB), r.MemGiB))
	}
	if a.DiskGiB < r.DiskGiB {
		out = append(out, fmt.Sprintf("%g of %g GiB disk free", hundredths(a.DiskGiB), r.DiskGiB))
	}
	return out
}

// hundredths rounds what is free to hundredths, as a worker declares it:
// charging jobs against it leaves float noise (11.430000000000007).
func hundredths(x float64) float64 { return math.Round(x*100) / 100 }

// capable reports whether w is not gone and declares every capability
// j requires, whatever it has free.
func capable(w *Worker, j Job) bool {
	if w.Gone {
		return false
	}
	for _, c := range j.Requires.Capabilities {
		if !w.Declaration.has(c) {
			return false
		}
	}
	return true
}

// place decides which of workers takes each of jobs now, charging each
// worker for what it takes, and returns the index of its worker per job,
// -1 for a job that waits. Jobs fewer workers are capable of are placed
// first, then in the order given, so a job only a scarce worker can run
// never waits behind jobs another worker can run. Of the workers a job
// fits, it goes to the one with the fewest capabilities it does not
// need, then to poller, then to the first in order.
func place(workers []Worker, jobs []Job, poller string) []int {
	capableOf := make([]int, len(jobs))
	order := make([]int, len(jobs))
	for k := range jobs {
		order[k] = k
		for i := range workers {
			if capable(&workers[i], jobs[k]) {
				capableOf[k]++
			}
		}
	}
	sort.SliceStable(order, func(a, b int) bool { return capableOf[order[a]] < capableOf[order[b]] })
	where := make([]int, len(jobs))
	for _, k := range order {
		best, bestSpare := -1, 0
		for i := range workers {
			if len(misfits(&workers[i], jobs[k])) > 0 {
				continue
			}
			s := workers[i].Declaration.spare(jobs[k].Requires)
			if best < 0 || s < bestSpare || s == bestSpare && workers[i].ID == poller {
				best, bestSpare = i, s
			}
		}
		where[k] = best
		if best >= 0 {
			workers[best].take(jobs[k].Requires)
		}
	}
	return where
}

// Poll offers the worker the first queued job that [place] gives it,
// charging a run slot and the job's resources until its next check-in:
// a job another worker without a capability the job does not need can
// take now is left to that worker. It returns false when no job is the
// worker's or the worker is gone, and [ErrUnknownWorker] before the
// worker's first check-in.
func (m *Matcher) Poll(workerID string) (Offer, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	w, ok := m.workers[workerID]
	if !ok {
		return Offer{}, false, ErrUnknownWorker
	}
	if m.gone(w) {
		return Offer{}, false, nil
	}
	workers := m.workersLocked()
	where := place(workers, m.pending, workerID)
	for i, j := range m.pending {
		if where[i] < 0 || workers[where[i]].ID != workerID {
			continue
		}
		ls, err := m.leaser.Acquire(j.ID, workerID, m.now())
		if err != nil {
			continue
		}
		toks, err := m.tokens.Mint(j)
		if err != nil {
			_ = m.leaser.Release(j.ID, workerID)
			m.tokens.Void(j.ID)
			return Offer{}, false, fmt.Errorf("worker: minting job tokens: %w", err)
		}
		m.pending = append(m.pending[:i:i], m.pending[i+1:]...)
		m.leased[j.ID] = j
		delete(m.steps, j.ID)
		if ch, ok := m.granted[j.ID]; ok {
			delete(m.granted, j.ID)
			ch <- workerID
		}
		w.take(j.Requires)
		flows := make(map[string]FlowAccess, len(j.Flows))
		for name, u := range j.Flows {
			flows[name] = FlowAccess{URL: u, Token: toks[name]}
		}
		return Offer{Job: j, Lease: Expiry{Expires: ls.Expires.UTC()}, Run: j.Run, Flows: flows}, true, nil
	}
	return Offer{}, false, nil
}

// Placeable reports whether a checked-in worker that is not gone could
// take a job that requires r now, placed with the queued jobs as a poll
// places them ([place]), and else why it waits, per worker. A job that
// waits here is better not submitted: no worker would take it.
//
// ahead is what jobs dispatched but not yet queued require: each is
// placed after the queued jobs, so a dispatch that sees one free slot
// sends one job to it, not one per issue it reads.
func (m *Matcher) Placeable(r Requires, ahead ...Requires) ([]string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	workers := m.workersLocked()
	jobs := slices.Clone(m.pending)
	for _, a := range ahead {
		jobs = append(jobs, Job{Requires: a})
	}
	probe := Job{Requires: r}
	jobs = append(jobs, probe)
	if where := place(workers, jobs, ""); where[len(jobs)-1] >= 0 {
		return nil, true
	}
	return waiting(probe, workers), false
}

// Queued reports whether jobID waits in the queue: submitted, and no
// worker leased it yet.
func (m *Matcher) Queued(jobID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.ContainsFunc(m.pending, func(j Job) bool { return j.ID == jobID })
}

// Holds reports whether jobID is queued or leased.
func (m *Matcher) Holds(jobID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, leased := m.leased[jobID]
	return leased || slices.ContainsFunc(m.pending, func(j Job) bool { return j.ID == jobID })
}

// held returns the leased job when workerID holds a live lease on
// jobID's attempt.
func (m *Matcher) held(workerID, jobID string, attempt int) (Job, error) {
	j, ok := m.leased[jobID]
	if !ok || j.Attempt != attempt {
		return Job{}, ErrLeaseLost
	}
	for _, ls := range m.leaser.Leases() {
		if ls.JobID == jobID && ls.WorkerID == workerID && m.now().Before(ls.Expires) {
			return j, nil
		}
	}
	return Job{}, ErrLeaseLost
}

// Heartbeat renews the caller's lease on the job's attempt, or returns
// [ErrLeaseLost] when the caller holds no live lease on it.
func (m *Matcher) Heartbeat(workerID, jobID string, attempt int) (Expiry, error) {
	return m.HeartbeatStep(workerID, jobID, attempt, Step{})
}

// HeartbeatStep renews the lease as [Matcher.Heartbeat] does and keeps
// the step the job reports; an empty step keeps the last one.
func (m *Matcher) HeartbeatStep(workerID, jobID string, attempt int, step Step) (Expiry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, err := m.held(workerID, jobID, attempt); err != nil {
		return Expiry{}, err
	}
	ls, err := m.leaser.Renew(jobID, workerID, m.now())
	if err != nil {
		return Expiry{}, err
	}
	if !step.empty() {
		m.steps[jobID] = step
	}
	return Expiry{Expires: ls.Expires.UTC()}, nil
}

// Report ends the job's attempt with the worker's outcome and releases
// its lease, or returns [ErrLeaseLost], changing nothing, when the
// caller holds no live lease on that attempt. The machine's state
// decides what returns to the worker's count until its next check-in.
func (m *Matcher) Report(workerID, jobID string, attempt int, out Outcome) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, err := m.held(workerID, jobID, attempt)
	if err != nil {
		return err
	}
	if err := m.leaser.Release(jobID, workerID); err != nil {
		return ErrLeaseLost
	}
	delete(m.leased, jobID)
	delete(m.steps, jobID)
	m.tokens.Void(jobID)
	if w := m.workers[workerID]; w != nil {
		w.release(j.Requires, out.Machine)
	}
	m.endLocked(jobID, out)
	return nil
}

func (m *Matcher) endLocked(jobID string, out Outcome) {
	delete(m.granted, jobID)
	if ch, ok := m.ends[jobID]; ok {
		delete(m.ends, jobID)
		ch <- out
	}
}

// Cancel withdraws a job, queued or leased, and ends it as cancelled.
// A worker's later heartbeat or report for it is refused, and the
// worker stops its machine.
func (m *Matcher) Cancel(jobID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pending = slices.DeleteFunc(m.pending, func(j Job) bool { return j.ID == jobID })
	if j, ok := m.leased[jobID]; ok {
		for _, ls := range m.leaser.Leases() {
			if ls.JobID != jobID {
				continue
			}
			_ = m.leaser.Release(jobID, ls.WorkerID)
			if w := m.workers[ls.WorkerID]; w != nil {
				w.release(j.Requires, MachineStopped)
			}
		}
		delete(m.leased, jobID)
		delete(m.steps, jobID)
		m.tokens.Void(jobID)
	}
	m.endLocked(jobID, Outcome{Cancelled: true, Reason: "job withdrawn"})
}

// ExpireLeases ends, as failed, the attempt of every job whose lease
// expired, and returns those jobs. The worker stops the machine when
// its next heartbeat is refused, so the machine counts as stopped.
func (m *Matcher) ExpireLeases() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var ids []string
	for _, ls := range m.leaser.Expired(m.now()) {
		j, ok := m.leased[ls.JobID]
		if !ok {
			continue
		}
		delete(m.leased, ls.JobID)
		delete(m.steps, ls.JobID)
		m.tokens.Void(ls.JobID)
		if w := m.workers[ls.WorkerID]; w != nil {
			w.release(j.Requires, MachineStopped)
		}
		m.endLocked(ls.JobID, Outcome{Expired: true, Machine: MachineStopped,
			Reason: fmt.Sprintf("lease held by worker %s expired", ls.WorkerID)})
		ids = append(ids, ls.JobID)
	}
	return ids
}

// Snapshot returns the state of workers, queued jobs and leases.
func (m *Matcher) Snapshot() State {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := State{Workers: m.workersLocked(), Queue: []Queued{}, Leases: []LeaseView{}}
	for _, j := range m.pending {
		s.Queue = append(s.Queue, Queued{Job: j, QueuedAt: j.QueuedAt, Waiting: waiting(j, s.Workers)})
	}
	for _, ls := range m.leaser.Leases() {
		if j, ok := m.leased[ls.JobID]; ok {
			s.Leases = append(s.Leases, LeaseView{Job: ls.JobID, Attempt: j.Attempt, Worker: ls.WorkerID, Expires: ls.Expires.UTC(),
				Step: m.steps[ls.JobID]})
		}
	}
	return s
}

// waiting says why j waits: per worker, what it lacks.
func waiting(j Job, workers []Worker) []string {
	if len(workers) == 0 {
		return []string{"no worker has checked in"}
	}
	var out []string
	for i := range workers {
		if why := misfits(&workers[i], j); len(why) > 0 {
			out = append(out, fmt.Sprintf("worker %s: %s", workers[i].ID, strings.Join(why, ", ")))
		}
	}
	if len(out) == 0 {
		out = []string{"waiting for a worker to poll"}
	}
	return out
}
