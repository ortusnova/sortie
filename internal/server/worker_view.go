package server

import (
	"context"
	"log/slog"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/hostview"
	"github.com/sortie-ai/sortie/internal/orchestrator"
	"github.com/sortie-ai/sortie/internal/worker"
)

// HostFunc returns the host Sortie runs its agents on, or nil when its
// runs go to workers elsewhere.
type HostFunc func(ctx context.Context) *hostview.Host

const (
	// estimateRuns is how many of a rule's newest succeeded runs its
	// estimate is the median of.
	estimateRuns = 20
	// estimateTTL is how long a rule's estimate is reused.
	estimateTTL = time.Minute
)

// What a wait on a worker is for, as waits[].kind serves it.
const (
	waitSlot       = "slot"
	waitCapability = "capability"
	waitDrain      = "drain"
)

// What an open pull request waits on, as pulls[].waits_on serves it.
// Sortie knows no live proof, so it never serves live_proof.
const (
	pullWaitsReview   = "review"
	pullWaitsCI       = "ci"
	pullWaitsConflict = "conflict"
	pullWaitsPerson   = "person"
	pullWaitsNone     = "none"
)

// workerView holds the members of GET /api/v1/state that say what each
// worker does and what waits on it.
type workerView struct {
	// Host is the host Sortie runs its agents on; omitted under
	// environment.kind remote.
	Host  *hostview.Host     `json:"host,omitempty"`
	Waits []waitResponse     `json:"waits"`
	Pulls []pullWaitResponse `json:"pulls"`
}

// runningWorkerView holds the members of a running row that place it on
// a worker and say where it is. Each is omitted when Sortie does not
// know it.
type runningWorkerView struct {
	// Worker is the host the run runs on: the worker that holds its
	// lease, its SSH host, or Sortie's own host.
	Worker string `json:"worker,omitempty"`
	// Phase, Step and StepAt are what the job last reported through its
	// worker's heartbeat.
	Phase  string     `json:"phase,omitempty"`
	Step   string     `json:"step,omitempty"`
	StepAt *time.Time `json:"step_at,omitempty"`
	// EstimatedEnd is the start plus the median length of the rule's
	// newest succeeded runs.
	EstimatedEnd *time.Time `json:"estimated_end,omitempty"`
}

// waitResponse is one thing that waits on a worker.
type waitResponse struct {
	// IssueIdentifier is empty for a wait of no issue, such as a drain.
	IssueIdentifier string `json:"issue_identifier,omitempty"`
	// Worker is empty when no one worker releases the wait.
	Worker  string    `json:"worker,omitempty"`
	Kind    string    `json:"kind"`
	Reason  string    `json:"reason"`
	Release string    `json:"release"`
	Since   time.Time `json:"since"`
}

// pullWaitResponse is one open pull request and what it waits on.
type pullWaitResponse struct {
	IssueIdentifier string `json:"issue_identifier"`
	Number          int    `json:"number"`
	Repository      string `json:"repository"`
	URL             string `json:"url,omitempty"`
	WaitsOn         string `json:"waits_on"`
	Reason          string `json:"reason"`
	// Since is when the escalation began for person, and otherwise when
	// this process first saw the pull request wait on WaitsOn.
	Since time.Time `json:"since"`
	// Worker is Sortie's own host; omitted under environment.kind remote.
	Worker string `json:"worker,omitempty"`
}

// workerViewState is what the worker view keeps between requests.
type workerViewState struct {
	mu        sync.Mutex
	estimates map[string]ruleEstimate
	pullSince map[string]time.Time
}

type ruleEstimate struct {
	at     time.Time
	median time.Duration
	ok     bool
}

// attachWorkerView fills the members of resp that say what each worker
// does and what waits on it.
func (s *Server) attachWorkerView(ctx context.Context, resp *stateResponse, snap orchestrator.RuntimeSnapshotResult) {
	if s.hostFn != nil && resp.Workers == nil {
		resp.Host = s.hostFn(ctx)
	}
	hostName := ""
	if resp.Host != nil {
		hostName = resp.Host.Name
	}
	leases := map[string]worker.LeaseView{}
	if resp.Workers != nil {
		for _, l := range resp.Workers.Leases {
			leases[l.Job] = l
		}
	}
	for i := range resp.Running {
		row := &resp.Running[i]
		switch l, ok := leases[row.IssueIdentifier]; {
		case ok:
			row.Worker, row.Phase, row.Step, row.StepAt = l.Worker, l.Phase, l.Step.Step, l.StepAt
		case row.SSHHost != "":
			row.Worker = row.SSHHost
		default:
			row.Worker = hostName
		}
		if median, ok := s.ruleEstimate(ctx, row.RuleName, snap.GeneratedAt); ok && !row.StartedAt.IsZero() {
			row.EstimatedEnd = new(row.StartedAt.Add(median).UTC())
		}
	}
	resp.Waits = s.waitsOf(resp, hostName)
	resp.Pulls = s.pullsOf(snap.PendingReactions, hostName, snap.GeneratedAt)
}

// ruleEstimate returns the median length of the rule's newest succeeded
// runs, read from run history at most once per estimateTTL.
func (s *Server) ruleEstimate(ctx context.Context, rule string, now time.Time) (time.Duration, bool) {
	if s.runHistory == nil {
		return 0, false
	}
	v := &s.workerView
	v.mu.Lock()
	if e, ok := v.estimates[rule]; ok && now.Sub(e.at) < estimateTTL {
		v.mu.Unlock()
		return e.median, e.ok
	}
	v.mu.Unlock()

	runs, err := s.runHistory.QueryRuns(ctx, RunHistoryQuery{Status: "succeeded", RuleName: new(rule), Limit: estimateRuns})
	if err != nil {
		s.logger.Warn("failed to read run history for a run's estimated end", slog.String("rule", rule), slog.Any("error", err))
		return 0, false
	}
	var lengths []time.Duration
	for _, r := range runs {
		start, err1 := time.Parse(time.RFC3339, r.StartedAt)
		end, err2 := time.Parse(time.RFC3339, r.CompletedAt)
		if err1 == nil && err2 == nil && end.After(start) {
			lengths = append(lengths, end.Sub(start))
		}
	}
	e := ruleEstimate{at: now}
	if len(lengths) > 0 {
		slices.Sort(lengths)
		n := len(lengths)
		e.median, e.ok = lengths[n/2], true
		if n%2 == 0 {
			e.median = (lengths[n/2-1] + lengths[n/2]) / 2
		}
	}
	v.mu.Lock()
	if v.estimates == nil {
		v.estimates = map[string]ruleEstimate{}
	}
	v.estimates[rule] = e
	v.mu.Unlock()
	return e.median, e.ok
}

// waitsOf lists what waits on each worker: issues held for an agent slot,
// jobs no worker took, and a paused dispatch while runs drain.
func (s *Server) waitsOf(resp *stateResponse, hostName string) []waitResponse {
	waits := []waitResponse{}
	for _, h := range resp.Held {
		w := waitResponse{IssueIdentifier: h.IssueIdentifier, Kind: waitSlot, Reason: h.Hold.ReasonText,
			Release: h.Hold.ReleaseText, Since: h.Hold.HeldSince.UTC()}
		switch orchestrator.SkipReason(h.Hold.Reason) {
		case orchestrator.SkipAgentSlotsFull:
			w.Worker = hostName
		case orchestrator.SkipSSHHostsFull:
		default:
			continue
		}
		waits = append(waits, w)
	}
	if resp.Workers != nil {
		for _, q := range resp.Workers.Queue {
			waits = append(waits, queueWait(q, resp.Workers.Workers))
		}
	}
	if p := resp.DispatchPaused; p != nil {
		reason := "dispatch is paused by " + p.By
		if p.Reason != "" {
			reason += ": " + p.Reason
		}
		for name, n := range runsByWorker(resp.Running) {
			waits = append(waits, waitResponse{Worker: name, Kind: waitDrain, Reason: reason,
				Release: plural(n, "run") + " here end", Since: p.Since.UTC()})
		}
	}
	sort.SliceStable(waits, func(a, b int) bool {
		if waits[a].Worker != waits[b].Worker {
			return waits[a].Worker < waits[b].Worker
		}
		return waits[a].IssueIdentifier < waits[b].IssueIdentifier
	})
	return waits
}

// queueWait says why a queued job waits: for a capability no worker
// declares, else for a slot or resources a capable worker frees.
func queueWait(q worker.Queued, workers []worker.Worker) waitResponse {
	var capable []string
	for _, w := range workers {
		if hasAll(w.Declaration, q.Job.Requires.Capabilities) {
			capable = append(capable, w.ID)
		}
	}
	w := waitResponse{IssueIdentifier: q.Job.Issue.Identifier, Reason: strings.Join(q.Waiting, "; "),
		Since: q.QueuedAt.UTC()}
	if w.IssueIdentifier == "" {
		w.IssueIdentifier = q.Job.ID
	}
	if len(capable) == 0 {
		w.Kind = waitCapability
		w.Release = "a worker checks in that has " + strings.Join(q.Job.Requires.Capabilities, ", ")
		if len(q.Job.Requires.Capabilities) == 0 {
			w.Release = "a worker checks in"
		}
		return w
	}
	w.Kind, w.Release = waitSlot, "a worker that can run it frees a slot"
	if len(capable) == 1 {
		w.Worker = capable[0]
		w.Release = "worker " + capable[0] + " frees a slot"
	}
	return w
}

func hasAll(d worker.Declaration, caps []string) bool {
	for _, c := range caps {
		switch {
		case c == "kvm" && d.KVM, c == "nested" && d.Nested, c == d.OS, c == d.Arch, slices.Contains(d.Backends, c):
		default:
			return false
		}
	}
	return true
}

// runsByWorker counts the running rows on each worker.
func runsByWorker(rows []runningEntryResponse) map[string]int {
	n := map[string]int{}
	for _, r := range rows {
		n[r.Worker]++
	}
	return n
}

// counted says "n nouns what", or else when n is not positive.
func counted(n int, noun, what, otherwise string) string {
	if n < 1 {
		return otherwise
	}
	return plural(n, noun) + " " + what
}

func plural(n int, noun string) string {
	if n == 1 {
		return "1 " + noun
	}
	return strconv.Itoa(n) + " " + noun + "s"
}

// pullsOf lists each open pull request that a pending reaction watches,
// once, with what it waits on.
func (s *Server) pullsOf(reactions []orchestrator.SnapshotPendingReaction, hostName string, now time.Time) []pullWaitResponse {
	type group struct {
		pr        orchestrator.SnapshotPullRequest
		ident     string
		reactions []orchestrator.SnapshotPendingReaction
	}
	groups := map[string]*group{}
	var keys []string
	for _, r := range reactions {
		if r.Kind == orchestrator.ReactionKindMergeCompletion || r.PullRequest.Number == 0 {
			continue
		}
		key := r.PullRequest.Repository + "#" + strconv.Itoa(r.PullRequest.Number)
		g, ok := groups[key]
		if !ok {
			g = &group{pr: r.PullRequest, ident: r.Identifier}
			groups[key] = g
			keys = append(keys, key)
		}
		if g.pr.URL == "" {
			g.pr.URL = r.PullRequest.URL
		}
		g.reactions = append(g.reactions, r)
	}
	sort.Strings(keys)

	v := &s.workerView
	v.mu.Lock()
	defer v.mu.Unlock()
	seen := make(map[string]time.Time, len(keys))
	pulls := make([]pullWaitResponse, 0, len(keys))
	for _, key := range keys {
		g := groups[key]
		waitsOn, reason, escalated := pullWait(g.reactions)
		sinceKey := key + " " + waitsOn
		since, ok := v.pullSince[sinceKey]
		if !ok {
			since = now
		}
		seen[sinceKey] = since
		if escalated != nil {
			since = *escalated
		}
		pulls = append(pulls, pullWaitResponse{IssueIdentifier: g.ident, Number: g.pr.Number, Repository: g.pr.Repository,
			URL: g.pr.URL, WaitsOn: waitsOn, Reason: reason, Since: since.UTC(), Worker: hostName})
	}
	v.pullSince = seen
	return pulls
}

// pullWait decides what a pull request waits on from the reactions that
// watch it: a person for a standing escalation, then a conflict, CI, and
// its review, and nothing once each check passed. escalated is when the
// escalation began.
func pullWait(reactions []orchestrator.SnapshotPendingReaction) (waitsOn, reason string, escalated *time.Time) {
	var conflict, ci, review string
	for _, r := range reactions {
		if r.EscalatedAt != nil {
			return pullWaitsPerson, "escalated to a person: " + strings.ReplaceAll(r.EscalationTrigger, "_", " "), r.EscalatedAt
		}
		check := r.LastCheck
		switch r.Kind {
		case orchestrator.ReactionKindMergeConflict:
			if check != nil && check.Mergeability != nil && check.Mergeability.Mergeable != nil && !*check.Mergeability.Mergeable {
				conflict = "it conflicts with its base branch"
			}
		case orchestrator.ReactionKindCI:
			switch {
			case check == nil || check.CI == nil:
				ci = "CI has not reported on it yet"
			case check.CI.Status == string(domain.CIStatusFailing):
				ci = counted(check.CI.Failing, "check", "failing", "CI fails")
			case check.CI.Status != string(domain.CIStatusPassing):
				ci = counted(check.CI.Pending, "check", "pending", "CI is pending")
			}
		case orchestrator.ReactionKindReview, orchestrator.ReactionKindBotReview:
			switch {
			case check == nil || check.Comments == nil:
				review = "its review has not been read yet"
			case check.Comments.Actionable > 0:
				review = plural(check.Comments.Actionable, "review comment") + " to address"
			case check.Comments.AwaitingApproval:
				review = "it waits for an approving review"
			}
		}
	}
	switch {
	case conflict != "":
		return pullWaitsConflict, conflict, nil
	case ci != "":
		return pullWaitsCI, ci, nil
	case review != "":
		return pullWaitsReview, review, nil
	}
	return pullWaitsNone, readyReason(reactions), nil
}

// readyReason says why a pull request waits on nothing: the checks the
// reactions made passed.
func readyReason(reactions []orchestrator.SnapshotPendingReaction) string {
	var names []string
	for _, r := range reactions {
		var name string
		switch r.Kind {
		case orchestrator.ReactionKindMergeConflict:
			name = "the conflict check"
		case orchestrator.ReactionKindCI:
			name = "CI"
		case orchestrator.ReactionKindReview, orchestrator.ReactionKindBotReview:
			name = "its review"
		}
		if name != "" && !slices.Contains(names, name) {
			names = append(names, name)
		}
	}
	if len(names) == 0 {
		return "ready to merge: no check Sortie makes is pending"
	}
	return "ready to merge: " + strings.Join(names, " and ") + " passed"
}
