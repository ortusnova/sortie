//go:build unix

package orchestrator

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/dataplane"
	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/persistence"
	"github.com/sortie-ai/sortie/internal/waittest"
	"github.com/sortie-ai/sortie/internal/worker"
)

// restartTracker is the tracker of a restart test: ONS-30 is active
// until a run hands it off, and every handoff is counted.
type restartTracker struct {
	*mockTrackerAdapter
	issue domain.Issue

	mu          sync.Mutex
	state       string
	transitions []string
	// holdRefresh, when set, blocks the per-turn state refresh until
	// the run's context ends; refreshed is closed at the first one.
	holdRefresh bool
	refreshed   chan struct{}
	refreshOnce sync.Once
}

func newRestartTracker() *restartTracker {
	issue := remoteRunIssue()
	return &restartTracker{mockTrackerAdapter: &mockTrackerAdapter{}, issue: issue, state: issue.State, refreshed: make(chan struct{})}
}

func (r *restartTracker) current() domain.Issue {
	r.mu.Lock()
	defer r.mu.Unlock()
	issue := r.issue
	issue.State = r.state
	return issue
}

func (r *restartTracker) FetchCandidateIssues(context.Context) ([]domain.Issue, error) {
	issue := r.current()
	if issue.State != "To Do" {
		return nil, nil
	}
	return []domain.Issue{issue}, nil
}

func (r *restartTracker) FetchIssueByID(context.Context, string) (domain.Issue, error) {
	return r.current(), nil
}

func (r *restartTracker) FetchIssueStatesByIDs(ctx context.Context, ids []string) (map[string]string, error) {
	r.mu.Lock()
	hold := r.holdRefresh
	r.mu.Unlock()
	if hold {
		r.refreshOnce.Do(func() { close(r.refreshed) })
		<-ctx.Done()
		return nil, context.Cause(ctx)
	}
	out := map[string]string{}
	for _, id := range ids {
		out[id] = r.current().State
	}
	return out, nil
}

func (r *restartTracker) FetchIssueStatesAndLabelsByIDs(_ context.Context, ids []string) (map[string]domain.IssueStateAndLabels, error) {
	out := map[string]domain.IssueStateAndLabels{}
	for _, id := range ids {
		out[id] = domain.IssueStateAndLabels{State: r.current().State}
	}
	return out, nil
}

func (r *restartTracker) TransitionIssue(_ context.Context, _ string, target string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.transitions = append(r.transitions, target)
	r.state = target
	return nil
}

func (r *restartTracker) handoffs() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []string
	for _, s := range r.transitions {
		if s == "Human Review" {
			out = append(out, s)
		}
	}
	return out
}

// restartHarness is one deployment across Sortie restarts: a database
// file, a server address the worker keeps dialing while the process
// behind it changes, the tracker, and the forge.
type restartHarness struct {
	t        *testing.T
	git      *gitFixture
	upstream string
	dbPath   string
	srv      *httptest.Server
	handler  atomic.Pointer[http.Handler]
	tracker  *restartTracker
	opener   *fakeOpener
	cfg      config.ServiceConfig
	// prepare adjusts each process before it runs.
	prepare func(*Orchestrator)
}

const restartHeader = "X-Sortie-Worker-Identity"

func newRestartHarness(t *testing.T, maxTurns int, leaseTTL time.Duration) *restartHarness {
	t.Helper()
	f := newGitFixture(t)
	h := &restartHarness{t: t, git: f, upstream: f.upstream(), dbPath: filepath.Join(t.TempDir(), "sortie.db"),
		tracker: newRestartTracker(), opener: &fakeOpener{pr: domain.PullRequest{Number: 12, Created: true}}}
	h.handler.Store(new(http.Handler(down{})))
	h.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		(*h.handler.Load()).ServeHTTP(w, r)
	}))
	t.Cleanup(h.srv.Close)

	cfg := baseRunConfig(t)
	cfg.Tracker.Kind = "mock"
	cfg.Tracker.HandoffState = "Human Review"
	cfg.Polling = config.PollingConfig{IntervalMS: int(time.Hour / time.Millisecond)}
	cfg.Agent.MaxConcurrentAgents = 1
	cfg.Agent.MaxTurns = maxTurns
	cfg.Agent.MaxSessions = 4
	cfg.Agent.MaxRetryBackoffMS = 1000
	flows := map[string]string{}
	for _, fl := range config.RemoteFlows {
		flows[fl] = h.srv.URL + dataplane.Prefix + fl
	}
	cfg.Environment = config.EnvironmentConfig{Kind: config.EnvironmentKindRemote, HostPrefix: []string{envPath(t)},
		Remote: config.RemoteConfig{IdentityHeader: restartHeader, Backend: "smolvm", Image: "ghcr.io/ortusnova/agent:1",
			Flows: flows, LeaseTTLMS: int(leaseTTL / time.Millisecond), Repository: runRepository(h.upstream)}}
	h.cfg = cfg
	return h
}

// down answers every request while no Sortie runs.
type down struct{}

func (down) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	http.Error(w, "no Sortie is running", http.StatusBadGateway)
}

// sortieProcess is one start of Sortie over the harness's database.
type sortieProcess struct {
	o       *Orchestrator
	store   *persistence.Store
	adapter *resumeAdapter
	cancel  context.CancelFunc
	done    chan struct{}
}

// resumeAdapter is a [lineAdapter] that records the session each launch
// resumes.
type resumeAdapter struct {
	lineAdapter
	mu      sync.Mutex
	resumes []string
}

func (a *resumeAdapter) StartSession(ctx context.Context, params domain.StartSessionParams) (domain.Session, error) {
	a.mu.Lock()
	a.resumes = append(a.resumes, params.ResumeSessionID)
	a.mu.Unlock()
	return a.lineAdapter.StartSession(ctx, params)
}

func (a *resumeAdapter) resumed() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return slices.Clone(a.resumes)
}

// start starts Sortie as the binary does: the store, the restored remote
// runs, then the server, then the event loop. Each of beforeLoop runs
// while the server answers and before the first tick, so a worker it
// checks in is there when that tick reads the issue.
func (h *restartHarness) start(beforeLoop ...func()) *sortieProcess {
	h.t.Helper()
	ctx := context.Background()
	store, err := persistence.Open(ctx, h.dbPath)
	if err != nil {
		h.t.Fatal(err)
	}
	if err := store.Migrate(ctx); err != nil {
		h.t.Fatal(err)
	}
	self, err := os.Executable()
	if err != nil {
		h.t.Fatal(err)
	}
	p := &sortieProcess{store: store, adapter: &resumeAdapter{}, done: make(chan struct{})}
	pf := passingPreflightRegistries()
	pf.ReloadWorkflow = func() error { return nil }
	cfg := h.cfg
	pf.ConfigFunc = func() config.ServiceConfig { return cfg }
	state := NewState(cfg.Polling.IntervalMS, cfg.Agent.MaxConcurrentAgents, 0, nil, AgentTotals{})
	p.o = NewOrchestrator(OrchestratorParams{
		State:           state,
		Logger:          testRunLogger(),
		TrackerAdapter:  h.tracker,
		AgentAdapter:    p.adapter,
		WorkflowManager: &stubWorkflowManager{config: cfg, template: mustParseTemplate(h.t, "Work on {{ .issue.identifier }}: {{ .issue.title }}")},
		Store:           store,
		PreflightParams: pf,
		DBPath:          h.dbPath,
		MCPServerBinary: self,
		PullRequestOpenerFunc: func(config.ServiceConfig) (domain.PullRequestOpener, error) {
			return h.opener, nil
		},
	})
	if h.prepare != nil {
		h.prepare(p.o)
	}
	p.o.RestoreRemoteRuns(ctx)
	mux := http.NewServeMux()
	mux.Handle(worker.Prefix, worker.Handler(p.o.Workers(), restartHeader))
	mux.Handle(dataplane.Prefix, p.o.DataPlane())
	h.handler.Store(new(http.Handler(mux)))
	for _, f := range beforeLoop {
		f()
	}

	runCtx, cancel := context.WithCancel(ctx)
	p.cancel = cancel
	go func() {
		defer close(p.done)
		p.o.Run(runCtx)
	}()
	h.t.Cleanup(p.stop)
	return p
}

// stop shuts the process down as a deploy does.
func (p *sortieProcess) stop() {
	select {
	case <-p.done:
		return
	default:
	}
	p.cancel()
	<-p.done
	_ = p.store.Close()
}

// stopAndGo stops the process and takes the server away with it.
func (h *restartHarness) stopAndGo(p *sortieProcess) {
	h.t.Helper()
	p.stop()
	h.handler.Store(new(http.Handler(down{})))
}

// recorded waits until the store holds ONS-30's run with its entry.
func (h *restartHarness) recorded(p *sortieProcess, check func(persistence.RemoteRun) error) {
	h.t.Helper()
	waittest.Until(h.t, func() error {
		runs, err := p.store.LoadRemoteRuns(context.Background())
		if err != nil {
			return err
		}
		if len(runs) != 1 || runs[0].Entry == "" {
			return fmt.Errorf("remote runs %+v, want ONS-30's with its entry", runs)
		}
		if check != nil {
			return check(runs[0])
		}
		return nil
	})
}

// handedOff waits until the run handed ONS-30 off and nothing runs.
func (h *restartHarness) handedOff(p *sortieProcess) {
	h.t.Helper()
	waittest.Until(h.t, func() error {
		snap, err := p.o.SnapshotFunc()()
		if err != nil {
			return err
		}
		if len(h.tracker.handoffs()) == 0 || len(snap.Running) != 0 {
			return fmt.Errorf("handoffs %v, running %d", h.tracker.handoffs(), len(snap.Running))
		}
		return nil
	})
}

func (h *restartHarness) history(p *sortieProcess) []persistence.RunHistory {
	h.t.Helper()
	rows, err := p.store.QueryRunHistory(context.Background(), persistence.RunHistoryFilter{IssueID: remoteIssueID, Limit: 10})
	if err != nil {
		h.t.Fatal(err)
	}
	return rows
}

// restartWorker is a worker that behaves as worker-daemon does across a
// Sortie restart: when the stdio stream breaks before the agent exited,
// it keeps the job's machine (its clone), dials the stream again until
// the attempt is served, and starts the agent again in the same
// workspace.
type restartWorker struct {
	*fakeWorker

	mu      sync.Mutex
	offers  int
	starts  int
	prompts [][]string // per agent start

	// hold holds the agent on a prompt until the stream breaks; it is
	// asked for every prompt of every agent start.
	hold func(start int, prompt string) bool
	// holding is closed when the agent first holds a prompt.
	holding     chan struct{}
	holdingOnce sync.Once
	// dieOnBreak makes the worker go away when the stream breaks.
	dieOnBreak bool

	beating chan struct{}
}

func newRestartWorker(h *restartHarness) *restartWorker {
	return &restartWorker{
		fakeWorker: &fakeWorker{t: h.t, git: h.git, base: h.srv.URL, id: "taichi", header: restartHeader},
		holding:    make(chan struct{}), beating: make(chan struct{}),
	}
}

// heartbeat renews the lease, as the daemon does, until the job ends;
// a failed renewal is retried at the next beat.
func (w *restartWorker) heartbeat() {
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-w.beating:
			return
		case <-t.C:
		}
		path := fmt.Sprintf("/api/v1/workers/jobs/%s/attempts/%d/heartbeat", w.offer.Job.ID, w.offer.Job.Attempt)
		req, _ := http.NewRequest(http.MethodPost, w.base+path, nil)
		req.Header.Set(worker.ProtocolHeader, "1")
		req.Header.Set(w.header, w.id)
		if resp, err := http.DefaultClient.Do(req); err == nil {
			_ = resp.Body.Close()
		}
	}
}

// run takes the job and works it to its report.
func (w *restartWorker) run() {
	w.take()
	w.mu.Lock()
	w.offers++
	w.mu.Unlock()
	go w.heartbeat()
	defer close(w.beating)
	w.seed()
	for w.session() {
		if w.dieOnBreak {
			return
		}
	}
	w.giveBack()
}

// dial opens the stdio stream, retrying until the attempt is served.
func (w *restartWorker) dial() (net.Conn, *bufio.Reader) {
	w.t.Helper()
	target, token := w.flow("stdio")
	u, _ := url.Parse(target)
	var conn net.Conn
	var in *bufio.Reader
	waittest.Until(w.t, func() error {
		c, err := net.Dial("tcp", u.Host)
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintf(c, "GET %s HTTP/1.1\r\nHost: %s\r\nAuthorization: Bearer %s\r\nConnection: Upgrade\r\nUpgrade: %s\r\n\r\n", u.Path, u.Host, token, dataplane.StdioProtocol)
		r := bufio.NewReader(c)
		resp, err := http.ReadResponse(r, nil)
		if err != nil {
			_ = c.Close()
			return err
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusSwitchingProtocols {
			_ = c.Close()
			return fmt.Errorf("stdio upgrade: %d", resp.StatusCode)
		}
		conn, in = c, r
		return nil
	})
	return conn, in
}

// session runs one agent over one stdio stream and reports whether the
// stream broke before the agent's input closed.
func (w *restartWorker) session() bool {
	conn, in := w.dial()
	defer conn.Close() //nolint:errcheck // test
	w.mu.Lock()
	w.starts++
	start := w.starts
	w.prompts = append(w.prompts, nil)
	w.mu.Unlock()

	stdinR, stdinW := io.Pipe()
	broken := make(chan struct{})
	go func() {
		for {
			typ, payload, err := dataplane.ReadFrame(in)
			if err != nil {
				close(broken)
				_ = stdinW.CloseWithError(errStreamBroken)
				return
			}
			if typ == dataplane.FrameStdinClosed {
				_ = stdinW.Close()
				return
			}
			_, _ = stdinW.Write(payload)
		}
	}()
	err := w.agent(start, stdinR, &frameWriter{conn: conn}, broken)
	select {
	case <-broken:
		return true
	default:
	}
	if err != nil {
		w.t.Errorf("agent: %v", err)
	}
	if err := dataplane.WriteExit(conn, 0); err != nil {
		w.t.Errorf("exit frame: %v", err)
	}
	return false
}

// agent is [fakeAgent] that may hold a prompt until the stream breaks.
// It commits its two changes once, at the first prompt it works.
func (w *restartWorker) agent(start int, in io.Reader, out io.Writer, broken <-chan struct{}) error {
	enc := json.NewEncoder(out)
	sc := bufio.NewScanner(in)
	sc.Buffer(make([]byte, 0, 64<<10), 4<<20)
	for sc.Scan() {
		var line fakeAgentLine
		if err := json.Unmarshal(sc.Bytes(), &line); err != nil {
			return err
		}
		w.mu.Lock()
		w.prompts[start-1] = append(w.prompts[start-1], line.Prompt)
		w.mu.Unlock()
		if w.hold != nil && w.hold(start, line.Prompt) {
			w.holdingOnce.Do(func() { close(w.holding) })
			<-broken
			return errStreamBroken
		}
		if _, err := os.Stat(filepath.Join(w.clone, "one.txt")); errors.Is(err, os.ErrNotExist) {
			for _, name := range []string{"one.txt", "two.txt"} {
				if err := os.WriteFile(filepath.Join(w.clone, name), []byte(name+"\n"), 0o600); err != nil {
					return err
				}
				if err := agentGit(w.clone, "add", "--", name); err != nil {
					return err
				}
				if err := agentGit(w.clone, "commit", "--quiet", "-m", "add "+name); err != nil {
					return err
				}
			}
			if err := os.MkdirAll(filepath.Join(w.clone, ".sortie"), 0o750); err != nil {
				return err
			}
			if err := os.WriteFile(filepath.Join(w.clone, ".sortie", "pr.json"), []byte(`{"title":"Add one and two","body":"Two changes."}`), 0o600); err != nil {
				return err
			}
		}
		if err := enc.Encode(fakeAgentLine{Message: "worked"}); err != nil {
			return err
		}
		if err := enc.Encode(fakeAgentLine{Done: true}); err != nil {
			return err
		}
	}
	return sc.Err()
}

func (w *restartWorker) seen() (offers, starts int, prompts [][]string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.offers, w.starts, slices.Clone(w.prompts)
}

// run-supervision: A deploy during a long run. Sortie restarts while
// ONS-30's agent is in the middle of its turn on taichi: the run goes on
// with its worker, the restarted Sortie resumes the session and runs the
// interrupted turn again, and the run hands off once, as one session.
func TestRestartMidTurnKeepsTheRun(t *testing.T) {
	h := newRestartHarness(t, 1, time.Minute)
	w := newRestartWorker(h)
	w.hold = func(start int, _ string) bool { return start == 1 }
	first := h.start(w.checkIn)
	worked := make(chan struct{})
	go func() {
		defer close(worked)
		w.run()
	}()
	waittest.Receive(t, w.holding, "the agent in the middle of its turn")
	h.recorded(first, func(r persistence.RemoteRun) error {
		if r.SessionID == "" || r.TurnsCompleted != 0 || r.WorkerID != "taichi" {
			return fmt.Errorf("run %+v, want taichi's session in its first turn", r)
		}
		return nil
	})
	dispatchID := h.dispatchID(first)
	h.stopAndGo(first)

	second := h.start()
	h.handedOff(second)
	waittest.Receive(t, worked, "the worker's report")

	offers, starts, prompts := w.seen()
	if offers != 1 || starts != 2 {
		t.Fatalf("offers %d, agent starts %d, want the one job and the agent started again once", offers, starts)
	}
	if len(prompts[1]) != 1 || prompts[1][0] != prompts[0][0] {
		t.Errorf("prompts %q, want the interrupted turn run again", prompts)
	}
	if got := second.adapter.resumed(); !slices.Equal(got, []string{"line-session"}) {
		t.Errorf("the restarted Sortie resumed sessions %q, want the recorded line-session", got)
	}
	h.oneSessionHandedOff(second, dispatchID, 1)
}

// A restart between two turns keeps the run: the restarted Sortie runs
// the second turn as the continuation, and only it.
func TestRestartBetweenTurnsKeepsTheRun(t *testing.T) {
	h := newRestartHarness(t, 2, time.Minute)
	h.tracker.holdRefresh = true
	w := newRestartWorker(h)
	first := h.start(w.checkIn)
	worked := make(chan struct{})
	go func() {
		defer close(worked)
		w.run()
	}()
	waittest.Receive(t, h.tracker.refreshed, "the refresh after the first turn")
	h.recorded(first, func(r persistence.RemoteRun) error {
		if r.TurnsCompleted != 1 {
			return fmt.Errorf("turns completed %d, want 1", r.TurnsCompleted)
		}
		return nil
	})
	dispatchID := h.dispatchID(first)
	h.stopAndGo(first)
	h.tracker.mu.Lock()
	h.tracker.holdRefresh = false
	h.tracker.mu.Unlock()

	second := h.start()
	h.handedOff(second)
	waittest.Receive(t, worked, "the worker's report")

	_, starts, prompts := w.seen()
	if starts != 2 || len(prompts[0]) != 1 || len(prompts[1]) != 1 || prompts[1][0] == prompts[0][0] {
		t.Fatalf("agent starts %d, prompts %q, want the first turn before the restart and the continuation after", starts, prompts)
	}
	h.oneSessionHandedOff(second, dispatchID, 2)
}

// A reaction run keeps its reaction across a restart: the restarted
// Sortie runs the interrupted CI fix turn again from the same
// continuation, and the run hands off once.
func TestRestartDuringAReactionRunKeepsTheRun(t *testing.T) {
	h := newRestartHarness(t, 1, time.Minute)
	starts := 0
	h.prepare = func(o *Orchestrator) {
		if starts++; starts > 1 {
			return
		}
		o.state.RetryAttempts[remoteIssueID] = &RetryEntry{
			IssueID: remoteIssueID, Identifier: "ONS-30", Attempt: 1, AgentKind: h.cfg.Agent.Kind,
			IssueIdentity: IssueIdentity{Title: "Add one and two", State: "To Do"},
			ReactionKind:  ReactionKindCI,
			ContinuationContext: map[string]any{"ci_failure": (&domain.CIResult{
				Status: domain.CIStatusFailing, Ref: "abc123", FailingCount: 1, LogExcerpt: "lint: unused variable",
				CheckRuns: []domain.CheckRun{{Name: "golangci-lint", Status: domain.CheckRunStatusCompleted, Conclusion: domain.CheckConclusionFailure, DetailsURL: "https://ci.example/1"}},
			}).ToTemplateMap()},
			Continuation: ContinuationTarget{PRNumber: 12, Owner: "org", Repo: "app", Attempt: 1},
		}
		o.state.Claimed[remoteIssueID] = struct{}{}
	}
	w := newRestartWorker(h)
	w.hold = func(start int, _ string) bool { return start == 1 }
	first := h.start(w.checkIn)
	worked := make(chan struct{})
	go func() {
		defer close(worked)
		w.run()
	}()
	waittest.Receive(t, w.holding, "the CI fix agent in the middle of its turn")
	h.recorded(first, nil)
	dispatchID := h.dispatchID(first)
	h.stopAndGo(first)

	second := h.start()
	waittest.Until(t, func() error {
		if snap, _ := second.o.SnapshotFunc()(); len(snap.Running) != 0 {
			return errors.New("still running")
		}
		_, starts, _ := w.seen()
		if starts != 2 {
			return fmt.Errorf("agent starts %d", starts)
		}
		return nil
	})
	waittest.Receive(t, worked, "the worker's report")

	// The rebuilt turn names the same failing check; the log excerpt,
	// which is never stored, is left out.
	_, _, prompts := w.seen()
	if len(prompts[1]) != 1 || !strings.Contains(prompts[0][0], "golangci-lint") || !strings.Contains(prompts[1][0], "golangci-lint") {
		t.Fatalf("prompts %q, want the CI fix turn, naming the failing check, run again", prompts)
	}
	if !strings.Contains(prompts[0][0], "unused variable") || strings.Contains(prompts[1][0], "unused variable") {
		t.Errorf("log excerpt in the prompts %q, want it only before the restart", prompts)
	}
	rows := h.history(second)
	if len(rows) != 1 || rows[0].DispatchID == nil || *rows[0].DispatchID != dispatchID || rows[0].Status != "succeeded" {
		t.Fatalf("run history %+v, want one succeeded run of dispatch %s", rows, dispatchID)
	}
}

// run-supervision: A worker gone during the restart. Its restored lease
// expires, the run fails as for any lost lease, and the issue is retried
// once, as a new job attempt; with no worker checked in, that retry waits
// for one.
func TestRestartWithTheWorkerGoneRetriesOnce(t *testing.T) {
	h := newRestartHarness(t, 1, 1500*time.Millisecond)
	w := newRestartWorker(h)
	w.hold = func(int, string) bool { return true }
	w.dieOnBreak = true
	first := h.start(w.checkIn)
	worked := make(chan struct{})
	go func() {
		defer close(worked)
		w.run()
	}()
	waittest.Receive(t, w.holding, "the agent in the middle of its turn")
	h.recorded(first, nil)
	h.stopAndGo(first)
	waittest.Receive(t, worked, "the worker gone")

	second := h.start()
	waittest.Until(t, func() error {
		snap, err := second.o.SnapshotFunc()()
		if err != nil {
			return err
		}
		if len(snap.Running) != 0 || len(snap.Retrying) != 1 || snap.Retrying[0].Identifier != "ONS-30" || snap.Retrying[0].Attempt != 1 {
			return fmt.Errorf("running %d, retrying %+v, want ONS-30's one retry", len(snap.Running), snap.Retrying)
		}
		return nil
	})
	if state := second.o.Workers().Snapshot(); len(state.Leases) != 0 {
		t.Errorf("leases %+v, want the gone worker's expired", state.Leases)
	}
	rows := h.history(second)
	if len(rows) != 1 || rows[0].Status == "succeeded" {
		t.Fatalf("run history %+v, want the one failed run", rows)
	}
	if len(h.tracker.handoffs()) != 0 {
		t.Errorf("handoffs %v, want none", h.tracker.handoffs())
	}
}

// dispatchID returns the dispatch of ONS-30's running run.
func (h *restartHarness) dispatchID(p *sortieProcess) string {
	h.t.Helper()
	snap, err := p.o.SnapshotFunc()()
	if err != nil || len(snap.Running) != 1 {
		h.t.Fatalf("running %+v %v, want ONS-30", snap.Running, err)
	}
	return snap.Running[0].DispatchID
}

// oneSessionHandedOff checks that the run handed off once as the one
// session it was: one run history row, of the first dispatch, with every
// turn, and the pull request opened once.
func (h *restartHarness) oneSessionHandedOff(p *sortieProcess, dispatchID string, turns int) {
	h.t.Helper()
	if got := h.tracker.handoffs(); len(got) != 1 {
		h.t.Errorf("handoffs %v, want one", got)
	}
	rows := h.history(p)
	if len(rows) != 1 || rows[0].DispatchID == nil || *rows[0].DispatchID != dispatchID || rows[0].Status != "succeeded" || rows[0].TurnsCompleted != turns {
		h.t.Fatalf("run history %+v, want one succeeded run of dispatch %s with %d turns", rows, dispatchID, turns)
	}
	if calls := h.opener.ensureCalls(); len(calls) != 1 {
		h.t.Errorf("pull requests %+v, want one", calls)
	}
	if tip := h.git.branch(h.upstream, "sortie/ONS-30"); tip == "" {
		h.t.Error("sortie/ONS-30 was not pushed")
	}
	runs, err := p.store.LoadRemoteRuns(context.Background())
	if err != nil || len(runs) != 0 {
		h.t.Errorf("remote runs %+v %v, want none left", runs, err)
	}
}

// A run no worker took records no worker.
func TestRemoteAttemptWithoutAGrantNamesNoWorker(t *testing.T) {
	t.Parallel()
	a := &remoteAttempt{}
	if got := a.adjust(WorkerResult{}).Worker; got != "" {
		t.Errorf("Worker = %q, want empty for an attempt no worker took", got)
	}
	a = &remoteAttempt{adopt: &remoteAdoption{run: persistence.RemoteRun{WorkerID: "taichi"}}}
	if got := a.adjust(WorkerResult{}).Worker; got != "taichi" {
		t.Errorf("Worker = %q, want taichi for an adopted attempt", got)
	}
}
