package orchestrator

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/environment"
	"github.com/sortie-ai/sortie/internal/gitsync"
	"github.com/sortie-ai/sortie/internal/persistence"
	"github.com/sortie-ai/sortie/internal/waittest"
)

// loopFreeBound is how long a tick and GET /api/v1/state may take while
// every adapter call blocks.
const loopFreeBound = 100 * time.Millisecond

// blockGate holds every call of the blocking fakes until the test
// releases it, and counts the calls by name.
type blockGate struct {
	released chan struct{}
	once     sync.Once
	entered  chan string

	mu      sync.Mutex
	calls   map[string]int
	blocked int
}

func newBlockGate() *blockGate {
	return &blockGate{released: make(chan struct{}), entered: make(chan string, 1024), calls: map[string]int{}}
}

// wait blocks call until the gate is released or ctx ends.
func (g *blockGate) wait(ctx context.Context, call string) error {
	g.mu.Lock()
	g.calls[call]++
	g.blocked++
	g.mu.Unlock()
	defer func() {
		g.mu.Lock()
		g.blocked--
		g.mu.Unlock()
	}()
	select {
	case g.entered <- call:
	default:
	}
	select {
	case <-g.released:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// release lets every blocked call, and every later one, through.
func (g *blockGate) release() { g.once.Do(func() { close(g.released) }) }

func (g *blockGate) isReleased() bool {
	select {
	case <-g.released:
		return true
	default:
		return false
	}
}

// awaitEntered waits until n calls have reached the gate.
func (g *blockGate) awaitEntered(t *testing.T, n int) {
	t.Helper()
	for range n {
		waittest.Receive(t, g.entered, "a blocking fake call")
	}
}

// count returns how many times call was made.
func (g *blockGate) count(call string) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.calls[call]
}

// blockedNow returns how many calls block at the gate.
func (g *blockGate) blockedNow() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.blocked
}

// blockingTracker is a tracker whose every call blocks on gate, then
// answers from answer, or with nothing when answer is nil.
type blockingTracker struct {
	gate   *blockGate
	answer domain.TrackerAdapter
}

func (b *blockingTracker) FetchCandidateIssues(ctx context.Context) ([]domain.Issue, error) {
	if err := b.gate.wait(ctx, "tracker.FetchCandidateIssues"); err != nil || b.answer == nil {
		return nil, err
	}
	return b.answer.FetchCandidateIssues(ctx)
}

func (b *blockingTracker) FetchIssueByID(ctx context.Context, id string) (domain.Issue, error) {
	if err := b.gate.wait(ctx, "tracker.FetchIssueByID"); err != nil || b.answer == nil {
		return domain.Issue{ID: id}, err
	}
	return b.answer.FetchIssueByID(ctx, id)
}

func (b *blockingTracker) FetchIssuesByStates(ctx context.Context, states []string) ([]domain.Issue, error) {
	if err := b.gate.wait(ctx, "tracker.FetchIssuesByStates"); err != nil || b.answer == nil {
		return nil, err
	}
	return b.answer.FetchIssuesByStates(ctx, states)
}

func (b *blockingTracker) FetchIssueStatesByIDs(ctx context.Context, ids []string) (map[string]string, error) {
	if err := b.gate.wait(ctx, "tracker.FetchIssueStatesByIDs"); err != nil || b.answer == nil {
		return map[string]string{}, err
	}
	return b.answer.FetchIssueStatesByIDs(ctx, ids)
}

func (b *blockingTracker) FetchIssueStatesAndLabelsByIDs(ctx context.Context, ids []string) (map[string]domain.IssueStateAndLabels, error) {
	if err := b.gate.wait(ctx, "tracker.FetchIssueStatesAndLabelsByIDs"); err != nil || b.answer == nil {
		return map[string]domain.IssueStateAndLabels{}, err
	}
	return b.answer.FetchIssueStatesAndLabelsByIDs(ctx, ids)
}

func (b *blockingTracker) FetchIssueStatesByIdentifiers(ctx context.Context, identifiers []string) (map[string]string, error) {
	if err := b.gate.wait(ctx, "tracker.FetchIssueStatesByIdentifiers"); err != nil || b.answer == nil {
		return map[string]string{}, err
	}
	return b.answer.FetchIssueStatesByIdentifiers(ctx, identifiers)
}

func (b *blockingTracker) FetchIssueComments(ctx context.Context, id string) ([]domain.Comment, error) {
	if err := b.gate.wait(ctx, "tracker.FetchIssueComments"); err != nil || b.answer == nil {
		return nil, err
	}
	return b.answer.FetchIssueComments(ctx, id)
}

func (b *blockingTracker) TransitionIssue(ctx context.Context, id, target string) error {
	if err := b.gate.wait(ctx, "tracker.TransitionIssue"); err != nil || b.answer == nil {
		return err
	}
	return b.answer.TransitionIssue(ctx, id, target)
}

func (b *blockingTracker) CommentIssue(ctx context.Context, id, text string) error {
	if err := b.gate.wait(ctx, "tracker.CommentIssue"); err != nil || b.answer == nil {
		return err
	}
	return b.answer.CommentIssue(ctx, id, text)
}

func (b *blockingTracker) AddLabel(ctx context.Context, id, label string) error {
	if err := b.gate.wait(ctx, "tracker.AddLabel"); err != nil || b.answer == nil {
		return err
	}
	return b.answer.AddLabel(ctx, id, label)
}

func (b *blockingTracker) RemoveLabel(ctx context.Context, id, label string) error {
	if err := b.gate.wait(ctx, "tracker.RemoveLabel"); err != nil || b.answer == nil {
		return err
	}
	return b.answer.RemoveLabel(ctx, id, label)
}

func (b *blockingTracker) SetTrackerStates(func() domain.TrackerStates) {}

// blockingForge is a forge whose every call blocks on gate, then answers
// with reverts for a revert listing and with nothing otherwise.
type blockingForge struct {
	gate *blockGate

	mu      sync.Mutex
	reverts []domain.MergedRevert
}

func (b *blockingForge) FetchPendingReviews(ctx context.Context, n int, _, _ string) ([]domain.ReviewComment, error) {
	if err := b.gate.wait(ctx, forgeCall("FetchPendingReviews", n)); err != nil {
		return nil, err
	}
	return []domain.ReviewComment{{ID: "c-1", Body: "Please rename it."}}, nil
}

func (b *blockingForge) FetchBotReviewComments(ctx context.Context, n int, _, _ string, _ []string) ([]domain.ReviewComment, error) {
	return []domain.ReviewComment{}, b.gate.wait(ctx, forgeCall("FetchBotReviewComments", n))
}

func (b *blockingForge) GetReviewDecision(ctx context.Context, n int, _, _ string) (domain.ReviewDecision, error) {
	return "", b.gate.wait(ctx, forgeCall("GetReviewDecision", n))
}

func (b *blockingForge) GetCIStatus(ctx context.Context, n int, _, _ string) (string, error) {
	if err := b.gate.wait(ctx, forgeCall("GetCIStatus", n)); err != nil {
		return "", err
	}
	return "success", nil
}

func (b *blockingForge) GetMergeability(ctx context.Context, n int, _, _ string) (domain.PRMergeStatus, error) {
	if err := b.gate.wait(ctx, forgeCall("GetMergeability", n)); err != nil {
		return domain.PRMergeStatus{}, err
	}
	return domain.PRMergeStatus{HeadSHA: headSHA(n)}, nil
}

func (b *blockingForge) MergePR(ctx context.Context, n int, _, _ string, _ domain.MergeStrategy, _, _, _ string) (domain.MergeResult, error) {
	return domain.MergeResult{}, b.gate.wait(ctx, forgeCall("MergePR", n))
}

func (b *blockingForge) DeleteBranch(ctx context.Context, _, _, branch string) error {
	return b.gate.wait(ctx, "forge.DeleteBranch "+branch)
}

func (b *blockingForge) ListLabelEvents(ctx context.Context, n int, _, _ string) ([]domain.LabelEvent, error) {
	return []domain.LabelEvent{}, b.gate.wait(ctx, forgeCall("ListLabelEvents", n))
}

func (b *blockingForge) RemoveLabel(ctx context.Context, n int, _, _, _ string) error {
	return b.gate.wait(ctx, forgeCall("RemoveLabel", n))
}

func (b *blockingForge) FetchPullRequestComments(ctx context.Context, n int, _, _ string) ([]domain.ReviewComment, error) {
	return []domain.ReviewComment{}, b.gate.wait(ctx, forgeCall("FetchPullRequestComments", n))
}

func (b *blockingForge) FindMergedReverts(ctx context.Context, owner, repo string, _ time.Time) ([]domain.MergedRevert, error) {
	if err := b.gate.wait(ctx, "forge.FindMergedReverts "+owner+"/"+repo); err != nil {
		return nil, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]domain.MergedRevert(nil), b.reverts...), nil
}

// forgeCall names the forge call method makes on pull request n.
func forgeCall(method string, n int) string {
	return "forge." + method + " #" + strconv.Itoa(n)
}

// blockingGit is a repository whose every git call blocks on gate.
type blockingGit struct{ gate *blockGate }

func (b *blockingGit) Seed(ctx context.Context, _ environment.Environment, ws environment.Workspace) error {
	return b.gate.wait(ctx, "git.Seed "+ws.Key)
}

func (b *blockingGit) Refresh(ctx context.Context, _ environment.Environment, ws environment.Workspace) error {
	return b.gate.wait(ctx, "git.Refresh "+ws.Key)
}

func (b *blockingGit) Export(ctx context.Context, _ environment.Environment, ws environment.Workspace) (gitsync.ExportResult, error) {
	return gitsync.ExportResult{}, b.gate.wait(ctx, "git.Export "+ws.Key)
}

// readEgress is an [EgressReader] that blocks on gate.
func (b *blockingGit) readEgress(ctx context.Context, repo config.Repository) (egressRead, error) {
	return egressRead{}, b.gate.wait(ctx, "git.ReadEgress "+repo.Project)
}

// blockingCommand runs commands, such as a platform fault's check, that
// block on gate.
type blockingCommand struct{ gate *blockGate }

func (b *blockingCommand) run(ctx context.Context, name string) error {
	return b.gate.wait(ctx, "command "+name)
}

// checkFault is a [PlatformFaultChecker] that blocks on gate.
func (b *blockingCommand) checkFault(ctx context.Context, check PlatformFaultCheck) error {
	return b.run(ctx, "fault-check "+check.FaultID)
}

var (
	_ domain.TrackerAdapter           = (*blockingTracker)(nil)
	_ domain.SCMAdapter               = (*blockingForge)(nil)
	_ domain.PullRequestCommentReader = (*blockingForge)(nil)
	_ domain.RevertFinder             = (*blockingForge)(nil)
	_ Repository                      = (*blockingGit)(nil)
	_ EgressReader                    = (*blockingGit)(nil).readEgress
	_ PlatformFaultChecker            = (*blockingCommand)(nil).checkFault
)

// loopHarness is an orchestrator on its event loop whose tracker, forge,
// git and command adapters all block on one gate, with GET /api/v1/state
// served from the loop's snapshot as the HTTP server serves it.
type loopHarness struct {
	o       *Orchestrator
	gate    *blockGate
	tracker *blockingTracker
	forge   *blockingForge
	git     *blockingGit
	command *blockingCommand
	store   *persistence.Store
	stories *storyTracker
	state   *httptest.Server
	stopped chan struct{}
	stop    func()
}

// newLoopHarness starts an orchestrator on store with workers off-loop
// workers. Every adapter call blocks until gate is released, and the
// tracker answers from the returned harness's stories after that.
func newLoopHarness(t *testing.T, store *persistence.Store, gate *blockGate, workers int) *loopHarness {
	t.Helper()
	cfg := gateConfig()
	cfg.Tracker.HandoffState = "In Review"
	cfg.Tracker.ActiveStates = []string{"Todo"}
	cfg.Tracker.TerminalStates = []string{"Done", "Canceled"}
	cfg.OffLoop = config.OffLoopConfig{Workers: workers}
	h := &loopHarness{gate: gate, store: store, stories: newStoryTracker(), stopped: make(chan struct{})}
	h.tracker = &blockingTracker{gate: gate, answer: h.stories}
	h.forge = &blockingForge{gate: gate}
	h.git = &blockingGit{gate: gate}
	h.command = &blockingCommand{gate: gate}
	h.o = &Orchestrator{
		state:           NewState(5000, 4, 0, nil, AgentTotals{}),
		logger:          discardLogger(),
		trackerAdapter:  h.tracker,
		workflowManager: &stubWorkflowManager{config: cfg},
		store:           store,
		scmAdapter:      h.forge,
		prCommentReader: h.forge,
		revertFinder:    h.forge,
		readEgress:      h.git.readEgress,
		faultChecker:    h.command.checkFault,
		pollHealth:      &pollRecord{},
		actionCh:        make(chan *loopAction),
		snapshotCh:      make(chan snapshotRequest),
		refreshCh:       make(chan struct{}, 1),
		stopping:        make(chan struct{}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	go h.loop(ctx)
	var once sync.Once
	h.stop = func() {
		once.Do(func() {
			cancel()
			waittest.Receive(t, h.stopped, "the event loop's shutdown")
		})
	}
	t.Cleanup(h.stop)

	snapshot := h.o.SnapshotFunc()
	h.state = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/state" {
			http.NotFound(w, r)
			return
		}
		snap, err := snapshot()
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(harnessState{GeneratedAt: snap.GeneratedAt, OffLoop: snap.OffLoop})
	}))
	t.Cleanup(h.state.Close)
	return h
}

// loop is the event loop's handling of loop actions and snapshots, and
// its shutdown once ctx ends, as [Orchestrator.Run] does them.
func (h *loopHarness) loop(ctx context.Context) {
	defer close(h.stopped)
	for {
		select {
		case <-ctx.Done():
			timer := time.NewTimer(time.Hour)
			h.o.shutdown(timer)
			return
		case action := <-h.o.actionCh:
			if action.state.CompareAndSwap(loopActionPending, loopActionTaken) {
				action.apply(ctx)
				close(action.done)
			}
		case req := <-h.o.snapshotCh:
			req.ReplyCh <- h.o.snapshot(time.Now())
		case <-h.o.refreshCh:
		}
	}
}

// harnessState is the part of GET /api/v1/state the harness serves.
type harnessState struct {
	GeneratedAt time.Time       `json:"generated_at"`
	OffLoop     SnapshotOffLoop `json:"off_loop"`
}

// onLoop runs fn on the harness's event loop and waits for it.
func (h *loopHarness) onLoop(t *testing.T, fn func(ctx context.Context)) {
	t.Helper()
	if h.o.runOnLoop(t.Context(), fn) != loopRan {
		t.Fatal("the loop did not run the action")
	}
}

// getState answers GET /api/v1/state.
func (h *loopHarness) getState(t *testing.T) harnessState {
	t.Helper()
	got, err := fetchHarnessState(t.Context(), h.state.URL)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func fetchHarnessState(ctx context.Context, base string) (harnessState, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/api/v1/state", nil)
	if err != nil {
		return harnessState{}, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return harnessState{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return harnessState{}, &stateStatusError{status: resp.Status}
	}
	var got harnessState
	return got, json.NewDecoder(resp.Body).Decode(&got)
}

type stateStatusError struct{ status string }

func (e *stateStatusError) Error() string { return "GET /api/v1/state answered " + e.status }

// assertLoopFree runs tick on the event loop while every call of the
// blocking fakes blocks, and fails the test unless the tick ends, and GET
// /api/v1/state then answers, each within loopFreeBound. It returns the
// state served.
func (h *loopHarness) assertLoopFree(t *testing.T, tick func(ctx context.Context)) harnessState {
	t.Helper()
	if h.gate.isReleased() {
		t.Fatal("assertLoopFree: the blocking fakes are released, so it proves nothing")
	}
	ended := make(chan loopOutcome, 1)
	go func() { ended <- h.o.runOnLoop(t.Context(), tick) }()
	outcome, ok := waittest.ReceiveWithin(ended, loopFreeBound)
	if !ok {
		t.Fatalf("the tick did not end within %v while every adapter call blocks", loopFreeBound)
	}
	if outcome != loopRan {
		t.Fatalf("the tick did not run: outcome %v", outcome)
	}

	type answer struct {
		state harnessState
		err   error
	}
	answered := make(chan answer, 1)
	go func() {
		got, err := fetchHarnessState(t.Context(), h.state.URL)
		answered <- answer{got, err}
	}()
	got, ok := waittest.ReceiveWithin(answered, loopFreeBound)
	if !ok {
		t.Fatalf("GET /api/v1/state did not answer within %v while every adapter call blocks", loopFreeBound)
	}
	if got.err != nil {
		t.Fatalf("GET /api/v1/state while every adapter call blocks: %v", got.err)
	}
	return got.state
}
