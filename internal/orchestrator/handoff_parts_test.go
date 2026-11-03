package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/persistence"
	"github.com/sortie-ai/sortie/internal/prompt"
	"github.com/sortie-ai/sortie/internal/waittest"
	"github.com/sortie-ai/sortie/internal/worker"
)

// handoffTracker is the fake tracker: it creates issues, labels them,
// and answers each one's state as the test sets it.
type handoffTracker struct {
	*mockTrackerAdapter

	mu      sync.Mutex
	created []domain.NewIssue
	ids     []string
	labels  map[string][]string
	states  map[string]string
	refuse  error
}

var _ domain.IssueCreator = (*handoffTracker)(nil)

func newHandoffTracker() *handoffTracker {
	return &handoffTracker{mockTrackerAdapter: &mockTrackerAdapter{}, labels: map[string][]string{}, states: map[string]string{}}
}

func (f *handoffTracker) CreateIssue(_ context.Context, issue domain.NewIssue) (domain.CreatedIssue, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.refuse != nil {
		return domain.CreatedIssue{}, f.refuse
	}
	f.created = append(f.created, issue)
	n := 200 + len(f.created)
	created := domain.CreatedIssue{ID: "id-" + strconv.Itoa(n), Identifier: "ONS-" + strconv.Itoa(n)}
	f.ids = append(f.ids, created.ID)
	f.states[created.ID] = issue.State
	return created, nil
}

func (f *handoffTracker) AddLabel(_ context.Context, issueID, label string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.labels[issueID] = append(f.labels[issueID], label)
	return nil
}

func (f *handoffTracker) FetchIssueStatesByIDs(_ context.Context, ids []string) (map[string]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := map[string]string{}
	for _, id := range ids {
		if s, ok := f.states[id]; ok {
			out[id] = s
		}
	}
	return out, nil
}

func (f *handoffTracker) setState(id, state string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.states[id] = state
}

func (f *handoffTracker) createdIssues() ([]domain.NewIssue, []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.created), slices.Clone(f.ids)
}

// handoffConfig is a remote workflow over the agentic-orchestration and
// sortie repositories, the first the default.
func handoffConfig(t *testing.T) config.ServiceConfig {
	t.Helper()
	cfg := remoteCfg()
	cfg.Workspace.Root = t.TempDir()
	cfg.Tracker.ActiveStates = []string{"To Do"}
	cfg.Tracker.TerminalStates = []string{"Done", "Canceled"}
	cfg.Tracker.HandoffState = "In Review"
	cfg.Environment.Remote.Repositories = []config.SmolvmRepository{
		{Name: "agentic-orchestration", URL: "git@github.com:ortusnova/agentic-orchestration.git", Base: "main", Provider: "github", Project: "ortusnova/agentic-orchestration"},
		{Name: "sortie", URL: "git@github.com:ortusnova/sortie.git", Base: "plan-bench", Provider: "github", Project: "ortusnova/sortie"},
	}
	cfg.Environment.Remote.DefaultRepository = "agentic-orchestration"
	return cfg
}

// handoffHarness is an orchestrator over a fake tracker, a fake forge and
// the worker matcher, whose event loop only runs the actions sent to it.
type handoffHarness struct {
	t       *testing.T
	o       *Orchestrator
	store   *persistence.Store
	tracker *handoffTracker
	forge   *supervisorForge
	ctx     context.Context
}

func newHandoffHarness(t *testing.T, cfg config.ServiceConfig) *handoffHarness {
	t.Helper()
	h := &handoffHarness{t: t, store: openRepeatedFailureStore(t), tracker: newHandoffTracker(), forge: newSupervisorForge()}
	h.o = &Orchestrator{
		state:                       NewState(5000, 4, 0, nil, AgentTotals{}),
		logger:                      discardLogger(),
		trackerAdapter:              h.tracker,
		workflowManager:             &stubWorkflowManager{config: cfg},
		store:                       h.store,
		scmAdapter:                  h.forge,
		autoMergeReactionConfigured: true,
		autoMergeConfig:             defaultAutoMergeConfig(),
		remote:                      newRemoteWorkers(time.Now),
		actionCh:                    make(chan *loopAction),
		refreshCh:                   make(chan struct{}, 1),
		stopping:                    make(chan struct{}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	h.ctx = ctx
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-ctx.Done():
				return
			case <-h.o.refreshCh:
			case action := <-h.o.actionCh:
				if action.state.CompareAndSwap(loopActionPending, loopActionTaken) {
					action.apply(ctx)
					close(action.done)
				}
			}
		}
	}()
	t.Cleanup(func() {
		cancel()
		<-done
		h.o.state.TrackerOpsWg.Wait()
	})
	return h
}

// loop runs fn on the event loop.
func (h *handoffHarness) loop(fn func(ctx context.Context)) {
	h.t.Helper()
	if got := h.o.runOnLoop(h.ctx, fn); got != loopRan {
		h.t.Fatalf("runOnLoop = %v, want it to run", got)
	}
}

// part returns a copy of the story's part, read on the loop.
func (h *handoffHarness) part(issueID, partID string) HandoffPart {
	h.t.Helper()
	var p HandoffPart
	h.loop(func(context.Context) {
		if got := h.o.state.HandoffParts[handoffPartKey(issueID, partID)]; got != nil {
			p = *got
		}
	})
	return p
}

// sessionRecords writes the hand-off a session of identifier leaves in
// its workspace and returns the parts Sortie reads from it.
func sessionRecords(t *testing.T, cfg config.ServiceConfig, identifier, handoff string) []HandoffPartSpec {
	t.Helper()
	ws := filepath.Join(cfg.Workspace.Root, identifier)
	if err := os.MkdirAll(filepath.Join(ws, ".sortie"), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ws, ".sortie", handoffPartsFile), []byte(handoff), 0o600); err != nil {
		t.Fatal(err)
	}
	specs := readHandoffParts(ws, discardLogger())
	if len(specs) == 0 {
		t.Fatalf("readHandoffParts(%s) read no part", handoff)
	}
	return specs
}

// autoMergeWatch puts a merge watch of the story's pull request in state.
func autoMergeWatch(state *State, issueID, identifier string, number int, owner, repo string) {
	state.PendingReactions[ReactionKey(issueID, ReactionKindAutoMerge)] = &PendingReaction{
		IssueID: issueID, Identifier: identifier, DisplayID: identifier, Attempt: 1, Kind: ReactionKindAutoMerge,
		CreatedAt: autoMergeBaseTime,
		KindData:  &AutoMergeReactionData{PRNumber: number, Owner: owner, Repo: repo, Branch: "sortie/" + identifier, SHA: "head-1"},
	}
}

// reconcileMergeAt runs the auto-merge pass on the loop at now and returns
// the wait of the story's merge watch.
func (h *handoffHarness) reconcileMergeAt(issueID string, now time.Time) string {
	h.t.Helper()
	var wait string
	h.loop(func(ctx context.Context) {
		params := autoMergeParams(&reviewReconcileStore{}, h.forge, h.tracker)
		params.Ctx = ctx
		params.NowFunc = func() time.Time { return now }
		reconcileAutoMerge(h.o.state, params, discardLogger(), ctx, newAutoMergeMetricsSpy())
		if p := h.o.state.PendingReactions[ReactionKey(issueID, ReactionKindAutoMerge)]; p != nil {
			wait = p.Wait
		}
	})
	return wait
}

// Scenario: A change Sortie needs is a patch in the pull request body.
// The review of ortusnova/agentic-orchestration#58 finds it needs a change
// to Sortie's review gate and records it as a hand-off part: a run opens
// that change as a pull request to ortusnova/sortie, and the pull request
// of the review merges only after it.
func TestScenario_AChangeSortieNeedsIsAPatchInThePullRequestBody(t *testing.T) {
	t.Parallel()
	cfg := handoffConfig(t)
	h := newHandoffHarness(t, cfg)
	const patch = "--- a/internal/orchestrator/review_gate.go\n+++ b/internal/orchestrator/review_gate.go\n@@ -1 +1 @@\n-old\n+new\n"
	specs := sessionRecords(t, cfg, "ONS-58", `{"parts": [{
		"id": "review-gate",
		"kind": "pull_request",
		"repository": "ortusnova/sortie",
		"need": "Sortie's review gate must accept a criteria verdict from the review stage",
		"check": "go test ./internal/orchestrator -run TestPlanReviewGate",
		"patch": `+strconv.Quote(patch)+`
	}]}`)

	h.loop(func(ctx context.Context) {
		autoMergeWatch(h.o.state, "id-58", "ONS-58", 58, "ortusnova", "agentic-orchestration")
		h.o.takeHandoffParts(ctx, WorkerResult{IssueID: "id-58", Identifier: "ONS-58", ExitKind: WorkerExitNormal, HandoffParts: specs})
	})

	created, ids := h.tracker.createdIssues()
	if len(created) != 1 {
		t.Fatalf("issues filed = %d, want the change filed once", len(created))
	}
	filed := created[0]
	if filed.State != "To Do" || filed.Kind != domain.IssueKindStory || filed.PlanKey != "handoff-ONS-58-review-gate" {
		t.Errorf("filed issue = %+v, want a dispatchable story keyed by the part", filed)
	}
	for _, want := range []string{"ONS-58", "ortusnova/sortie", "review gate must accept", "TestPlanReviewGate", "```diff\n" + strings.TrimRight(patch, "\n") + "\n```"} {
		if !strings.Contains(filed.Title+"\n"+filed.Body, want) {
			t.Errorf("filed issue lacks %q:\n%s\n%s", want, filed.Title, filed.Body)
		}
	}
	if got := h.tracker.labels[ids[0]]; !slices.Equal(got, []string{config.RepositoryLabelPrefix + "sortie"}) {
		t.Errorf("labels of the filed issue = %v, want it to run in sortie", got)
	}
	p := h.part("id-58", "review-gate")
	if p.Status != HandoffWorking || p.WorkIdentifier != "ONS-201" {
		t.Errorf("part = %+v, want it carried by ONS-201", p)
	}

	if wait := h.reconcileMergeAt("id-58", autoMergeBaseTime); wait != waitHandoffParts || len(h.forge.merges) != 0 {
		t.Fatalf("auto-merge with the change open: wait %q, merges %v; want #58 held for the hand-off", wait, h.forge.merges)
	}
	if got := h.o.Supervisor().Merge(h.ctx, "ONS-58"); got.Status != SupervisorPrecondition || got.Code != "handoff_open" {
		t.Errorf("supervisor merge with the change open = %+v, want handoff_open", got)
	}

	h.tracker.setState(ids[0], "In Review")
	h.loop(func(ctx context.Context) { h.o.advanceHandoffParts(ctx, time.Now().UTC()) })
	if p := h.part("id-58", "review-gate"); p.Status != HandoffWorking {
		t.Fatalf("part with its change in review = %+v, want it still open", p)
	}

	h.tracker.setState(ids[0], "Done")
	h.loop(func(ctx context.Context) { h.o.advanceHandoffParts(ctx, time.Now().UTC()) })
	if p := h.part("id-58", "review-gate"); p.Status != HandoffDone || !strings.Contains(p.Result, "ONS-201 reached Done") {
		t.Fatalf("part after its change merged = %+v, want it done", p)
	}
	if wait := h.reconcileMergeAt("id-58", autoMergeBaseTime.Add(10*time.Minute)); len(h.forge.merges) != 1 {
		t.Fatalf("auto-merge after the change: wait %q, merges %v; want #58 merged", wait, h.forge.merges)
	}

	rows, err := h.store.LoadHandoffParts(context.Background())
	if err != nil || len(rows) != 1 || rows[0].Status != HandoffDone || rows[0].WorkIdentifier != "ONS-201" || rows[0].Patch != patch {
		t.Errorf("stored parts = %+v, %v; want the done part with its patch", rows, err)
	}
}

// A run that records the same part again changes nothing: the part is
// filed once.
func TestTakeHandoffParts_ThePartAgainChangesNothing(t *testing.T) {
	t.Parallel()
	cfg := handoffConfig(t)
	h := newHandoffHarness(t, cfg)
	specs := sessionRecords(t, cfg, "ONS-58", `{"parts":[{"id":"gate","repository":"sortie","need":"n","check":"c"}]}`)
	for range 2 {
		h.loop(func(ctx context.Context) {
			h.o.takeHandoffParts(ctx, WorkerResult{IssueID: "id-58", Identifier: "ONS-58", HandoffParts: specs})
		})
	}
	if created, _ := h.tracker.createdIssues(); len(created) != 1 {
		t.Errorf("issues filed = %d, want 1", len(created))
	}
}

// A change the tracker refuses to file stays pending and is filed on a
// later pass.
func TestHandoffChange_ARefusedFilingIsTriedAgain(t *testing.T) {
	t.Parallel()
	cfg := handoffConfig(t)
	h := newHandoffHarness(t, cfg)
	h.tracker.refuse = errors.New("tracker down")
	specs := sessionRecords(t, cfg, "ONS-58", `{"parts":[{"id":"gate","repository":"sortie","need":"n","check":"c"}]}`)
	h.loop(func(ctx context.Context) {
		h.o.takeHandoffParts(ctx, WorkerResult{IssueID: "id-58", Identifier: "ONS-58", HandoffParts: specs})
	})
	if p := h.part("id-58", "gate"); p.Status != HandoffPending || !strings.Contains(p.Result, "tracker down") {
		t.Fatalf("part after a refused filing = %+v, want it pending with the refusal", p)
	}
	h.tracker.mu.Lock()
	h.tracker.refuse = nil
	h.tracker.mu.Unlock()
	h.loop(func(ctx context.Context) { h.o.advanceHandoffParts(ctx, time.Now().UTC()) })
	if p := h.part("id-58", "gate"); p.Status != HandoffWorking {
		t.Errorf("part after the next pass = %+v, want it filed", p)
	}
}

// A change in a repository the workflow does not open is a platform fault
// whose person action opens it, and the story waits for it.
func TestHandoffChange_AnUnlistedRepositoryIsAPersonAction(t *testing.T) {
	t.Parallel()
	cfg := handoffConfig(t)
	h := newHandoffHarness(t, cfg)
	specs := sessionRecords(t, cfg, "ONS-58", `{"parts":[{"id":"infra","repository":"ortusnova/infra","need":"open port 443","check":"curl it"}]}`)
	h.loop(func(ctx context.Context) {
		h.o.takeHandoffParts(ctx, WorkerResult{IssueID: "id-58", Identifier: "ONS-58", HandoffParts: specs})
	})
	p := h.part("id-58", "infra")
	var fault SnapshotPlatformFault
	h.loop(func(context.Context) {
		if f := h.o.state.PlatformFaults[p.FaultID]; f != nil {
			fault = snapshotPlatformFault(f)
		}
		if h.o.state.FaultHolds["id-58"] != p.FaultID {
			t.Errorf("fault holds = %v, want the story held", h.o.state.FaultHolds)
		}
	})
	if p.Status != HandoffFault || fault.Class != PlatformFaultClassHandoffChange || fault.Action == nil || fault.Action.Where != "ortusnova/infra" {
		t.Errorf("part %+v, fault %+v; want a person action in ortusnova/infra", p, fault)
	}
	if created, _ := h.tracker.createdIssues(); len(created) != 0 {
		t.Errorf("issues filed = %d, want none", len(created))
	}
}

// fakeWorker dials the worker API as a worker daemon does.
type fakeHandoffWorker struct {
	t   *testing.T
	url string
	id  string
}

func (w fakeHandoffWorker) post(path string, body any) *http.Response {
	w.t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			w.t.Fatal(err)
		}
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, w.url+path, &buf)
	if err != nil {
		w.t.Fatal(err)
	}
	req.Header.Set(worker.ProtocolHeader, "1")
	req.Header.Set("X-W", w.id)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		w.t.Fatal(err)
	}
	return resp
}

func (w fakeHandoffWorker) checkIn(capabilities ...string) {
	w.t.Helper()
	d := map[string]any{"os": "linux", "arch": "x86_64", "kvm": true, "nested": true, "backends": []string{"smolvm"},
		"run_slots": 2, "running": 0, "free": map[string]any{"cpu": 8, "mem_gib": 16, "disk_gib": 100}}
	if capabilities != nil {
		d["capabilities"] = capabilities
	}
	resp := w.post("/api/v1/workers/checkin", d)
	resp.Body.Close() //nolint:errcheck // test
	if resp.StatusCode != http.StatusOK {
		w.t.Fatalf("check-in = %d", resp.StatusCode)
	}
}

func (w fakeHandoffWorker) poll() (worker.Offer, bool) {
	w.t.Helper()
	resp := w.post("/api/v1/workers/poll", nil)
	defer resp.Body.Close() //nolint:errcheck // test
	if resp.StatusCode == http.StatusNoContent {
		return worker.Offer{}, false
	}
	var offer worker.Offer
	if err := json.NewDecoder(resp.Body).Decode(&offer); err != nil {
		w.t.Fatal(err)
	}
	return offer, true
}

func (w fakeHandoffWorker) report(offer worker.Offer, outcome, reason string) {
	w.t.Helper()
	resp := w.post(fmt.Sprintf("/api/v1/workers/jobs/%s/attempts/%d/report", offer.Job.ID, offer.Job.Attempt),
		map[string]string{"outcome": outcome, "reason": reason, "machine": "deleted"})
	resp.Body.Close() //nolint:errcheck // test
	if resp.StatusCode != http.StatusNoContent {
		w.t.Fatalf("report = %d", resp.StatusCode)
	}
}

func (h *handoffHarness) worker(id string) fakeHandoffWorker {
	srv := httptest.NewServer(worker.Handler(h.o.remote.matcher, "X-W"))
	h.t.Cleanup(srv.Close)
	return fakeHandoffWorker{t: h.t, url: srv.URL, id: id}
}

const homelabProof = `{"parts":[{
	"id": "live-chain",
	"kind": "job",
	"capabilities": ["reach:homelab"],
	"need": "the live chain reaches gitea.homelab.lan, which this machine's egress refuses",
	"check": "curl -fsS https://gitea.homelab.lan/api/healthz"
}]}`

// Scenario: A live proof the machine cannot reach. A homelab story's live
// chain needs a host its machine's egress refuses: the proof runs as a
// job on a worker that reaches it, and the story waits for the result.
func TestScenario_ALiveProofRunsOnAWorkerThatReachesIt(t *testing.T) {
	t.Parallel()
	cfg := handoffConfig(t)
	h := newHandoffHarness(t, cfg)
	far, near := h.worker("p1"), h.worker("homelab-1")
	far.checkIn()
	near.checkIn("reach:homelab")
	specs := sessionRecords(t, cfg, "ONS-77", homelabProof)

	h.loop(func(ctx context.Context) {
		autoMergeWatch(h.o.state, "id-77", "ONS-77", 31, "ortusnova", "agentic-orchestration")
		h.o.takeHandoffParts(ctx, WorkerResult{IssueID: "id-77", Identifier: "ONS-77", HandoffParts: specs})
	})
	if p := h.part("id-77", "live-chain"); p.Status != HandoffWorking || p.JobAttempt != 1 {
		t.Fatalf("part = %+v, want its check queued", p)
	}

	if _, ok := far.poll(); ok {
		t.Fatal("a worker without reach:homelab was offered the proof")
	}
	offer, ok := near.poll()
	if !ok {
		t.Fatal("the worker that reaches the host was offered nothing")
	}
	if offer.Job.Kind != worker.JobKindCheck || offer.Job.ID != "handoff-ONS-77-live-chain" ||
		offer.Run.Command != "sh" || !slices.Equal(offer.Run.Args, []string{"-c", "curl -fsS https://gitea.homelab.lan/api/healthz"}) ||
		!slices.Contains(offer.Job.Requires.Capabilities, "reach:homelab") || offer.Run.Env["SORTIE_HANDOFF_PART"] != "live-chain" {
		t.Errorf("offer = %+v, want the check job", offer)
	}
	if len(offer.Flows) != 0 || offer.Run.SeedPath != "" {
		t.Errorf("offer flows %v, seed %q; want a check job with neither", offer.Flows, offer.Run.SeedPath)
	}

	if wait := h.reconcileMergeAt("id-77", autoMergeBaseTime); wait != waitHandoffParts || len(h.forge.merges) != 0 {
		t.Fatalf("auto-merge while the proof runs: wait %q, merges %v; want the story waiting", wait, h.forge.merges)
	}

	near.report(offer, "succeeded", "200 OK")
	waittest.Until(t, func() error {
		if p := h.part("id-77", "live-chain"); p.Status != HandoffDone {
			return fmt.Errorf("part = %+v, want it done", p)
		}
		return nil
	})
	if p := h.part("id-77", "live-chain"); !strings.Contains(p.Result, "200 OK") {
		t.Errorf("result = %q, want the worker's reason", p.Result)
	}
	if h.reconcileMergeAt("id-77", autoMergeBaseTime.Add(10*time.Minute)); len(h.forge.merges) != 1 {
		t.Errorf("merges after the proof = %v, want the story merged", h.forge.merges)
	}
}

// Scenario: A live proof the machine cannot reach, and no worker reaches
// it either: the refusal is a platform fault with a person action, the
// story waits, and a worker that checks in later with the capability
// runs the proof.
func TestScenario_ALiveProofNoWorkerReachesIsAPlatformFault(t *testing.T) {
	t.Parallel()
	cfg := handoffConfig(t)
	h := newHandoffHarness(t, cfg)
	far := h.worker("p1")
	far.checkIn()
	specs := sessionRecords(t, cfg, "ONS-77", homelabProof)

	h.loop(func(ctx context.Context) {
		h.o.takeHandoffParts(ctx, WorkerResult{IssueID: "id-77", Identifier: "ONS-77", HandoffParts: specs})
	})
	p := h.part("id-77", "live-chain")
	var fault SnapshotPlatformFault
	h.loop(func(context.Context) {
		if f := h.o.state.PlatformFaults[p.FaultID]; f != nil {
			fault = snapshotPlatformFault(f)
		}
	})
	if p.Status != HandoffFault || fault.Class != PlatformFaultClassHandoffUnplaced || fault.Action == nil ||
		!strings.Contains(fault.Action.Action, "reach:homelab") || !strings.Contains(fault.Action.Action, "curl -fsS") ||
		len(fault.Holds) != 1 || fault.Holds[0].Identifier != "ONS-77" {
		t.Fatalf("part %+v, fault %+v; want a person-action fault holding ONS-77", p, fault)
	}
	if _, ok := far.poll(); ok {
		t.Error("the refused proof was offered to a worker that does not reach the host")
	}

	h.loop(func(ctx context.Context) { h.o.advanceHandoffParts(ctx, time.Now().UTC()) })
	if p := h.part("id-77", "live-chain"); p.Status != HandoffFault {
		t.Fatalf("part with still no worker = %+v, want it held", p)
	}

	near := h.worker("homelab-1")
	near.checkIn("reach:homelab")
	h.loop(func(ctx context.Context) { h.o.advanceHandoffParts(ctx, time.Now().UTC()) })
	h.loop(func(context.Context) {
		if len(h.o.state.PlatformFaults) != 0 || h.o.state.FaultHolds["id-77"] != "" {
			t.Errorf("faults %v, holds %v after a worker reaches the host; want the refusal lifted", h.o.state.PlatformFaults, h.o.state.FaultHolds)
		}
	})
	if p := h.part("id-77", "live-chain"); p.Status != HandoffWorking {
		t.Fatalf("part after a worker reaches the host = %+v, want its check queued", p)
	}
	offer, ok := near.poll()
	if !ok || offer.Job.Kind != worker.JobKindCheck {
		t.Fatalf("offer = %+v, %v; want the check", offer, ok)
	}
	near.report(offer, "failed", "connection refused")
	waittest.Until(t, func() error {
		if p := h.part("id-77", "live-chain"); p.Status != HandoffFault {
			return fmt.Errorf("part = %+v, want the failed check held for a person", p)
		}
		return nil
	})
	h.loop(func(context.Context) {
		f := h.o.state.PlatformFaults[h.o.state.FaultHolds["id-77"]]
		if f == nil || f.Class != PlatformFaultClassHandoffCheckFailed || !strings.Contains(f.Cause, "connection refused") {
			t.Errorf("fault = %+v, want the failed check", f)
		}
	})
}

// A person marking the action of a part's fault done closes the part, and
// the story is released.
func TestHandoffPerson_MarkedDoneClosesThePart(t *testing.T) {
	t.Parallel()
	cfg := handoffConfig(t)
	h := newHandoffHarness(t, cfg)
	specs := sessionRecords(t, cfg, "ONS-60", `{"parts":[{
		"id": "deploy-key",
		"need": "a deploy key with write access to ortusnova/homelab",
		"check": "git push --dry-run from the runner succeeds",
		"action": "Add the runner's deploy key with write access",
		"where": "github.com/ortusnova/homelab/settings/keys"
	}]}`)
	if specs[0].Kind != HandoffKindPerson {
		t.Fatalf("kind = %q, want a part with no repository or capability to be a person's", specs[0].Kind)
	}
	h.loop(func(ctx context.Context) {
		h.o.takeHandoffParts(ctx, WorkerResult{IssueID: "id-60", Identifier: "ONS-60", HandoffParts: specs})
	})
	p := h.part("id-60", "deploy-key")
	var action string
	h.loop(func(context.Context) {
		if f := h.o.state.PlatformFaults[p.FaultID]; f != nil && f.Action != nil {
			action = f.Action.Action + " @ " + f.Action.Where
		}
	})
	if p.Status != HandoffFault || !strings.Contains(action, "Add the runner's deploy key") || !strings.Contains(action, "settings/keys") {
		t.Fatalf("part %+v, action %q; want the person action", p, action)
	}

	got := h.o.PlatformFaultDoneFunc()(h.ctx, p.FaultID, "ops")
	if !got.Resolved {
		t.Fatalf("action done = %+v, want the fault resolved", got)
	}
	if p := h.part("id-60", "deploy-key"); p.Status != HandoffDone || p.Result != "done by ops" {
		t.Errorf("part after the action = %+v, want it done by ops", p)
	}
	h.loop(func(context.Context) {
		if handoffOpen(h.o.state, "id-60") || h.o.state.FaultHolds["id-60"] != "" {
			t.Error("the story is still held after its part closed")
		}
	})
}

// The parts a run records reach the orchestrator with its exit, and the
// exit hands them on: through the worker's read of the workspace and the
// exit handler.
func TestHandleWorkerExit_TakesTheRecordedParts(t *testing.T) {
	t.Parallel()
	story := candidate("id-61", "ONS-61")
	h, store := faultHarness(t, story)
	specs := sessionRecords(t, h.wm.Config(), "ONS-61", `{"parts":[{"id":"perm","need":"admin on the org","check":"gh api orgs/x"}]}`)
	s := h.o.state
	s.Running[story.ID] = &RunningEntry{Identifier: story.Identifier, Issue: story, StartedAt: time.Now().UTC(), AgentKind: "kind-a"}
	s.Claimed[story.ID] = struct{}{}
	h.o.handleWorkerExit(context.Background(), WorkerResult{
		IssueID: story.ID, Identifier: story.Identifier, ExitKind: WorkerExitNormal, AgentAdapter: "kind-a",
		SoftStop: true, SoftStopReason: "needs-human-review", HandoffParts: specs,
	})
	p := s.HandoffParts[handoffPartKey(story.ID, "perm")]
	if p == nil || p.Status != HandoffFault || s.FaultHolds[story.ID] != p.FaultID {
		t.Fatalf("part = %+v, holds %v; want the part held for a person", p, s.FaultHolds)
	}
	rows, err := store.LoadHandoffParts(context.Background())
	if err != nil || len(rows) != 1 || rows[0].Status != HandoffFault {
		t.Fatalf("stored = %+v, %v", rows, err)
	}

	restarted := NewState(5000, 4, 0, nil, AgentTotals{})
	PopulateHandoffParts(restarted, rows, discardLogger())
	if !handoffOpen(restarted, story.ID) {
		t.Error("the part did not survive a restart")
	}
}

// A check job does not outlive the process that queued it: at a restart
// its part is pending again, so the check runs again.
func TestPopulateHandoffParts_AQueuedCheckRunsAgain(t *testing.T) {
	t.Parallel()
	state := NewState(5000, 4, 0, nil, AgentTotals{})
	PopulateHandoffParts(state, []persistence.HandoffPart{
		{IssueID: "i", PartID: "proof", Identifier: "ONS-1", Kind: HandoffKindJob, Need: "n", Check: "c", Status: HandoffWorking, JobAttempt: 1},
		{IssueID: "i", PartID: "change", Identifier: "ONS-1", Kind: HandoffKindPullRequest, Need: "n", Check: "c", Status: HandoffWorking, WorkIssueID: "w"},
		{IssueID: "i", PartID: "old", Identifier: "ONS-1", Kind: HandoffKindPerson, Need: "n", Check: "c", Status: HandoffDone},
	}, discardLogger())
	if got := state.HandoffParts[handoffPartKey("i", "proof")].Status; got != HandoffPending {
		t.Errorf("job part = %q, want pending", got)
	}
	if got := state.HandoffParts[handoffPartKey("i", "change")].Status; got != HandoffWorking {
		t.Errorf("filed change = %q, want still working", got)
	}
	if got := len(openHandoffParts(state, "i")); got != 2 {
		t.Errorf("open parts = %d, want 2", got)
	}
}

// The story is kept out of Done while a part is open: the merge-completion
// reaction does not move it, nor does the merged-to-done pass.
func TestHandoffParts_KeepTheStoryOutOfDone(t *testing.T) {
	t.Parallel()
	state := NewState(5000, 4, 0, nil, AgentTotals{})
	state.HandoffParts[handoffPartKey("id-9", "proof")] = &HandoffPart{
		HandoffPartSpec: HandoffPartSpec{ID: "proof", Kind: HandoffKindJob}, IssueID: "id-9", Identifier: "ONS-9", Status: HandoffWorking,
	}

	tracker := &mockTrackerAdapter{fetchStatesFn: func(context.Context, []string) (map[string]string, error) {
		return map[string]string{"id-9": "In Review"}, nil
	}}
	forge := &controlledSCMAdapter{getMergeabilityFn: func(context.Context, int, string, string) (domain.PRMergeStatus, error) {
		return domain.PRMergeStatus{Merged: true, MergeCommitSHA: "m1"}, nil
	}}
	key := ReactionKey("id-9", ReactionKindMergeCompletion)
	state.PendingReactions[key] = &PendingReaction{IssueID: "id-9", Identifier: "ONS-9", Kind: ReactionKindMergeCompletion,
		KindData: &MergeCompletionReactionData{PRNumber: 9, Owner: "o", Repo: "r"}}
	params := ReconcileParams{
		TrackerAdapter: tracker, SCMAdapter: forge, Store: &reviewReconcileStore{}, Ctx: context.Background(), Logger: discardLogger(),
		TerminalStates: []string{"Done"}, HandoffState: "In Review", MergeCompletionReactionConfigured: true,
		MergeCompletionConfig: MergeCompletionReactionConfig{TargetState: "Done", PollIntervalMS: 60000},
	}
	reconcileMergeCompletion(state, params, discardLogger(), context.Background(), &domain.NoopMetrics{})
	if len(tracker.transitionCalls) != 0 {
		t.Errorf("transitions = %+v, want none while the part is open", tracker.transitionCalls)
	}
	if p := state.PendingReactions[key]; p == nil || p.Wait != waitHandoffParts {
		t.Errorf("merge-completion watch = %+v, want it waiting on the hand-off", p)
	}

	merged := &fakeSatisfaction{rows: map[string][]persistence.PlanSatisfaction{"id-9": {{IssueID: "id-9", Kind: persistence.PlanSatisfactionMerged, Ref: "m1"}}}}
	issue := domain.Issue{ID: "id-9", Identifier: "ONS-9", State: "To Do"}
	got := completeMergedCandidates(context.Background(), state, tracker, merged, "Done", []string{"Done"}, []domain.Issue{issue}, discardLogger())
	if len(got) != 0 || len(tracker.transitionCalls) != 0 {
		t.Errorf("merged-to-done = %+v, transitions %+v; want the story left out and not moved", got, tracker.transitionCalls)
	}

	state.HandoffParts[handoffPartKey("id-9", "proof")].Status = HandoffDone
	got = completeMergedCandidates(context.Background(), state, tracker, merged, "Done", []string{"Done"}, []domain.Issue{issue}, discardLogger())
	if len(got) != 1 || got[0].State != "Done" {
		t.Errorf("merged-to-done after the part closed = %+v, want the story moved to Done", got)
	}
}

func TestParseHandoffParts(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		file     string
		wantIDs  []string
		wantKind []string
		problems int
		wantErr  bool
	}{
		{name: "not an object", file: `[]`, wantErr: true},
		{name: "no parts list", file: `{"part": []}`, wantErr: true},
		{name: "an empty list", file: `{"parts": []}`},
		{
			name:     "kinds inferred",
			file:     `{"parts":[{"id":"a","repository":"o/r","need":"n","check":"c"},{"id":"b","capabilities":["kvm"],"need":"n","check":"c"},{"id":"c","need":"n","check":"c"}]}`,
			wantIDs:  []string{"a", "b", "c"},
			wantKind: []string{HandoffKindPullRequest, HandoffKindJob, HandoffKindPerson},
		},
		{
			name:     "a part without what it needs or how to check it is dropped",
			file:     `{"parts":[{"id":"a","need":"n"},{"id":"b","check":"c"},{"id":"ok","need":"n","check":"c"}]}`,
			wantIDs:  []string{"ok"},
			wantKind: []string{HandoffKindPerson},
			problems: 2,
		},
		{
			name:     "an unknown kind, a pull request without a repository, a bad or repeated id",
			file:     `{"parts":[{"kind":"robot","need":"n","check":"c"},{"kind":"pull_request","need":"n","check":"c"},{"id":"Bad id!","need":"n","check":"c"},{"id":"x","need":"n","check":"c"},{"id":"X","need":"m","check":"c"}]}`,
			wantIDs:  []string{"x"},
			wantKind: []string{HandoffKindPerson},
			problems: 4,
		},
		{
			name:     "an id is derived from what the part needs",
			file:     `{"parts":[{"need":"n","check":"c"}]}`,
			wantIDs:  []string{"part-"},
			wantKind: []string{HandoffKindPerson},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			specs, problems, err := parseHandoffParts([]byte(tt.file))
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, want error %v", err, tt.wantErr)
			}
			if len(problems) != tt.problems {
				t.Errorf("problems = %v, want %d", problems, tt.problems)
			}
			if len(specs) != len(tt.wantIDs) {
				t.Fatalf("specs = %+v, want %v", specs, tt.wantIDs)
			}
			for i, s := range specs {
				if !strings.HasPrefix(s.ID, tt.wantIDs[i]) || s.Kind != tt.wantKind[i] {
					t.Errorf("spec %d = %+v, want id %q kind %q", i, s, tt.wantIDs[i], tt.wantKind[i])
				}
			}
		})
	}
}

func TestReadHandoffParts_ReadsTheFileWithItsBounds(t *testing.T) {
	t.Parallel()
	ws := t.TempDir()
	if got := readHandoffParts(ws, discardLogger()); got != nil {
		t.Errorf("no file = %+v, want no part", got)
	}
	if err := os.MkdirAll(filepath.Join(ws, ".sortie"), 0o750); err != nil {
		t.Fatal(err)
	}
	big := `{"parts":[{"need":"` + strings.Repeat("x", handoffPartsMaxBytes) + `","check":"c"}]}`
	if err := os.WriteFile(filepath.Join(ws, ".sortie", handoffPartsFile), []byte(big), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := readHandoffParts(ws, discardLogger()); got != nil {
		t.Errorf("oversized file = %d parts, want none", len(got))
	}
	if err := os.WriteFile(filepath.Join(ws, ".sortie", handoffPartsFile), []byte(`{"parts":[{"need":"a\u0007b","check":"c"}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := readHandoffParts(ws, discardLogger()); len(got) != 1 || got[0].Need != "ab" {
		t.Errorf("parts = %+v, want the control character removed", got)
	}
}

// The worker reads the hand-off the session left in the workspace and
// hands its parts to the exit.
func TestRunWorkerAttempt_HandsOnTheRecordedParts(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	cfg := defaultWorkerConfig(tmpDir)
	cfg.Agent.MaxTurns = 1
	agent := &mockAgentAdapter{runTurnFn: func(_ context.Context, session domain.Session, _ domain.RunTurnParams) (domain.TurnResult, error) {
		dir := filepath.Join(tmpDir, "TEST-1", ".sortie")
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return domain.TurnResult{}, err
		}
		if err := os.WriteFile(filepath.Join(dir, handoffPartsFile), []byte(homelabProof), 0o600); err != nil {
			return domain.TurnResult{}, err
		}
		return domain.TurnResult{SessionID: session.ID, ExitReason: domain.EventTurnCompleted}, nil
	}}
	ec := newExitCapture()
	RunWorkerAttempt(context.Background(), workerTestIssue(), nil, WorkerDeps{
		TrackerAdapter:         &mockTrackerAdapter{},
		AgentAdapter:           agent,
		ConfigFunc:             func() config.ServiceConfig { return cfg },
		PromptTemplateByIDFunc: func(string) *prompt.Template { return mustParseTemplate(t, "{{ .issue.title }}") },
		OnEvent:                func(string, domain.AgentEvent) {},
		OnExit:                 ec.onExit,
		Logger:                 discardLogger(),
	})
	result := ec.waitResult(t)
	if result.ExitKind != WorkerExitNormal || len(result.HandoffParts) != 1 || result.HandoffParts[0].ID != "live-chain" ||
		!slices.Equal(result.HandoffParts[0].Capabilities, []string{"reach:homelab"}) {
		t.Fatalf("result = %+v, parts %+v; want the recorded part", result.ExitKind, result.HandoffParts)
	}
}
