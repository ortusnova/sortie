package orchestrator

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/persistence"
)

// parkRecordHead is the head of the parked story's pull request.
const parkRecordHead = "4d2c1aa9e1f0b7c3d5e6f7a8b9c0d1e2f3a4b5c6"

// recordTracker is a fake tracker holding the parked story ONS-39: its
// labels, its state, and the moves Sortie makes.
type recordTracker struct {
	*supervisorTracker
	mu          sync.Mutex
	state       string
	transitions []string
}

func (r *recordTracker) FetchIssueByID(ctx context.Context, id string) (domain.Issue, error) {
	issue, err := r.supervisorTracker.FetchIssueByID(ctx, id)
	r.mu.Lock()
	defer r.mu.Unlock()
	issue.Identifier, issue.State = "ONS-39", r.state
	return issue, err
}

func (r *recordTracker) TransitionIssue(_ context.Context, _ string, target string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.transitions = append(r.transitions, target)
	r.state = target
	return nil
}

func (r *recordTracker) CommentIssue(context.Context, string, string) error { return nil }

func (r *recordTracker) moves() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.transitions)
}

// parkRecordFixture is a harness with ONS-39 parked after its criteria
// review, its pull request o/homelab#155 open at parkRecordHead with the
// given comments, and the auto-merge, CI, merge-conflict and
// merge-completion reactions configured.
type parkRecordFixture struct {
	*supervisorHarness
	tracker  *recordTracker
	comments *commentForge
	ci       *mockCIProvider
}

func newParkRecordFixture(t *testing.T, state *State, labels []string, outcome string, comments ...domain.ReviewComment) *parkRecordFixture {
	t.Helper()
	if state == nil {
		state = stateWithParked("id-39", "ONS-39")
		park := state.Parked["id-39"]
		park.RuleName, park.AgentKind, park.TemplateID = "check", "agent-client-protocol", "review-template"
		park.Statement = "The click-proofs on the preview could not record their result."
	}
	h := parkedStoryHarness(t, supervisorFinder{"o/homelab:sortie/ONS-39": {Number: 155}})
	h.o.state = state
	cfg := h.o.workflowManager.Config()
	cfg.Tracker.HandoffState = "In Review"
	cfg.Tracker.ActiveStates = []string{"Todo"}
	cfg.Tracker.TerminalStates = []string{"Done", "Canceled"}
	h.o.workflowManager = &stubWorkflowManager{config: cfg}
	h.tracker.labels = append([]string{config.RepositoryLabelPrefix + "homelab"}, labels...)
	f := &parkRecordFixture{
		supervisorHarness: h,
		tracker:           &recordTracker{supervisorTracker: h.tracker, state: "Todo"},
		comments:          &commentForge{conversation: comments},
		ci:                &mockCIProvider{result: domain.CIResult{Status: domain.CIStatusPassing}},
	}
	h.o.trackerAdapter = f.tracker
	h.o.prCommentReader = f.comments
	h.o.ciProvider = f.ci
	h.o.autoMergeReactionConfigured = true
	h.o.autoMergeConfig = defaultAutoMergeConfig()
	h.o.mergeConflictReactionConfigured = true
	h.o.mergeConflictConfig = defaultMergeConflictConfig()
	h.o.mergeCompletionReactionConfigured = true
	if outcome != "" {
		if err := h.store.UpsertCriteriaVerdict(t.Context(), persistence.CriteriaVerdict{IssueID: "id-39", Outcome: outcome, RecordedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	return f
}

// release runs the park record pass on the event loop at now.
func (f *parkRecordFixture) release(t *testing.T, now time.Time) {
	t.Helper()
	onLoop(t, f.o, func() { f.o.releaseParksByRecord(t.Context(), f.o.workflowManager.Config(), now) })
}

// reconcileParams is what the poll tick hands the reaction passes.
func (f *parkRecordFixture) reconcileParams(now time.Time) ReconcileParams {
	cfg := f.o.workflowManager.Config()
	return ReconcileParams{
		ReviewGate:                      planReviewGate(cfg, f.o.trackerAdapter, f.store),
		ClickProofs:                     planClickProofs(cfg, f.o.prCommentReader),
		TrackerAdapter:                  f.o.trackerAdapter,
		ActiveStates:                    cfg.Tracker.ActiveStates,
		TerminalStates:                  cfg.Tracker.TerminalStates,
		HandoffState:                    cfg.Tracker.HandoffState,
		Store:                           f.store,
		OnRetryFire:                     noopRetryFire,
		NowFunc:                         func() time.Time { return now },
		CIProvider:                      f.ci,
		CIFeedback:                      defaultCIFeedback(),
		SCMAdapter:                      f.forge,
		AutoMergeConfig:                 f.o.autoMergeConfig,
		AutoMergeReactionConfigured:     true,
		MergeConflictConfig:             f.o.mergeConflictConfig,
		MergeConflictReactionConfigured: true,
	}
}

// autoMerge runs one auto-merge pass on the event loop.
func (f *parkRecordFixture) autoMerge(t *testing.T, now time.Time) {
	t.Helper()
	params := f.reconcileParams(now)
	onLoop(t, f.o, func() {
		reconcileAutoMerge(f.o.state, params, discardLogger(), t.Context(), newAutoMergeMetricsSpy())
	})
	f.o.state.TrackerOpsWg.Wait()
}

// view is what the event loop holds of ONS-39.
type parkRecordView struct {
	park, release *ParkedEntry
	retry         *RetryEntry
	running       bool
	pending       []string
	snapshot      RuntimeSnapshotResult
}

func (f *parkRecordFixture) view(t *testing.T) parkRecordView {
	t.Helper()
	var v parkRecordView
	onLoop(t, f.o, func() {
		s := f.o.state
		v.park, v.release, v.retry = s.Parked["id-39"], s.ParkReleases["id-39"], s.RetryAttempts["id-39"]
		_, v.running = s.Running["id-39"]
		for _, p := range s.PendingReactions {
			v.pending = append(v.pending, p.Kind)
		}
		slices.Sort(v.pending)
		v.snapshot = RuntimeSnapshot(s, time.Now())
	})
	return v
}

func recordComment(id, marker, sha string) domain.ReviewComment {
	return domain.ReviewComment{ID: id, Reviewer: "ortus-kit", Body: "Result on the preview: all pass.\n\n`" + marker + " " + sha + "`\n"}
}

var parkRecordNow = time.Date(2026, 11, 4, 9, 0, 0, 0, time.UTC)

// Scenario "A record lands on a parked story": a parked ui story whose
// verdict is met and whose pull request records the click-proofs passed at
// its head is released with no session, and Sortie merges it once CI
// passes and it does not conflict.
func TestParkRecord_UIStoryClickProofsAtHeadMerges(t *testing.T) {
	t.Parallel()
	f := newParkRecordFixture(t, nil, []string{config.ReviewUILabel}, CriteriaOutcomeMet,
		domain.ReviewComment{ID: "c-0", Reviewer: "ana", Body: "Preview: https://pr-155.preview.example"},
		recordComment("c-7", ClickProofsMarker, parkRecordHead[:12]))

	f.release(t, parkRecordNow)

	v := f.view(t)
	if v.park != nil {
		t.Fatal("the story is still parked after its record landed")
	}
	if v.retry != nil || v.running {
		t.Fatalf("retry %+v, running %v; want no session started", v.retry, v.running)
	}
	if v.release == nil || v.release.ReleaseGesture != unparkTriggerRecord || v.release.Record == nil {
		t.Fatalf("release = %+v, want the record as the release", v.release)
	}
	want := ParkRecord{PullRequest: "o/homelab#155", Head: parkRecordHead, Comments: []string{"c-7"}}
	if r := v.release.Record; r.PullRequest != want.PullRequest || r.Head != want.Head || !slices.Equal(r.Comments, want.Comments) {
		t.Errorf("record = %+v, want %+v", *r, want)
	}
	if got := f.tracker.moves(); !slices.Equal(got, []string{"In Review"}) {
		t.Errorf("tracker moves = %v, want the handoff state", got)
	}
	if want := []string{ReactionKindCI, ReactionKindAutoMerge, ReactionKindMergeCompletion, ReactionKindMergeConflict}; !slices.Equal(v.pending, want) {
		t.Errorf("watches = %v, want %v", v.pending, want)
	}
	var seeded *AutoMergeReactionData
	onLoop(t, f.o, func() {
		seeded, _ = f.o.state.PendingReactions[ReactionKey("id-39", ReactionKindAutoMerge)].KindData.(*AutoMergeReactionData)
	})
	if seeded == nil || seeded.SHA != parkRecordHead || seeded.PRNumber != 155 || seeded.Branch != "sortie/ONS-39" {
		t.Errorf("auto-merge entry = %+v, want o/homelab#155 at the head", seeded)
	}
	rows := v.snapshot.ParkReleases
	if len(rows) != 1 || rows[0].Gesture != "record" || rows[0].PullRequest != "o/homelab#155" || rows[0].Head != parkRecordHead || !slices.Equal(rows[0].Comments, []string{"c-7"}) {
		t.Errorf("snapshot park releases = %+v, want the record's release", rows)
	}
	outcomes, err := f.store.IssueOutcomes(t.Context(), []string{"id-39"})
	if o := outcomes["id-39"]; err != nil || o.Kind != persistence.IssueOutcomeHandedOff || o.PRNumber != 155 || o.PRRepository != "o/homelab" || o.ReleaseHead != parkRecordHead || o.ReleaseComment != "c-7" {
		t.Errorf("outcome = %+v, %v, want the handoff naming the pull request, head and comment", o, err)
	}
	parks, err := f.store.ListParkedIssues(t.Context())
	if err != nil || len(parks) != 1 || parks[0].ReleaseGesture != "record" || parks[0].ReleasePullRequest != "o/homelab#155" || parks[0].ReleaseHead != parkRecordHead || parks[0].ReleaseComment != "c-7" {
		t.Errorf("park rows = %+v, %v, want the release kept with the record", parks, err)
	}

	f.autoMerge(t, parkRecordNow.Add(time.Second))

	if !slices.Equal(f.forge.merges, []string{parkRecordHead}) {
		t.Fatalf("merges = %v, want one at the head", f.forge.merges)
	}
	outcomes, _ = f.store.IssueOutcomes(t.Context(), []string{"id-39"})
	if o := outcomes["id-39"]; o.Kind != persistence.IssueOutcomeMerged || o.ReleaseHead != parkRecordHead || o.ReleaseComment != "c-7" {
		t.Errorf("outcome after the merge = %+v, want merged, still naming the record", o)
	}
	if v := f.view(t); v.retry != nil || v.running {
		t.Errorf("retry %+v, running %v after the merge; want no session", v.retry, v.running)
	}
}

// A parked story whose verdict leaves a criterion to a live proof is
// released by the live-proof record at its head, and the review gate lets
// the live-proof verdict merge with that record.
func TestParkRecord_LiveProofAtHeadMerges(t *testing.T) {
	t.Parallel()
	f := newParkRecordFixture(t, nil, []string{config.ReviewCriteriaLabel}, CriteriaOutcomeLiveProof,
		recordComment("lp-1", LiveProofMarker, parkRecordHead))

	f.release(t, parkRecordNow)
	v := f.view(t)
	if v.park != nil || v.release == nil || v.release.Record == nil || !slices.Equal(v.release.Record.Comments, []string{"lp-1"}) {
		t.Fatalf("park %+v, release %+v; want it released by the live-proof record", v.park, v.release)
	}
	f.autoMerge(t, parkRecordNow.Add(time.Second))
	if !slices.Equal(f.forge.merges, []string{parkRecordHead}) {
		t.Fatalf("merges = %v, want one at the head", f.forge.merges)
	}
}

// A ui story whose verdict leaves a live proof needs both records at the
// head.
func TestParkRecord_UIStoryWithALiveProofNeedsBothRecords(t *testing.T) {
	t.Parallel()
	f := newParkRecordFixture(t, nil, []string{config.ReviewUILabel}, CriteriaOutcomeLiveProof,
		recordComment("c-7", ClickProofsMarker, parkRecordHead))
	f.release(t, parkRecordNow)
	if v := f.view(t); v.park == nil {
		t.Fatal("released by the click-proofs alone, want the live proof too")
	}

	f.comments.conversation = append(f.comments.conversation, recordComment("lp-1", LiveProofMarker, parkRecordHead[:7]))
	f.release(t, parkRecordNow.Add(time.Minute))
	v := f.view(t)
	if v.park != nil || v.release == nil || !slices.Equal(v.release.Record.Comments, []string{"c-7", "lp-1"}) {
		t.Fatalf("park %+v, release %+v; want it released naming both comments", v.park, v.release)
	}
	f.autoMerge(t, parkRecordNow.Add(time.Minute+time.Second))
	if len(f.forge.merges) != 1 {
		t.Errorf("merges = %v, want one", f.forge.merges)
	}
}

// A record does nothing when it names an older head, is the wrong kind, or
// the verdict is refused or missing: the park holds with its question, and
// nothing is merged or watched.
func TestParkRecord_RecordDoesNothing(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name     string
		labels   []string
		outcome  string
		comments []domain.ReviewComment
	}{
		{"the record names an older head", []string{config.ReviewUILabel}, CriteriaOutcomeMet,
			[]domain.ReviewComment{recordComment("c-1", ClickProofsMarker, "0123456789abcdef")}},
		{"a live-proof record on a ui story whose verdict is met", []string{config.ReviewUILabel}, CriteriaOutcomeMet,
			[]domain.ReviewComment{recordComment("c-1", LiveProofMarker, parkRecordHead)}},
		{"a click-proofs record on a story whose verdict is live-proof", []string{config.ReviewCriteriaLabel}, CriteriaOutcomeLiveProof,
			[]domain.ReviewComment{recordComment("c-1", ClickProofsMarker, parkRecordHead)}},
		{"a refused verdict", []string{config.ReviewUILabel}, CriteriaOutcomeRefused,
			[]domain.ReviewComment{recordComment("c-1", ClickProofsMarker, parkRecordHead)}},
		{"no criteria verdict", []string{config.ReviewUILabel}, "",
			[]domain.ReviewComment{recordComment("c-1", ClickProofsMarker, parkRecordHead)}},
		{"a story that is not a ui story and whose verdict is met", []string{config.ReviewCriteriaLabel}, CriteriaOutcomeMet,
			[]domain.ReviewComment{recordComment("c-1", ClickProofsMarker, parkRecordHead), recordComment("c-2", LiveProofMarker, parkRecordHead)}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f := newParkRecordFixture(t, nil, tt.labels, tt.outcome, tt.comments...)
			f.release(t, parkRecordNow)
			f.autoMerge(t, parkRecordNow.Add(time.Second))
			v := f.view(t)
			if v.park == nil || v.release != nil {
				t.Fatalf("park %+v, release %+v; want the park held", v.park, v.release)
			}
			if v.park.Statement != "The click-proofs on the preview could not record their result." {
				t.Errorf("park statement = %q, want the question unchanged", v.park.Statement)
			}
			if len(v.pending) != 0 || len(f.forge.merges) != 0 || len(f.tracker.moves()) != 0 {
				t.Errorf("watches %v, merges %v, moves %v; want none", v.pending, f.forge.merges, f.tracker.moves())
			}
		})
	}
}

// Failing CI keeps the merge back and starts the ci_failure reaction.
func TestParkRecord_FailingCIStartsItsReaction(t *testing.T) {
	t.Parallel()
	f := newParkRecordFixture(t, nil, []string{config.ReviewUILabel}, CriteriaOutcomeMet,
		recordComment("c-7", ClickProofsMarker, parkRecordHead))
	f.forge.ci = "failure"
	f.ci.result = domain.CIResult{Status: domain.CIStatusFailing, FailingCount: 1, CheckRuns: []domain.CheckRun{
		{Name: "lint", Status: domain.CheckRunStatusCompleted, Conclusion: domain.CheckConclusionFailure},
	}}
	f.release(t, parkRecordNow)

	params := f.reconcileParams(parkRecordNow.Add(time.Second))
	onLoop(t, f.o, func() {
		reconcileCIStatus(f.o.state, params, discardLogger(), t.Context(), newCIMetricsSpy())
		reconcileAutoMerge(f.o.state, params, discardLogger(), t.Context(), newAutoMergeMetricsSpy())
	})
	f.o.state.TrackerOpsWg.Wait()

	if len(f.forge.merges) != 0 {
		t.Errorf("merges = %v with failing CI, want none", f.forge.merges)
	}
	v := f.view(t)
	if v.retry == nil || v.retry.ReactionKind != ReactionKindCI {
		t.Errorf("retry = %+v, want the ci_failure fix run", v.retry)
	}
	if !slices.Contains(v.pending, ReactionKindAutoMerge) {
		t.Errorf("watches = %v, want auto-merge still waiting", v.pending)
	}
}

// A conflict keeps the merge back and starts the merge_conflicts reaction.
func TestParkRecord_ConflictStartsItsReaction(t *testing.T) {
	t.Parallel()
	f := newParkRecordFixture(t, nil, []string{config.ReviewUILabel}, CriteriaOutcomeMet,
		recordComment("c-7", ClickProofsMarker, parkRecordHead))
	f.release(t, parkRecordNow)
	f.forge.status.Mergeability = domain.MergeabilityDirty
	f.forge.status.BaseBranch = "master"

	params := f.reconcileParams(parkRecordNow.Add(time.Second))
	onLoop(t, f.o, func() {
		reconcileMergeConflicts(f.o.state, params, discardLogger(), t.Context(), newMergeConflictMetricsSpy())
		reconcileAutoMerge(f.o.state, params, discardLogger(), t.Context(), newAutoMergeMetricsSpy())
	})
	f.o.state.TrackerOpsWg.Wait()

	if len(f.forge.merges) != 0 {
		t.Errorf("merges = %v with a conflict, want none", f.forge.merges)
	}
	if v := f.view(t); v.retry == nil || v.retry.ReactionKind != ReactionKindMergeConflict {
		t.Errorf("retry = %+v, want the merge_conflicts fix run", v.retry)
	}
}

// A restart while the story is parked still releases it once the record
// lands, and a restart after the release keeps the release and its record.
func TestParkRecord_RestartWhileParked(t *testing.T) {
	t.Parallel()
	before := newParkRecordFixture(t, nil, []string{config.ReviewUILabel}, CriteriaOutcomeMet)
	if err := before.store.UpsertParkedIssue(t.Context(), parkedIssueRow("id-39", before.o.state.Parked["id-39"])); err != nil {
		t.Fatal(err)
	}
	before.release(t, parkRecordNow)
	if v := before.view(t); v.park == nil {
		t.Fatal("released with no record")
	}

	restarted := func(store *persistence.Store) *State {
		t.Helper()
		rows, err := store.ListParkedIssues(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		state := NewState(5000, 4, 0, nil, AgentTotals{})
		PopulateParked(state, rows, discardLogger())
		return state
	}
	f := newParkRecordFixture(t, restarted(before.store), []string{config.ReviewUILabel}, "",
		recordComment("c-7", ClickProofsMarker, parkRecordHead))
	f.store = before.store
	f.o.store = before.store

	f.release(t, parkRecordNow.Add(10*time.Second))
	v := f.view(t)
	if v.park != nil || v.release == nil || v.retry != nil {
		t.Fatalf("park %+v, release %+v, retry %+v; want it released by the record with no session", v.park, v.release, v.retry)
	}
	f.autoMerge(t, parkRecordNow.Add(11*time.Second))
	if len(f.forge.merges) != 1 {
		t.Errorf("merges = %v, want one", f.forge.merges)
	}

	again := restarted(before.store)
	if r := again.ParkReleases["id-39"]; r == nil || r.ReleaseGesture != "record" || r.Record == nil || r.Record.Head != parkRecordHead || !slices.Equal(r.Record.Comments, []string{"c-7"}) {
		t.Errorf("release after a restart = %+v, want the record kept", r)
	}
}

// Sortie reads a parked story's pull request once per auto-merge poll
// interval, so a record that lands after a read is seen on the next one.
func TestParkRecord_ReadsOncePerPollInterval(t *testing.T) {
	t.Parallel()
	f := newParkRecordFixture(t, nil, []string{config.ReviewUILabel}, CriteriaOutcomeMet)
	f.release(t, parkRecordNow)
	if f.comments.commentCalls != 1 {
		t.Fatalf("comment reads = %d, want 1", f.comments.commentCalls)
	}
	f.comments.conversation = []domain.ReviewComment{recordComment("c-7", ClickProofsMarker, parkRecordHead)}
	f.release(t, parkRecordNow.Add(10*time.Second))
	if f.comments.commentCalls != 1 {
		t.Errorf("comment reads = %d within the poll interval, want 1", f.comments.commentCalls)
	}
	if v := f.view(t); v.park == nil {
		t.Fatal("released before the next read")
	}
	interval := time.Duration(f.o.autoMergeConfig.PollIntervalMS) * time.Millisecond
	f.release(t, parkRecordNow.Add(interval))
	if v := f.view(t); v.park != nil || v.release == nil {
		t.Errorf("park %+v, release %+v one poll interval later; want it released", v.park, v.release)
	}
}
