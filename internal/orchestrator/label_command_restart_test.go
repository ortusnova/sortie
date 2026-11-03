package orchestrator

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/persistence"
)

// labelJournalSCM is a label-event journal that records Sortie's own label
// removal as an unlabeled event, as the platform does.
type labelJournalSCM struct {
	labelReviewSCMFake
}

func (s *labelJournalSCM) RemoveLabel(ctx context.Context, prNumber int, owner, repo, label string) error {
	if err := s.labelReviewSCMFake.RemoveLabel(ctx, prNumber, owner, repo, label); err != nil {
		return err
	}
	s.events = append(s.events, labelEvent("sortie-remove-"+label, label, "sortie", false, labelReviewBaseTime.Add(-10*time.Second)))
	return nil
}

func (s *labelJournalSCM) labelPresent(label string) bool {
	return labelPresent(s.events, label)
}

// labelCommandKind is one label command reaction under test.
type labelCommandKind struct {
	kind      string
	label     string
	configure func(*PendingReactionRecoveryParams)
	reconcile func(*State, *persistence.Store, domain.SCMAdapter)
}

func labelCommandKinds() []labelCommandKind {
	return []labelCommandKind{
		{
			kind:      ReactionKindLabelReview,
			label:     defaultLabelReviewConfig().ReviewLabel,
			configure: func(p *PendingReactionRecoveryParams) { p.LabelReviewReactionConfigured = true },
			reconcile: func(state *State, store *persistence.Store, scm domain.SCMAdapter) {
				reconcileLabelReviewCommands(state, labelReviewParams(store, scm), discardLogger(), context.Background(), &domain.NoopMetrics{})
			},
		},
		{
			kind:      ReactionKindLabelFix,
			label:     defaultLabelFixConfig().FixLabel,
			configure: func(p *PendingReactionRecoveryParams) { p.LabelFixReactionConfigured = true },
			reconcile: func(state *State, store *persistence.Store, scm domain.SCMAdapter) {
				reconcileLabelFixCommands(state, labelFixParams(store, scm), discardLogger(), context.Background(), &domain.NoopMetrics{})
			},
		},
	}
}

// labelCommandProcess is one Sortie process over a shared database file,
// with its own store and runtime state, watching one label command kind.
type labelCommandProcess struct {
	t     *testing.T
	k     labelCommandKind
	scm   *labelJournalSCM
	store *persistence.Store
	state *State

	dispatched []dispatchedWorker
}

// startLabelCommandProcess opens the store and rebuilds the runtime state as
// startup does: the persisted retries, then the pending reactions recovered
// from run history.
func startLabelCommandProcess(t *testing.T, k labelCommandKind, dbPath, wsRoot string, scm *labelJournalSCM) *labelCommandProcess {
	t.Helper()
	ctx := context.Background()
	store, err := persistence.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("persistence.Open: %v", err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	p := &labelCommandProcess{t: t, k: k, scm: scm, store: store, state: NewState(5000, 1, 0, nil, AgentTotals{})}

	retries, err := store.LoadRetryEntriesForRecovery(ctx, labelReviewBaseTime.UnixMilli())
	if err != nil {
		t.Fatalf("LoadRetryEntriesForRecovery: %v", err)
	}
	PopulateRetries(p.state, retries, discardLogger())

	run := freshRun(restartIssueID, restartIdentifier, "owner/repo#31", 1)
	params := defaultRecoveryParams(wsRoot, &recoveryTrackerStub{states: map[string]string{restartIssueID: restartHandoff}})
	params.HandoffState = restartHandoff
	params.ReviewReactionConfigured = false
	k.configure(&params)
	if _, err := RecoverPendingReactions(ctx, p.state, []persistence.RunHistory{run}, params); err != nil {
		t.Fatalf("RecoverPendingReactions: %v", err)
	}
	if _, ok := p.state.PendingReactions[ReactionKey(restartIssueID, k.kind)]; !ok {
		t.Fatalf("startup rebuilt no %s watch; want one", k.kind)
	}
	return p
}

func (p *labelCommandProcess) stop() {
	p.t.Helper()
	for _, entry := range p.state.RetryAttempts {
		if entry.TimerHandle != nil {
			entry.TimerHandle.Stop()
		}
	}
	if err := p.store.Close(); err != nil {
		p.t.Fatalf("store.Close: %v", err)
	}
}

// reconcile runs one due pass of the kind's reaction.
func (p *labelCommandProcess) reconcile() {
	p.t.Helper()
	if pending, ok := p.state.PendingReactions[ReactionKey(restartIssueID, p.k.kind)]; ok {
		pending.PendingRetryAt = time.Time{}
	}
	p.k.reconcile(p.state, p.store, p.scm)
}

// fireRetry fires the issue's retry timer now.
func (p *labelCommandProcess) fireRetry() {
	p.t.Helper()
	entry, ok := p.state.RetryAttempts[restartIssueID]
	if !ok {
		p.t.Fatalf("no retry to fire; want the %s continuation queued", p.k.kind)
	}
	if entry.TimerHandle != nil {
		entry.TimerHandle.Stop()
	}
	entry.scheduledAt = time.Time{}
	tracker := &mockRetryTracker{fetchedIssue: candidateIssue(restartIssueID, restartIdentifier, restartHandoff)}
	params := capturingRetryParams(p.t, nil, tracker, &p.dispatched)
	params.Store = p.store
	params.SCMAdapter = p.scm
	params.HandoffState = restartHandoff
	HandleRetryTimer(p.state, restartIssueID, params)
}

func (p *labelCommandProcess) occupySlots() {
	p.state.Running["OTHER"] = &RunningEntry{Identifier: "OTHER", Issue: domain.Issue{ID: "OTHER", State: "In Progress"}}
}

func (p *labelCommandProcess) finishRun() {
	delete(p.state.Running, restartIssueID)
	delete(p.state.Claimed, restartIssueID)
}

func (p *labelCommandProcess) commandDispatches() int {
	n := 0
	for _, w := range p.dispatched {
		if w.reactionKind == p.k.kind {
			n++
		}
	}
	return n
}

func newLabelCommandFixture(t *testing.T, k labelCommandKind) (dbPath, wsRoot string, scm *labelJournalSCM) {
	t.Helper()
	dbPath = t.TempDir() + "/sortie.db"
	wsRoot = t.TempDir()
	writeRecoverySCM(t, wsRoot, restartIdentifier, domain.SCMMetadata{
		Branch:   "feature/restart",
		SHA:      "head-1",
		PushedAt: freshSCMTime(1),
		PRNumber: 31,
		Owner:    "owner",
		Repo:     "repo",
	})
	scm = &labelJournalSCM{labelReviewSCMFake{events: []domain.LabelEvent{
		labelEvent("1", k.label, "alice", true, labelReviewBaseTime.Add(-1*time.Minute)),
	}}}
	return dbPath, wsRoot, scm
}

// A label command whose run was queued for a busy slot when Sortie stopped
// runs after the restart, once a slot frees. The command label stays on the
// pull request until the run starts.
func TestLabelCommand_QueuedForASlot_SurvivesRestart(t *testing.T) {
	t.Parallel()

	for _, k := range labelCommandKinds() {
		t.Run(k.kind, func(t *testing.T) {
			t.Parallel()

			dbPath, wsRoot, scm := newLabelCommandFixture(t, k)

			before := startLabelCommandProcess(t, k, dbPath, wsRoot, scm)
			before.occupySlots()
			before.reconcile()
			before.fireRetry()
			if n := before.commandDispatches(); n != 0 {
				t.Fatalf("dispatches with every slot busy = %d, want 0", n)
			}
			if _, queued := before.state.RetryAttempts[restartIssueID]; !queued {
				t.Fatal("command run left the retry queue with every slot busy; want it rescheduled")
			}
			// A later pass while the run waits neither queues a second run
			// nor records the command handled.
			before.reconcile()
			if !scm.labelPresent(k.label) {
				t.Fatal("command label removed while its run is only queued; want it kept until the run starts")
			}
			mark, _, err := before.store.GetReactionFingerprint(context.Background(), restartIssueID, k.kind)
			if err != nil {
				t.Fatalf("GetReactionFingerprint: %v", err)
			}
			if mark != "" {
				t.Fatalf("command recorded handled (mark %q) while its run is only queued; want no mark", mark)
			}
			before.stop()

			after := startLabelCommandProcess(t, k, dbPath, wsRoot, scm)
			after.reconcile()
			if _, queued := after.state.RetryAttempts[restartIssueID]; !queued {
				t.Fatal("after the restart the watch queued no run for the command the lost run was for; want one")
			}
			if !scm.labelPresent(k.label) {
				t.Fatal("command label removed before the run started; want it kept")
			}
			after.fireRetry()
			if n := after.commandDispatches(); n != 1 {
				t.Fatalf("command dispatches after the restart = %d, want 1", n)
			}
			if scm.labelPresent(k.label) {
				t.Error("command label still present after the run started; want it removed")
			}
			if scm.removeCalls != 1 {
				t.Errorf("RemoveLabel calls = %d, want 1", scm.removeCalls)
			}

			after.finishRun()
			after.reconcile()
			if entry, queued := after.state.RetryAttempts[restartIssueID]; queued {
				t.Errorf("the watch queued %q again for the command its started run handled; want none", entry.ReactionKind)
			}
			after.stop()
		})
	}
}

// A label command whose run started before Sortie stopped is not run again
// after the restart.
func TestLabelCommand_Started_NotRepeatedAfterRestart(t *testing.T) {
	t.Parallel()

	for _, k := range labelCommandKinds() {
		t.Run(k.kind, func(t *testing.T) {
			t.Parallel()

			dbPath, wsRoot, scm := newLabelCommandFixture(t, k)

			before := startLabelCommandProcess(t, k, dbPath, wsRoot, scm)
			before.reconcile()
			before.fireRetry()
			if n := before.commandDispatches(); n != 1 {
				t.Fatalf("dispatches with a free slot = %d, want 1", n)
			}
			mark, _, err := before.store.GetReactionFingerprint(context.Background(), restartIssueID, k.kind)
			if err != nil {
				t.Fatalf("GetReactionFingerprint: %v", err)
			}
			if want := labelReviewMark(scm.events[0]); mark != want {
				t.Fatalf("mark after the run started = %q, want %q", mark, want)
			}
			before.stop()

			// Even when the label removal never reached the platform, the
			// mark alone keeps the command from running again.
			scm.events = scm.events[:1]

			after := startLabelCommandProcess(t, k, dbPath, wsRoot, scm)
			after.reconcile()
			if entry, queued := after.state.RetryAttempts[restartIssueID]; queued {
				t.Errorf("after the restart the watch queued %q for the command its started run handled; want none", entry.ReactionKind)
			}
			if _, watching := after.state.PendingReactions[ReactionKey(restartIssueID, k.kind)]; !watching {
				t.Error("watch dropped after the restart; want it kept for a later command")
			}
			after.stop()
		})
	}
}

// openLabelCommandStore opens a migrated SQLite store in a temporary
// directory, closed when the test ends.
func openLabelCommandStore(t *testing.T) *persistence.Store {
	t.Helper()
	ctx := context.Background()
	store, err := persistence.Open(ctx, t.TempDir()+"/sortie.db")
	if err != nil {
		t.Fatalf("persistence.Open: %v", err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

// startLabelCommandRun fires the issue's queued label command retry with a
// free slot, so its run starts, and returns the reaction kinds dispatched.
func startLabelCommandRun(t *testing.T, state *State, issueID string, store RetryTimerStore, scm domain.SCMAdapter, log *slog.Logger) []string {
	t.Helper()
	entry, ok := state.RetryAttempts[issueID]
	if !ok {
		t.Fatal("no retry to fire; want the label command queued")
	}
	if entry.TimerHandle != nil {
		entry.TimerHandle.Stop()
	}
	entry.scheduledAt = time.Time{}
	var dispatched []dispatchedWorker
	tracker := &mockRetryTracker{fetchedIssue: candidateIssue(issueID, entry.Identifier, restartHandoff)}
	params := capturingRetryParams(t, nil, tracker, &dispatched)
	params.Store = store
	params.SCMAdapter = scm
	params.HandoffState = restartHandoff
	if log != nil {
		params.Logger = log
	}
	HandleRetryTimer(state, issueID, params)
	kinds := make([]string, 0, len(dispatched))
	for _, w := range dispatched {
		kinds = append(kinds, w.reactionKind)
	}
	return kinds
}
