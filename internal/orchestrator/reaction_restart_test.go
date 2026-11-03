package orchestrator

import (
	"context"
	"testing"
	"time"

	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/persistence"
)

// reactionRestartFixture holds what both sides of a simulated restart
// share: the database file, the workspace root the rebuilt watch reads its
// pull request from, and the pull request's conflicted head.
type reactionRestartFixture struct {
	dbPath string
	wsRoot string
	scm    *mergeabilitySCM
}

const (
	restartIssueID    = "ISS-RESTART"
	restartIdentifier = "PROJ-RESTART"
	restartHandoff    = "In Review"
)

func newReactionRestartFixture(t *testing.T) reactionRestartFixture {
	t.Helper()
	f := reactionRestartFixture{
		dbPath: t.TempDir() + "/sortie.db",
		wsRoot: t.TempDir(),
		scm: &mergeabilitySCM{fn: func() (domain.PRMergeStatus, error) {
			return dirtyStatus("head-conflicted", "main"), nil
		}},
	}
	writeRecoverySCM(t, f.wsRoot, restartIdentifier, domain.SCMMetadata{
		Branch:   "feature/restart",
		SHA:      "head-conflicted",
		PushedAt: freshSCMTime(1),
		PRNumber: 31,
		Owner:    "owner",
		Repo:     "repo",
	})
	return f
}

// process is one Sortie process over the fixture's database: its own
// store and its own runtime state.
type restartProcess struct {
	t     *testing.T
	f     reactionRestartFixture
	store *persistence.Store
	state *State

	dispatched []dispatchedWorker
}

// start opens the store and rebuilds the runtime state as startup does:
// the persisted retries, then the pending reactions recovered from run
// history.
func (f reactionRestartFixture) start(t *testing.T) *restartProcess {
	t.Helper()
	ctx := context.Background()
	store, err := persistence.Open(ctx, f.dbPath)
	if err != nil {
		t.Fatalf("persistence.Open: %v", err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	p := &restartProcess{t: t, f: f, store: store, state: NewState(5000, 1, 0, nil, AgentTotals{})}

	retries, err := store.LoadRetryEntriesForRecovery(ctx, mcBaseTime.UnixMilli())
	if err != nil {
		t.Fatalf("LoadRetryEntriesForRecovery: %v", err)
	}
	PopulateRetries(p.state, retries, discardLogger())

	run := freshRun(restartIssueID, restartIdentifier, "owner/repo#31", 1)
	params := defaultRecoveryParams(f.wsRoot, &recoveryTrackerStub{states: map[string]string{restartIssueID: restartHandoff}})
	params.HandoffState = restartHandoff
	params.MergeConflictReactionConfigured = true
	if _, err := RecoverPendingReactions(ctx, p.state, []persistence.RunHistory{run}, params); err != nil {
		t.Fatalf("RecoverPendingReactions: %v", err)
	}
	if _, ok := p.state.PendingReactions[ReactionKey(restartIssueID, ReactionKindMergeConflict)]; !ok {
		t.Fatal("startup rebuilt no merge-conflict watch; want one")
	}
	return p
}

// stop ends the process. Its runtime state, the retry queue included, is
// lost with it.
func (p *restartProcess) stop() {
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

// reconcile runs one merge-conflict pass.
func (p *restartProcess) reconcile() {
	p.t.Helper()
	params := mergeConflictParams(p.store, p.f.scm, nil)
	params.MergeConflictConfig.MaxRetries = 3
	reconcileMergeConflicts(p.state, params, discardLogger(), context.Background(), &domain.NoopMetrics{})
}

// fireRetry fires the issue's retry timer now.
func (p *restartProcess) fireRetry() {
	p.t.Helper()
	entry, ok := p.state.RetryAttempts[restartIssueID]
	if !ok {
		p.t.Fatal("no retry to fire; want the merge-conflict continuation queued")
	}
	if entry.TimerHandle != nil {
		entry.TimerHandle.Stop()
	}
	entry.scheduledAt = time.Time{}
	tracker := &mockRetryTracker{fetchedIssue: candidateIssue(restartIssueID, restartIdentifier, restartHandoff)}
	params := capturingRetryParams(p.t, nil, tracker, &p.dispatched)
	params.Store = p.store
	params.HandoffState = restartHandoff
	HandleRetryTimer(p.state, restartIssueID, params)
}

// occupySlots fills every agent slot with another issue's run.
func (p *restartProcess) occupySlots() {
	p.state.Running["OTHER"] = &RunningEntry{Identifier: "OTHER", Issue: domain.Issue{ID: "OTHER", State: "In Progress"}}
}

// finishRun ends the issue's run as a worker exit would.
func (p *restartProcess) finishRun() {
	delete(p.state.Running, restartIssueID)
	delete(p.state.Claimed, restartIssueID)
}

func (p *restartProcess) mergeConflictDispatches() int {
	n := 0
	for _, w := range p.dispatched {
		if w.reactionKind == ReactionKindMergeConflict {
			n++
		}
	}
	return n
}

// A merge-conflict continuation that was queued for a busy slot when Sortie
// stopped runs after the restart, once a slot frees.
func TestReactionContinuation_QueuedForASlot_SurvivesRestart(t *testing.T) {
	t.Parallel()

	f := newReactionRestartFixture(t)

	before := f.start(t)
	before.occupySlots()
	before.reconcile()
	before.fireRetry()
	if n := before.mergeConflictDispatches(); n != 0 {
		t.Fatalf("dispatches with every slot busy = %d, want 0", n)
	}
	if _, queued := before.state.RetryAttempts[restartIssueID]; !queued {
		t.Fatal("continuation left the retry queue with every slot busy; want it rescheduled")
	}
	before.stop()

	after := f.start(t)
	after.reconcile()
	if _, queued := after.state.RetryAttempts[restartIssueID]; !queued {
		t.Fatal("after the restart the watch queued no continuation for the conflicted head the lost run was for; want one")
	}
	after.fireRetry()
	if n := after.mergeConflictDispatches(); n != 1 {
		t.Errorf("merge-conflict dispatches after the restart = %d, want 1", n)
	}
	after.stop()
}

// A merge-conflict continuation that ran to completion before Sortie
// stopped is not run again for the same head after the restart.
func TestReactionContinuation_Completed_NotRepeatedAfterRestart(t *testing.T) {
	t.Parallel()

	f := newReactionRestartFixture(t)

	before := f.start(t)
	before.reconcile()
	before.fireRetry()
	if n := before.mergeConflictDispatches(); n != 1 {
		t.Fatalf("dispatches with a free slot = %d, want 1", n)
	}
	before.finishRun()
	before.stop()

	after := f.start(t)
	after.reconcile()
	if entry, queued := after.state.RetryAttempts[restartIssueID]; queued {
		t.Errorf("after the restart the watch queued %q for the head its completed run was for; want none", entry.ReactionKind)
	}
	if _, watching := after.state.PendingReactions[ReactionKey(restartIssueID, ReactionKindMergeConflict)]; !watching {
		t.Error("watch dropped after the restart; want it kept for a later head")
	}
	after.stop()
}
