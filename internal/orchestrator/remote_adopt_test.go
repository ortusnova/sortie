package orchestrator

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/sortie-ai/sortie/internal/domain"
)

// A stored entry comes back as the run had it, its continuation context
// with the shapes the reaction templates and reconcile passes read, and
// never with a CI log excerpt.
func TestRemoteRunEntryRoundTrip(t *testing.T) {
	attempt := 2
	entry := &RunningEntry{
		Identifier: "ONS-30", Issue: domain.Issue{ID: "issue-30", Identifier: "ONS-30", Title: "Add one and two"}, DispatchID: "D1", RetryAttempt: &attempt,
		StartedAt: time.Date(2026, 11, 3, 8, 0, 0, 0, time.UTC), AgentKind: "agent-client-protocol",
		RuleName: "build", StageLineage: StageLineage{ChainID: "C1"}, ReactionKind: ReactionKindReview,
		ContinuationContext: map[string]any{
			reviewCommentsKey: []map[string]any{{"id": "c1", "body": "fix this", "line": 12}},
			"ci_failure":      map[string]any{"failing_count": 1, "log_excerpt": "secret=hunter2", "check_runs": []map[string]any{}},
			"labels":          []string{"a", "b"},
		},
		Continuation: ContinuationTarget{PRNumber: 12, Owner: "org", Repo: "app", Attempt: 1},
		SessionID:    "ses-1", TurnCount: 3, AgentInputTokens: 1200, AgentOutputTokens: 300, AgentTotalTokens: 1500,
	}
	raw, err := json.Marshal(snapshotRemoteEntry(entry))
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeRemoteRunEntry(string(raw))
	if err != nil {
		t.Fatal(err)
	}
	restored := &RunningEntry{}
	got.restoreInto(restored)

	want := map[string]any{
		reviewCommentsKey: []map[string]any{{"id": "c1", "body": "fix this", "line": 12}},
		"ci_failure":      map[string]any{"failing_count": 1, "log_excerpt": "", "check_runs": []any{}},
		"labels":          []string{"a", "b"},
	}
	if !reflect.DeepEqual(restored.ContinuationContext, want) {
		t.Errorf("continuation = %#v, want %#v", restored.ContinuationContext, want)
	}
	if ids := continuationCommentIDs(ReactionKindReview, restored.ContinuationContext); !reflect.DeepEqual(ids, []string{"c1"}) {
		t.Errorf("comment IDs = %v, want c1", ids)
	}
	if entry.ContinuationContext["ci_failure"].(map[string]any)["log_excerpt"] != "secret=hunter2" {
		t.Error("the snapshot changed the running entry's continuation")
	}
	if got.DispatchID != "D1" || *got.RetryAttempt != 2 || restored.RuleName != "build" || restored.StageLineage.ChainID != "C1" ||
		restored.Continuation.PRNumber != 12 || restored.SessionID != "ses-1" || restored.TurnCount != 3 ||
		restored.AgentInputTokens != 1200 || restored.AgentTotalTokens != 1500 || restored.LastReportedInputTokens != 0 {
		t.Errorf("restored entry %+v", restored)
	}
	if _, err := decodeRemoteRunEntry(""); err == nil {
		t.Error("an entry never recorded decoded")
	}
}

// An adopted run's stall clock starts at its adoption: the events of a run
// that went on while no process ran were never seen, so a run older than
// the stall timeout is not stopped as stalled on the first pass after a
// restart, and one silent for the timeout after its adoption is.
func TestAdoptedRunStallClockStartsAtAdoption(t *testing.T) {
	t.Parallel()

	const timeout = 15 * time.Minute
	started := time.Now().Add(-time.Hour)
	state := NewState(5000, 4, 0, nil, AgentTotals{})
	a := &remoteAdoption{entry: remoteRunEntry{
		Issue:      domain.Issue{ID: "issue-19", Identifier: "ONS-19", State: "In Progress"},
		DispatchID: "D19", StartedAt: started, TurnCount: 15,
	}}
	done := make(chan struct{})
	before := time.Now()
	adoptIssue(t.Context(), state, a, func(ctx context.Context, _ domain.Issue, _ *int) {
		<-ctx.Done()
		close(done)
	})
	entry := state.Running["issue-19"]
	if entry == nil {
		t.Fatal("the adopted run is not running")
	}
	if !entry.StartedAt.Equal(started) {
		t.Errorf("StartedAt = %v, want the run's start %v", entry.StartedAt, started)
	}
	if ref := stallReference(entry); ref.Before(before) {
		t.Errorf("stall reference = %v, before the adoption at %v", ref, before)
	}

	for _, tt := range []struct {
		name     string
		now      time.Time
		wantStop bool
	}{
		{"the first pass after the restart", time.Now(), false},
		{"silent for the timeout after its adoption", entry.AdoptedAt.Add(timeout + time.Second), true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			params := defaultReconcileParams(t, &mockReconcileStore{}, &mockReconcileTracker{states: map[string]string{"issue-19": "In Progress"}})
			params.StallTimeoutMS = int(timeout.Milliseconds())
			params.NowFunc = func() time.Time { return tt.now }
			s := NewState(5000, 4, 0, nil, AgentTotals{})
			cc := &cancelCounter{}
			s.Running["issue-19"] = &RunningEntry{
				Identifier: "ONS-19", Issue: domain.Issue{ID: "issue-19", State: "In Progress"},
				StartedAt: started, AdoptedAt: entry.AdoptedAt, CancelFunc: cc.cancel,
			}
			s.Claimed["issue-19"] = struct{}{}
			ReconcileRunningIssues(s, params)
			if got := cc.count == 1; got != tt.wantStop {
				t.Errorf("stopped = %v, want %v", got, tt.wantStop)
			}
		})
	}

	entry.CancelFunc()
	<-done
	state.WorkerWg.Wait()
}
