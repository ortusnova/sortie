package orchestrator

import (
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
