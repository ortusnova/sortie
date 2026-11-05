package server

import (
	"testing"
	"time"

	"github.com/sortie-ai/sortie/internal/orchestrator"
)

// The state API serves the story in flight with its phase and deadline,
// and the queue behind it, whose entries have no deadline yet.
func TestMergeAndProveResponse(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC)
	got := toMergeAndProveResponse(orchestrator.SnapshotMergeAndProve{
		InFlight: &orchestrator.SnapshotMergeAndProveEntry{
			IssueID: "id-1", Identifier: "ONS-1", Phase: orchestrator.MergeAndProveProving, PullRequest: "o/kit#1",
			Head: "h1", MergeCommit: "m1", Deployed: []orchestrator.DeployedHost{{Host: "p1", Branch: "plan-bench", Commit: "d1"}},
			QueuedAt: at, PhaseAt: at.Add(5 * time.Minute), Deadline: at.Add(35 * time.Minute), Waiting: "no record yet",
		},
		Queue: []orchestrator.SnapshotMergeAndProveEntry{{
			IssueID: "id-2", Identifier: "ONS-2", Position: 1, Phase: orchestrator.MergeAndProveQueued,
			PullRequest: "o/kit#2", Head: "h2", QueuedAt: at, PhaseAt: at,
		}},
	})
	in := got.InFlight
	if in == nil || in.Phase != "proving" || in.Deadline == nil || !in.Deadline.Equal(at.Add(35*time.Minute)) ||
		in.MergeCommit == nil || *in.MergeCommit != "m1" || len(in.Deployed) != 1 || in.Deployed[0].Branch != "plan-bench" || in.Waiting == nil {
		t.Fatalf("in_flight = %+v, want ONS-1 proving with its deadline", in)
	}
	if len(got.Queue) != 1 {
		t.Fatalf("queue = %+v, want ONS-2", got.Queue)
	}
	q := got.Queue[0]
	if q.Position != 1 || q.Deadline != nil || q.MergeCommit != nil || q.Deployed == nil || q.RevertPullRequest != nil {
		t.Errorf("queued = %+v, want position 1 with no deadline, merge or revert", q)
	}
	if empty := toMergeAndProveResponse(orchestrator.SnapshotMergeAndProve{}); empty.InFlight != nil || empty.Queue == nil {
		t.Errorf("empty = %+v, want null in flight and an empty queue", empty)
	}
}
