package persistence

import (
	"context"
	"reflect"
	"testing"
)

func TestMergeAndProve_SaveLoadDelete(t *testing.T) {
	t.Parallel()
	s := openTestStore(t)
	ctx := context.Background()
	migrateOrFatal(t, s)

	second := MergeAndProve{
		IssueID: "b", Identifier: "ONS-2", Seq: 2, Phase: "queued", PullRequest: "o/r#2", Head: "h2",
		QueuedAt: "2026-11-04T10:01:00Z", PhaseAt: "2026-11-04T10:01:00Z",
	}
	first := MergeAndProve{
		IssueID: "a", Identifier: "ONS-1", Seq: 1, Phase: "proving", PullRequest: "o/r#1", Branch: "sortie/ONS-1",
		Head: "h1", Criteria: "it works live", MergeCommit: "m1", Deployed: "p1=d1,taichi=d2",
		QueuedAt: "2026-11-04T10:00:00Z", PhaseAt: "2026-11-04T10:05:00Z", Deadline: "2026-11-04T10:35:00Z",
		RevertPR: 0, Reason: "", AgentKind: "claude-code", ConfiguredModel: "opus", RuleName: "review", TemplateID: "fix",
	}
	for _, r := range []MergeAndProve{second, first} {
		if err := s.SaveMergeAndProve(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	first.Phase, first.RevertPR, first.Reason = "reverting", 9, "no proven record"
	if err := s.SaveMergeAndProve(ctx, first); err != nil {
		t.Fatal(err)
	}
	got, err := s.LoadMergeAndProve(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, []MergeAndProve{first, second}) {
		t.Errorf("LoadMergeAndProve() = %+v, want the two rows in queue order", got)
	}
	if err := s.DeleteMergeAndProve(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	got, err = s.LoadMergeAndProve(ctx)
	if err != nil || !reflect.DeepEqual(got, []MergeAndProve{second}) {
		t.Errorf("after delete = %+v, %v, want the second row", got, err)
	}
}
