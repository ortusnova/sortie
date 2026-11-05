package persistence

import (
	"context"
	"fmt"
	"testing"
)

func TestMigration1004_BackfillsLatchedMerges(t *testing.T) {
	t.Parallel()
	s := openTestStore(t)
	ctx := context.Background()
	migrateToVersion(t, s, 1003)
	if err := s.UpsertReactionFingerprint(ctx, "iss-a", "merge-completion", "abc123"); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertReactionFingerprint(ctx, "iss-b", "merge", "not-a-merge-completion"); err != nil {
		t.Fatal(err)
	}
	migrateOrFatal(t, s)
	migrateOrFatal(t, s) // a second run is a no-op

	got, err := s.LoadPlanSatisfaction(ctx, []string{"iss-a", "iss-b"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || len(got["iss-a"]) != 1 || got["iss-a"][0].Kind != PlanSatisfactionMerged || got["iss-a"][0].Ref != "abc123" {
		t.Errorf("after backfill = %+v, want one merged row for iss-a with abc123", got)
	}
}

func TestPlanSatisfaction_UpsertAndLoad(t *testing.T) {
	t.Parallel()
	s := openTestStore(t)
	ctx := context.Background()
	migrateOrFatal(t, s)

	rows := []PlanSatisfaction{
		{IssueID: "a", Kind: PlanSatisfactionMerged, Ref: "sha1", PR: "o/r#1"},
		{IssueID: "a", Kind: PlanSatisfactionMerged, Ref: "sha2", PR: "o/r#2"},
		{IssueID: "b", Kind: PlanSatisfactionNoChange, Ref: "plan:billing/x"},
	}
	for _, r := range rows {
		if err := s.UpsertPlanSatisfaction(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.LoadPlanSatisfaction(ctx, []string{"a", "b", "c"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got["a"]) != 1 || got["a"][0].Ref != "sha2" || got["a"][0].PR != "o/r#2" {
		t.Errorf("a = %+v, want the later merge", got["a"])
	}
	if len(got["b"]) != 1 || got["b"][0].Kind != PlanSatisfactionNoChange {
		t.Errorf("b = %+v, want the no-change row", got["b"])
	}
	if _, ok := got["c"]; ok {
		t.Error("c has a row, want none")
	}
}

// 500 ids load in one query, and more than one batch still loads all.
func TestPlanSatisfaction_LoadManyIDs(t *testing.T) {
	t.Parallel()
	s := openTestStore(t)
	ctx := context.Background()
	migrateOrFatal(t, s)
	ids := make([]string, 2000)
	for i := range ids {
		ids[i] = fmt.Sprintf("id-%d", i)
		if i%100 == 0 {
			if err := s.UpsertPlanSatisfaction(ctx, PlanSatisfaction{IssueID: ids[i], Kind: PlanSatisfactionMerged, Ref: "x"}); err != nil {
				t.Fatal(err)
			}
		}
	}
	got, err := s.LoadPlanSatisfaction(ctx, ids)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 20 {
		t.Errorf("loaded %d issues, want 20", len(got))
	}
}

// A merged row the forge shows reverted is deleted; the issue's other
// kinds and other issues' rows stay.
func TestPlanSatisfaction_DeleteAndList(t *testing.T) {
	t.Parallel()
	s := openTestStore(t)
	ctx := context.Background()
	migrateOrFatal(t, s)

	for _, r := range []PlanSatisfaction{
		{IssueID: "a", Kind: PlanSatisfactionMerged, Ref: "sha1", PR: "o/r#1"},
		{IssueID: "a", Kind: PlanSatisfactionNoChange, Ref: "commit:sha0"},
		{IssueID: "b", Kind: PlanSatisfactionMerged, Ref: "sha2", PR: "o/r#2"},
	} {
		if err := s.UpsertPlanSatisfaction(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	merged, err := s.ListPlanSatisfaction(ctx, PlanSatisfactionMerged)
	if err != nil || len(merged) != 2 {
		t.Fatalf("ListPlanSatisfaction(merged) = %+v, %v, want two rows", merged, err)
	}
	if err := s.DeletePlanSatisfaction(ctx, "a", PlanSatisfactionMerged); err != nil {
		t.Fatal(err)
	}
	if err := s.DeletePlanSatisfaction(ctx, "a", PlanSatisfactionMerged); err != nil {
		t.Fatalf("second delete: %v, want no error", err)
	}
	got, err := s.LoadPlanSatisfaction(ctx, []string{"a", "b"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got["a"]) != 1 || got["a"][0].Kind != PlanSatisfactionNoChange || len(got["b"]) != 1 {
		t.Errorf("after delete = %+v, want a's no-change row and b's merge", got)
	}
}
