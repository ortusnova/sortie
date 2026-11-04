package persistence

import (
	"context"
	"testing"
)

// A story keeps only its latest verdict, and a story with none reports
// none.
func TestCriteriaVerdict_LatestReplacesTheEarlier(t *testing.T) {
	t.Parallel()
	s := openTestStore(t)
	ctx := context.Background()
	migrateOrFatal(t, s)

	if _, ok, err := s.LoadCriteriaVerdict(ctx, "a"); ok || err != nil {
		t.Fatalf("verdict before any review = %v, %v; want none", ok, err)
	}
	if err := s.UpsertCriteriaVerdict(ctx, CriteriaVerdict{IssueID: "a", DispatchID: "d1", Outcome: "met", Criteria: `[{"verdict":"met"}]`}); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertCriteriaVerdict(ctx, CriteriaVerdict{IssueID: "a", DispatchID: "d2", Outcome: "refused", Problem: "no verdict"}); err != nil {
		t.Fatal(err)
	}
	v, ok, err := s.LoadCriteriaVerdict(ctx, "a")
	if err != nil || !ok || v.DispatchID != "d2" || v.Outcome != "refused" || v.Problem != "no verdict" || v.Criteria != "" || v.RecordedAt.IsZero() {
		t.Errorf("latest verdict = %+v, %v, %v; want d2's refusal", v, ok, err)
	}
}
