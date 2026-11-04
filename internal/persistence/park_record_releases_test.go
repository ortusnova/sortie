package persistence

import (
	"testing"
)

// An outcome naming a record release keeps it, and every later outcome of
// the same pull request names it too; an outcome of another pull request
// does not.
func TestIssueOutcomes_KeepTheLatestRecordReleaseOfThePullRequest(t *testing.T) {
	t.Parallel()
	s := openTestStore(t)
	migrateOrFatal(t, s)
	ctx := t.Context()
	record := func(o IssueOutcome) {
		t.Helper()
		if err := s.RecordIssueOutcome(ctx, o); err != nil {
			t.Fatal(err)
		}
	}
	record(IssueOutcome{IssueID: "id-1", Kind: IssueOutcomeHandedOff, ObservedAt: "2026-11-04T09:00:00Z", State: "In Review",
		PRNumber: 7, PRRepository: "o/r", ReleaseHead: "aaaaaaa", ReleaseComment: "c-1"})
	record(IssueOutcome{IssueID: "id-1", Kind: IssueOutcomeHandedOff, ObservedAt: "2026-11-04T10:00:00Z", State: "In Review",
		PRNumber: 7, PRRepository: "o/r", ReleaseHead: "bbbbbbb", ReleaseComment: "c-2,lp-1"})
	record(IssueOutcome{IssueID: "id-1", Kind: IssueOutcomeMerged, ObservedAt: "2026-11-04T10:05:00Z", MergeReactionKind: "merge",
		PRNumber: 7, PRRepository: "o/r"})
	record(IssueOutcome{IssueID: "id-2", Kind: IssueOutcomeHandedOff, ObservedAt: "2026-11-04T10:00:00Z", State: "In Review",
		PRNumber: 8, PRRepository: "o/r", ReleaseHead: "ccccccc"})
	record(IssueOutcome{IssueID: "id-2", Kind: IssueOutcomeMerged, ObservedAt: "2026-11-04T11:00:00Z", MergeReactionKind: "merge",
		PRNumber: 9, PRRepository: "o/r"})

	got, err := s.IssueOutcomes(ctx, []string{"id-1", "id-2"})
	if err != nil {
		t.Fatal(err)
	}
	if o := got["id-1"]; o.Kind != IssueOutcomeMerged || o.ReleaseHead != "bbbbbbb" || o.ReleaseComment != "c-2,lp-1" {
		t.Errorf("id-1 = %+v, want the merge naming the latest record release", o)
	}
	if o := got["id-2"]; o.Kind != IssueOutcomeMerged || o.ReleaseHead != "" || o.ReleaseComment != "" {
		t.Errorf("id-2 = %+v, want a merge of another pull request naming no release", o)
	}
}
