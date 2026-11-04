package persistence

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func TestSupervisorIssues_SaveAndList(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := openTestStore(t)
	migrateOrFatal(t, s)

	if got, err := s.SupervisorIssues(ctx); err != nil || got == nil || len(got) != 0 {
		t.Fatalf("SupervisorIssues() on a new store = %v, %v, want an empty slice", got, err)
	}
	at := time.Date(2026, 11, 2, 10, 0, 0, 0, time.UTC)
	older := SupervisorIssue{IssueID: "id-1", Identifier: "ONS-1", Kind: "platform_defect", Stories: []string{}, Signature: "deploy | state | stale", FiledAt: at}
	newer := SupervisorIssue{IssueID: "id-2", Identifier: "ONS-2", Kind: "follow_up", Stories: []string{"ONS-39", "ONS-77"}, FiledAt: at.Add(time.Minute)}
	for _, issue := range []SupervisorIssue{older, newer} {
		if err := s.SaveSupervisorIssue(ctx, issue); err != nil {
			t.Fatalf("SaveSupervisorIssue(%s): %v", issue.IssueID, err)
		}
	}
	got, err := s.SupervisorIssues(ctx)
	if err != nil || !reflect.DeepEqual(got, []SupervisorIssue{newer, older}) {
		t.Errorf("SupervisorIssues() = %+v, %v, want the newer first", got, err)
	}
	if err := s.SaveSupervisorIssue(ctx, SupervisorIssue{IssueID: "id-3"}); err == nil {
		t.Error("SaveSupervisorIssue without a kind succeeded")
	}
}

// A question recorded before its raiser was kept counts as the
// supervisor's; any other fault has no raiser.
func TestMigrate_Migration1017_LegacyQuestionsAreTheSupervisors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := openTestStore(t)
	migrateToVersion(t, s, 1016)
	for _, row := range []struct{ id, class string }{{"pf-q", "supervisor_question"}, {"pf-x", "token_scope"}} {
		if _, err := s.db.ExecContext(ctx,
			`INSERT INTO platform_faults (id, signature, class, recorded_at) VALUES (?, ?, ?, '2026-11-02T10:00:00Z')`,
			row.id, row.class+": x", row.class); err != nil {
			t.Fatalf("insert %s: %v", row.id, err)
		}
	}
	migrateOrFatal(t, s)

	open, err := s.LoadOpenPlatformFaults(ctx)
	if err != nil || len(open) != 2 {
		t.Fatalf("LoadOpenPlatformFaults() = %+v, %v, want both", open, err)
	}
	for _, f := range open {
		want := ""
		if f.Class == "supervisor_question" {
			want = "supervisor"
		}
		if f.RaisedBy != want || f.QuestionKind != "" {
			t.Errorf("%s raised by %q kind %q, want %q and no kind", f.ID, f.RaisedBy, f.QuestionKind, want)
		}
	}
}
