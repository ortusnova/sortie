package persistence

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestSupervisorActions_NewestFirstAndOutliveARestart(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sortie.db")
	open := func() *Store {
		t.Helper()
		s, err := Open(ctx, path)
		if err != nil {
			t.Fatalf("Open(%s): %v", path, err)
		}
		if err := s.Migrate(ctx); err != nil {
			t.Fatalf("Migrate: %v", err)
		}
		return s
	}

	s := open()
	at := time.Date(2026, 11, 3, 9, 0, 0, 0, time.UTC)
	merge := SupervisorAction{
		Action: "record_merge", Route: "POST /api/v1/ONS-7/merge-record", IssueIdentifier: "ONS-7",
		Evidence: "the forge shows #12 merged", Result: SupervisorDone, Status: 200,
		PullRequest: "o/r#12", MergeCommit: "abc123", ActedAt: at,
	}
	refused := SupervisorAction{
		Action: "POST /api/v1/plan/sync", Route: "POST /api/v1/plan/sync", Result: SupervisorRefused, Status: 403,
		Detail: "forbidden: not a supervisor action", ActedAt: at.Add(time.Minute),
	}
	for _, a := range []SupervisorAction{merge, refused} {
		if _, err := s.InsertSupervisorAction(ctx, a); err != nil {
			t.Fatalf("InsertSupervisorAction: %v", err)
		}
	}
	if _, err := s.InsertSupervisorAction(ctx, SupervisorAction{Action: "answer"}); err == nil {
		t.Error("InsertSupervisorAction without a result succeeded, want an error")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s = open()
	defer s.Close() //nolint:errcheck // test
	got, err := s.RecentSupervisorActions(ctx, 10)
	if err != nil {
		t.Fatalf("RecentSupervisorActions: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("actions = %d, want 2", len(got))
	}
	if got[0].Result != SupervisorRefused || got[1].Action != "record_merge" {
		t.Errorf("order = %s, %s, want the refusal first", got[0].Action, got[1].Action)
	}
	m := got[1]
	m.ID = 0
	if m != merge {
		t.Errorf("merge = %+v, want %+v", m, merge)
	}
	if one, err := s.RecentSupervisorActions(ctx, 1); err != nil || len(one) != 1 || one[0].Result != SupervisorRefused {
		t.Errorf("limit 1 = %+v, %v, want only the newest", one, err)
	}
}
