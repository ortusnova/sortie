package persistence

import (
	"context"
	"path/filepath"
	"testing"
)

func TestRemoteRuns_OutliveARestartUntilDeleted(t *testing.T) {
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
	if err := s.SaveRemoteRun(ctx, RemoteRun{IssueID: "id-30", JobID: "ONS-30", Attempt: 1, WorkerID: "taichi", Record: `{"job":1}`}); err != nil {
		t.Fatalf("SaveRemoteRun: %v", err)
	}
	if err := s.SaveRemoteRun(ctx, RemoteRun{IssueID: "id-31", JobID: "ONS-31", Attempt: 2, WorkerID: "p1", Record: `{"job":2}`}); err != nil {
		t.Fatalf("SaveRemoteRun: %v", err)
	}
	if err := s.UpdateRemoteRunEntry(ctx, "id-30", `{"dispatch_id":"D1"}`); err != nil {
		t.Fatalf("UpdateRemoteRunEntry: %v", err)
	}
	if err := s.UpdateRemoteRunSession(ctx, "id-30", "ses-1", 3, true); err != nil {
		t.Fatalf("UpdateRemoteRunSession: %v", err)
	}
	if err := s.UpdateRemoteRunEntry(ctx, "id-none", `{}`); err != nil {
		t.Fatalf("UpdateRemoteRunEntry of an issue with no row: %v", err)
	}
	if err := s.SaveRemoteRun(ctx, RemoteRun{IssueID: "id-30"}); err == nil {
		t.Fatal("SaveRemoteRun accepted a row with no job or worker")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s = open()
	t.Cleanup(func() { _ = s.Close() })
	runs, err := s.LoadRemoteRuns(ctx)
	if err != nil {
		t.Fatalf("LoadRemoteRuns: %v", err)
	}
	if len(runs) != 2 {
		t.Fatalf("loaded %d runs, want 2: %+v", len(runs), runs)
	}
	got := runs[0]
	if got.IssueID != "id-30" || got.JobID != "ONS-30" || got.Attempt != 1 || got.WorkerID != "taichi" || got.Record != `{"job":1}` ||
		got.Entry != `{"dispatch_id":"D1"}` || got.SessionID != "ses-1" || got.TurnsCompleted != 3 || !got.SessionEnded || got.UpdatedAt.IsZero() {
		t.Errorf("run = %+v", got)
	}
	if runs[1].IssueID != "id-31" || runs[1].Entry != "" || runs[1].SessionEnded {
		t.Errorf("second run = %+v", runs[1])
	}

	if err := s.DeleteRemoteRun(ctx, "id-30"); err != nil {
		t.Fatalf("DeleteRemoteRun: %v", err)
	}
	runs, err = s.LoadRemoteRuns(ctx)
	if err != nil || len(runs) != 1 || runs[0].IssueID != "id-31" {
		t.Fatalf("after delete: %+v %v", runs, err)
	}
}
