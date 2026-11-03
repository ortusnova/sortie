package persistence

import (
	"context"
	"maps"
	"path/filepath"
	"testing"
	"time"
)

func TestSessionGrants_SumPerIssueAndOutliveARestart(t *testing.T) {
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
	at := time.Date(2026, 11, 2, 9, 0, 0, 0, time.UTC)
	grants := []struct {
		grant SessionGrant
		total int
	}{
		{SessionGrant{IssueID: "id-68", Identifier: "ONS-68", Sessions: 4, GrantedBy: "brian", Reason: "one more try", GrantedAt: at}, 4},
		{SessionGrant{IssueID: "id-50", Identifier: "ONS-50", Sessions: 1, GrantedBy: "brian"}, 1},
		{SessionGrant{IssueID: "id-68", Identifier: "ONS-68", Sessions: 2, GrantedBy: "workbench"}, 6},
	}
	for _, g := range grants {
		total, err := s.AddSessionGrant(ctx, g.grant)
		if err != nil || total != g.total {
			t.Fatalf("AddSessionGrant(%+v) = %d, %v, want %d", g.grant, total, err, g.total)
		}
	}
	for _, bad := range []SessionGrant{{IssueID: "id-68", Sessions: 0}, {Sessions: 1}} {
		if _, err := s.AddSessionGrant(ctx, bad); err == nil {
			t.Errorf("AddSessionGrant(%+v) = nil error, want refused", bad)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s = open()
	t.Cleanup(func() { closeStore(t, s) })
	totals, err := s.SessionGrantTotals(ctx)
	if want := map[string]int{"id-68": 6, "id-50": 1}; err != nil || !maps.Equal(totals, want) {
		t.Errorf("SessionGrantTotals() after a restart = %v, %v, want %v", totals, err, want)
	}
	for id, want := range map[string]int{"id-68": 6, "id-none": 0} {
		if got, err := s.SessionGrantTotal(ctx, id); err != nil || got != want {
			t.Errorf("SessionGrantTotal(%s) = %d, %v, want %d", id, got, err, want)
		}
	}

	var by, reason, grantedAt string
	if err := s.db.QueryRowContext(ctx,
		`SELECT granted_by, reason, granted_at FROM session_grants WHERE issue_id = 'id-68' ORDER BY id LIMIT 1`,
	).Scan(&by, &reason, &grantedAt); err != nil {
		t.Fatal(err)
	}
	if by != "brian" || reason != "one more try" || grantedAt != "2026-11-02T09:00:00Z" {
		t.Errorf("first grant row = %q, %q, %q, want brian, one more try, 2026-11-02T09:00:00Z", by, reason, grantedAt)
	}
}
