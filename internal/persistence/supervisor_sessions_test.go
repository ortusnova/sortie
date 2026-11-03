package persistence

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestSupervisorSessions_UsageAndLatestOutliveARestart(t *testing.T) {
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
	midnight := time.Date(2026, 11, 3, 0, 0, 0, 0, time.UTC)
	yesterday, err := s.StartSupervisorSession(ctx, SupervisorSession{TriggerKind: "sweep", StartedAt: midnight.Add(-time.Hour)})
	if err != nil {
		t.Fatalf("StartSupervisorSession: %v", err)
	}
	if err := s.EndSupervisorSession(ctx, yesterday, midnight.Add(-50*time.Minute), "completed", "", 900); err != nil {
		t.Fatalf("EndSupervisorSession: %v", err)
	}
	// Half a second past midnight sorts before midnight in RFC 3339 text
	// with trimmed zeros; the fixed-width layout keeps it inside the day.
	today, err := s.StartSupervisorSession(ctx, SupervisorSession{
		TriggerKind: "park", TriggerIssue: "ONS-7", TriggerDetail: "agent_blocked", Triggers: 2,
		StartedAt: midnight.Add(500 * time.Millisecond),
	})
	if err != nil {
		t.Fatalf("StartSupervisorSession: %v", err)
	}
	if err := s.EndSupervisorSession(ctx, today, midnight.Add(time.Minute), "completed", "", 1200); err != nil {
		t.Fatalf("EndSupervisorSession: %v", err)
	}
	if _, err := s.StartSupervisorSession(ctx, SupervisorSession{StartedAt: midnight}); err == nil {
		t.Error("StartSupervisorSession without a trigger succeeded, want an error")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s = open()
	defer s.Close() //nolint:errcheck // test
	usage, err := s.SupervisorUsageSince(ctx, midnight)
	if err != nil {
		t.Fatalf("SupervisorUsageSince: %v", err)
	}
	if usage != (SupervisorUsage{Sessions: 1, Tokens: 1200}) {
		t.Errorf("usage since midnight = %+v, want 1 session and 1200 tokens", usage)
	}
	latest, ok, err := s.LatestSupervisorSession(ctx)
	if err != nil || !ok {
		t.Fatalf("LatestSupervisorSession = %v, %v", ok, err)
	}
	if latest.ID != today || latest.TriggerKind != "park" || latest.TriggerIssue != "ONS-7" || latest.Triggers != 2 ||
		latest.Outcome != "completed" || latest.Tokens != 1200 || !latest.StartedAt.Equal(midnight.Add(500*time.Millisecond)) ||
		!latest.EndedAt.Equal(midnight.Add(time.Minute)) {
		t.Errorf("latest = %+v, want today's park session", latest)
	}
}

func TestLatestSupervisorSession_NoneStarted(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "sortie.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close() //nolint:errcheck // test
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := s.LatestSupervisorSession(ctx); ok || err != nil {
		t.Errorf("LatestSupervisorSession = %v, %v, want none", ok, err)
	}
}
