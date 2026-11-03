package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/sortie-ai/sortie/internal/persistence"
)

// The cost_budget tool reports agent.max_sessions plus the sessions granted
// to the issue, so an agent on a granted issue does not read its budget as
// spent.
func TestBuildSessionToolRegistry_CostBudgetCountsGrants(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "grants.db")
	seedDB(t, dbPath)
	rw, err := persistence.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rw.AddSessionGrant(ctx, persistence.SessionGrant{IssueID: "issue-granted", Identifier: "ONS-68", Sessions: 4, GrantedBy: "brian"}); err != nil {
		t.Fatalf("AddSessionGrant: %v", err)
	}
	if err := rw.Close(); err != nil {
		t.Fatal(err)
	}

	budgetSessions := func(t *testing.T, issueID string, maxSessions int) any {
		t.Helper()
		result, err := BuildSessionToolRegistry(ctx, slog.New(slog.DiscardHandler), SessionToolParams{
			DBPath:      dbPath,
			IssueID:     issueID,
			MaxSessions: maxSessions,
		})
		if err != nil {
			t.Fatalf("BuildSessionToolRegistry: %v", err)
		}
		t.Cleanup(func() { closeResult(t, result) })
		tool, ok := result.Registry.Get("cost_budget")
		if !ok {
			t.Fatal("cost_budget tool not registered")
		}
		out, err := tool.Execute(ctx, json.RawMessage(`{}`))
		if err != nil {
			t.Fatalf("cost_budget Execute: %v", err)
		}
		var top struct {
			Data map[string]any `json:"data"`
		}
		if err := json.Unmarshal(out, &top); err != nil {
			t.Fatalf("unmarshal cost_budget response %q: %v", out, err)
		}
		return top.Data["budget_sessions"]
	}

	for _, tt := range []struct {
		name        string
		issueID     string
		maxSessions int
		want        float64
	}{
		{"a granted issue", "issue-granted", 2, 6},
		{"an issue without a grant", "issue-other", 2, 2},
		{"no session budget", "issue-granted", 0, 0},
	} {
		if got := budgetSessions(t, tt.issueID, tt.maxSessions); got != tt.want {
			t.Errorf("%s: budget_sessions = %v, want %v", tt.name, got, tt.want)
		}
	}
}
