package persistence

import (
	"context"
	"reflect"
	"testing"
)

func TestHandoffParts_SaveLoadAndClose(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := openTestStore(t)
	migrateOrFatal(t, s)

	if parts, err := s.LoadHandoffParts(ctx); err != nil || parts == nil || len(parts) != 0 {
		t.Fatalf("LoadHandoffParts() on a new store = %v, %v, want an empty slice", parts, err)
	}

	proof := HandoffPart{
		IssueID: "id-77", PartID: "live-chain", Identifier: "ONS-77", Kind: "job",
		Need: "the live chain reaches gitea.homelab.lan", Check: "curl -fsS https://gitea.homelab.lan",
		Capabilities: []string{"reach:homelab"}, Status: "working", JobAttempt: 1,
		RecordedAt: "2026-11-03T10:00:00Z", UpdatedAt: "2026-11-03T10:00:00Z",
	}
	change := HandoffPart{
		IssueID: "id-58", PartID: "review-gate", Identifier: "ONS-58", Kind: "pull_request",
		Need: "a review gate change", Check: "go test ./...", Repository: "ortusnova/sortie", Patch: "--- a\n+++ b\n",
		Status: "working", WorkIssueID: "id-201", WorkIdentifier: "ONS-201",
		RecordedAt: "2026-11-03T11:00:00Z", UpdatedAt: "2026-11-03T11:00:00Z",
	}
	for _, p := range []HandoffPart{proof, change} {
		if err := s.SaveHandoffPart(ctx, p); err != nil {
			t.Fatalf("SaveHandoffPart(%s): %v", p.PartID, err)
		}
	}
	change.Status, change.Result, change.DoneAt = HandoffPartDone, "ONS-201 reached Done", "2026-11-03T12:00:00Z"
	if err := s.SaveHandoffPart(ctx, change); err != nil {
		t.Fatalf("SaveHandoffPart(closed): %v", err)
	}

	got, err := s.LoadHandoffParts(ctx)
	if err != nil {
		t.Fatalf("LoadHandoffParts: %v", err)
	}
	change.Capabilities = []string{}
	if want := []HandoffPart{proof, change}; !reflect.DeepEqual(got, want) {
		t.Errorf("LoadHandoffParts() =\n%+v\nwant\n%+v", got, want)
	}
}
