package sentence

import (
	"strings"
	"testing"
	"time"

	"github.com/sortie-ai/sortie/internal/orchestrator"
)

func TestStall(t *testing.T) {
	t.Parallel()

	ref := time.Date(2026, 11, 3, 16, 29, 43, 0, time.UTC)
	now := ref.Add(50 * time.Minute)

	tests := []struct {
		name      string
		entry     orchestrator.SnapshotRunningEntry
		wantShort string
		wantLong  string
	}{
		{
			name: "between tool calls the stall timeout is named",
			entry: orchestrator.SnapshotRunningEntry{
				StallReference: ref, QuietAt: ref.Add(30 * time.Minute), StallAt: ref.Add(60 * time.Minute),
			},
			wantShort: "No events for 50m; Sortie stops it after 1h",
			wantLong:  "`agent.stall_timeout_ms`",
		},
		{
			name: "inside a tool call the tool-call bound is named",
			entry: orchestrator.SnapshotRunningEntry{
				StallReference: ref, QuietAt: ref.Add(45 * time.Minute), StallAt: ref.Add(90 * time.Minute),
				StallToolCallBound: true, InToolCallSince: ref,
			},
			wantShort: "In a tool call with no events for 50m; Sortie stops it after 1h 30m",
			wantLong:  "`agent.tool_call_timeout_ms`",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, quiet := Stall(&tt.entry, Context{Now: now})
			if !quiet {
				t.Fatal("Stall() quiet = false, want true past the quiet instant")
			}
			if got.Short != tt.wantShort {
				t.Errorf("Stall() short = %q, want %q", got.Short, tt.wantShort)
			}
			if !strings.Contains(got.Long, tt.wantLong) {
				t.Errorf("Stall() long = %q, want it to name %s", got.Long, tt.wantLong)
			}
		})
	}
}
