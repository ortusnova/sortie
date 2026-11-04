package server

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/sortie-ai/sortie/internal/orchestrator"
	"github.com/sortie-ai/sortie/internal/sentence"
)

// A park its pull request record released is served in park_releases,
// naming the pull request, the head and the comment.
func TestStateResponse_ParkReleasesNameTheRecord(t *testing.T) {
	t.Parallel()
	snap := orchestrator.RuntimeSnapshotResult{ParkReleases: []orchestrator.SnapshotParkRelease{{
		IssueID: "id-39", Identifier: "ONS-39", Reason: "agent_blocked",
		ParkedAt:   time.Date(2026, 11, 4, 8, 0, 0, 0, time.UTC),
		ReleasedAt: time.Date(2026, 11, 4, 9, 0, 0, 0, time.UTC),
		Gesture:    "record", PullRequest: "o/homelab#155", Head: "4d2c1aa9", Comments: []string{"c-7"},
	}, {
		IssueID: "id-40", Identifier: "ONS-40", Reason: "agent_blocked", Gesture: "answered",
	}}}

	got := toStateResponse(snap, sentence.Context{}, nil)

	if len(got.ParkReleases) != 2 {
		t.Fatalf("park_releases = %+v, want two rows", got.ParkReleases)
	}
	assertJSONKeys(t, got.ParkReleases[0], map[string]any{
		"issue_id":         "id-39",
		"issue_identifier": "ONS-39",
		"reason":           "agent_blocked",
		"parked_at":        "2026-11-04T08:00:00Z",
		"released_at":      "2026-11-04T09:00:00Z",
		"release":          map[string]any{"gesture": "record", "head": "4d2c1aa9", "comments": []any{"c-7"}},
		"pull_request":     map[string]any{"number": 155, "repository": "o/homelab", "head_sha": "4d2c1aa9"},
	})
	if got.ParkReleases[1].PullRequest != nil || got.ParkReleases[1].Release.Gesture != "answered" {
		t.Errorf("answered release = %+v, want no pull request", got.ParkReleases[1])
	}
}

// The outcome of a park a record released names its pull request, head
// and comments, before and after the merge.
func TestIssueOutcomeResponse_ParkRelease(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"handed_off", "merged"} {
		o := IssueOutcome{Kind: kind, ObservedAt: "2026-11-04T09:00:00Z", State: "In Review", MergeReactionKind: "merge",
			PRNumber: 155, PRRepository: "o/homelab", ReleaseHead: "4d2c1aa9", ReleaseComment: "c-7,lp-1"}
		if kind == "merged" {
			o.State = ""
		}
		resp := toIssueOutcomeResponse(o, "ONS-39", "")
		raw, err := json.Marshal(resp)
		if err != nil {
			t.Fatal(err)
		}
		var got map[string]any
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Fatal(err)
		}
		release, _ := got["park_release"].(map[string]any)
		pr, _ := got["pull_request"].(map[string]any)
		if release["gesture"] != "record" || release["head"] != "4d2c1aa9" || len(release["comments"].([]any)) != 2 || pr["number"] != float64(155) {
			t.Errorf("%s outcome = %s, want the park release and its pull request", kind, raw)
		}
	}
	plain := toIssueOutcomeResponse(IssueOutcome{Kind: "handed_off", State: "In Review"}, "ONS-41", "")
	if plain.ParkRelease != nil || plain.PullRequest != nil {
		t.Errorf("plain handoff = %+v, want no park release", plain)
	}
}
