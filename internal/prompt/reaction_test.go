package prompt

import (
	"strings"
	"testing"
)

func TestBuildReactionTurnPrompt_Selection(t *testing.T) {
	t.Parallel()
	fix := map[string]any{"label_fix": map[string]any{"pr_number": "7", "owner": "o", "repo": "r", "branch": "feat", "actor": "a", "requested_at": "now"}}
	tests := []struct {
		name, body, want string
		turn             int
	}{
		{name: "branch on turn 1", body: "{{ if .label_fix }}FIX{{ else }}WORK{{ end }}", want: "FIX", turn: 1},
		{name: "branch on turn 2", body: "{{ if .label_fix }}FIX{{ else }}WORK{{ end }}", want: "Continue with the instructions", turn: 2},
		{name: "branch with its own continuation on turn 2", body: "{{ if .label_fix }}{{ if .run.is_continuation }}MORE{{ else }}FIX{{ end }}{{ else }}WORK{{ end }}", want: "MORE", turn: 2},
		{name: "unguarded key on turn 2", body: "{{ .label_fix.branch }}", want: "Continue with the instructions", turn: 2},
		{name: "no branch", body: "WORK", want: "## Fix this pull request", turn: 1},
		{name: "whitespace-only branch", body: "{{ if .label_fix }} {{ else }}WORK{{ end }}", want: "## Fix this pull request", turn: 1},
		{name: "unguarded key", body: "{{ .label_fix.branch }}", want: "feat", turn: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := BuildReactionTurnPrompt(mustParseAnalyze(t, tt.body), nil, nil, tt.turn, 3, fix)
			if err != nil || !strings.HasPrefix(got, tt.want) {
				t.Errorf("BuildReactionTurnPrompt(%q, turn %d) = %q, %v, want prefix %q", tt.body, tt.turn, got, err, tt.want)
			}
		})
	}
}

// A fix branch with no continuation branch of its own, as the stage-0
// workflow had, sent its whole merge instructions again on every turn, and
// the agent redid a merge it had already committed.
func TestBuildReactionTurnPrompt_LaterTurnOfABranchWithoutContinuation(t *testing.T) {
	t.Parallel()
	tmpl := mustParseAnalyze(t, "{{ if .merge_conflict }}Run git merge origin/{{ .merge_conflict.base }} and commit the merge.{{ else }}WORK{{ end }}")
	conflict := map[string]any{"merge_conflict": map[string]any{"pr_number": "137", "branch": "sortie/ONS-83", "base": "master", "head_sha": "17d67f1e"}}

	first, err := BuildReactionTurnPrompt(tmpl, nil, nil, 1, 20, conflict)
	if err != nil || first != "Run git merge origin/master and commit the merge." {
		t.Fatalf("turn 1 = %q, %v; want the operator's branch", first, err)
	}
	second, err := BuildReactionTurnPrompt(tmpl, map[string]any{"identifier": "ONS-83", "title": "T"}, nil, 2, 20, conflict)
	if err != nil {
		t.Fatalf("turn 2 error = %v", err)
	}
	if strings.Contains(second, "git merge") {
		t.Errorf("turn 2 = %q, want no repeat of the first turn's task", second)
	}
	for _, want := range []string{"resolving the merge conflicts", "earlier turns of this run are in this workspace", "origin/<branch> does not move during the run", "Do not repeat a fix"} {
		if !strings.Contains(second, want) {
			t.Errorf("turn 2 = %q, want it to hold %q", second, want)
		}
	}
}

func TestBuiltinPromptsRenderForEveryContinuationKey(t *testing.T) {
	t.Parallel()
	for _, k := range continuationKeys {
		t.Run(k.name, func(t *testing.T) {
			t.Parallel()
			fields := map[string]any{}
			for f, children := range templateFieldSchema[k.name] {
				fields[f] = "v-" + f
				if children != nil {
					fields[f] = []any{map[string]any{"name": "v-name", "status": "v-status", "conclusion": "v-conclusion", "details_url": "v-url"}}
				}
			}
			data := any(fields)
			if len(fields) == 0 {
				data = []any{map[string]any{"id": "v-id", "file": "v-file", "start_line": "v-start", "end_line": "v-end", "reviewer": "v-reviewer", "body": "v-body"}}
			}
			for turn := 1; turn <= 2; turn++ {
				got, err := BuildReactionTurnPrompt(mustParseAnalyze(t, "plain"), map[string]any{"identifier": "ID-1", "title": "T"}, nil, turn, 3, map[string]any{k.name: data})
				if err != nil || strings.TrimSpace(got) == "" || got == "plain" || (turn == 1 && !strings.Contains(got, "v-")) {
					t.Errorf("BuildReactionTurnPrompt(%q, turn %d) = %q, %v, want the built-in prompt holding a sample value", k.name, turn, got, err)
				}
			}
		})
	}
}
