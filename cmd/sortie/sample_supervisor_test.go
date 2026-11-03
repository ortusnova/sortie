package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sortie-ai/sortie/internal/prompt"
)

// The sample supervisor prompt renders in strict mode for a trigger with a
// story and for one without.
func TestSampleSupervisorPromptRenders(t *testing.T) {
	t.Parallel()

	path := filepath.Join(repoRoot(t), "examples", "supervisor.md")
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	tmpl, err := prompt.ParseSupervisor(string(body), path)
	if err != nil {
		t.Fatalf("ParseSupervisor: %v", err)
	}
	for _, trigger := range []prompt.SupervisorTrigger{
		{Kind: "park", IssueIdentifier: "ONS-7", Detail: "parked: agent_blocked"},
		{Kind: "sweep", Detail: "the periodic sweep"},
	} {
		got, err := tmpl.Render(prompt.SupervisorContext{
			Token: "sup-1", APIURL: "http://127.0.0.1:7678", Triggers: []prompt.SupervisorTrigger{trigger}, State: map[string]any{},
		})
		if err != nil {
			t.Fatalf("Render(%s): %v", trigger.Kind, err)
		}
		if !strings.Contains(got, "This session\nstarted on "+trigger.Kind) || !strings.Contains(got, "/api/v1/supervisor/questions") {
			t.Errorf("Render(%s) =\n%s", trigger.Kind, got)
		}
	}
}
