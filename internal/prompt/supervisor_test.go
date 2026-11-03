package prompt

import (
	"errors"
	"strings"
	"testing"
)

func TestSupervisorTemplate_RenderCarriesTokenTriggerAndState(t *testing.T) {
	t.Parallel()

	tmpl, err := ParseSupervisor("Started by {{ .trigger.kind }} on {{ .trigger.issue_identifier }}; {{ len .triggers }} triggers; parked {{ len .state.parked }}.", "supervisor.md")
	if err != nil {
		t.Fatalf("ParseSupervisor: %v", err)
	}
	got, err := tmpl.Render(SupervisorContext{
		Token:  "sup-token-1",
		APIURL: "http://127.0.0.1:7678",
		Triggers: []SupervisorTrigger{
			{Kind: "park", IssueIdentifier: "ONS-7", Detail: "agent_blocked", At: "2026-11-03T09:00:00Z"},
			{Kind: "sweep"},
		},
		State: map[string]any{"parked": []map[string]string{{"issue_identifier": "ONS-7"}}},
	})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	for _, want := range []string{
		"Started by park on ONS-7; 2 triggers; parked 1.",
		"Supervisor token (send as `Authorization: Bearer <token>`): sup-token-1",
		"Sortie API: http://127.0.0.1:7678",
		"- park ONS-7: agent_blocked (2026-11-03T09:00:00Z)",
		"- sweep\n",
		`"issue_identifier": "ONS-7"`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("rendered prompt lacks %q:\n%s", want, got)
		}
	}
}

func TestSupervisorTemplate_StrictAndNeedsATrigger(t *testing.T) {
	t.Parallel()

	tmpl, err := ParseSupervisor("{{ .nothing }}", "supervisor.md")
	if err != nil {
		t.Fatalf("ParseSupervisor: %v", err)
	}
	ctx := SupervisorContext{Triggers: []SupervisorTrigger{{Kind: "sweep"}}, State: map[string]any{}}
	var te *TemplateError
	if _, err := tmpl.Render(ctx); !errors.As(err, &te) || te.Kind != ErrTemplateRender {
		t.Errorf("Render of an unknown variable = %v, want a render error", err)
	}
	ok, _ := ParseSupervisor("fine", "supervisor.md")
	if _, err := ok.Render(SupervisorContext{State: map[string]any{}}); err == nil {
		t.Error("Render without a trigger succeeded, want an error")
	}
	if _, err := ParseSupervisor("{{ .trigger", "supervisor.md"); !errors.As(err, &te) || te.Kind != ErrTemplateParse {
		t.Errorf("ParseSupervisor of broken syntax = %v, want a parse error", err)
	}
}
