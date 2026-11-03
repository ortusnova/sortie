package prompt

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"text/template"
)

// supervisorTemplateName is the parse name of a supervisor prompt.
const supervisorTemplateName = "supervisor"

// SupervisorTrigger is one event that started a supervisor session, as the
// "trigger" and "triggers" template variables carry it.
type SupervisorTrigger struct {
	Kind            string
	IssueIdentifier string
	Detail          string
	At              string
}

// SupervisorContext is what a supervisor session's prompt is rendered
// with. Triggers holds at least one trigger, the first the one the
// "trigger" variable names. State is the state digest; it must marshal to
// a JSON object.
type SupervisorContext struct {
	Token    string
	APIURL   string
	Triggers []SupervisorTrigger
	State    any
}

// SupervisorTemplate is a parsed supervisor prompt. Obtain via
// [ParseSupervisor]. Safe for concurrent [SupervisorTemplate.Render] calls.
type SupervisorTemplate struct {
	tmpl   *template.Template
	source string
}

// ParseSupervisor compiles a supervisor prompt body read from source with
// strict mode (missingkey=error) and the standard FuncMap. Returns a
// [*TemplateError] with Kind [ErrTemplateParse] on failure.
func ParseSupervisor(body, source string) (*SupervisorTemplate, error) {
	tmpl, err := newSet(supervisorTemplateName).Parse(body)
	if err != nil {
		return nil, &TemplateError{Kind: ErrTemplateParse, Source: source, Line: leadingLine(err, supervisorTemplateName), Err: err}
	}
	return &SupervisorTemplate{tmpl: tmpl, source: source}, nil
}

func supervisorTriggerToMap(t SupervisorTrigger) map[string]any {
	return map[string]any{
		"kind":             t.Kind,
		"issue_identifier": t.IssueIdentifier,
		"detail":           t.Detail,
		"at":               t.At,
	}
}

// Render executes the prompt with the template variables "trigger" (the
// first trigger), "triggers" (every trigger), "state" (the digest as
// nested maps and lists), "state_json" (the digest as indented JSON), and
// "supervisor" (with "token" and "api_url"), then appends a section that
// holds the token, the API URL, every trigger and the digest, so a prompt
// that references none of them still carries them. Returns a
// [*TemplateError] with Kind [ErrTemplateRender] on failure.
func (t *SupervisorTemplate) Render(c SupervisorContext) (string, error) {
	if len(c.Triggers) == 0 {
		return "", &TemplateError{Kind: ErrTemplateRender, Source: t.source, Err: fmt.Errorf("a supervisor prompt needs a trigger")}
	}
	stateJSON, err := json.MarshalIndent(c.State, "", "  ")
	if err != nil {
		return "", &TemplateError{Kind: ErrTemplateRender, Source: t.source, Err: fmt.Errorf("state digest: %w", err)}
	}
	var state map[string]any
	if err := json.Unmarshal(stateJSON, &state); err != nil {
		return "", &TemplateError{Kind: ErrTemplateRender, Source: t.source, Err: fmt.Errorf("state digest: %w", err)}
	}
	triggers := make([]any, 0, len(c.Triggers))
	for _, tr := range c.Triggers {
		triggers = append(triggers, supervisorTriggerToMap(tr))
	}
	vars := map[string]any{
		"trigger":    supervisorTriggerToMap(c.Triggers[0]),
		"triggers":   triggers,
		"state":      state,
		"state_json": string(stateJSON),
		"supervisor": map[string]any{"token": c.Token, "api_url": c.APIURL},
	}

	var buf bytes.Buffer
	if err := t.tmpl.Execute(&buf, vars); err != nil {
		return "", &TemplateError{Kind: ErrTemplateRender, Source: t.source, Line: leadingLine(err, supervisorTemplateName), Err: err}
	}
	out := strings.TrimRight(buf.String(), "\n")

	var b strings.Builder
	b.WriteString(out)
	b.WriteString("\n\n## Sortie supervisor session\n\n")
	fmt.Fprintf(&b, "Sortie API: %s\n", c.APIURL)
	fmt.Fprintf(&b, "Supervisor token (send as `Authorization: Bearer <token>`): %s\n\n", c.Token)
	b.WriteString("What started this session:\n\n")
	for _, tr := range c.Triggers {
		fmt.Fprintf(&b, "- %s", tr.Kind)
		if tr.IssueIdentifier != "" {
			fmt.Fprintf(&b, " %s", tr.IssueIdentifier)
		}
		if tr.Detail != "" {
			fmt.Fprintf(&b, ": %s", tr.Detail)
		}
		if tr.At != "" {
			fmt.Fprintf(&b, " (%s)", tr.At)
		}
		b.WriteString("\n")
	}
	b.WriteString("\nState digest:\n\n```json\n")
	b.Write(stateJSON)
	b.WriteString("\n```\n")
	return b.String(), nil
}
