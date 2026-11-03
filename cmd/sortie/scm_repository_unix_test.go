//go:build unix

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/registry"
	"github.com/sortie-ai/sortie/internal/workflow"
)

// repositoryWorkflow returns a workflow under the remote kind whose
// repository names provider, followed by extra top-level YAML.
func repositoryWorkflow(t *testing.T, provider, extra string) (dir, path string) {
	t.Helper()
	dir = t.TempDir()
	issues := filepath.Join(dir, "issues.json")
	if err := os.WriteFile(issues, []byte("[]"), 0o600); err != nil {
		t.Fatal(err)
	}
	var flows strings.Builder
	for _, f := range config.RemoteFlows {
		fmt.Fprintf(&flows, "      %s: { url: https://edge.example/%s }\n", f, f)
	}
	content := fmt.Sprintf(`---
tracker:
  kind: file
  project: DEMO
  active_states:
    - "To Do"
  terminal_states:
    - Done
  handoff_state: Review
file:
  path: %q
agent:
  kind: mock
  max_turns: 1
polling:
  interval_ms: 500
workspace:
  root: %q
environment:
  kind: remote
  remote:
    image: ghcr.io/ortusnova/agent:1
    flows:
%s    repository:
      url: https://example.com/org/app.git
      base: main
      provider: %s
      project: org/app
%s
---
Fix issue {{ .issue.identifier }}.
`, issues, filepath.Join(dir, "workspaces"), flows.String(), provider, extra)
	return dir, writeCustomWorkflowFile(t, dir, []byte(content))
}

// runFor runs the program on path until it exits or the test timeout
// passes, and returns its exit code and log.
func runFor(t *testing.T, path string) (int, string) {
	t.Helper()
	var stdout bytes.Buffer
	var stderr lockedBuf
	code := runUntilStarted(t, []string{"--port", "0", path}, &stdout, &stderr)
	return code, stderr.String()
}

// validateErrors runs sortie validate on path and returns its errors.
func validateErrors(t *testing.T, path string) []validateDiag {
	t.Helper()
	var stdout, stderr bytes.Buffer
	run(context.Background(), []string{"validate", "--format", "json", path}, &stdout, &stderr)
	var out validateOutput
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		t.Fatalf("validate output %q: %v; stderr:\n%s", stdout.String(), err, stderr.String())
	}
	return out.Errors
}

// No t.Parallel: run sets up the process-wide logger, and t.Chdir.
func TestRun_RepositoryProviderMismatchStopsStartup(t *testing.T) {
	dir, path := repositoryWorkflow(t, "gitea", `gitea:
  api_key: unused
  endpoint: https://gitea.example
github:
  api_key: unused
reactions:
  ci_failure:
    provider: github
    max_retries: 2
    escalation: label`)
	t.Chdir(dir)

	d := diagWithCheck(validateErrors(t, path), "config."+repositoryProviderField)
	if d == nil || !strings.Contains(d.Message, "reactions.ci_failure.provider") {
		t.Errorf("validate diagnostic = %+v, want one for %s naming reactions.ci_failure.provider", d, repositoryProviderField)
	}

	code, logs := runFor(t, path)
	if code != 1 {
		t.Fatalf("exit code = %d, want 1; stderr:\n%s", code, logs)
	}
	if !strings.Contains(logs, repositoryProviderField) || !strings.Contains(logs, "reactions.ci_failure.provider") {
		t.Errorf("stderr = %s, want both settings named", logs)
	}
}

func TestRun_RepositoryProviderWithoutPullRequestsStopsStartup(t *testing.T) {
	dir, path := repositoryWorkflow(t, scmWithoutPullRequestsKind, "")
	t.Chdir(dir)

	if d := diagWithCheck(validateErrors(t, path), "config."+repositoryProviderField); d == nil || !strings.Contains(d.Message, "cannot open pull requests") {
		t.Errorf("validate diagnostic = %+v, want one saying the adapter cannot open pull requests", d)
	}
	code, logs := runFor(t, path)
	if code != 1 || !strings.Contains(logs, "cannot open pull requests") {
		t.Fatalf("exit code = %d, want 1 naming the missing capability; stderr:\n%s", code, logs)
	}
}

func TestRun_ExportAloneBuildsTheSCMAdapter(t *testing.T) {
	t.Run("a valid adapter configuration starts", func(t *testing.T) {
		dir, path := repositoryWorkflow(t, "github", `github:
  api_key: unused`)
		t.Chdir(dir)

		code, logs := runFor(t, path)
		if code != 0 {
			t.Fatalf("exit code = %d, want 0; stderr:\n%s", code, logs)
		}
		if !strings.Contains(logs, "pull requests for pushed issue branches enabled") || strings.Contains(logs, "failed to construct SCM adapter") {
			t.Errorf("stderr = %s, want the pull request adapter built for export alone", logs)
		}
		if strings.Contains(logs, "review comment routing enabled") || strings.Contains(logs, "auto_merge reaction enabled") {
			t.Errorf("stderr = %s, want no SCM reaction enabled", logs)
		}
	})

	t.Run("an invalid adapter configuration stops startup", func(t *testing.T) {
		// The gitea adapter requires an endpoint; only the export uses it.
		dir, path := repositoryWorkflow(t, "gitea", `gitea:
  api_key: unused`)
		t.Chdir(dir)

		code, logs := runFor(t, path)
		if code != 1 || !strings.Contains(logs, "failed to construct SCM adapter") {
			t.Fatalf("exit code = %d, want 1 after the adapter failed to build; stderr:\n%s", code, logs)
		}
	})
}

func TestWorkflowValidate_ReloadRefusesARepositoryProviderMismatch(t *testing.T) {
	t.Parallel()

	_, path := repositoryWorkflow(t, "github", "")
	mgr, err := workflow.NewManager(path, slog.New(slog.DiscardHandler),
		workflow.WithValidateFunc(workflowValidate),
		workflow.WithAgentKindProbe(registry.Agents.Has),
		workflow.WithRetiredAgents(registry.RetiredAgentOf),
		workflow.WithAdvisoryFunc(workflowAdvisories))
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	mismatched := strings.Replace(string(data), "\n---\nFix", "\nreactions:\n  ci_failure:\n    provider: gitlab\n---\nFix", 1)
	if err := os.WriteFile(path, []byte(mismatched), 0o600); err != nil {
		t.Fatal(err)
	}
	reloadErr := mgr.Reload()
	if reloadErr == nil || !strings.Contains(reloadErr.Error(), "reactions.ci_failure.provider") {
		t.Fatalf("Reload() error = %v, want a refusal naming reactions.ci_failure.provider", reloadErr)
	}
	if mgr.Config().CIFeedback.Kind != "" {
		t.Errorf("CI feedback kind after a rejected reload = %q, want the previous empty one", mgr.Config().CIFeedback.Kind)
	}
}
