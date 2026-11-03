//go:build unix

package orchestrator

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/environment"
	"github.com/sortie-ai/sortie/internal/environment/environmenttest"
	"github.com/sortie-ai/sortie/internal/prompt"
	"github.com/sortie-ai/sortie/internal/workspace"
	"github.com/sortie-ai/sortie/internal/workspacekit"
)

// staleKind is a second environment kind registered by this file's
// tests, standing in for the kind that created an existing workspace.
const staleKind = "orchestrator-test-stale"

var (
	staleKindMu     sync.Mutex
	staleKindParams []environment.Params
	staleKindEnvs   []*environmenttest.Recorder
)

// registerStaleKind registers [staleKind] once; its constructor records
// its params and each environment it returns.
func registerStaleKind() {
	if environment.Registry.Has(staleKind) {
		return
	}
	environment.Registry.Register(staleKind, func(p environment.Params) (environment.Environment, error) {
		staleKindMu.Lock()
		defer staleKindMu.Unlock()
		env := &environmenttest.Recorder{KindName: staleKind}
		staleKindParams = append(staleKindParams, p)
		staleKindEnvs = append(staleKindEnvs, env)
		return env, nil
	})
}

// TestRunWorkerAttempt_WorkspaceOfAnotherKindIsRecreated pins the
// worker's wiring of a kind switch: an existing workspace without a
// record was created by the local kind, so a dispatch under another kind
// removes it through the registered local kind, prepares it again with
// after_create, and records the configured kind. A workspace recorded
// as another registered kind is removed by that kind, built from the
// attempt's environment block with only the kind changed.
func TestRunWorkerAttempt_WorkspaceOfAnotherKindIsRecreated(t *testing.T) {
	root := t.TempDir()
	cfg := defaultWorkerConfig(root)
	cfg.Agent.MaxTurns = 1
	cfg.Hooks.AfterCreate = "echo created > created.txt"

	issue := workerTestIssue()
	existing, err := workspace.Ensure(root, issue.Identifier)
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(existing.Path, "kept.txt"), []byte("old\n"), 0o600); err != nil {
		t.Fatalf("write workspace file: %v", err)
	}

	env := recordingEnvironment(t)
	ec := newExitCapture()
	RunWorkerAttempt(context.Background(), issue, nil, WorkerDeps{
		TrackerAdapter:         &mockTrackerAdapter{},
		AgentAdapter:           &mockAgentAdapter{},
		ConfigFunc:             func() config.ServiceConfig { return cfg },
		PromptTemplateByIDFunc: func(_ string) *prompt.Template { return mustParseTemplate(t, "{{ .issue.title }}") },
		OnEvent:                func(_ string, _ domain.AgentEvent) {},
		OnExit:                 ec.onExit,
		Logger:                 discardLogger(),
		EnvironmentFunc: func(config.EnvironmentConfig) (environment.Environment, error) {
			return env, nil
		},
	})
	result := ec.waitResult(t)

	if result.WorkspacePath != existing.Path {
		t.Fatalf("WorkspacePath = %q (error %v), want %q", result.WorkspacePath, result.Error, existing.Path)
	}
	if _, err := os.Stat(filepath.Join(existing.Path, "kept.txt")); !os.IsNotExist(err) {
		t.Errorf("kept.txt stat error = %v, want the stale workspace removed", err)
	}
	if _, err := os.Stat(filepath.Join(existing.Path, "created.txt")); err != nil {
		t.Errorf("after_create output: %v, want after_create run again", err)
	}
	data, err := workspacekit.ReadSortieFile(existing.Path, workspace.EnvironmentRecordFile, 4096)
	if err != nil {
		t.Fatalf("read environment record: %v", err)
	}
	if want := `{"kind":"` + env.Kind() + `"}`; string(data) != want {
		t.Errorf("record = %s, want %s", data, want)
	}

	// The workspace now records the recorder kind. Recorded as the
	// registered stale kind instead, the next dispatch removes it there.
	registerStaleKind()
	// The registry outlives a test, so a repeated run (-count) starts the
	// record of builds afresh.
	staleKindMu.Lock()
	staleKindParams, staleKindEnvs = nil, nil
	staleKindMu.Unlock()
	if err := workspacekit.WriteSortieFile(existing.Path, workspace.EnvironmentRecordFile, []byte(`{"kind":"`+staleKind+`"}`)); err != nil {
		t.Fatalf("write environment record: %v", err)
	}
	if err := os.WriteFile(filepath.Join(existing.Path, "kept.txt"), []byte("old\n"), 0o600); err != nil {
		t.Fatalf("write workspace file: %v", err)
	}
	cfg.Environment.HostPrefix = []string{"marker"}
	ec = newExitCapture()
	RunWorkerAttempt(context.Background(), issue, nil, WorkerDeps{
		TrackerAdapter:         &mockTrackerAdapter{},
		AgentAdapter:           &mockAgentAdapter{},
		ConfigFunc:             func() config.ServiceConfig { return cfg },
		PromptTemplateByIDFunc: func(_ string) *prompt.Template { return mustParseTemplate(t, "{{ .issue.title }}") },
		OnEvent:                func(_ string, _ domain.AgentEvent) {},
		OnExit:                 ec.onExit,
		Logger:                 discardLogger(),
		EnvironmentFunc: func(config.EnvironmentConfig) (environment.Environment, error) {
			return env, nil
		},
	})
	result = ec.waitResult(t)
	if result.WorkspacePath != existing.Path {
		t.Fatalf("second WorkspacePath = %q (error %v), want %q", result.WorkspacePath, result.Error, existing.Path)
	}
	if _, err := os.Stat(filepath.Join(existing.Path, "kept.txt")); !os.IsNotExist(err) {
		t.Errorf("second kept.txt stat error = %v, want the stale workspace removed", err)
	}

	staleKindMu.Lock()
	defer staleKindMu.Unlock()
	if len(staleKindEnvs) != 1 {
		t.Fatalf("stale kind built %d times, want 1", len(staleKindEnvs))
	}
	if got, want := staleKindEnvs[0].Ops(), []environmenttest.Operation{environmenttest.OpRemove}; !slices.Equal(got, want) {
		t.Errorf("stale kind ops = %q, want %q", got, want)
	}
	if p := staleKindParams[0]; p.Config.Kind != staleKind || !slices.Equal(p.Config.HostPrefix, []string{"marker"}) {
		t.Errorf("stale kind config = %+v, want the attempt's block with kind %q", p.Config, staleKind)
	}
}
