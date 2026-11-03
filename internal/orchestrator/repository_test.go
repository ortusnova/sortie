package orchestrator

import (
	"log/slog"
	"testing"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/gitsync"
)

func TestRepositorySync(t *testing.T) {
	t.Parallel()

	repo := &config.Repository{URL: "https://example.com/org/app.git", Base: "main", BranchPrefix: "sortie/", MaxBundleMiB: 512}
	state := t.TempDir()
	logger := slog.Default()

	for _, cfg := range []config.EnvironmentConfig{
		{},
		{Kind: config.EnvironmentKindLocal},
		{Kind: config.EnvironmentKindRemote},
	} {
		got, err := repositorySync(cfg, state, logger)
		if err != nil || got != nil {
			t.Errorf("repositorySync(%+v) = %v, %v; want nil, nil", cfg, got, err)
		}
	}

	smolvm := config.EnvironmentConfig{Kind: config.EnvironmentKindRemote, Remote: config.RemoteConfig{Repository: repo}}
	got, err := repositorySync(smolvm, state, logger)
	if err != nil {
		t.Fatalf("repositorySync(smolvm with repository) error = %v", err)
	}
	syncer, ok := got.(*gitsync.Syncer)
	if !ok {
		t.Fatalf("repositorySync(smolvm with repository) = %T, want *gitsync.Syncer", got)
	}
	if branch := syncer.IssueBranch("ENG-42"); branch != "sortie/ENG-42" {
		t.Errorf("IssueBranch(ENG-42) = %q, want sortie/ENG-42", branch)
	}

	if _, err := repositorySync(smolvm, "", logger); err == nil {
		t.Error("repositorySync(no state directory) error = nil, want an error")
	}
}
