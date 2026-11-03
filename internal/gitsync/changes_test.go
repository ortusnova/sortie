//go:build unix

package gitsync

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"testing"

	"github.com/sortie-ai/sortie/internal/config"
)

func TestChangedPaths_NoMirrorHasNone(t *testing.T) {
	f := newSyncFixture(t, "ENG-80", config.SmolvmRepository{})
	paths, err := f.syncer.ChangedPaths(context.Background(), "ENG-80")
	if err != nil || paths != nil {
		t.Fatalf("ChangedPaths() before any seed = %v, %v, want none", paths, err)
	}
	if _, err := os.Stat(f.syncer.mirror.Dir()); !os.IsNotExist(err) {
		t.Errorf("mirror Stat error = %v, want the mirror left uncreated", err)
	}
}

func TestChangedPaths_PushedBranchAndRefusedImport(t *testing.T) {
	f := seeded(t, "ENG-81", config.SmolvmRepository{})
	ctx := context.Background()
	if paths, err := f.syncer.ChangedPaths(ctx, "ENG-81"); err != nil || len(paths) != 0 {
		t.Fatalf("ChangedPaths() of a seeded branch = %v, %v, want none", paths, err)
	}

	f.agentCommit("feature.txt", []byte("feature\n"))
	if _, err := f.export(); err != nil {
		t.Fatalf("Export() error = %v", err)
	}
	if err := os.MkdirAll(filepath.Join(f.ws.HostPath, ".github", "workflows"), 0o750); err != nil {
		t.Fatal(err)
	}
	refused := f.agentCommit(".github/workflows/ci.yml", []byte("on: push\n"))
	// An export whose push the forge refused leaves its import here.
	cmd := exec.Command("git", "--git-dir="+f.syncer.mirror.Dir(), "update-ref", IncomingRef("ENG-81"), refused) //nolint:gosec // G204: the test's own setup
	cmd.Env = f.up.env
	bundle := filepath.Join(t.TempDir(), "refused.bundle")
	f.git("bundle", "create", "--quiet", bundle, "HEAD")
	fetch := exec.Command("git", "--git-dir="+f.syncer.mirror.Dir(), "fetch", "--quiet", bundle, "HEAD") //nolint:gosec // G204: the test's own setup
	fetch.Env = f.up.env
	if out, err := fetch.CombinedOutput(); err != nil {
		t.Fatalf("fetch the refused commit into the mirror: %v: %s", err, out)
	}
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("update-ref: %v: %s", err, out)
	}

	paths, err := f.syncer.ChangedPaths(ctx, "ENG-81")
	if err != nil {
		t.Fatalf("ChangedPaths() error = %v", err)
	}
	if want := []string{".github/workflows/ci.yml", "feature.txt"}; !slices.Equal(paths, want) {
		t.Errorf("ChangedPaths() = %v, want %v", paths, want)
	}
}
