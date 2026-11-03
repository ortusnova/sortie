package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/environment/smolvm"
	"github.com/sortie-ai/sortie/internal/hostview"
)

// The host view tells Sortie's machines by the label the smolvm
// environment puts on them.
func TestHostViewKnowsSortieMachines(t *testing.T) {
	if hostview.LabelSortieManaged != smolvm.LabelManaged {
		t.Fatalf("hostview reads %q, smolvm labels %q", hostview.LabelSortieManaged, smolvm.LabelManaged)
	}
}

func TestHostMode(t *testing.T) {
	for kind, want := range map[string]hostview.Mode{
		config.EnvironmentKindRemote: hostview.ModeElsewhere,
		config.EnvironmentKindSmolvm: hostview.ModeMachines,
		config.EnvironmentKindLocal:  hostview.ModeHost,
		"":                           hostview.ModeHost,
	} {
		if got := hostMode(kind); got != want {
			t.Errorf("hostMode(%q) = %v, want %v", kind, got, want)
		}
	}
}

func TestHostDiskPath(t *testing.T) {
	cache := t.TempDir()
	t.Setenv("XDG_CACHE_HOME", cache)
	t.Setenv("HOME", cache)
	root := filepath.Join(t.TempDir(), "workspaces")
	if got := hostDiskPath(config.EnvironmentKindSmolvm, root); got != root {
		t.Errorf("without smolvm's directory: %q, want the workspace root", got)
	}
	if got := hostDiskPath(config.EnvironmentKindLocal, root); got != root {
		t.Errorf("under local: %q", got)
	}
	dir, err := os.UserCacheDir()
	if err != nil {
		t.Skipf("no cache directory: %v", err)
	}
	machines := filepath.Join(dir, "smolvm")
	if err := os.MkdirAll(machines, 0o700); err != nil {
		t.Fatal(err)
	}
	if got := hostDiskPath(config.EnvironmentKindSmolvm, root); got != machines {
		t.Errorf("with smolvm's directory: %q, want %q", got, machines)
	}
}
