//go:build unix

package gitsync

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/environment"
	"github.com/sortie-ai/sortie/internal/environment/environmenttest"
)

// transferEnv is a recording environment that names its own transfer
// directory.
type transferEnv struct {
	*environmenttest.Recorder
	dir string
}

func (e transferEnv) GuestTransferDir() string { return e.dir }

var _ environment.TransferDirNamer = transferEnv{}

// An environment that names its transfer directory gets every bundle,
// in and out, there and never in the syncer's own.
func TestSeedAndExport_UseTheEnvironmentsTransferDir(t *testing.T) {
	f := newSyncFixture(t, "ENG-70", config.Repository{})
	dir := filepath.Join(t.TempDir(), "user-transfer")
	env := transferEnv{Recorder: f.env, dir: dir}
	ctx := context.Background()

	if err := f.syncer.Seed(ctx, env, f.ws); err != nil {
		t.Fatalf("Seed() error = %v", err)
	}
	in := filepath.Join(dir, "in.bundle")
	seed := f.argvs()
	if want := []string{"sh", "-c", `umask 077 && mkdir -p "$1" && cat > "$1/in.bundle"`, "sh", dir}; len(seed) == 0 || !slices.Equal(seed[0], want) {
		t.Fatalf("first seed command = %q, want %q", seed, want)
	}
	if !slices.ContainsFunc(seed, func(argv []string) bool { return slices.Contains(argv, in) }) {
		t.Errorf("seed commands = %q, want the clone from %s", seed, in)
	}

	head := f.agentCommit("feature.txt", []byte("feature\n"))
	before := len(f.argvs())
	res, err := f.syncer.Export(ctx, env, f.ws)
	if err != nil || res.Commit != head {
		t.Fatalf("Export() = %+v, %v; want %s exported", res, err, head)
	}
	out := filepath.Join(dir, "out.bundle")
	want := [][]string{
		{"git", "rev-parse", "--verify", "--quiet", "refs/heads/sortie/ENG-70"},
		{"sh", "-c", exportBundleScript, "sh", dir, "refs/heads/sortie/ENG-70", "refs/remotes/origin/main", "refs/remotes/origin/sortie/ENG-70"},
		{"cat", "--", out},
		{"rm", "-f", "--", out},
	}
	if got := f.guestCommandsSince(before); !slices.EqualFunc(got, want, slices.Equal) {
		t.Errorf("export commands =\n%q\nwant\n%q", got, want)
	}
	if _, err := os.Stat(f.guestDir); !os.IsNotExist(err) {
		t.Errorf("syncer's own directory %s: Stat error = %v, want it never created", f.guestDir, err)
	}
	for _, name := range []string{"in.bundle", "out.bundle"} {
		if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
			t.Errorf("%s: Stat error = %v, want it removed", name, err)
		}
	}
}
