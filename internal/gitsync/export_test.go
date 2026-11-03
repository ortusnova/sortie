//go:build unix

package gitsync

import (
	"context"
	"crypto/rand"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/environment"
	"github.com/sortie-ai/sortie/internal/environment/local"
)

// rewritingEnv runs every command through Inner after rewrite changes
// its argv, standing in for a guest that answers differently than git
// would.
type rewritingEnv struct {
	local.Environment
	rewrite func(argv []string) []string
}

func (e rewritingEnv) Command(ctx context.Context, ws environment.Workspace, spec environment.CommandSpec) (*exec.Cmd, error) {
	spec.Argv = e.rewrite(slices.Clone(spec.Argv))
	return e.Environment.Command(ctx, ws, spec)
}

// serveBundle makes the guest's read of the exported bundle return the
// file at bundle instead.
func (f *syncFixture) serveBundle(bundle string) {
	f.env.Inner = rewritingEnv{rewrite: func(argv []string) []string {
		if argv[0] == "cat" {
			return []string{"cat", "--", bundle}
		}
		return argv
	}}
}

// seeded returns a fixture whose workspace was seeded.
func seeded(t *testing.T, key string, repo config.Repository) *syncFixture {
	t.Helper()
	f := newSyncFixture(t, key, repo)
	if err := f.syncer.Seed(context.Background(), f.env, f.ws); err != nil {
		t.Fatalf("Seed() error = %v", err)
	}
	return f
}

// agentCommit commits name with content on the guest's current branch,
// as an agent would, and returns the commit.
func (f *syncFixture) agentCommit(name string, content []byte) string {
	f.t.Helper()
	if err := os.WriteFile(filepath.Join(f.ws.HostPath, name), content, 0o600); err != nil {
		f.t.Fatal(err)
	}
	f.git("add", "--", name)
	f.git("commit", "--quiet", "-m", "agent "+name)
	return f.git("rev-parse", "HEAD")
}

// refs returns the mirror's refs, for before-and-after comparisons.
func (f *syncFixture) refs() string {
	f.t.Helper()
	return mirrorRefs(f.t, f.up.env, f.syncer.mirror)
}

// assertNoExportLeft checks that neither the guest's exported bundle nor
// a host temporary bundle directory remains.
func (f *syncFixture) assertNoExportLeft() {
	f.t.Helper()
	if _, err := os.Stat(filepath.Join(f.guestDir, "out.bundle")); !os.IsNotExist(err) {
		f.t.Errorf("guest out.bundle: Stat error = %v, want it removed", err)
	}
	f.assertNoBundlesLeft()
}

func TestExport_ImportsCommittedWorkOnly(t *testing.T) {
	f := seeded(t, "ENG-60", config.Repository{})
	head := f.agentCommit("feature.txt", []byte("feature\n"))
	if err := os.WriteFile(filepath.Join(f.ws.HostPath, "draft.txt"), []byte("uncommitted\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	before := len(f.argvs())

	res, err := f.syncer.Export(context.Background(), f.env, f.ws)
	if err != nil {
		t.Fatalf("Export() error = %v", err)
	}
	if res.Branch != "sortie/ENG-60" || res.Commit != head || res.NothingToExport {
		t.Errorf("Export() = %+v, want branch sortie/ENG-60 at %s", res, head)
	}
	out := filepath.Join(f.guestDir, "out.bundle")
	want := [][]string{
		{"git", "rev-parse", "--verify", "--quiet", "refs/heads/sortie/ENG-60"},
		{"sh", "-c", exportBundleScript, "sh", f.guestDir, "refs/heads/sortie/ENG-60", "refs/remotes/origin/main", "refs/remotes/origin/sortie/ENG-60"},
		{"cat", "--", out},
		{"rm", "-f", "--", out},
	}
	if got := f.guestCommandsSince(before); !slices.EqualFunc(got, want, slices.Equal) {
		t.Errorf("environment commands =\n%q\nwant\n%q", got, want)
	}
	m := f.syncer.mirror.Dir()
	if got := fixtureGit(t, f.up.env, m, "--git-dir="+m, "rev-parse", "refs/sortie/incoming/ENG-60"); got != head {
		t.Errorf("refs/sortie/incoming/ENG-60 = %s, want %s", got, head)
	}
	if got := fixtureGit(t, f.up.env, m, "--git-dir="+m, "ls-tree", "--name-only", head); got != "README\nfeature.txt" {
		t.Errorf("exported tree = %q, want only committed files", got)
	}
	if got := f.git("status", "--porcelain"); got != "?? draft.txt" {
		t.Errorf("guest status = %q, want the uncommitted file kept", got)
	}
	f.assertNoExportLeft()
}

func TestExport_MissingBranchExportsNothing(t *testing.T) {
	f := seeded(t, "ENG-61", config.Repository{})
	f.git("checkout", "--quiet", "--detach")
	f.git("branch", "--quiet", "-D", "sortie/ENG-61")
	refs := f.refs()
	before := len(f.argvs())

	res, err := f.syncer.Export(context.Background(), f.env, f.ws)
	if err != nil {
		t.Fatalf("Export() error = %v", err)
	}
	if !res.NothingToExport || res.Commit != "" || res.Pushed {
		t.Errorf("Export() = %+v, want nothing to export", res)
	}
	if got := f.guestCommandsSince(before); len(got) != 1 || got[0][1] != "rev-parse" {
		t.Errorf("environment commands = %q, want only the branch lookup", got)
	}
	if got := f.refs(); got != refs {
		t.Errorf("mirror refs =\n%s\nwant them unchanged:\n%s", got, refs)
	}
	f.assertNoExportLeft()
}

func TestExport_UnchangedBranchImportsNothing(t *testing.T) {
	t.Run("new branch at base", func(t *testing.T) {
		f := seeded(t, "ENG-62", config.Repository{})
		refs := f.refs()
		res, err := f.syncer.Export(context.Background(), f.env, f.ws)
		if err != nil {
			t.Fatalf("Export() error = %v", err)
		}
		if res.Commit != f.up.git("rev-parse", "main") || res.NothingToExport || res.Pushed || res.UpToDate {
			t.Errorf("Export() = %+v, want the base commit, no push, and no upstream issue branch", res)
		}
		if got := f.refs(); got != refs {
			t.Errorf("mirror refs =\n%s\nwant them unchanged:\n%s", got, refs)
		}
		f.assertNoExportLeft()
	})
	t.Run("upstream branch tip", func(t *testing.T) {
		f := newSyncFixture(t, "ENG-63", config.Repository{})
		f.up.git("checkout", "--quiet", "-b", "sortie/ENG-63")
		tip := f.up.commit("work.txt", "an earlier run\n")
		f.up.git("checkout", "--quiet", "main")
		if err := f.syncer.Seed(context.Background(), f.env, f.ws); err != nil {
			t.Fatalf("Seed() error = %v", err)
		}
		refs := f.refs()
		res, err := f.syncer.Export(context.Background(), f.env, f.ws)
		if err != nil {
			t.Fatalf("Export() error = %v", err)
		}
		if res.Commit != tip || res.Pushed || !res.UpToDate {
			t.Errorf("Export() = %+v, want the upstream tip %s up to date and no push", res, tip)
		}
		if got := f.refs(); got != refs {
			t.Errorf("mirror refs =\n%s\nwant them unchanged:\n%s", got, refs)
		}
	})
}

func TestExport_RefusesABundleNamingAnotherRef(t *testing.T) {
	f := seeded(t, "ENG-64", config.Repository{})
	f.agentCommit("feature.txt", []byte("feature\n"))
	f.git("branch", "main", "origin/main")
	evil := filepath.Join(t.TempDir(), "evil.bundle")
	f.git("bundle", "create", "--quiet", evil, "refs/heads/sortie/ENG-64", "refs/heads/main")
	f.serveBundle(evil)
	refs := f.refs()

	_, err := f.syncer.Export(context.Background(), f.env, f.ws)
	if err == nil || !strings.Contains(err.Error(), `"refs/heads/main"`) {
		t.Fatalf("Export() error = %v, want one naming refs/heads/main", err)
	}
	if got := f.refs(); got != refs {
		t.Errorf("mirror refs =\n%s\nwant them unchanged:\n%s", got, refs)
	}
	f.assertNoExportLeft()
}

func TestExport_RefusesAnOversizedBundle(t *testing.T) {
	f := seeded(t, "ENG-65", config.Repository{MaxBundleMiB: 1})
	blob := make([]byte, 5<<20)
	if _, err := rand.Read(blob); err != nil {
		t.Fatal(err)
	}
	f.agentCommit("big.bin", blob)
	refs := f.refs()

	_, err := f.syncer.Export(context.Background(), f.env, f.ws)
	if err == nil || !strings.Contains(err.Error(), "max_bundle_mib (1 MiB)") {
		t.Fatalf("Export() error = %v, want one naming the limit", err)
	}
	if got := f.refs(); got != refs {
		t.Errorf("mirror refs =\n%s\nwant them unchanged:\n%s", got, refs)
	}
	f.assertNoExportLeft()
}

// bigBase gives the upstream's main a random blob of size bytes, so a
// bundle that carries the base's history is larger than size.
func bigBase(t *testing.T, f *syncFixture, size int) {
	t.Helper()
	blob := make([]byte, size)
	if _, err := rand.Read(blob); err != nil {
		t.Fatal(err)
	}
	f.up.commit("history.bin", string(blob))
}

func TestExport_BundleCarriesOnlyTheRunsWork(t *testing.T) {
	t.Run("new branch", func(t *testing.T) {
		f := newSyncFixture(t, "ENG-70", config.Repository{MaxBundleMiB: 1})
		bigBase(t, f, 3<<20)
		if err := f.syncer.Seed(context.Background(), f.env, f.ws); err != nil {
			t.Fatalf("Seed() error = %v", err)
		}
		head := f.agentCommit("feature.txt", []byte("feature\n"))

		res, err := f.syncer.Export(context.Background(), f.env, f.ws)
		if err != nil {
			t.Fatalf("Export() error = %v, want the base's history left out of the bundle", err)
		}
		if res.Commit != head || !res.Pushed {
			t.Errorf("Export() = %+v, want %s pushed", res, head)
		}
		if got := f.up.git("rev-parse", "sortie/ENG-70"); got != head {
			t.Errorf("upstream sortie/ENG-70 = %s, want %s", got, head)
		}
		f.assertNoExportLeft()
	})
	t.Run("on an earlier push", func(t *testing.T) {
		f := newSyncFixture(t, "ENG-71", config.Repository{MaxBundleMiB: 1})
		f.up.git("checkout", "--quiet", "-b", "sortie/ENG-71")
		bigBase(t, f, 3<<20)
		f.up.git("checkout", "--quiet", "main")
		if err := f.syncer.Seed(context.Background(), f.env, f.ws); err != nil {
			t.Fatalf("Seed() error = %v", err)
		}
		head := f.agentCommit("more.txt", []byte("more\n"))

		// Sortie never pushed this branch, so the lease refuses the push;
		// the import before it shows the bundle fit.
		_, err := f.syncer.Export(context.Background(), f.env, f.ws)
		if !errors.Is(err, ErrBranchMoved) {
			t.Fatalf("Export() error = %v, want only the lease's refusal, after the import", err)
		}
		m := f.syncer.mirror.Dir()
		if got := fixtureGit(t, f.up.env, m, "--git-dir="+m, "rev-parse", "refs/sortie/incoming/ENG-71"); got != head {
			t.Errorf("refs/sortie/incoming/ENG-71 = %s, want %s", got, head)
		}
	})
	t.Run("nothing new on a large base", func(t *testing.T) {
		f := newSyncFixture(t, "ENG-72", config.Repository{MaxBundleMiB: 1})
		bigBase(t, f, 3<<20)
		f.up.commit("small.txt", "small\n")
		if err := f.syncer.Seed(context.Background(), f.env, f.ws); err != nil {
			t.Fatalf("Seed() error = %v", err)
		}
		res, err := f.syncer.Export(context.Background(), f.env, f.ws)
		if err != nil {
			t.Fatalf("Export() error = %v, want the tip alone bundled", err)
		}
		if res.Commit != f.up.git("rev-parse", "main") || res.Pushed {
			t.Errorf("Export() = %+v, want the base commit and no push", res)
		}
	})
}

func TestExport_RefusesABranchThatIsNotACommit(t *testing.T) {
	f := seeded(t, "ENG-66", config.Repository{})
	f.agentCommit("feature.txt", []byte("feature\n"))
	// git refuses to point a branch at a tag, so the bundle of a tag is
	// renamed in its header, as a hostile guest could.
	f.git("tag", "-a", "-m", "a tag", "v1")
	tagged := filepath.Join(t.TempDir(), "tag.bundle")
	f.git("bundle", "create", "--quiet", tagged, "refs/tags/v1")
	data, err := os.ReadFile(tagged) //nolint:gosec // G304: the test's own file
	if err != nil {
		t.Fatal(err)
	}
	evil := filepath.Join(t.TempDir(), "evil.bundle")
	if err := os.WriteFile(evil, []byte(strings.Replace(string(data), "refs/tags/v1", "refs/heads/sortie/ENG-66", 1)), 0o600); err != nil {
		t.Fatal(err)
	}
	f.serveBundle(evil)
	refs := f.refs()

	_, err = f.syncer.Export(context.Background(), f.env, f.ws)
	if err == nil || !strings.Contains(err.Error(), "not a commit") {
		t.Fatalf("Export() error = %v, want one about a non-commit", err)
	}
	if got := f.refs(); got != refs {
		t.Errorf("mirror refs =\n%s\nwant the incoming ref restored:\n%s", got, refs)
	}
}

func TestExport_UnreachableUpstream(t *testing.T) {
	f := seeded(t, "ENG-67", config.Repository{})
	if err := os.Rename(f.up.dir, f.up.dir+"-gone"); err != nil {
		t.Fatal(err)
	}

	// Nothing new: the mirror's last state shows it, with a warning.
	res, err := f.syncer.Export(context.Background(), f.env, f.ws)
	if err != nil || res.Pushed {
		t.Fatalf("Export() of an unchanged branch = %+v, %v; want success without a push", res, err)
	}
	if !strings.Contains(f.logs.String(), "upstream fetch failed") {
		t.Errorf("logs = %q, want a warning about the upstream fetch", f.logs.String())
	}

	// New commits need the upstream.
	f.agentCommit("feature.txt", []byte("feature\n"))
	refs := f.refs()
	if _, err := f.syncer.Export(context.Background(), f.env, f.ws); err == nil || !strings.Contains(err.Error(), "fetch") {
		t.Fatalf("Export() of new commits error = %v, want the upstream fetch failure", err)
	}
	if got := f.refs(); got != refs {
		t.Errorf("mirror refs =\n%s\nwant them unchanged:\n%s", got, refs)
	}
}
