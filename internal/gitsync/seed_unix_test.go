//go:build unix

package gitsync

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sortie-ai/sortie/internal/config"
)

// workerClone clones the seed at path into a new directory, as a worker
// does, and returns the directory.
func (f *syncFixture) workerClone(seed string) string {
	f.t.Helper()
	dir := filepath.Join(f.t.TempDir(), "clone")
	fixtureGit(f.t, f.up.env, filepath.Dir(dir), "clone", "--quiet", seed, dir)
	return dir
}

// workerCommit commits name in the clone at dir and returns the commit.
func (f *syncFixture) workerCommit(dir, name string) string {
	f.t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(name+"\n"), 0o600); err != nil {
		f.t.Fatal(err)
	}
	fixtureGit(f.t, f.up.env, dir, "add", "--", name)
	fixtureGit(f.t, f.up.env, dir, "commit", "--quiet", "-m", "worker "+name)
	return fixtureGit(f.t, f.up.env, dir, "rev-parse", "HEAD")
}

func (f *syncFixture) buildSeed() (Seed, string) {
	f.t.Helper()
	dst := filepath.Join(f.t.TempDir(), "seed.bundle")
	seed, err := f.syncer.BuildSeed(context.Background(), "ONS-30", dst)
	if err != nil {
		f.t.Fatalf("BuildSeed() error = %v", err)
	}
	return seed, dst
}

// remote-runs: Seed. git clone of the seed gives the repository on the
// issue branch, at the base branch for a new issue branch.
func TestBuildSeed_NewIssueBranchCheckedOut(t *testing.T) {
	f := newSyncFixture(t, "ONS-30", config.Repository{})
	seed, path := f.buildSeed()

	main := f.up.git("rev-parse", "main")
	if seed.Branch != "sortie/ONS-30" || seed.Commit != main {
		t.Fatalf("seed = %+v, want sortie/ONS-30 at main %s", seed, main)
	}
	dir := f.workerClone(path)
	if got := fixtureGit(t, f.up.env, dir, "rev-parse", "--abbrev-ref", "HEAD"); got != "sortie/ONS-30" {
		t.Errorf("clone is on %q, want sortie/ONS-30", got)
	}
	if got := fixtureGit(t, f.up.env, dir, "rev-parse", "HEAD"); got != main {
		t.Errorf("clone HEAD = %s, want main %s", got, main)
	}
	if got := fixtureGit(t, f.up.env, dir, "cat-file", "-p", "HEAD:README"); got != "hello" {
		t.Errorf("README = %q, want the upstream's", got)
	}
	if after := f.refs(); strings.Contains(after, "sortie/ONS-30") || !strings.Contains(after, "refs/heads/main") {
		t.Errorf("mirror refs after the seed = %q, want no issue branch added", after)
	}
}

func TestBuildSeed_ExistingUpstreamBranchAtItsTip(t *testing.T) {
	f := newSyncFixture(t, "ONS-30", config.Repository{})
	tip := f.humanCommit("sortie/ONS-30", "earlier.txt")
	seed, path := f.buildSeed()
	if seed.Commit != tip {
		t.Fatalf("seed commit = %s, want the upstream branch tip %s", seed.Commit, tip)
	}
	if got := fixtureGit(t, f.up.env, f.workerClone(path), "rev-parse", "HEAD"); got != tip {
		t.Errorf("clone HEAD = %s, want %s", got, tip)
	}
}

// returnBundle bundles the clone's issue branch less the seed, as a
// worker returns it.
func (f *syncFixture) returnBundle(dir string, refs ...string) string {
	f.t.Helper()
	out := filepath.Join(f.t.TempDir(), "out.bundle")
	fixtureGit(f.t, f.up.env, dir, append([]string{"bundle", "create", "--quiet", out}, refs...)...)
	return out
}

// remote-runs: Pull request. Two commits on the issue branch come back
// as a bundle and are pushed.
func TestImportReturned_PushesTheNewCommits(t *testing.T) {
	f := newSyncFixture(t, "ONS-30", config.Repository{})
	seed, path := f.buildSeed()
	dir := f.workerClone(path)
	f.workerCommit(dir, "one.txt")
	second := f.workerCommit(dir, "two.txt")
	bundle := f.returnBundle(dir, "refs/heads/sortie/ONS-30", "^origin/sortie/ONS-30")

	res, err := f.syncer.ImportReturned(context.Background(), "ONS-30", bundle, seed)
	if err != nil {
		t.Fatalf("ImportReturned() error = %v", err)
	}
	if !res.Pushed || res.Commit != second || res.Branch != "sortie/ONS-30" {
		t.Fatalf("result = %+v, want %s pushed", res, second)
	}
	if got := f.upstreamBranch("sortie/ONS-30"); got != second {
		t.Errorf("upstream branch = %s, want %s", got, second)
	}
	if got := f.up.git("rev-list", "--count", "main..sortie/ONS-30"); got != "2" {
		t.Errorf("upstream branch holds %s new commits, want 2", got)
	}
}

func TestImportReturned_UnchangedBranchPushesNothing(t *testing.T) {
	f := newSyncFixture(t, "ONS-30", config.Repository{})
	seed, path := f.buildSeed()
	bundle := f.returnBundle(f.workerClone(path), "refs/heads/sortie/ONS-30")
	res, err := f.syncer.ImportReturned(context.Background(), "ONS-30", bundle, seed)
	if err != nil || res.Pushed {
		t.Fatalf("result = %+v, %v; want nothing pushed", res, err)
	}
	if got := f.upstreamBranch("sortie/ONS-30"); got != "" {
		t.Errorf("upstream branch = %s, want none", got)
	}
}

// remote-runs: Not the branch. A bundle naming another ref, or commits
// not on top of the seed, is refused naming the ref, and nothing is
// pushed.
func TestImportReturned_RefusesNotTheBranch(t *testing.T) {
	for name, prepare := range map[string]func(f *syncFixture, dir string) (string, string){
		"another ref": func(f *syncFixture, dir string) (string, string) {
			fixtureGit(f.t, f.up.env, dir, "checkout", "--quiet", "-b", "other")
			f.workerCommit(dir, "one.txt")
			return f.returnBundle(dir, "refs/heads/other", "^origin/sortie/ONS-30"), `"refs/heads/other"`
		},
		"not on top of the seed": func(f *syncFixture, dir string) (string, string) {
			fixtureGit(f.t, f.up.env, dir, "checkout", "--quiet", "--orphan", "fresh")
			f.workerCommit(dir, "one.txt")
			fixtureGit(f.t, f.up.env, dir, "branch", "--force", "sortie/ONS-30", "fresh")
			return f.returnBundle(dir, "refs/heads/sortie/ONS-30"), "refs/heads/sortie/ONS-30"
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newSyncFixture(t, "ONS-30", config.Repository{})
			seed, path := f.buildSeed()
			bundle, ref := prepare(f, f.workerClone(path))
			before := f.refs()

			res, err := f.syncer.ImportReturned(context.Background(), "ONS-30", bundle, seed)
			if err == nil || !strings.Contains(err.Error(), ref) {
				t.Fatalf("ImportReturned() = %+v, %v; want a refusal naming %s", res, err, ref)
			}
			if res.Pushed || f.upstreamBranch("sortie/ONS-30") != "" {
				t.Errorf("result %+v, upstream branch %q; want nothing pushed", res, f.upstreamBranch("sortie/ONS-30"))
			}
			if after := f.refs(); after != before {
				t.Errorf("mirror refs changed:\n%s\nwant\n%s", after, before)
			}
		})
	}
}
