//go:build unix

package gitsync

import (
	"context"
	"errors"
	"os"
	"os/exec"
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

// workerSeed seeds a workspace from the seed at path as the worker daemon
// does: it fetches every ref of the bundle as is, checks out branch, and
// returns the directory and the commits of the bundle's refs, which the
// worker's returned bundle excludes.
func (f *syncFixture) workerSeed(seed, branch string) (string, []string) {
	f.t.Helper()
	dir := filepath.Join(f.t.TempDir(), "workspace")
	fixtureGit(f.t, f.up.env, filepath.Dir(dir), "init", "--quiet", dir)
	fixtureGit(f.t, f.up.env, dir, "fetch", "--quiet", "--update-head-ok", seed, "+refs/*:refs/*")
	fixtureGit(f.t, f.up.env, dir, "symbolic-ref", "HEAD", "refs/heads/"+branch)
	fixtureGit(f.t, f.up.env, dir, "reset", "--quiet", "--hard")
	var tips []string
	for line := range strings.SplitSeq(fixtureGit(f.t, f.up.env, dir, "bundle", "list-heads", seed), "\n") {
		if sha, _, ok := strings.Cut(line, " "); ok {
			tips = append(tips, sha)
		}
	}
	return dir, tips
}

// remote-runs: Seed. The worker's workspace holds the base branch as
// origin/<base> and an upstream issue branch as origin/<issue branch>,
// as a local workspace's does, so a run told to merge origin/<base>
// finds it.
func TestBuildSeed_WorkerWorkspaceHoldsTheBaseAsOrigin(t *testing.T) {
	f := newSyncFixture(t, "ONS-30", config.Repository{})
	tip := f.humanCommit("sortie/ONS-30", "earlier.txt")
	main := f.up.commit("later.txt", "the base moved on\n")
	_, path := f.buildSeed()

	dir, _ := f.workerSeed(path, "sortie/ONS-30")
	for ref, want := range map[string]string{"HEAD": tip, "origin/main": main, "origin/sortie/ONS-30": tip} {
		if got := fixtureGit(t, f.up.env, dir, "rev-parse", ref); got != want {
			t.Errorf("%s in the worker's workspace = %s, want %s", ref, got, want)
		}
	}
}

// remote-runs: Merge conflict. A rebase run merges origin/<base> into a
// conflicting issue branch in the worker's workspace, and the returned
// merge is pushed: the upstream issue branch then holds the base.
func TestImportReturned_PushesAMergeOfTheBase(t *testing.T) {
	f := newSyncFixture(t, "ONS-30", config.Repository{})
	f.humanCommit("sortie/ONS-30", "shared.txt")
	main := f.up.commit("shared.txt", "the base changed it too\n")
	seed, path := f.buildSeed()

	dir, tips := f.workerSeed(path, "sortie/ONS-30")
	merge := exec.Command("git", "merge", "--quiet", "origin/main") //nolint:gosec // G204: the test's own command
	merge.Dir, merge.Env = dir, f.up.env
	if out, err := merge.CombinedOutput(); err == nil {
		t.Fatalf("git merge origin/main succeeded, want a conflict: %s", out)
	}
	fixtureGit(t, f.up.env, dir, "rev-parse", "--verify", "MERGE_HEAD")
	if err := os.WriteFile(filepath.Join(dir, "shared.txt"), []byte("both\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	fixtureGit(t, f.up.env, dir, "add", "--", "shared.txt")
	fixtureGit(t, f.up.env, dir, "commit", "--quiet", "--no-edit")
	merged := fixtureGit(t, f.up.env, dir, "rev-parse", "HEAD")
	bundle := f.returnBundle(dir, append([]string{"refs/heads/sortie/ONS-30", "--not"}, tips...)...)

	res, err := f.syncer.ImportReturned(context.Background(), "ONS-30", bundle, seed)
	if err != nil {
		t.Fatalf("ImportReturned() error = %v", err)
	}
	if !res.Pushed || res.Commit != merged {
		t.Fatalf("result = %+v, want the merge %s pushed", res, merged)
	}
	if got := f.upstreamBranch("sortie/ONS-30"); got != merged {
		t.Errorf("upstream branch = %s, want %s", got, merged)
	}
	f.up.git("merge-base", "--is-ancestor", main, "sortie/ONS-30")
}

// remote-runs: Pull request. The returned branch is pushed while the
// upstream branch still holds the commit the seed carried, though a
// person pushed that commit after Sortie's last push; a push made after
// the seed was built is refused, and nothing is pushed.
func TestImportReturned_LeaseIsTheSeededUpstreamCommit(t *testing.T) {
	for name, tc := range map[string]struct {
		pushAfterSeed bool
		wantErr       error
	}{
		"upstream unchanged since the seed": {},
		"upstream moved after the seed":     {pushAfterSeed: true, wantErr: ErrBranchMoved},
	} {
		t.Run(name, func(t *testing.T) {
			f := newSyncFixture(t, "ONS-30", config.Repository{})
			seeded := f.humanCommit("sortie/ONS-30", "human.txt")
			seed, path := f.buildSeed()
			dir, tips := f.workerSeed(path, "sortie/ONS-30")
			mine := f.workerCommit(dir, "worker.txt")
			want := mine
			if tc.pushAfterSeed {
				want = f.humanCommit("sortie/ONS-30", "later.txt")
			}
			bundle := f.returnBundle(dir, append([]string{"refs/heads/sortie/ONS-30", "--not"}, tips...)...)

			res, err := f.syncer.ImportReturned(context.Background(), "ONS-30", bundle, seed)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("ImportReturned() = %+v, %v; want error %v", res, err, tc.wantErr)
			}
			if got := f.upstreamBranch("sortie/ONS-30"); got != want {
				t.Errorf("upstream branch = %s, want %s (seeded %s)", got, want, seeded)
			}
		})
	}
}
