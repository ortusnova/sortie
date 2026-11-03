//go:build unix

package gitsync

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/environment"
	"github.com/sortie-ai/sortie/internal/environment/environmenttest"
	"github.com/sortie-ai/sortie/internal/environment/local"
)

// syncFixture runs a [Syncer] against a local upstream, with every
// environment command running on this host in place of the guest: the
// workspace is a temporary directory, and so is the guest bundle
// directory.
type syncFixture struct {
	t        *testing.T
	up       *upstream
	syncer   *Syncer
	env      *environmenttest.Recorder
	ws       environment.Workspace
	guestDir string
	logs     *bytes.Buffer
}

func newSyncFixture(t *testing.T, key string, repo config.Repository) *syncFixture {
	t.Helper()
	requireGit(t)
	isolateGit(t, "")
	up := newUpstream(t)
	if repo.URL == "" {
		repo.URL = up.dir
	}
	if repo.Base == "" {
		repo.Base = "main"
	}
	if repo.BranchPrefix == "" {
		repo.BranchPrefix = "sortie/"
	}
	logs := &bytes.Buffer{}
	syncer, err := New(Params{Repository: repo, StateDir: t.TempDir(), Logger: slog.New(slog.NewTextHandler(logs, nil))})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	guestDir := filepath.Join(t.TempDir(), "guest-git")
	syncer.guestDir = guestDir
	wsDir := filepath.Join(t.TempDir(), key)
	if err := os.Mkdir(wsDir, 0o750); err != nil {
		t.Fatal(err)
	}
	return &syncFixture{
		t:        t,
		up:       up,
		syncer:   syncer,
		env:      &environmenttest.Recorder{Inner: local.Environment{}},
		ws:       environment.Workspace{Key: key, HostPath: wsDir, GuestPath: wsDir},
		guestDir: guestDir,
		logs:     logs,
	}
}

// git runs git in the workspace, as the guest would.
func (f *syncFixture) git(args ...string) string {
	f.t.Helper()
	return fixtureGit(f.t, f.up.env, f.ws.HostPath, args...)
}

// argvs returns the argv of every command run in the environment.
func (f *syncFixture) argvs() [][]string {
	var out [][]string
	for _, c := range f.env.Commands() {
		out = append(out, c.Spec.Argv)
	}
	return out
}

// assertNoBundlesLeft checks that neither the guest bundle nor a host
// temporary bundle directory remains.
func (f *syncFixture) assertNoBundlesLeft() {
	f.t.Helper()
	if _, err := os.Stat(filepath.Join(f.guestDir, "in.bundle")); !os.IsNotExist(err) {
		f.t.Errorf("guest bundle: Stat error = %v, want it removed", err)
	}
	entries, err := os.ReadDir(filepath.Dir(f.syncer.mirror.Dir()))
	if err != nil {
		f.t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "bundle-") {
			f.t.Errorf("host bundle directory %s left behind", e.Name())
		}
	}
}

func TestSeed_NewIssueBranchAtBase(t *testing.T) {
	f := newSyncFixture(t, "ENG-42", config.Repository{})
	main := f.up.git("rev-parse", "main")

	if err := f.syncer.Seed(context.Background(), f.env, f.ws); err != nil {
		t.Fatalf("Seed() error = %v", err)
	}

	bundle := filepath.Join(f.guestDir, "in.bundle")
	want := [][]string{
		{"sh", "-c", `umask 077 && mkdir -p "$1" && cat > "$1/in.bundle"`, "sh", f.guestDir},
		{"git", "clone", "--quiet", "--no-checkout", "--origin", "origin", "--", bundle, f.ws.GuestPath},
		{"sh", "-c", excludeControlDirScript},
		{"git", "checkout", "--quiet", "--no-track", "-b", "sortie/ENG-42", "origin/main"},
		{"git", "remote", "set-url", "origin", f.up.dir},
		{"rm", "-f", "--", bundle},
	}
	if got := f.argvs(); !slices.EqualFunc(got, want, slices.Equal) {
		t.Errorf("environment commands =\n%q\nwant\n%q", got, want)
	}
	for i, c := range f.env.Commands() {
		if c.Spec.Stdin != (i == 0) || c.Spec.Dir != "" {
			t.Errorf("command %d spec stdin = %v, dir = %q; want stdin only for the transfer, at the workspace root", i, c.Spec.Stdin, c.Spec.Dir)
		}
	}

	if got := f.git("rev-parse", "--abbrev-ref", "HEAD"); got != "sortie/ENG-42" {
		t.Errorf("current branch = %q, want sortie/ENG-42", got)
	}
	if head, base := f.git("rev-parse", "HEAD"), f.git("rev-parse", "origin/main"); head != main || base != main {
		t.Errorf("HEAD = %s, origin/main = %s; want both at upstream main %s", head, base, main)
	}
	if got := f.git("status", "--porcelain"); got != "" {
		t.Errorf("status = %q, want a clean checkout", got)
	}
	if got := f.git("for-each-ref", "--format=%(refname)"); got != "refs/heads/sortie/ENG-42\nrefs/remotes/origin/main" {
		t.Errorf("guest refs = %q, want only the issue branch and origin/main", got)
	}
	if out, err := exec.Command("git", "-C", f.ws.HostPath, "config", "branch.sortie/ENG-42.remote").Output(); err == nil { //nolint:gosec // G204: the test's own check
		t.Errorf("issue branch upstream = %q, want none for a new branch", out)
	}
	if got := f.git("remote", "get-url", "origin"); got != f.up.dir {
		t.Errorf("origin URL = %q, want %q", got, f.up.dir)
	}
	f.assertNoBundlesLeft()
}

func TestSeed_ExistingUpstreamBranchAtItsTip(t *testing.T) {
	f := newSyncFixture(t, "ENG-43", config.Repository{})
	f.up.git("checkout", "--quiet", "-b", "sortie/ENG-43")
	tip := f.up.commit("work.txt", "earlier run\n")
	f.up.git("checkout", "--quiet", "main")
	f.up.git("checkout", "--quiet", "-b", "unrelated")
	f.up.commit("other.txt", "other\n")
	f.up.git("checkout", "--quiet", "main")

	if err := f.syncer.Seed(context.Background(), f.env, f.ws); err != nil {
		t.Fatalf("Seed() error = %v", err)
	}
	if got := f.argvs()[3]; !slices.Equal(got, []string{"git", "checkout", "--quiet", "--track", "-b", "sortie/ENG-43", "origin/sortie/ENG-43"}) {
		t.Errorf("checkout = %q, want the issue branch tracking its upstream", got)
	}
	if got := f.git("rev-parse", "--abbrev-ref", "HEAD"); got != "sortie/ENG-43" {
		t.Errorf("current branch = %q, want sortie/ENG-43", got)
	}
	if got := f.git("rev-parse", "HEAD"); got != tip {
		t.Errorf("HEAD = %s, want the upstream branch tip %s", got, tip)
	}
	if got := f.git("rev-parse", "--abbrev-ref", "sortie/ENG-43@{upstream}"); got != "origin/sortie/ENG-43" {
		t.Errorf("issue branch upstream = %q, want origin/sortie/ENG-43", got)
	}
	if got := f.git("for-each-ref", "--format=%(refname)", "refs/remotes"); got != "refs/remotes/origin/main\nrefs/remotes/origin/sortie/ENG-43" {
		t.Errorf("guest remote refs = %q, want only the base and the issue branch", got)
	}
	f.assertNoBundlesLeft()
}

func TestSeed_ExcludesTheControlDirectoryLink(t *testing.T) {
	f := newSyncFixture(t, "ENG-48", config.Repository{})
	// An empty template leaves the clone without .git/info, as a guest
	// git configured that way would.
	t.Setenv("GIT_TEMPLATE_DIR", t.TempDir())

	if err := f.syncer.Seed(context.Background(), f.env, f.ws); err != nil {
		t.Fatalf("Seed() error = %v", err)
	}
	control := t.TempDir()
	if err := os.WriteFile(filepath.Join(control, "issue.json"), []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(control, filepath.Join(f.ws.HostPath, ".sortie")); err != nil {
		t.Fatal(err)
	}
	if got := f.git("status", "--porcelain", "--untracked-files=all"); got != "" {
		t.Errorf("status with the control directory linked = %q, want it clean", got)
	}
	f.git("add", "-A")
	if got := f.git("diff", "--cached", "--name-only"); got != "" {
		t.Errorf("staged after add -A = %q, want nothing", got)
	}
	// Only the workspace root's .sortie is excluded.
	if err := os.Mkdir(filepath.Join(f.ws.HostPath, "sub"), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(f.ws.HostPath, "sub", ".sortie"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if got := f.git("status", "--porcelain", "--untracked-files=all"); got != "?? sub/.sortie" {
		t.Errorf("status with a nested .sortie = %q, want it untracked", got)
	}
}

func TestSeed_InvalidIssueBranchRunsNothing(t *testing.T) {
	f := newSyncFixture(t, "ENG-44", config.Repository{BranchPrefix: "bad..prefix/"})

	err := f.syncer.Seed(context.Background(), f.env, f.ws)
	if err == nil || !strings.Contains(err.Error(), `"bad..prefix/ENG-44"`) {
		t.Fatalf("Seed() error = %v, want one naming the branch", err)
	}
	if got := f.argvs(); len(got) != 0 {
		t.Errorf("environment commands = %q, want none", got)
	}
}

func TestSeed_UpstreamFetchFailureRunsNothing(t *testing.T) {
	f := newSyncFixture(t, "ENG-45", config.Repository{URL: "/nonexistent/upstream.git"})

	err := f.syncer.Seed(context.Background(), f.env, f.ws)
	if err == nil || !strings.Contains(err.Error(), "fetch /nonexistent/upstream.git") {
		t.Fatalf("Seed() error = %v, want the upstream fetch failure", err)
	}
	if got := f.argvs(); len(got) != 0 {
		t.Errorf("environment commands = %q, want none", got)
	}
}

func TestSeed_MissingBaseRunsNothing(t *testing.T) {
	f := newSyncFixture(t, "ENG-46", config.Repository{Base: "develop"})

	err := f.syncer.Seed(context.Background(), f.env, f.ws)
	if err == nil || !strings.Contains(err.Error(), `base branch "develop"`) {
		t.Fatalf("Seed() error = %v, want one naming the base branch", err)
	}
	if got := f.argvs(); len(got) != 0 {
		t.Errorf("environment commands = %q, want none", got)
	}
}

func TestSeed_FailedCloneStillRemovesTheGuestBundle(t *testing.T) {
	f := newSyncFixture(t, "ENG-47", config.Repository{})
	if err := os.WriteFile(filepath.Join(f.ws.HostPath, "occupied"), nil, 0o600); err != nil {
		t.Fatal(err)
	}

	err := f.syncer.Seed(context.Background(), f.env, f.ws)
	if err == nil || !strings.Contains(err.Error(), "clone the repository") {
		t.Fatalf("Seed() error = %v, want the clone failure", err)
	}
	argvs := f.argvs()
	if last := argvs[len(argvs)-1]; last[0] != "rm" {
		t.Errorf("last environment command = %q, want the bundle removal", last)
	}
	f.assertNoBundlesLeft()
}

func TestGuestRemoteURL(t *testing.T) {
	t.Parallel()

	tests := []struct{ in, want string }{
		{"https://x-access-token:ghp_secret@github.com/org/app.git", "https://github.com/org/app.git"},
		{"https://ghp_secret@github.com/org/app.git", "https://github.com/org/app.git"},
		{"HTTP://user@host/app.git", "HTTP://host/app.git"},
		{"https://github.com/org/app.git", "https://github.com/org/app.git"},
		{"ssh://git:pw@github.com/org/app.git", "ssh://git@github.com/org/app.git"},
		{"ssh://git@github.com/org/app.git", "ssh://git@github.com/org/app.git"},
		{"git@github.com:org/app.git", "git@github.com:org/app.git"},
		{"/srv/git/app.git", "/srv/git/app.git"},
		{"https://host/path@v1/app.git", "https://host/path@v1/app.git"},
	}
	for _, tt := range tests {
		if got := GuestRemoteURL(tt.in); got != tt.want {
			t.Errorf("GuestRemoteURL(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

// guestCommandsSince returns the argv of every environment command after
// the first n.
func (f *syncFixture) guestCommandsSince(n int) [][]string {
	return f.argvs()[n:]
}

// localWork makes a local commit on the guest's issue branch, stages one
// change, and leaves another file modified and one untracked, and
// returns the state Refresh must not change.
func (f *syncFixture) localWork() (head, status, staged string) {
	f.t.Helper()
	write := func(name, content string) {
		f.t.Helper()
		if err := os.WriteFile(filepath.Join(f.ws.HostPath, name), []byte(content), 0o600); err != nil {
			f.t.Fatal(err)
		}
	}
	write("committed.txt", "agent commit\n")
	f.git("add", "committed.txt")
	f.git("commit", "--quiet", "-m", "agent work")
	write("staged.txt", "staged\n")
	f.git("add", "staged.txt")
	write("README", "modified, not staged\n")
	write("untracked.txt", "untracked\n")
	return f.git("rev-parse", "HEAD"), f.git("status", "--porcelain"), f.git("diff", "--cached")
}

func TestRefresh_UpdatesOnlyRemoteTrackingRefs(t *testing.T) {
	f := newSyncFixture(t, "ENG-50", config.Repository{})
	f.up.git("checkout", "--quiet", "-b", "sortie/ENG-50")
	f.up.commit("work.txt", "an earlier run\n")
	f.up.git("checkout", "--quiet", "main")
	ctx := context.Background()
	if err := f.syncer.Seed(ctx, f.env, f.ws); err != nil {
		t.Fatalf("Seed() error = %v", err)
	}
	head, status, staged := f.localWork()

	newMain := f.up.commit("main.txt", "moved on\n")
	f.up.git("checkout", "--quiet", "sortie/ENG-50")
	newTip := f.up.commit("human.txt", "a human pushed this\n")
	f.up.git("checkout", "--quiet", "main")

	before := len(f.argvs())
	if err := f.syncer.Refresh(ctx, f.env, f.ws); err != nil {
		t.Fatalf("Refresh() error = %v", err)
	}

	bundle := filepath.Join(f.guestDir, "in.bundle")
	want := [][]string{
		{"sh", "-c", `umask 077 && mkdir -p "$1" && cat > "$1/in.bundle"`, "sh", f.guestDir},
		{"git", "fetch", "--quiet", "--prune", "--no-tags", "--", bundle, "+refs/heads/*:refs/remotes/origin/*"},
		{"rm", "-f", "--", bundle},
	}
	if got := f.guestCommandsSince(before); !slices.EqualFunc(got, want, slices.Equal) {
		t.Errorf("environment commands =\n%q\nwant\n%q", got, want)
	}
	if got := f.git("rev-parse", "origin/main"); got != newMain {
		t.Errorf("origin/main = %s, want the new upstream main %s", got, newMain)
	}
	if got := f.git("rev-parse", "origin/sortie/ENG-50"); got != newTip {
		t.Errorf("origin/sortie/ENG-50 = %s, want the new upstream tip %s", got, newTip)
	}
	if got := f.git("rev-parse", "--abbrev-ref", "HEAD"); got != "sortie/ENG-50" {
		t.Errorf("current branch = %q, want sortie/ENG-50", got)
	}
	if got := f.git("rev-parse", "HEAD"); got != head {
		t.Errorf("HEAD = %s, want the local commit %s unchanged", got, head)
	}
	if got := f.git("status", "--porcelain"); got != status {
		t.Errorf("status =\n%s\nwant it unchanged:\n%s", got, status)
	}
	if got := f.git("diff", "--cached"); got != staged {
		t.Errorf("staged changes =\n%s\nwant them unchanged:\n%s", got, staged)
	}
	if data, err := os.ReadFile(filepath.Join(f.ws.HostPath, "README")); err != nil || string(data) != "modified, not staged\n" {
		t.Errorf("modified file = %q, %v; want its uncommitted content kept", data, err)
	}
	f.assertNoBundlesLeft()
}

func TestRefresh_UpstreamFailureSendsTheMirrorsLastState(t *testing.T) {
	f := newSyncFixture(t, "ENG-51", config.Repository{})
	ctx := context.Background()
	if err := f.syncer.Seed(ctx, f.env, f.ws); err != nil {
		t.Fatalf("Seed() error = %v", err)
	}

	// The mirror learns of a new main commit, as another worker's fetch
	// would bring it, and then the upstream becomes unreachable.
	newMain := f.up.commit("main.txt", "moved on\n")
	l := lockMirror(t, f.syncer.mirror)
	if err := l.FetchUpstream(ctx); err != nil {
		t.Fatalf("FetchUpstream() error = %v", err)
	}
	l.Unlock()
	if err := os.Rename(f.up.dir, f.up.dir+"-gone"); err != nil {
		t.Fatal(err)
	}

	if err := f.syncer.Refresh(ctx, f.env, f.ws); err != nil {
		t.Fatalf("Refresh() with the upstream unreachable error = %v, want nil", err)
	}
	if !strings.Contains(f.logs.String(), "level=WARN") || !strings.Contains(f.logs.String(), "upstream fetch failed") {
		t.Errorf("logs = %q, want a warning about the upstream fetch", f.logs.String())
	}
	if got := f.git("rev-parse", "origin/main"); got != newMain {
		t.Errorf("origin/main = %s, want the mirror's last main %s", got, newMain)
	}
}

func TestRefresh_PrunesAnIssueBranchTheUpstreamDeleted(t *testing.T) {
	f := newSyncFixture(t, "ENG-52", config.Repository{})
	f.up.git("checkout", "--quiet", "-b", "sortie/ENG-52")
	tip := f.up.commit("work.txt", "an earlier run\n")
	f.up.git("checkout", "--quiet", "main")
	ctx := context.Background()
	if err := f.syncer.Seed(ctx, f.env, f.ws); err != nil {
		t.Fatalf("Seed() error = %v", err)
	}
	f.up.git("branch", "--quiet", "-D", "sortie/ENG-52")

	if err := f.syncer.Refresh(ctx, f.env, f.ws); err != nil {
		t.Fatalf("Refresh() error = %v", err)
	}
	if got := f.git("for-each-ref", "--format=%(refname)"); got != "refs/heads/sortie/ENG-52\nrefs/remotes/origin/main" {
		t.Errorf("guest refs = %q, want the local branch kept and origin/sortie/ENG-52 pruned", got)
	}
	if got := f.git("rev-parse", "HEAD"); got != tip {
		t.Errorf("HEAD = %s, want the local branch at %s unchanged", got, tip)
	}
}
