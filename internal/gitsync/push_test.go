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
	"time"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/failcause"
	"github.com/sortie-ai/sortie/internal/redact"
)

var _ failcause.Carrier = (*Error)(nil)

// mirrorRef returns the commit ref holds in the fixture's mirror, or ""
// when the mirror has no such ref.
func (f *syncFixture) mirrorRef(ref string) string {
	f.t.Helper()
	m := f.syncer.mirror.Dir()
	out, err := exec.Command("git", "--git-dir="+m, "rev-parse", "--verify", "--quiet", ref).Output() //nolint:gosec // G204: the test's own check
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

// upstreamBranch returns the upstream's commit of branch, or "" when the
// upstream has no such branch.
func (f *syncFixture) upstreamBranch(branch string) string {
	f.t.Helper()
	cmd := exec.Command("git", "rev-parse", "--verify", "--quiet", "refs/heads/"+branch) //nolint:gosec // G204: the test's own check
	cmd.Dir = f.up.dir
	cmd.Env = f.up.env
	out, err := cmd.Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

// humanCommit commits name on the upstream's branch, creating the branch
// at main when it does not exist, and leaves main checked out.
func (f *syncFixture) humanCommit(branch, name string) string {
	f.t.Helper()
	if f.upstreamBranch(branch) == "" {
		f.up.git("checkout", "--quiet", "-b", branch)
	} else {
		f.up.git("checkout", "--quiet", branch)
	}
	commit := f.up.commit(name, "a human pushed this\n")
	f.up.git("checkout", "--quiet", "main")
	return commit
}

func (f *syncFixture) export() (ExportResult, error) {
	f.t.Helper()
	return f.syncer.Export(context.Background(), f.env, f.ws)
}

func TestExport_FirstPush(t *testing.T) {
	f := seeded(t, "ENG-70", config.Repository{})
	head := f.agentCommit("feature.txt", []byte("feature\n"))
	start := time.Now()

	res, err := f.export()
	if err != nil {
		t.Fatalf("Export() error = %v", err)
	}
	if !res.Pushed || res.Commit != head || res.PushedAt.Before(start.Add(-time.Second)) || res.UpToDate {
		t.Errorf("Export() = %+v, want %s pushed now", res, head)
	}
	if got := f.upstreamBranch("sortie/ENG-70"); got != head {
		t.Errorf("upstream sortie/ENG-70 = %q, want %s", got, head)
	}
	if got := f.mirrorRef(PushedRef("ENG-70")); got != head {
		t.Errorf("%s = %q, want %s", PushedRef("ENG-70"), got, head)
	}
	if got := f.mirrorRef("refs/heads/sortie/ENG-70"); got != head {
		t.Errorf("mirror refs/heads/sortie/ENG-70 = %q, want %s", got, head)
	}

	// Exporting again with nothing new pushes nothing.
	res, err = f.export()
	if err != nil || res.Pushed || res.Commit != head || !res.UpToDate {
		t.Errorf("second Export() = %+v, %v; want %s up to date without a push", res, err, head)
	}
}

func TestExport_RebaseReplacesSortiesOwnPush(t *testing.T) {
	f := seeded(t, "ENG-71", config.Repository{})
	first := f.agentCommit("feature.txt", []byte("feature\n"))
	if _, err := f.export(); err != nil {
		t.Fatalf("first Export() error = %v", err)
	}

	// main moves on; the agent rebases its branch onto it.
	f.up.commit("main.txt", "moved on\n")
	ctx := context.Background()
	if err := f.syncer.Refresh(ctx, f.env, f.ws); err != nil {
		t.Fatalf("Refresh() error = %v", err)
	}
	f.git("rebase", "--quiet", "origin/main")
	rebased := f.git("rev-parse", "HEAD")
	if rebased == first {
		t.Fatal("the rebase kept the commit, want a replacement")
	}

	res, err := f.export()
	if err != nil {
		t.Fatalf("Export() after the rebase error = %v", err)
	}
	if !res.Pushed || res.Commit != rebased {
		t.Errorf("Export() = %+v, want %s pushed", res, rebased)
	}
	if got := f.upstreamBranch("sortie/ENG-71"); got != rebased {
		t.Errorf("upstream sortie/ENG-71 = %q, want the rebased %s", got, rebased)
	}
	if got := f.mirrorRef(PushedRef("ENG-71")); got != rebased {
		t.Errorf("%s = %q, want %s", PushedRef("ENG-71"), got, rebased)
	}
}

func TestExport_HumanCommitAfterSortiesPushFails(t *testing.T) {
	f := seeded(t, "ENG-72", config.Repository{})
	first := f.agentCommit("feature.txt", []byte("feature\n"))
	if _, err := f.export(); err != nil {
		t.Fatalf("first Export() error = %v", err)
	}
	human := f.humanCommit("sortie/ENG-72", "human.txt")
	f.agentCommit("more.txt", []byte("more\n"))

	res, err := f.export()
	if !errors.Is(err, ErrBranchMoved) || !strings.Contains(err.Error(), "branch moved upstream") {
		t.Fatalf("Export() error = %v, want %v", err, ErrBranchMoved)
	}
	if res.Pushed {
		t.Errorf("Export().Pushed = true, want false")
	}
	if got := f.upstreamBranch("sortie/ENG-72"); got != human {
		t.Errorf("upstream sortie/ENG-72 = %q, want the human's %s", got, human)
	}
	if got := f.mirrorRef(PushedRef("ENG-72")); got != first {
		t.Errorf("%s = %q, want Sortie's earlier push %s", PushedRef("ENG-72"), got, first)
	}
}

func TestExport_HumanCreatedTheBranchBeforeSortiesFirstPush(t *testing.T) {
	f := seeded(t, "ENG-73", config.Repository{})
	human := f.humanCommit("sortie/ENG-73", "human.txt")
	f.agentCommit("feature.txt", []byte("feature\n"))

	_, err := f.export()
	if !errors.Is(err, ErrBranchMoved) {
		t.Fatalf("Export() error = %v, want %v", err, ErrBranchMoved)
	}
	if got := f.upstreamBranch("sortie/ENG-73"); got != human {
		t.Errorf("upstream sortie/ENG-73 = %q, want the human's %s", got, human)
	}
	if got := f.mirrorRef(PushedRef("ENG-73")); got != "" {
		t.Errorf("%s = %q, want it absent", PushedRef("ENG-73"), got)
	}
}

func TestPushIncoming_PushesWhatARefusedExportImported(t *testing.T) {
	f := seeded(t, "ENG-74", config.Repository{})
	ctx := context.Background()

	if pushed, err := f.syncer.PushIncoming(ctx, "ENG-74"); pushed || err != nil {
		t.Fatalf("PushIncoming() before any export = %v, %v, want nothing to push", pushed, err)
	}

	// The upstream refuses every push while the marker file exists.
	marker := filepath.Join(t.TempDir(), "refuse")
	if err := os.WriteFile(marker, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	hook := "#!/bin/sh\nif [ -e '" + marker + "' ]; then echo '" + workflowScopeRefusal + "' >&2; exit 1; fi\nexit 0\n"
	if err := os.MkdirAll(filepath.Join(f.up.dir, ".git", "hooks"), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(f.up.dir, ".git", "hooks", "pre-receive"), []byte(hook), 0o700); err != nil { //nolint:gosec // G306: a hook must be executable
		t.Fatal(err)
	}
	head := f.agentCommit("feature.txt", []byte("feature\n"))
	if _, err := f.export(); err == nil {
		t.Fatal("Export() error = nil, want the upstream's refusal")
	}

	if pushed, err := f.syncer.PushIncoming(ctx, "ENG-74"); !pushed || err == nil {
		t.Fatalf("PushIncoming() while the upstream refuses = %v, %v, want a refused push", pushed, err)
	}

	if err := os.Remove(marker); err != nil {
		t.Fatal(err)
	}
	if pushed, err := f.syncer.PushIncoming(ctx, "ENG-74"); !pushed || err != nil {
		t.Fatalf("PushIncoming() once the upstream accepts = %v, %v, want a push", pushed, err)
	}
	if got := f.upstreamBranch("sortie/ENG-74"); got != head {
		t.Errorf("upstream sortie/ENG-74 = %q, want %s", got, head)
	}
	if got := f.mirrorRef(PushedRef("ENG-74")); got != head {
		t.Errorf("%s = %q, want %s", PushedRef("ENG-74"), got, head)
	}
	if pushed, err := f.syncer.PushIncoming(ctx, "ENG-74"); pushed || err != nil {
		t.Errorf("PushIncoming() after the push = %v, %v, want nothing to push", pushed, err)
	}
}

func TestExport_HumanCommitOnTopOfSortiesPushIsUpToDate(t *testing.T) {
	f := seeded(t, "ENG-74", config.Repository{})
	first := f.agentCommit("feature.txt", []byte("feature\n"))
	if _, err := f.export(); err != nil {
		t.Fatalf("first Export() error = %v", err)
	}
	// A person builds on Sortie's push; the environment keeps the older
	// commit, which the upstream branch already holds.
	human := f.humanCommit("sortie/ENG-74", "human.txt")

	res, err := f.export()
	if err != nil {
		t.Fatalf("Export() error = %v, want the older commit up to date", err)
	}
	if res.Pushed || res.Commit != first || !res.UpToDate {
		t.Errorf("Export() = %+v, want %s up to date without a push", res, first)
	}
	if got := f.upstreamBranch("sortie/ENG-74"); got != human {
		t.Errorf("upstream sortie/ENG-74 = %q, want the human's %s", got, human)
	}
	if got := f.mirrorRef(PushedRef("ENG-74")); got != first {
		t.Errorf("%s = %q, want Sortie's earlier push %s", PushedRef("ENG-74"), got, first)
	}
}

func TestExport_MergedAndDeletedBranchIsNotPushedAgain(t *testing.T) {
	f := seeded(t, "ENG-75", config.Repository{})
	first := f.agentCommit("feature.txt", []byte("feature\n"))
	if _, err := f.export(); err != nil {
		t.Fatalf("first Export() error = %v", err)
	}
	f.up.git("merge", "--quiet", "--no-ff", "-m", "merge sortie/ENG-75", "sortie/ENG-75")
	f.up.git("branch", "--quiet", "-D", "sortie/ENG-75")

	res, err := f.export()
	if err != nil {
		t.Fatalf("Export() error = %v, want the merged commit left alone", err)
	}
	if res.Pushed || res.Commit != first || res.UpToDate {
		t.Errorf("Export() = %+v, want %s without a push and no upstream issue branch", res, first)
	}
	if got := f.upstreamBranch("sortie/ENG-75"); got != "" {
		t.Errorf("upstream sortie/ENG-75 = %q, want it absent", got)
	}
}

func TestExport_RewindOfSortiesOwnPushIsPushed(t *testing.T) {
	f := seeded(t, "ENG-76", config.Repository{})
	first := f.agentCommit("feature.txt", []byte("feature\n"))
	f.agentCommit("mistake.txt", []byte("mistake\n"))
	if _, err := f.export(); err != nil {
		t.Fatalf("first Export() error = %v", err)
	}
	f.git("reset", "--quiet", "--hard", first)

	res, err := f.export()
	if err != nil {
		t.Fatalf("Export() error = %v", err)
	}
	if !res.Pushed || res.Commit != first {
		t.Errorf("Export() = %+v, want the rewind to %s pushed", res, first)
	}
	if got := f.upstreamBranch("sortie/ENG-76"); got != first {
		t.Errorf("upstream sortie/ENG-76 = %q, want %s", got, first)
	}
}

// workflowScopeRefusal is the line GitHub gives for a push its token's
// scopes do not allow.
const workflowScopeRefusal = "refusing to allow a Personal Access Token to create or update workflow `.github/workflows/talos-installer.yml` without `workflow` scope"

// refusingBareUpstream returns a bare copy of a new upstream whose
// pre-receive hook writes workflowScopeRefusal and token to its standard
// error and refuses every push.
func refusingBareUpstream(t *testing.T, token string) string {
	t.Helper()
	up := newUpstream(t)
	bare := filepath.Join(t.TempDir(), "upstream.git")
	fixtureGit(t, up.env, filepath.Dir(bare), "clone", "--quiet", "--bare", "--template=", up.dir, bare)
	hook := "#!/bin/sh\n" +
		"echo '" + workflowScopeRefusal + "' >&2\n" +
		"echo 'the token " + token + " lacks the scope' >&2\n" +
		"exit 1\n"
	if err := os.MkdirAll(filepath.Join(bare, "hooks"), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bare, "hooks", "pre-receive"), []byte(hook), 0o700); err != nil { //nolint:gosec // G306: a hook must be executable
		t.Fatal(err)
	}
	return bare
}

func TestPush_RefusedPushKeepsItsWholeCause(t *testing.T) {
	requireGit(t)
	isolateGit(t, "")
	const token = "ghp_gitsyncPushTestToken0123456789"
	redact.Add("test.gitsync push token", token)
	bare := refusingBareUpstream(t, token)
	m, err := OpenMirror(t.TempDir(), bare)
	if err != nil {
		t.Fatal(err)
	}
	l := lockMirror(t, m)
	defer l.Unlock()
	ctx := context.Background()
	if err := l.FetchUpstream(ctx); err != nil {
		t.Fatalf("FetchUpstream() error = %v", err)
	}

	err = l.Push(ctx, "refs/heads/main", "sortie/ONS-49", "")
	if err == nil {
		t.Fatal("Push() error = nil, want the hook's refusal")
	}
	if errors.Is(err, ErrBranchMoved) {
		t.Errorf("Push() error = %v, want a refusal other than %v", err, ErrBranchMoved)
	}
	if !strings.Contains(err.Error(), "push sortie/ONS-49 to "+bare+": [remote rejected] (pre-receive hook declined): git push") {
		t.Errorf("Push() error = %q, want it to name the remote's reason", err)
	}

	cause := failcause.Of(err)
	for _, want := range []string{
		"git push: exit status 1",
		"stdout:\nTo " + bare + "\n!\trefs/heads/main:refs/heads/sortie/ONS-49\t[remote rejected] (pre-receive hook declined)",
		"stderr:\n",
		"remote: " + workflowScopeRefusal,
		"remote: the token " + redact.Marker + " lacks the scope",
		"failed to push some refs",
	} {
		if !strings.Contains(cause, want) {
			t.Errorf("failcause.Of(Push()) = %q, want it to hold %q", cause, want)
		}
	}
	if strings.Contains(cause, token) {
		t.Errorf("failcause.Of(Push()) = %q, want the token masked", cause)
	}
}

func TestPush_TwoRefusalsOfTheSamePushGiveOneSignature(t *testing.T) {
	requireGit(t)
	isolateGit(t, "")
	ctx := context.Background()

	refuse := func(branch string) error {
		t.Helper()
		bare := refusingBareUpstream(t, "")
		m, err := OpenMirror(t.TempDir(), bare)
		if err != nil {
			t.Fatal(err)
		}
		l := lockMirror(t, m)
		defer l.Unlock()
		if err := l.FetchUpstream(ctx); err != nil {
			t.Fatalf("FetchUpstream() error = %v", err)
		}
		err = l.Push(ctx, "refs/heads/main", branch, "")
		if err == nil {
			t.Fatal("Push() error = nil, want the hook's refusal")
		}
		return err
	}

	// Each run pushes from its own mirror to its own upstream path, and
	// the second is a later run on another issue's branch name.
	first := failcause.Sign("export", refuse("sortie/ONS-49"))
	second := failcause.Sign("export", refuse("sortie/ONS-50"))
	if first != second {
		t.Fatalf("two refusals of the same push give two signatures:\n %q\n %q", first, second)
	}
	if first.Class != "git_push" || first.Transient() {
		t.Errorf("signature class = %q, want git_push and not transient", first.Class)
	}
	if !strings.Contains(first.Cause, "[remote rejected] (pre-receive hook declined)") {
		t.Errorf("signature cause = %q, want the remote's reason", first.Cause)
	}
}

func TestRefusal(t *testing.T) {
	t.Parallel()

	const dst = "refs/heads/sortie/K"
	tests := []struct{ name, out, want string }{
		{"remote rejected", "To /up\n!\trefs/sortie/incoming/K:refs/heads/sortie/K\t[remote rejected] (" + workflowScopeRefusal + ")\nDone\n", "[remote rejected] (" + workflowScopeRefusal + ")"},
		{"other ref", "To /up\n!\trefs/sortie/incoming/K:refs/heads/sortie/KX\t[remote rejected] (hook declined)\nDone\n", ""},
		{"accepted", "To /up\n*\trefs/sortie/incoming/K:refs/heads/sortie/K\t[new branch]\nDone\n", ""},
		{"no output", "", ""},
	}
	for _, tt := range tests {
		if got := refusal(tt.out, dst); got != tt.want {
			t.Errorf("refusal(%s) = %q, want %q", tt.name, got, tt.want)
		}
	}
}

func TestLeaseRejected(t *testing.T) {
	t.Parallel()

	const dst = "refs/heads/sortie/K"
	tests := []struct {
		name, out string
		want      bool
	}{
		{"stale info", "To /up\n!\trefs/sortie/incoming/K:refs/heads/sortie/K\t[rejected] (stale info)\nDone\n", true},
		{"hook declined", "To /up\n!\trefs/sortie/incoming/K:refs/heads/sortie/K\t[remote rejected] (pre-receive hook declined)\nDone\n", false},
		{"other ref stale", "To /up\n!\trefs/sortie/incoming/K:refs/heads/sortie/KX\t[rejected] (stale info)\nDone\n", false},
		{"accepted", "To /up\n*\trefs/sortie/incoming/K:refs/heads/sortie/K\t[new branch]\nDone\n", false},
		{"no output", "", false},
	}
	for _, tt := range tests {
		if got := leaseRejected(tt.out, dst); got != tt.want {
			t.Errorf("leaseRejected(%s) = %v, want %v", tt.name, got, tt.want)
		}
	}
}
