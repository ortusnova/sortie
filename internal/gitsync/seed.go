package gitsync

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
)

// Seed is a workspace seed built on the host: a bundle file that a
// worker clones into a workspace of its own.
type Seed struct {
	// Branch is the issue branch the clone checks out.
	Branch string

	// Commit is the issue branch's commit in the seed: the upstream
	// issue branch when it exists, otherwise the base branch.
	Commit string

	// Upstream reports that Commit is the upstream issue branch's, so a
	// returned branch is pushed only while the upstream branch still
	// holds it. A seed without it is pushed with the lease on Sortie's
	// last push.
	Upstream bool `json:",omitempty"`

	// Base is the base branch's commit the seed carries as
	// refs/remotes/origin/<base>.
	Base string `json:",omitempty"`
}

// BuildSeed writes the workspace seed of the workspace key to dst, a
// file that must not exist. It fetches the upstream into the mirror,
// failing when that fails, and bundles HEAD and refs/heads/<issue
// branch>: the upstream issue branch when it exists, otherwise a new
// branch at the base branch. Beside them it bundles the upstream's base
// branch as refs/remotes/origin/<base> and its issue branch, when it
// exists, as refs/remotes/origin/<issue branch>. A worker fetches every
// ref of the bundle as it is, so its workspace has the origin/* refs a
// local workspace has, and a run can merge origin/<base>; `git clone` of
// the bundle gives the repository with the issue branch checked out, and
// origin/<issue branch> marks the seed. The bundle is built in a scratch
// repository that borrows the mirror's objects, so no ref of the mirror
// changes.
func (s *Syncer) BuildSeed(ctx context.Context, key, dst string) (Seed, error) {
	branch := s.IssueBranch(key)
	l, err := s.mirror.Lock(ctx)
	if err != nil {
		return Seed{}, fmt.Errorf("lock the repository mirror: %w", err)
	}
	defer l.Unlock()

	if err := l.CheckBranchName(ctx, branch); err != nil {
		return Seed{}, err
	}
	if err := l.FetchUpstream(ctx); err != nil {
		return Seed{}, err
	}
	base, ok, err := l.Ref(ctx, s.repo.Base)
	if err != nil {
		return Seed{}, err
	} else if !ok {
		return Seed{}, fmt.Errorf("base branch %q is not in the mirror of %s", s.repo.Base, s.mirror.displayURL())
	}
	commit, upstreamBranch, err := l.Ref(ctx, branch)
	if err != nil {
		return Seed{}, err
	}
	if !upstreamBranch {
		commit = base
	}

	scratch, err := os.MkdirTemp(filepath.Dir(s.mirror.Dir()), "seed-")
	if err != nil {
		return Seed{}, fmt.Errorf("create seed directory: %w", err)
	}
	defer os.RemoveAll(scratch) //nolint:errcheck // a leftover scratch repository only costs disk
	git := runner{binary: l.m.git.binary, gitDir: scratch}
	if _, err := git.run(ctx, "init", "--bare", "--quiet", "--template="); err != nil {
		return Seed{}, fmt.Errorf("create the seed repository: %w", err)
	}
	alternates := filepath.Join(scratch, "objects", "info", "alternates")
	if err := os.WriteFile(alternates, []byte(filepath.Join(s.mirror.Dir(), "objects")+"\n"), 0o600); err != nil {
		return Seed{}, fmt.Errorf("link the seed repository to the mirror: %w", err)
	}
	ref := "refs/heads/" + branch
	steps := [][]string{
		{"update-ref", "--no-deref", ref, commit},
		{"symbolic-ref", "HEAD", ref},
		{"update-ref", "--no-deref", "refs/remotes/origin/" + s.repo.Base, base},
	}
	bundle := []string{"bundle", "create", "--quiet", dst, "HEAD", ref, "refs/remotes/origin/" + s.repo.Base}
	if upstreamBranch && branch != s.repo.Base {
		steps = append(steps, []string{"update-ref", "--no-deref", "refs/remotes/origin/" + branch, commit})
		bundle = append(bundle, "refs/remotes/origin/"+branch)
	}
	for _, args := range append(steps, bundle) {
		if _, err := git.run(ctx, args...); err != nil {
			return Seed{}, fmt.Errorf("build the seed of %s: %w", branch, err)
		}
	}
	return Seed{Branch: branch, Commit: commit, Upstream: upstreamBranch, Base: base}, nil
}

// ImportReturned takes the issue branch of the workspace key back from
// the bundle a worker returned at bundlePath, and pushes its new
// commits, as [Syncer.Export] does with a bundle read out of an
// environment. The bundle must name refs/heads/<issue branch> and no
// other ref, and the branch's commit must be seed.Commit or descend
// from it. Any other bundle is refused, naming the ref, and nothing is
// imported or pushed. When the seed carried the upstream issue branch,
// the push holds only while the upstream branch is still at that
// commit, which the returned work builds on, even when someone other
// than Sortie pushed it.
func (s *Syncer) ImportReturned(ctx context.Context, key, bundlePath string, seed Seed) (ExportResult, error) {
	branch := s.IssueBranch(key)
	res := ExportResult{Branch: branch}
	if err := s.checkExportRefs(ctx, key, branch); err != nil {
		return res, err
	}
	var lease *string
	if seed.Upstream {
		lease = &seed.Commit
	}
	return s.importAndPush(ctx, key, bundlePath, seed.Commit, lease, res)
}
