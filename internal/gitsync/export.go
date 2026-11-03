package gitsync

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/sortie-ai/sortie/internal/agent/procutil"
	"github.com/sortie-ai/sortie/internal/environment"
)

// ExportResult reports what [Syncer.Export] did.
type ExportResult struct {
	// Branch is the issue branch.
	Branch string

	// Commit is the issue branch's commit in the environment; empty
	// when NothingToExport is set.
	Commit string

	// NothingToExport is true when the environment has no issue branch.
	NothingToExport bool

	// Pushed is true when Commit was pushed to the upstream's issue
	// branch.
	Pushed bool

	// PushedAt is when the push completed; zero unless Pushed.
	PushedAt time.Time

	// UpToDate is true when nothing was pushed because the upstream's
	// issue branch, as the mirror last saw it, already holds Commit: the
	// work is upstream, as after an earlier push. Commit is then the
	// branch's tip, or an ancestor of a tip someone else pushed on top
	// of Sortie's last push. It is false when the upstream has no issue
	// branch, even if Commit is on the base branch.
	UpToDate bool
}

// IncomingRef returns the mirror ref the issue branch of the workspace
// key is imported into.
func IncomingRef(key string) string { return "refs/sortie/incoming/" + key }

// PushedRef returns the mirror ref that holds the commit Sortie last
// pushed to the upstream's issue branch of the workspace key.
func PushedRef(key string) string { return "refs/sortie/pushed/" + key }

// Export takes the committed issue branch out of ws's environment,
// imports it into the mirror, and pushes it to the upstream when it has
// new commits. Uncommitted changes stay in the environment.
//
// In the environment it bundles refs/heads/<issue branch> under the
// guest bundle directory, then reads the bundle out through a command,
// failing once it exceeds the repository's max_bundle_mib, and removes
// it. An environment without the issue branch has nothing to export:
// Export succeeds with NothingToExport set.
//
// On the host, with the mirror locked, the bundle must name exactly
// refs/heads/<issue branch> and nothing else. Export then fetches the
// upstream into the mirror. When the branch's commit equals the
// upstream's issue branch, or the base branch while the upstream has no
// issue branch, there is nothing to push and Export succeeds without
// importing anything, setting UpToDate in the first case. The same
// holds when the push's lease, below, could not hold and that branch
// already has the commit as an ancestor: someone built on Sortie's push,
// or merged it into the base branch and deleted the issue branch, while
// the environment kept the older commit. With the upstream unreachable
// the mirror's last state decides all this. Otherwise it imports the
// branch into [IncomingRef], which must then hold a commit, and a failed
// upstream fetch fails the export.
//
// The import is pushed to the upstream's issue branch with a lease: the
// push succeeds only while that branch holds [PushedRef], the commit
// Sortie pushed last, or is absent when Sortie never pushed it. So the
// agent may replace Sortie's own push, as after a rebase or a rewind,
// but a commit anyone else pushed since is never overwritten: unless
// that commit already holds the export's, the export fails with
// [ErrBranchMoved] and the upstream keeps that commit. After the push
// Export points [PushedRef] and the mirror's issue branch at the commit.
//
// Every failure before the import leaves the mirror's refs as they were,
// apart from the upstream fetch.
func (s *Syncer) Export(ctx context.Context, env environment.Environment, ws environment.Workspace) (ExportResult, error) {
	branch := s.IssueBranch(ws.Key)
	res := ExportResult{Branch: branch}
	if err := s.checkExportRefs(ctx, ws.Key, branch); err != nil {
		return res, err
	}

	dir, err := os.MkdirTemp(filepath.Dir(s.mirror.Dir()), "bundle-")
	if err != nil {
		return res, fmt.Errorf("create bundle directory: %w", err)
	}
	defer os.RemoveAll(dir) //nolint:errcheck // a leftover temporary bundle only costs disk
	bundle := filepath.Join(dir, "out.bundle")

	found, err := s.receiveBundle(ctx, env, ws, branch, bundle)
	if err != nil {
		return res, err
	}
	if !found {
		res.NothingToExport = true
		return res, nil
	}

	return s.importAndPush(ctx, ws.Key, bundle, "", nil, res)
}

// importAndPush imports the bundle at bundlePath into the mirror and
// pushes it, as [Syncer.Export] describes, with the branch's commit
// required to descend from onTopOf when that is not empty. A non-nil
// lease is the commit the upstream branch must hold for the push;
// otherwise the push leases on Sortie's last push.
func (s *Syncer) importAndPush(ctx context.Context, key, bundlePath, onTopOf string, lease *string, res ExportResult) (ExportResult, error) {
	branch := res.Branch
	l, err := s.mirror.Lock(ctx)
	if err != nil {
		return res, fmt.Errorf("lock the repository mirror: %w", err)
	}
	defer l.Unlock()
	commit, needPush, upToDate, err := s.importBundle(ctx, l, key, branch, bundlePath, onTopOf)
	res.Commit = commit
	res.UpToDate = upToDate
	if err != nil || !needPush {
		return res, err
	}
	if lease != nil {
		err = s.pushOver(ctx, l, key, branch, commit, *lease)
	} else {
		err = s.push(ctx, l, key, branch, commit)
	}
	if err != nil {
		return res, err
	}
	res.Pushed = true
	res.PushedAt = time.Now().UTC()
	return res, nil
}

// PushIncoming pushes again what an earlier export of key imported into
// [IncomingRef] and could not push, as that export would have. It
// reports false, and pushes nothing, when the mirror holds no such
// import: no incoming ref, or one equal to [PushedRef].
func (s *Syncer) PushIncoming(ctx context.Context, key string) (bool, error) {
	branch := s.IssueBranch(key)
	if err := s.checkExportRefs(ctx, key, branch); err != nil {
		return false, err
	}
	l, err := s.mirror.Lock(ctx)
	if err != nil {
		return false, fmt.Errorf("lock the repository mirror: %w", err)
	}
	defer l.Unlock()
	commit, ok, err := l.RefValue(ctx, IncomingRef(key))
	if err != nil || !ok {
		return false, err
	}
	pushed, _, err := l.RefValue(ctx, PushedRef(key))
	if err != nil {
		return false, err
	}
	if pushed == commit {
		return false, nil
	}
	return true, s.push(ctx, l, key, branch, commit)
}

// push pushes the incoming ref of key to the upstream's branch with a
// lease on [PushedRef], then records commit as Sortie's last push and
// as the mirror's copy of the upstream branch.
func (s *Syncer) push(ctx context.Context, l *Locked, key, branch, commit string) error {
	expected, _, err := l.RefValue(ctx, PushedRef(key))
	if err != nil {
		return err
	}
	return s.pushOver(ctx, l, key, branch, commit, expected)
}

// pushOver pushes the incoming ref of key to the upstream's branch while
// the branch holds expected, or is absent when expected is empty, then
// records commit as Sortie's last push and as the mirror's copy of the
// upstream branch.
func (s *Syncer) pushOver(ctx context.Context, l *Locked, key, branch, commit, expected string) error {
	if err := l.Push(ctx, IncomingRef(key), branch, expected); err != nil {
		return err
	}
	if err := l.SetRef(ctx, PushedRef(key), commit); err != nil {
		return err
	}
	return l.SetRef(ctx, "refs/heads/"+branch, commit)
}

// checkExportRefs fails when branch, or the incoming or pushed ref of
// key, is not a valid name.
func (s *Syncer) checkExportRefs(ctx context.Context, key, branch string) error {
	l, err := s.mirror.Lock(ctx)
	if err != nil {
		return fmt.Errorf("lock the repository mirror: %w", err)
	}
	defer l.Unlock()
	if err := l.CheckBranchName(ctx, branch); err != nil {
		return err
	}
	if err := l.CheckRefName(ctx, IncomingRef(key)); err != nil {
		return err
	}
	return l.CheckRefName(ctx, PushedRef(key))
}

// guestOutBundle returns the exported bundle's path inside the
// environment.
func (s *Syncer) guestOutBundle(env environment.Environment) string {
	return path.Join(s.transferDir(env), "out.bundle")
}

// exportBundleScript bundles the issue branch ($2) in the environment,
// leaving out the commits the mirror already holds: those of the guest's
// origin/<base> ($3) and origin/<issue branch> ($4), which came from the
// mirror. A branch with no commit beyond them is bundled as its tip alone,
// on its parent, since git refuses an empty bundle. So the bundle carries
// the run's work, not the repository's history: smolvm caps a command's
// output at about 11 MiB, which a large repository's whole history passes.
// The guest decides only what the bundle leaves out; the host checks the
// bundle as before, and its import fails when the mirror lacks a commit it
// needs.
const exportBundleScript = `umask 077 && mkdir -p "$1" && rm -f -- "$1/out.bundle" || exit
set -- "$1" "$2" "$3" "$4" ""
for r in "$3" "$4"; do
  if git rev-parse --quiet --verify "$r^{commit}" >/dev/null; then set -- "$1" "$2" "$3" "$4" "$5 ^$r"; fi
done
if [ -n "$5" ] && [ "$(git rev-list --count "$2" $5)" = 0 ]; then
  if git rev-parse --quiet --verify "$2^" >/dev/null; then set -- "$1" "$2" "$3" "$4" "^$2^"; else set -- "$1" "$2" "$3" "$4" ""; fi
fi
exec git bundle create --quiet "$1/out.bundle" "$2" $5`

// receiveBundle bundles branch in ws's environment and reads the bundle
// into dst on the host. It reports false, having written nothing, when
// the environment has no such branch.
func (s *Syncer) receiveBundle(ctx context.Context, env environment.Environment, ws environment.Workspace, branch, dst string) (bool, error) {
	err := s.guest(ctx, env, ws, nil, "git", "rev-parse", "--verify", "--quiet", "refs/heads/"+branch)
	if err != nil {
		if procutil.ExtractExitCode(err) == 1 {
			return false, nil
		}
		return false, fmt.Errorf("look up issue branch %q in the environment: %w", branch, err)
	}

	defer s.removeGuestFile(ctx, env, ws, s.guestOutBundle(env))
	if err := s.guest(ctx, env, ws, nil, "sh", "-c", exportBundleScript, "sh", s.transferDir(env), "refs/heads/"+branch,
		"refs/remotes/origin/"+s.repo.Base, "refs/remotes/origin/"+branch); err != nil {
		return false, fmt.Errorf("bundle issue branch %q in the environment: %w", branch, err)
	}
	if err := s.readGuestFile(ctx, env, ws, s.guestOutBundle(env), dst); err != nil {
		return false, err
	}
	return true, nil
}

// readGuestFile copies the file at guestPath in ws's environment to dst,
// a new host file, through cat run in the environment. It stops the
// command and fails, naming the limit, once the file exceeds
// max_bundle_mib.
func (s *Syncer) readGuestFile(ctx context.Context, env environment.Environment, ws environment.Workspace, guestPath, dst string) error {
	f, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600) //nolint:gosec // G304: a file in Sortie's own temporary directory
	if err != nil {
		return fmt.Errorf("create the exported bundle file: %w", err)
	}
	defer f.Close() //nolint:errcheck // closed and checked below on success

	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	cmd, err := env.Command(readCtx, ws, environment.CommandSpec{Argv: []string{"cat", "--", guestPath}})
	if err != nil {
		return err
	}
	out := &cappedFile{file: f, limit: int64(s.maxBundleMiB) << 20, stop: cancel}
	stderr := procutil.NewTailBuffer(guestOutputCap)
	res, err := procutil.RunCapture(readCtx, cmd, 0, procutil.CaptureParams{Stdout: out, Stderr: stderr, Logger: s.logger})
	if out.exceeded {
		return fmt.Errorf("the exported bundle exceeds max_bundle_mib (%d MiB)", s.maxBundleMiB)
	}
	if out.err != nil {
		return fmt.Errorf("write the exported bundle file: %w", out.err)
	}
	if err == nil {
		err = res.WaitErr
	}
	if err != nil {
		if msg := strings.TrimSpace(string(stderr.Bytes())); msg != "" {
			return fmt.Errorf("read the exported bundle from the environment: %w: %s", err, msg)
		}
		return fmt.Errorf("read the exported bundle from the environment: %w", err)
	}
	if !res.OutputComplete {
		return errors.New("read the exported bundle from the environment: output incomplete")
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("write the exported bundle file: %w", err)
	}
	return nil
}

// cappedFile writes to file until more than limit bytes arrive; then it
// writes nothing more, records that, and calls stop. Every write
// reports success so the capture keeps draining.
type cappedFile struct {
	file     *os.File
	limit    int64
	stop     func()
	n        int64
	exceeded bool
	err      error
}

func (c *cappedFile) Write(p []byte) (int, error) {
	if c.exceeded || c.err != nil {
		return len(p), nil
	}
	if c.n+int64(len(p)) > c.limit {
		c.exceeded = true
		c.stop()
		return len(p), nil
	}
	if _, err := c.file.Write(p); err != nil {
		c.err = err
		c.stop()
		return len(p), nil
	}
	c.n += int64(len(p))
	return len(p), nil
}

// importBundle checks the bundle at bundlePath, fetches the upstream,
// and imports the bundle's branch into the incoming ref of key unless
// there is nothing to push. It returns the branch's commit, whether it
// must be pushed, and whether the upstream's issue branch holds it.
// With onTopOf set, an imported commit that does not descend from it is
// refused and the incoming ref restored.
func (s *Syncer) importBundle(ctx context.Context, l *Locked, key, branch, bundlePath, onTopOf string) (commit string, needPush, upToDate bool, err error) {
	heads, err := l.ListHeads(ctx, bundlePath)
	if err != nil {
		return "", false, false, err
	}
	want := "refs/heads/" + branch
	for _, h := range heads {
		if h.Ref != want {
			return "", false, false, fmt.Errorf("the exported bundle names unexpected ref %q; only %s is accepted", h.Ref, want)
		}
	}
	if len(heads) != 1 {
		return "", false, false, fmt.Errorf("the exported bundle names %s %d times, want once", want, len(heads))
	}
	commit = heads[0].Object
	if !isObjectID(commit) {
		return "", false, false, fmt.Errorf("the exported bundle gives %s the invalid object id %q", want, commit)
	}

	fetchErr := l.FetchUpstream(ctx)
	if fetchErr != nil {
		s.logger.WarnContext(ctx, "upstream fetch failed; comparing the export with the mirror's last state",
			slog.String("repository", s.mirror.displayURL()), slog.Any("error", fetchErr))
	}
	unchanged, upToDate, err := s.matchesUpstream(ctx, l, key, branch, commit)
	if err != nil {
		return commit, false, false, err
	}
	if unchanged {
		return commit, false, upToDate, nil
	}
	if fetchErr != nil {
		return commit, false, false, fetchErr
	}

	incoming := IncomingRef(key)
	previous, _, err := l.RefValue(ctx, incoming)
	if err != nil {
		return commit, false, false, err
	}
	if err := l.FetchBundle(ctx, bundlePath, "+"+want+":"+incoming); err != nil {
		return commit, false, false, err
	}
	got, ok, err := l.RefValue(ctx, incoming)
	if err == nil && (!ok || got != commit) {
		err = fmt.Errorf("%s holds %q after the import, want %s", incoming, got, commit)
	}
	if err == nil {
		var typ string
		if typ, err = l.ObjectType(ctx, commit); err == nil && typ != "commit" {
			err = fmt.Errorf("the exported %s is a %s, not a commit", want, typ)
		}
	}
	if err == nil && onTopOf != "" {
		var onTop bool
		if onTop, err = l.isAncestorOf(ctx, onTopOf, commit); err == nil && !onTop {
			err = fmt.Errorf("the returned bundle's %s at %s is not on top of the seed %s", want, commit, onTopOf)
		}
	}
	if err != nil {
		if restoreErr := l.SetRef(ctx, incoming, previous); restoreErr != nil {
			return commit, false, false, errors.Join(err, restoreErr)
		}
		return commit, false, false, err
	}
	return commit, true, false, nil
}

// matchesUpstream reports whether there is nothing to push for commit,
// and whether the mirror's issue branch holds it. There is nothing to
// push when commit equals the mirror's issue branch, or its base branch
// when the mirror has no issue branch. There is nothing to push either
// when that branch already holds commit and the push's lease could not
// hold, because the issue branch is not [PushedRef] of key: someone
// built on the work, or merged it and deleted the branch. While the
// issue branch is Sortie's own push, a commit it holds is a rewind and
// is pushed.
func (s *Syncer) matchesUpstream(ctx context.Context, l *Locked, key, branch, commit string) (unchanged, onBranch bool, err error) {
	tip, ok, err := l.Ref(ctx, branch)
	if err != nil {
		return false, false, err
	}
	if !ok {
		base, baseOK, err := l.Ref(ctx, s.repo.Base)
		if err != nil || !baseOK {
			return false, false, err
		}
		if base == commit {
			return true, false, nil
		}
		tip = base
	} else if tip == commit {
		return true, true, nil
	}

	pushed, _, err := l.RefValue(ctx, PushedRef(key))
	if err != nil {
		return false, false, err
	}
	if (ok && tip == pushed) || (!ok && pushed == "") {
		return false, false, nil
	}
	held, err := l.isAncestorOf(ctx, commit, tip)
	if err != nil || !held {
		return false, false, err
	}
	return true, ok, nil
}
