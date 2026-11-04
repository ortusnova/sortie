package gitsync

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/sortie-ai/sortie/internal/agent/procutil"
	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/environment"
)

// GuestGitDir is the directory inside the environment that bundles pass
// through unless the environment names its own ([environment.TransferDirNamer]).
// It is on the guest's tmpfs, outside the workspace and outside
// the control directory the host shares.
const GuestGitDir = "/run/sortie/git"

// defaultMaxBundleMiB caps an exported bundle when the repository sets
// no cap, the configuration's own default.
const defaultMaxBundleMiB = 512

// guestOutputCap bounds the output a failed guest command reports.
const guestOutputCap = 4 << 10

// excludeControlDirScript adds /.sortie to the clone's info/exclude, run
// at the workspace root. The control directory link placed there after
// after_create then never shows as untracked, so a commit of every
// change cannot carry it onto the issue branch.
const excludeControlDirScript = `exclude="$(git rev-parse --git-path info/exclude)" && mkdir -p "$(dirname "$exclude")" && printf '%s\n' /.sortie >> "$exclude"`

// guestCleanupTimeout bounds the removal of a bundle from the guest.
const guestCleanupTimeout = 30 * time.Second

// Params configures a [Syncer].
type Params struct {
	// Repository is the configured repository.
	Repository config.Repository

	// StateDir is Sortie's state directory; the mirror lives under it.
	StateDir string

	// Logger receives warnings. Nil is [slog.Default].
	Logger *slog.Logger
}

// Syncer places the configured repository in a workspace's environment
// and keeps it current, through the repository's mirror.
type Syncer struct {
	repo   config.Repository
	mirror *Mirror
	logger *slog.Logger

	// guestDir is the directory bundles pass through in an environment
	// that names none of its own.
	guestDir string

	// maxBundleMiB caps an exported bundle, in MiB.
	maxBundleMiB int
}

// New returns the syncer of p.Repository, whose mirror lives under
// p.StateDir.
func New(p Params) (*Syncer, error) {
	if p.Repository.Base == "" {
		return nil, errors.New("gitsync: repository base branch is empty")
	}
	m, err := OpenMirror(p.StateDir, p.Repository.URL)
	if err != nil {
		return nil, err
	}
	logger := p.Logger
	if logger == nil {
		logger = slog.Default()
	}
	maxBundleMiB := p.Repository.MaxBundleMiB
	if maxBundleMiB <= 0 {
		maxBundleMiB = defaultMaxBundleMiB
	}
	return &Syncer{repo: p.Repository, mirror: m, logger: logger, guestDir: GuestGitDir, maxBundleMiB: maxBundleMiB}, nil
}

// IssueBranch returns the issue branch of the workspace key: the
// configured branch prefix followed by the key.
func (s *Syncer) IssueBranch(key string) string { return s.repo.BranchPrefix + key }

// GuestRemoteURL returns raw as the guest's origin shows it, without the
// credentials it may carry. An http, https, ftp, or ftps URL loses its
// whole userinfo, since a token is often given as the user name. Any
// other URL with a scheme loses the password of its userinfo and keeps
// the user name, such as git in ssh://git@host/repo. An scp-like
// address, which cannot carry a password, is returned unchanged.
func GuestRemoteURL(raw string) string {
	scheme, rest, ok := strings.Cut(raw, "://")
	if !ok {
		return raw
	}
	authorityEnd := len(rest)
	if i := strings.IndexAny(rest, "/?#"); i >= 0 {
		authorityEnd = i
	}
	at := strings.LastIndex(rest[:authorityEnd], "@")
	if at < 0 {
		return raw
	}
	userinfo, hostAndPath := rest[:at], rest[at+1:]
	switch strings.ToLower(scheme) {
	case "http", "https", "ftp", "ftps":
		return scheme + "://" + hostAndPath
	}
	user, _, _ := strings.Cut(userinfo, ":")
	return scheme + "://" + user + "@" + hostAndPath
}

// Seed places the repository in ws's newly created environment, whose
// workspace directory must be empty. It fetches the upstream into the
// mirror, failing when that fails, and sends the environment a bundle of
// the base branch and, when the upstream has it, the issue branch. In
// the environment it clones the bundle into the workspace with the
// remote origin, excludes /.sortie from the clone so the control
// directory link never reaches a commit, checks out the issue branch,
// and sets origin's URL to
// the repository URL without credentials ([GuestRemoteURL]).
//
// The issue branch tracks origin/<issue branch> when the upstream has
// it. Otherwise it is created at origin/<base> without an upstream, so
// it never tracks the base branch.
//
// An issue branch that is not a valid git branch name fails before
// anything is fetched, with an error naming the branch.
func (s *Syncer) Seed(ctx context.Context, env environment.Environment, ws environment.Workspace) error {
	branch := s.IssueBranch(ws.Key)
	hasBranch, err := s.sendBundle(ctx, env, ws, branch, true)
	if err != nil {
		return err
	}
	defer s.removeGuestBundle(ctx, env, ws)

	if err := s.guest(ctx, env, ws, nil, "git", "clone", "--quiet", "--no-checkout", "--origin", "origin", "--", s.guestBundle(env), ws.GuestPath); err != nil {
		return fmt.Errorf("clone the repository in the environment: %w", err)
	}
	if err := s.guest(ctx, env, ws, nil, "sh", "-c", excludeControlDirScript); err != nil {
		return fmt.Errorf("exclude the control directory in the environment: %w", err)
	}
	checkout := []string{"git", "checkout", "--quiet", "--no-track", "-b", branch, "origin/" + s.repo.Base}
	if hasBranch {
		checkout = []string{"git", "checkout", "--quiet", "--track", "-b", branch, "origin/" + branch}
	}
	if err := s.guest(ctx, env, ws, nil, checkout...); err != nil {
		return fmt.Errorf("check out issue branch %q in the environment: %w", branch, err)
	}
	if err := s.guest(ctx, env, ws, nil, "git", "remote", "set-url", "origin", GuestRemoteURL(s.repo.URL)); err != nil {
		return fmt.Errorf("set origin's URL in the environment: %w", err)
	}
	return nil
}

// Refresh brings the upstream's base branch and issue branch into ws's
// existing environment as origin/<base> and origin/<issue branch>,
// leaving its local branches, index, and work tree as they are. It
// fetches the upstream into the mirror first; when that fails it logs a
// warning and sends the mirror's last state instead. In the environment
// it fetches the bundle into refs/remotes/origin/* with pruning, so
// origin/* holds exactly the upstream's two branches: a remote-tracking
// ref of a branch the upstream deleted, or that the bundle does not
// carry, is removed.
func (s *Syncer) Refresh(ctx context.Context, env environment.Environment, ws environment.Workspace) error {
	branch := s.IssueBranch(ws.Key)
	if _, err := s.sendBundle(ctx, env, ws, branch, false); err != nil {
		return err
	}
	defer s.removeGuestBundle(ctx, env, ws)

	if err := s.guest(ctx, env, ws, nil, "git", "fetch", "--quiet", "--prune", "--no-tags", "--", s.guestBundle(env), "+refs/heads/*:refs/remotes/origin/*"); err != nil {
		return fmt.Errorf("refresh origin in the environment: %w", err)
	}
	return nil
}

// sendBundle builds the bundle of the base branch and, when the mirror
// has it, branch, and writes it to the guest bundle path in ws's
// environment. It reports whether the bundle holds branch. A failed
// upstream fetch fails when strict is set; otherwise it is logged and
// the bundle is built from the mirror's last state.
func (s *Syncer) sendBundle(ctx context.Context, env environment.Environment, ws environment.Workspace, branch string, strict bool) (bool, error) {
	bundle, hasBranch, err := s.buildBundle(ctx, branch, strict)
	if err != nil {
		return false, err
	}
	defer os.RemoveAll(filepath.Dir(bundle)) //nolint:errcheck // a leftover temporary bundle only costs disk

	f, err := os.Open(bundle) //nolint:gosec // G304: the bundle Sortie just wrote under its state directory
	if err != nil {
		return false, fmt.Errorf("open bundle: %w", err)
	}
	defer f.Close() //nolint:errcheck // read only
	script := `umask 077 && mkdir -p "$1" && cat > "$1/in.bundle"`
	if err := s.guest(ctx, env, ws, f, "sh", "-c", script, "sh", s.transferDir(env)); err != nil {
		return false, fmt.Errorf("send the repository bundle to the environment: %w", err)
	}
	return hasBranch, nil
}

// buildBundle locks the mirror, checks branch's name, fetches the
// upstream, and writes the bundle of the base branch, plus branch when
// the mirror has it, to a new temporary directory under the state
// directory. The caller removes that directory. A failed upstream fetch
// is handled as [Syncer.sendBundle] describes for strict.
func (s *Syncer) buildBundle(ctx context.Context, branch string, strict bool) (string, bool, error) {
	l, err := s.mirror.Lock(ctx)
	if err != nil {
		return "", false, fmt.Errorf("lock the repository mirror: %w", err)
	}
	defer l.Unlock()

	if err := l.CheckBranchName(ctx, branch); err != nil {
		return "", false, err
	}
	if err := l.FetchUpstream(ctx); err != nil {
		if strict {
			return "", false, err
		}
		s.logger.WarnContext(ctx, "upstream fetch failed; refreshing the environment from the mirror's last state",
			slog.String("repository", s.mirror.displayURL()), slog.Any("error", err))
	}
	if _, ok, err := l.Ref(ctx, s.repo.Base); err != nil {
		return "", false, err
	} else if !ok {
		return "", false, fmt.Errorf("base branch %q is not in the mirror of %s", s.repo.Base, s.mirror.displayURL())
	}
	_, hasBranch, err := l.Ref(ctx, branch)
	if err != nil {
		return "", false, err
	}

	dir, err := os.MkdirTemp(filepath.Dir(s.mirror.Dir()), "bundle-")
	if err != nil {
		return "", false, fmt.Errorf("create bundle directory: %w", err)
	}
	bundle := filepath.Join(dir, "in.bundle")
	branches := []string{s.repo.Base}
	if hasBranch && branch != s.repo.Base {
		branches = append(branches, branch)
	}
	if err := l.CreateBundle(ctx, bundle, branches...); err != nil {
		_ = os.RemoveAll(dir) //nolint:errcheck // best effort after a failure
		return "", false, err
	}
	return bundle, hasBranch, nil
}

// guestBundle returns the bundle's path inside the environment.
func (s *Syncer) guestBundle(env environment.Environment) string {
	return path.Join(s.transferDir(env), "in.bundle")
}

// transferDir returns the directory bundles pass through in env: the one
// env names, or the syncer's own.
func (s *Syncer) transferDir(env environment.Environment) string {
	return environment.GuestTransferDir(env, s.guestDir)
}

// removeGuestBundle deletes the bundle from the environment, even when
// ctx has ended. A failure is logged: the bundle is on the guest's tmpfs
// and the next transfer replaces it.
func (s *Syncer) removeGuestBundle(ctx context.Context, env environment.Environment, ws environment.Workspace) {
	s.removeGuestFile(ctx, env, ws, s.guestBundle(env))
}

// removeGuestFile deletes the file at guestPath from the environment,
// even when ctx has ended, logging a failure.
func (s *Syncer) removeGuestFile(ctx context.Context, env environment.Environment, ws environment.Workspace, guestPath string) {
	rmCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), guestCleanupTimeout)
	defer cancel()
	if err := s.guest(rmCtx, env, ws, nil, "rm", "-f", "--", guestPath); err != nil {
		s.logger.WarnContext(ctx, "removing the repository bundle from the environment failed",
			slog.String("workspace", ws.HostPath), slog.Any("error", err))
	}
}

// guest runs argv in ws's environment at the workspace root, with stdin
// as its standard input when it is not nil. A failure carries the tail of
// the command's output, secrets masked.
func (s *Syncer) guest(ctx context.Context, env environment.Environment, ws environment.Workspace, stdin io.Reader, argv ...string) error {
	cmd, err := env.Command(ctx, ws, environment.CommandSpec{Argv: argv, Stdin: stdin != nil})
	if err != nil {
		return err
	}
	cmd.Stdin = stdin
	output := procutil.NewTailBuffer(guestOutputCap)
	res, err := procutil.RunCapture(ctx, cmd, 0, procutil.CaptureParams{Stdout: output, Stderr: output, Logger: s.logger})
	if err == nil {
		err = res.WaitErr
	}
	if err != nil {
		if out := strings.TrimSpace(string(output.Bytes())); out != "" {
			return fmt.Errorf("%s %s: %w: %s", argv[0], argv[1], err, out)
		}
		return fmt.Errorf("%s %s: %w", argv[0], argv[1], err)
	}
	return nil
}

// CommitOnBase reports whether commit is on the base branch, as the
// mirror last fetched it. It runs only on the host mirror, never in a
// workspace the agent controls.
func (s *Syncer) CommitOnBase(ctx context.Context, commit string) (bool, error) {
	locked, err := s.mirror.Lock(ctx)
	if err != nil {
		return false, err
	}
	defer locked.Unlock()
	return locked.IsAncestor(ctx, commit, s.repo.Base)
}
