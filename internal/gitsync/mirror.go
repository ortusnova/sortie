// Package gitsync moves git data between an upstream repository and a
// workspace's execution environment through a bare mirror that only
// Sortie writes. Host git runs only on that mirror, always with hooks,
// the filesystem monitor, and unchecked objects turned off; git data
// enters and leaves the environment only as a bundle piped through a
// command run in it, so host git never opens a repository the
// environment controls.
package gitsync

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sortie-ai/sortie/internal/httpkit"
	"github.com/sortie-ai/sortie/internal/redact"
)

// reposDir is the directory under the state directory that holds every
// mirror and its lock file.
const reposDir = "repos"

// lockPoll is how often a waiter retries a lock file another process
// holds.
const lockPoll = 25 * time.Millisecond

// MirrorPath returns the bare mirror directory of url under stateDir:
// <stateDir>/repos/<first 16 hex digits of sha256(url)>.git.
func MirrorPath(stateDir, url string) string {
	sum := sha256.Sum256([]byte(url))
	return filepath.Join(stateDir, reposDir, hex.EncodeToString(sum[:])[:16]+".git")
}

// Mirror is the bare mirror of one upstream URL. Every operation runs
// while the mirror is locked; see [Mirror.Lock].
type Mirror struct {
	url  string
	dir  string
	lock string
	git  runner
}

// OpenMirror returns the mirror of url under stateDir, an absolute
// directory. It touches nothing on disk; [Mirror.Lock] creates the
// mirror on first use. The credentials url carries are registered with
// [redact].
func OpenMirror(stateDir, url string) (*Mirror, error) {
	if stateDir == "" || !filepath.IsAbs(stateDir) {
		return nil, fmt.Errorf("gitsync: state directory %q is not an absolute path", stateDir)
	}
	if url == "" {
		return nil, errors.New("gitsync: repository url is empty")
	}
	// git may repeat the URL in its messages; registering its
	// credentials masks them in every error and log that carries one.
	redact.AddURLCredentials("environment.remote.repository.url", url)
	dir := MirrorPath(filepath.Clean(stateDir), url)
	return &Mirror{
		url:  url,
		dir:  dir,
		lock: strings.TrimSuffix(dir, ".git") + ".lock",
		git:  runner{gitDir: dir},
	}, nil
}

// Dir returns the mirror's directory.
func (m *Mirror) Dir() string { return m.dir }

// displayURL returns the upstream URL without credentials, for errors
// and logs.
func (m *Mirror) displayURL() string { return httpkit.RedactURLUserinfo(m.url) }

// inProcess holds one token per mirror directory, so goroutines of this
// process queue for a mirror without each blocking on its lock file.
var inProcess = struct {
	sync.Mutex
	tokens map[string]chan struct{}
}{tokens: make(map[string]chan struct{})}

func processToken(dir string) chan struct{} {
	inProcess.Lock()
	defer inProcess.Unlock()
	token, ok := inProcess.tokens[dir]
	if !ok {
		token = make(chan struct{}, 1)
		inProcess.tokens[dir] = token
	}
	return token
}

// Locked is a mirror held exclusively by one caller. Its methods are the
// only way to read or write the mirror. The holder must call
// [Locked.Unlock].
type Locked struct {
	m     *Mirror
	file  *os.File
	token chan struct{}
	once  sync.Once
}

// Lock waits until it holds the mirror exclusively, across goroutines
// and processes, and creates the mirror as an empty bare repository when
// it does not exist yet. It gives up when ctx ends.
func (m *Mirror) Lock(ctx context.Context) (*Locked, error) {
	token := processToken(m.dir)
	select {
	case token <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	file, err := m.lockFile(ctx)
	if err != nil {
		<-token
		return nil, err
	}
	l := &Locked{m: m, file: file, token: token}
	if err := l.ensure(ctx); err != nil {
		l.Unlock()
		return nil, err
	}
	return l, nil
}

// lockFile opens the mirror's lock file and waits for its exclusive
// lock.
func (m *Mirror) lockFile(ctx context.Context) (*os.File, error) {
	if err := os.MkdirAll(filepath.Dir(m.lock), 0o700); err != nil {
		return nil, fmt.Errorf("gitsync: create %s: %w", filepath.Dir(m.lock), err)
	}
	file, err := os.OpenFile(m.lock, os.O_RDWR|os.O_CREATE, 0o600) //nolint:gosec // G304: the lock path is derived from the state directory
	if err != nil {
		return nil, fmt.Errorf("gitsync: open lock file: %w", err)
	}
	for {
		locked, err := tryLockFile(file)
		if err != nil {
			_ = file.Close() //nolint:errcheck // the lock was never taken
			return nil, fmt.Errorf("gitsync: lock %s: %w", m.lock, err)
		}
		if locked {
			return file, nil
		}
		select {
		case <-ctx.Done():
			_ = file.Close() //nolint:errcheck // the lock was never taken
			return nil, ctx.Err()
		case <-time.After(lockPoll):
		}
	}
}

// Unlock releases the mirror. Later calls do nothing.
func (l *Locked) Unlock() {
	l.once.Do(func() {
		_ = l.file.Close() //nolint:errcheck // closing the file releases its lock
		<-l.token
	})
}

// ensure creates the mirror as a bare repository, without template
// files, unless it already is one.
func (l *Locked) ensure(ctx context.Context) error {
	if _, err := os.Stat(filepath.Join(l.m.dir, "HEAD")); err == nil {
		return nil
	}
	if err := os.MkdirAll(l.m.dir, 0o700); err != nil {
		return fmt.Errorf("gitsync: create mirror %s: %w", l.m.dir, err)
	}
	if _, err := l.m.git.run(ctx, "init", "--quiet", "--bare", "--template="); err != nil {
		return fmt.Errorf("gitsync: create mirror %s: %w", l.m.dir, err)
	}
	return nil
}

// FetchUpstream fetches every upstream branch into the mirror's
// refs/heads/*, with the host's git configuration and credentials, and
// prunes the branches the upstream no longer has. Refs outside
// refs/heads/* are never pruned. Tags are not fetched.
func (l *Locked) FetchUpstream(ctx context.Context) error {
	if _, err := l.m.git.run(ctx, "fetch", "--quiet", "--prune", "--no-tags", "--", l.m.url, "+refs/heads/*:refs/heads/*"); err != nil {
		return fmt.Errorf("fetch %s into the mirror: %w", l.m.displayURL(), err)
	}
	return nil
}

// FetchBundle fetches refspec from the bundle file at bundlePath into
// the mirror, checking every object it receives.
func (l *Locked) FetchBundle(ctx context.Context, bundlePath, refspec string) error {
	if _, err := l.m.git.run(ctx, "fetch", "--quiet", "--no-tags", "--", bundlePath, refspec); err != nil {
		return fmt.Errorf("import bundle into the mirror: %w", err)
	}
	return nil
}

// CheckBranchName fails, naming branch, when branch is not a valid git
// branch name.
func (l *Locked) CheckBranchName(ctx context.Context, branch string) error {
	if _, err := l.m.git.run(ctx, "check-ref-format", "--branch", branch); err != nil {
		if exitedWith(err, 1) || exitedWith(err, 128) {
			return fmt.Errorf("issue branch %q is not a valid git branch name: %w", branch, err)
		}
		return fmt.Errorf("check issue branch %q: %w", branch, err)
	}
	return nil
}

// Ref returns the commit refs/heads/<branch> holds in the mirror, and
// false when the mirror has no such branch.
func (l *Locked) Ref(ctx context.Context, branch string) (string, bool, error) {
	out, err := l.m.git.run(ctx, "rev-parse", "--verify", "--quiet", "--end-of-options", "refs/heads/"+branch+"^{commit}")
	if exitedWith(err, 1) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("resolve refs/heads/%s in the mirror: %w", branch, err)
	}
	return strings.TrimSpace(string(out)), true, nil
}

// CheckRefName fails, naming ref, when ref is not a valid full ref name.
func (l *Locked) CheckRefName(ctx context.Context, ref string) error {
	if _, err := l.m.git.run(ctx, "check-ref-format", ref); err != nil {
		if exitedWith(err, 1) || exitedWith(err, 128) {
			return fmt.Errorf("ref %q is not a valid git ref name: %w", ref, err)
		}
		return fmt.Errorf("check ref %q: %w", ref, err)
	}
	return nil
}

// RefValue returns the object the full ref name ref holds in the mirror,
// unpeeled, and false when the mirror has no such ref.
func (l *Locked) RefValue(ctx context.Context, ref string) (string, bool, error) {
	out, err := l.m.git.run(ctx, "rev-parse", "--verify", "--quiet", "--end-of-options", ref)
	if exitedWith(err, 1) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("resolve %s in the mirror: %w", ref, err)
	}
	return strings.TrimSpace(string(out)), true, nil
}

// ObjectType returns the type of the object id in the mirror, such as
// "commit". id must be a full hexadecimal object id.
func (l *Locked) ObjectType(ctx context.Context, id string) (string, error) {
	if !isObjectID(id) {
		return "", fmt.Errorf("%q is not an object id", id)
	}
	out, err := l.m.git.run(ctx, "cat-file", "-t", id)
	if err != nil {
		return "", fmt.Errorf("read the type of %s in the mirror: %w", id, err)
	}
	return strings.TrimSpace(string(out)), nil
}

// SetRef points the full ref name ref at id, or deletes ref when id is
// empty.
func (l *Locked) SetRef(ctx context.Context, ref, id string) error {
	args := []string{"update-ref", "--no-deref", "-d", ref}
	if id != "" {
		args = []string{"update-ref", "--no-deref", ref, id}
	}
	if _, err := l.m.git.run(ctx, args...); err != nil {
		return fmt.Errorf("update %s in the mirror: %w", ref, err)
	}
	return nil
}

// isObjectID reports whether s is a full SHA-1 or SHA-256 object id in
// lowercase hexadecimal.
func isObjectID(s string) bool {
	if len(s) != 40 && len(s) != 64 {
		return false
	}
	for _, c := range s {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// BundleHead is one ref a bundle names.
type BundleHead struct {
	// Object is the object id the bundle gives the ref.
	Object string

	// Ref is the ref's full name.
	Ref string
}

// ListHeads returns the refs the bundle file at bundlePath, an absolute
// path, names, in the bundle's order. It reads only the bundle's header.
func (l *Locked) ListHeads(ctx context.Context, bundlePath string) ([]BundleHead, error) {
	out, err := l.m.git.run(ctx, "bundle", "list-heads", bundlePath)
	if err != nil {
		return nil, fmt.Errorf("read the bundle's refs: %w", err)
	}
	var heads []BundleHead
	for line := range strings.Lines(string(out)) {
		line = strings.TrimRight(line, "\n")
		if line == "" {
			continue
		}
		object, ref, _ := strings.Cut(line, " ")
		heads = append(heads, BundleHead{Object: object, Ref: ref})
	}
	return heads, nil
}

// ErrBranchMoved reports a push refused because the upstream branch no
// longer holds the commit the push expected.
var ErrBranchMoved = errors.New("branch moved upstream")

// Push pushes the mirror's full ref src to the upstream's
// refs/heads/<branch>, with the host's git configuration and
// credentials, only while the upstream branch holds expected, or is
// absent when expected is empty. The upstream decides that, not a
// remote-tracking ref, so a replacement that is not a fast-forward is
// allowed when the lease holds. A push refused because the branch holds
// something else fails with [ErrBranchMoved]. Any other refusal names
// the remote's reason for it, and the [*Error] it wraps keeps the push's
// porcelain output and standard error.
func (l *Locked) Push(ctx context.Context, src, branch, expected string) error {
	dst := "refs/heads/" + branch
	out, err := l.m.git.run(ctx, "push", "--porcelain", "--no-follow-tags",
		"--force-with-lease="+dst+":"+expected, "--", l.m.url, src+":"+dst)
	if err == nil {
		return nil
	}
	if leaseRejected(string(out), dst) {
		return fmt.Errorf("push %s to %s: %w", branch, l.m.displayURL(), ErrBranchMoved)
	}
	if summary := refusal(string(out), dst); summary != "" {
		return fmt.Errorf("push %s to %s: %s: %w", branch, l.m.displayURL(), summary, err)
	}
	return fmt.Errorf("push %s to %s: %w", branch, l.m.displayURL(), err)
}

// leaseRejected reports whether the porcelain output of a push shows dst
// refused for a lease that no longer holds.
func leaseRejected(porcelain, dst string) bool {
	return strings.Contains(refusal(porcelain, dst), "(stale info)")
}

// refusal returns the summary the porcelain output of a push gives for
// dst refused, such as "[remote rejected] (pre-receive hook declined)",
// or "" when it shows no refusal of dst.
func refusal(porcelain, dst string) string {
	for line := range strings.Lines(porcelain) {
		flag, rest, ok := strings.Cut(strings.TrimRight(line, "\r\n"), "\t")
		if !ok || flag != "!" {
			continue
		}
		refs, summary, _ := strings.Cut(rest, "\t")
		if strings.HasSuffix(refs, ":"+dst) {
			return strings.TrimSpace(summary)
		}
	}
	return ""
}

// CreateBundle writes a bundle of refs/heads/<branch> for each branch to
// path, which must not exist.
func (l *Locked) CreateBundle(ctx context.Context, path string, branches ...string) error {
	args := []string{"bundle", "create", "--quiet", path}
	for _, b := range branches {
		args = append(args, "refs/heads/"+b)
	}
	if _, err := l.m.git.run(ctx, args...); err != nil {
		return fmt.Errorf("create bundle: %w", err)
	}
	return nil
}

// IsAncestor reports whether commit is reachable from the mirror's branch.
// A commit the mirror does not hold is not.
func (l *Locked) IsAncestor(ctx context.Context, commit, branch string) (bool, error) {
	return l.isAncestorOf(ctx, commit, "refs/heads/"+branch)
}

// isAncestorOf reports whether commit is reachable from rev, a commit or
// a full ref name. A commit the mirror does not hold is not.
func (l *Locked) isAncestorOf(ctx context.Context, commit, rev string) (bool, error) {
	_, err := l.m.git.run(ctx, "merge-base", "--is-ancestor", "--", commit, rev)
	if err == nil {
		return true, nil
	}
	var gerr *Error
	// Exit code 1 means not an ancestor; 128 means git does not know the
	// commit, which the mirror then does not hold.
	if errors.As(err, &gerr) && (gerr.ExitCode == 1 || gerr.ExitCode == 128) {
		return false, nil
	}
	return false, err
}

// ErrFileTooLarge reports a file larger than the bound its reader set.
var ErrFileTooLarge = errors.New("file too large")

// FileAt returns the content of the file at path, relative to the
// repository root, in the tree of commit, and false when that tree holds
// no file there. A file larger than maxBytes is not read: the error wraps
// [ErrFileTooLarge] and names its size. commit must be a full
// hexadecimal object id.
func (l *Locked) FileAt(ctx context.Context, commit, path string, maxBytes int64) ([]byte, bool, error) {
	if !isObjectID(commit) {
		return nil, false, fmt.Errorf("%q is not an object id", commit)
	}
	object := commit + ":" + path
	out, err := l.m.git.run(ctx, "cat-file", "-t", object)
	if exitedWith(err, 128) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("read %s at %s in the mirror: %w", path, commit, err)
	}
	if typ := strings.TrimSpace(string(out)); typ != "blob" {
		return nil, true, fmt.Errorf("%s at %s is a %s, not a file", path, commit, typ)
	}
	out, err = l.m.git.run(ctx, "cat-file", "-s", object)
	if err != nil {
		return nil, true, fmt.Errorf("read the size of %s at %s in the mirror: %w", path, commit, err)
	}
	size, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
	if err != nil {
		return nil, true, fmt.Errorf("read the size of %s at %s in the mirror: %w", path, commit, err)
	}
	if size > maxBytes {
		return nil, true, fmt.Errorf("%s at %s is %d bytes, over the limit of %d: %w", path, commit, size, maxBytes, ErrFileTooLarge)
	}
	data, err := l.m.git.run(ctx, "cat-file", "blob", object)
	if err != nil {
		return nil, true, fmt.Errorf("read %s at %s in the mirror: %w", path, commit, err)
	}
	return data, true, nil
}
