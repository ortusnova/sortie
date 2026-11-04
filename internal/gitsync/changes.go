package gitsync

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// ChangedPaths returns the paths, sorted, that the issue branch of the
// workspace key changes against the base branch, as the mirror holds
// them: the branch the mirror last fetched from the upstream and the
// import [IncomingRef] holds, which an export that could not push left
// there. It fetches nothing and runs only on the host mirror. A mirror
// that does not exist yet, or that holds neither ref, has none.
func (s *Syncer) ChangedPaths(ctx context.Context, key string) ([]string, error) {
	if _, err := os.Stat(filepath.Join(s.mirror.Dir(), "HEAD")); err != nil {
		return nil, nil
	}
	branch := s.IssueBranch(key)
	if err := s.checkExportRefs(ctx, key, branch); err != nil {
		return nil, err
	}
	l, err := s.mirror.Lock(ctx)
	if err != nil {
		return nil, fmt.Errorf("lock the repository mirror: %w", err)
	}
	defer l.Unlock()
	base, ok, err := l.Ref(ctx, s.repo.Base)
	if err != nil || !ok {
		return nil, err
	}
	seen := map[string]bool{}
	var paths []string
	for _, ref := range []string{"refs/heads/" + branch, IncomingRef(key)} {
		commit, ok, err := l.RefValue(ctx, ref)
		if err != nil {
			return nil, err
		}
		if !ok || commit == base {
			continue
		}
		out, err := l.m.git.run(ctx, "diff", "--name-only", "--no-renames", "-z", "--end-of-options", base+"..."+commit)
		if err != nil {
			return nil, fmt.Errorf("list the paths %s changes in the mirror: %w", ref, err)
		}
		for p := range strings.SplitSeq(string(out), "\x00") {
			if p != "" && !seen[p] {
				seen[p] = true
				paths = append(paths, p)
			}
		}
	}
	slices.Sort(paths)
	return paths, nil
}
