package gitsync

import (
	"context"
	"fmt"
)

// BaseFile is a file read at the tip of the repository's base branch.
type BaseFile struct {
	// Branch is the base branch.
	Branch string

	// Commit is the base branch's commit the file was read at.
	Commit string

	// Found reports that the commit's tree holds the file.
	Found bool

	// Data is the file's content; nil when it was not found.
	Data []byte
}

// ReadBaseFile fetches the upstream into the mirror, failing when that
// fails, and returns the file at path, relative to the repository root,
// at the tip of the base branch. It never reads an issue branch. A file
// larger than maxBytes returns an error that wraps [ErrFileTooLarge],
// with Branch and Commit set.
func (s *Syncer) ReadBaseFile(ctx context.Context, path string, maxBytes int64) (BaseFile, error) {
	l, err := s.mirror.Lock(ctx)
	if err != nil {
		return BaseFile{}, fmt.Errorf("lock the repository mirror: %w", err)
	}
	defer l.Unlock()
	if err := l.FetchUpstream(ctx); err != nil {
		return BaseFile{}, err
	}
	base, ok, err := l.Ref(ctx, s.repo.Base)
	if err != nil {
		return BaseFile{}, err
	} else if !ok {
		return BaseFile{}, fmt.Errorf("base branch %q is not in the mirror of %s", s.repo.Base, s.mirror.displayURL())
	}
	return s.fileAt(ctx, l, base, path, maxBytes)
}

// FileAt returns the file at path, relative to the repository root, at
// commit of the base branch as the mirror holds it, without a fetch.
// commit must be a full hexadecimal object id, such as a seed's
// [Seed.Base].
func (s *Syncer) FileAt(ctx context.Context, commit, path string, maxBytes int64) (BaseFile, error) {
	l, err := s.mirror.Lock(ctx)
	if err != nil {
		return BaseFile{}, fmt.Errorf("lock the repository mirror: %w", err)
	}
	defer l.Unlock()
	return s.fileAt(ctx, l, commit, path, maxBytes)
}

func (s *Syncer) fileAt(ctx context.Context, l *Locked, commit, path string, maxBytes int64) (BaseFile, error) {
	f := BaseFile{Branch: s.repo.Base, Commit: commit}
	data, found, err := l.FileAt(ctx, commit, path, maxBytes)
	f.Found = found
	if err != nil {
		return f, err
	}
	f.Data = data
	return f, nil
}
