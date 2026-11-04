//go:build unix

package gitsync

import (
	"context"
	"errors"
	"testing"

	"github.com/sortie-ai/sortie/internal/config"
)

// The file is read at the tip of the base branch, never from an issue
// branch that holds another version of it.
func TestReadBaseFile_ReadsTheBaseBranchOnly(t *testing.T) {
	f := newSyncFixture(t, "ONS-30", config.Repository{})
	base := f.up.commit(".sortie/egress.yaml", "base\n")
	f.up.git("checkout", "--quiet", "-b", "sortie/ONS-30")
	f.up.commit(".sortie/egress.yaml", "issue branch\n")
	f.up.git("checkout", "--quiet", "main")

	got, err := f.syncer.ReadBaseFile(context.Background(), ".sortie/egress.yaml", 1024)
	if err != nil {
		t.Fatalf("ReadBaseFile() error = %v", err)
	}
	if !got.Found || string(got.Data) != "base\n" || got.Commit != base || got.Branch != "main" {
		t.Errorf("ReadBaseFile() = %+v %q, want the base's file at %s", got, got.Data, base)
	}

	// A commit merged to the base later is read after the next fetch.
	merged := f.up.commit(".sortie/egress.yaml", "merged\n")
	got, err = f.syncer.ReadBaseFile(context.Background(), ".sortie/egress.yaml", 1024)
	if err != nil || string(got.Data) != "merged\n" || got.Commit != merged {
		t.Errorf("ReadBaseFile() after a merge = %+v %q, %v; want %s", got, got.Data, err, merged)
	}
}

func TestReadBaseFile_NoFile(t *testing.T) {
	f := newSyncFixture(t, "ONS-30", config.Repository{})
	got, err := f.syncer.ReadBaseFile(context.Background(), ".sortie/egress.yaml", 1024)
	if err != nil || got.Found || got.Data != nil || got.Commit != f.up.git("rev-parse", "main") {
		t.Errorf("ReadBaseFile() = %+v, %v; want not found at main", got, err)
	}
}

func TestReadBaseFile_RefusesALargeFileAndADirectory(t *testing.T) {
	f := newSyncFixture(t, "ONS-30", config.Repository{})
	f.up.commit(".sortie/egress.yaml", "0123456789\n")
	got, err := f.syncer.ReadBaseFile(context.Background(), ".sortie/egress.yaml", 4)
	if !errors.Is(err, ErrFileTooLarge) || !got.Found || got.Commit == "" {
		t.Errorf("ReadBaseFile() of a large file = %+v, %v; want ErrFileTooLarge with the commit", got, err)
	}
	if _, err := f.syncer.ReadBaseFile(context.Background(), ".sortie", 1024); err == nil {
		t.Error("ReadBaseFile() of a directory succeeded, want an error")
	}
}

// FileAt reads the seed's base commit as the mirror holds it.
func TestFileAt_ReadsTheSeedsBase(t *testing.T) {
	f := newSyncFixture(t, "ONS-30", config.Repository{})
	f.up.commit(".sortie/egress.yaml", "at the seed\n")
	seed, _ := f.buildSeed()
	if seed.Base != f.up.git("rev-parse", "main") {
		t.Fatalf("seed.Base = %s, want main", seed.Base)
	}
	f.up.commit(".sortie/egress.yaml", "after the seed\n")
	got, err := f.syncer.FileAt(context.Background(), seed.Base, ".sortie/egress.yaml", 1024)
	if err != nil || string(got.Data) != "at the seed\n" || got.Commit != seed.Base {
		t.Errorf("FileAt() = %+v %q, %v; want the seed's base file", got, got.Data, err)
	}
	if _, err := f.syncer.FileAt(context.Background(), "main", ".sortie/egress.yaml", 1024); err == nil {
		t.Error("FileAt() of a branch name succeeded, want an object id required")
	}
}
