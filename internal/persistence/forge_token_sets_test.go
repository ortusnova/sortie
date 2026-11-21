package persistence

import (
	"context"
	"path/filepath"
	"testing"
)

func TestForgeTokenSets_OutliveARestartUntilDeleted(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sortie.db")
	open := func() *Store {
		t.Helper()
		s, err := Open(ctx, path)
		if err != nil {
			t.Fatalf("Open(%s): %v", path, err)
		}
		if err := s.Migrate(ctx); err != nil {
			t.Fatalf("Migrate: %v", err)
		}
		return s
	}

	s := open()
	a := ForgeTokenSet{ID: "attempt:ONS-30#1", Kind: "attempt", JobID: "ONS-30", Attempt: 1, Grant: `{"owners":[]}`, Sealed: "c2VhbGVk"}
	b := ForgeTokenSet{ID: "sandbox:fts-1", Kind: "sandbox", Grant: `{"owners":[]}`, Sealed: "b3RoZXI"}
	for _, set := range []ForgeTokenSet{a, b} {
		if err := s.SaveForgeTokenSet(ctx, set); err != nil {
			t.Fatalf("SaveForgeTokenSet: %v", err)
		}
	}
	a.Ended, a.RevokeError, a.RevokeAttempts = true, "HTTP 502", 1
	if err := s.SaveForgeTokenSet(ctx, a); err != nil {
		t.Fatalf("SaveForgeTokenSet (update): %v", err)
	}
	if err := s.SaveForgeTokenSet(ctx, ForgeTokenSet{ID: "x", Kind: "attempt"}); err == nil {
		t.Error("SaveForgeTokenSet without sealed tokens succeeded")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s = open()
	defer func() { _ = s.Close() }()
	sets, err := s.LoadForgeTokenSets(ctx)
	if err != nil {
		t.Fatalf("LoadForgeTokenSets: %v", err)
	}
	if len(sets) != 2 || sets[0].ID != a.ID || !sets[0].Ended || sets[0].RevokeError != "HTTP 502" || sets[0].RevokeAttempts != 1 ||
		sets[0].JobID != "ONS-30" || sets[0].Attempt != 1 || sets[0].Sealed != a.Sealed || sets[1].Kind != "sandbox" || sets[0].CreatedAt.IsZero() {
		t.Fatalf("after a restart: %+v", sets)
	}
	if err := s.DeleteForgeTokenSet(ctx, a.ID); err != nil {
		t.Fatalf("DeleteForgeTokenSet: %v", err)
	}
	if sets, _ := s.LoadForgeTokenSets(ctx); len(sets) != 1 || sets[0].ID != b.ID {
		t.Fatalf("after the delete: %+v", sets)
	}
}
