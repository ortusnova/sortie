package persistence

import (
	"context"
	"strings"
	"testing"
)

// Upstream migrations are numbered from 1 with no gap, and fork migrations
// from 1001 with no gap, so an upstream merge adds only versions below the
// fork range.
func TestMigrations_ForkRangeStartsAt1001(t *testing.T) {
	t.Parallel()
	upstream, fork := 0, 1000
	for _, m := range migrations {
		switch {
		case m.Version < 1001:
			if m.Version != upstream+1 {
				t.Errorf("migration %d (%s) follows upstream migration %d, want %d", m.Version, m.Description, upstream, upstream+1)
			}
			upstream = m.Version
		default:
			if m.Version != fork+1 {
				t.Errorf("migration %d (%s) follows fork migration %d, want %d", m.Version, m.Description, fork, fork+1)
			}
			fork = m.Version
		}
	}
}

// recordOldForkNumbering builds the database an earlier fork build left:
// upstream migrations 1 to 24, then the fork's three run_history columns
// recorded as versions 25 to 27.
func recordOldForkNumbering(t *testing.T, s *Store) {
	t.Helper()
	ctx := context.Background()
	migrateToVersion(t, s, 24)
	for old, current := range map[int]int{25: 1001, 26: 1002, 27: 1003} {
		for _, m := range migrations {
			if m.Version != current {
				continue
			}
			if err := s.applyMigration(ctx, Migration{Version: old, Description: m.Description, SQL: m.SQL}); err != nil {
				t.Fatalf("apply fork migration %d as %d: %v", current, old, err)
			}
		}
	}
}

func TestMigrate_RefusesOldForkNumbering(t *testing.T) {
	t.Parallel()
	s := openTestStore(t)
	recordOldForkNumbering(t, s)

	err := s.Migrate(context.Background())
	if err == nil || !strings.Contains(err.Error(), "delete the database file") {
		t.Fatalf("Migrate() = %v, want the old-fork-numbering refusal", err)
	}
}

// An upstream database at version 25 or later, without the fork's columns,
// is not mistaken for an old fork database.
func TestMigrate_UpstreamVersion25IsNotRefused(t *testing.T) {
	t.Parallel()
	s := openTestStore(t)
	migrateToVersion(t, s, 25)

	migrateOrFatal(t, s)
	if !hasColumn(t, s, "run_history", "held_ms") {
		t.Error("fork migration 1001 did not run on an upstream database")
	}
}

// A database migrated with the fork range migrates again with no change.
func TestMigrate_ForkRangeIsIdempotent(t *testing.T) {
	t.Parallel()
	s := openTestStore(t)
	migrateOrFatal(t, s)
	migrateOrFatal(t, s)
}

func hasColumn(t *testing.T, s *Store, table, column string) bool {
	t.Helper()
	var n int
	if err := s.db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM pragma_table_info(?) WHERE name = ?`, table, column).Scan(&n); err != nil {
		t.Fatalf("pragma_table_info(%s): %v", table, err)
	}
	return n == 1
}

// A database a fork build migrated before the merge holds upstream's
// migrations up to 24 and the fork's from 1001. The merged build applies
// upstream's newer migrations after the fork's, and every fork column and
// table survives them.
func TestMigrate_ForkDatabaseTakesLaterUpstreamMigrations(t *testing.T) {
	t.Parallel()
	s := openTestStore(t)
	ctx := context.Background()
	migrateToVersion(t, s, 24)
	for _, m := range migrations {
		if m.Version < 1001 {
			continue
		}
		if err := s.applyMigration(ctx, m); err != nil {
			t.Fatalf("apply fork migration %d: %v", m.Version, err)
		}
	}

	migrateOrFatal(t, s)

	applied, err := s.appliedVersions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range migrations {
		if _, ok := applied[m.Version]; !ok {
			t.Errorf("migration %d (%s) not applied", m.Version, m.Description)
		}
	}
	for _, column := range []string{"held_ms", "environment_image_sha256", "exported_commit", "error_detail", "failure_signature", "dispatch_id", "skills"} {
		if !hasColumn(t, s, "run_history", column) {
			t.Errorf("run_history lacks %s after the upgrade", column)
		}
	}
	if !hasColumn(t, s, "park_record_releases", "head") || !hasColumn(t, s, "parked_issues", "release_head") {
		t.Error("the park record release columns lost after the upgrade")
	}
	if !hasColumn(t, s, "plan_satisfaction", "issue_id") {
		t.Error("plan_satisfaction lost after the upgrade")
	}
	if !hasColumn(t, s, "session_grants", "sessions") {
		t.Error("session_grants lost after the upgrade")
	}
}
