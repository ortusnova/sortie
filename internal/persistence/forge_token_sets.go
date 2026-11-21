package persistence

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ForgeTokenSet is one set of GitHub tokens Sortie minted, as a
// restarted process needs it to refresh or revoke them. Grant and Sealed
// are opaque to this package; Sealed holds the tokens, encrypted.
type ForgeTokenSet struct {
	ID string
	// Kind is "attempt" or "sandbox".
	Kind string
	// JobID and Attempt name the attempt of an attempt's set.
	JobID   string
	Attempt int
	Grant   string
	Sealed  string
	// Ended reports that the set's attempt or sandbox ended: its tokens
	// are only kept until they are revoked or expire.
	Ended bool
	// RevokeError is the last revocation's error, and RevokeAttempts how
	// many revocations failed.
	RevokeError    string
	RevokeAttempts int
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// SaveForgeTokenSet records s, replacing the row of its ID.
func (s *Store) SaveForgeTokenSet(ctx context.Context, set ForgeTokenSet) error {
	if set.ID == "" || set.Kind == "" || set.Sealed == "" {
		return errors.New("save forge token set: an ID, a kind and the sealed tokens are required")
	}
	created := set.CreatedAt
	if created.IsZero() {
		created = time.Now()
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT OR REPLACE INTO forge_token_sets
			(id, kind, job_id, attempt, grant_json, sealed, ended, revoke_error, revoke_attempts, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		set.ID, set.Kind, set.JobID, set.Attempt, set.Grant, set.Sealed, set.Ended, set.RevokeError, set.RevokeAttempts,
		created.UTC().Format(time.RFC3339Nano), nowText(),
	)
	if err != nil {
		return fmt.Errorf("save forge token set: %w", err)
	}
	return nil
}

// DeleteForgeTokenSet forgets a set, once none of its tokens can be
// used.
func (s *Store) DeleteForgeTokenSet(ctx context.Context, id string) error {
	if _, err := s.db.ExecContext(ctx, `DELETE FROM forge_token_sets WHERE id = ?`, id); err != nil {
		return fmt.Errorf("delete forge token set: %w", err)
	}
	return nil
}

// LoadForgeTokenSets returns every recorded set, ordered by ID.
func (s *Store) LoadForgeTokenSets(ctx context.Context) ([]ForgeTokenSet, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, kind, job_id, attempt, grant_json, sealed, ended, revoke_error, revoke_attempts, created_at, updated_at
		FROM forge_token_sets ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("query forge token sets: %w", err)
	}
	defer rows.Close() //nolint:errcheck // read-only query; close error is non-actionable
	var out []ForgeTokenSet
	for rows.Next() {
		var set ForgeTokenSet
		var created, updated string
		if err := rows.Scan(&set.ID, &set.Kind, &set.JobID, &set.Attempt, &set.Grant, &set.Sealed, &set.Ended,
			&set.RevokeError, &set.RevokeAttempts, &created, &updated); err != nil {
			return nil, fmt.Errorf("scan forge token set: %w", err)
		}
		if t, err := time.Parse(time.RFC3339Nano, created); err == nil {
			set.CreatedAt = t
		}
		if t, err := time.Parse(time.RFC3339Nano, updated); err == nil {
			set.UpdatedAt = t
		}
		out = append(out, set)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate forge token sets: %w", err)
	}
	return out, nil
}
