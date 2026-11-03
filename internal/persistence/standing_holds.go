package persistence

import (
	"context"
	"fmt"
)

// StandingHold is one row of the standing_holds table: a lease on an issue
// that is not running, which keeps it out of dispatch until a release.
type StandingHold struct {
	IssueID    string
	Identifier string
	Holder     string // who holds the issue, as the request said; may be empty
	Reason     string // why, as the request said; may be empty
	Evidence   string // what the holder is doing, as the latest request said
	HeldAt     string // RFC 3339
	RenewedAt  string // RFC 3339
	Until      string // RFC 3339: when the lease ends without a renewal
	ExpiredAt  string // RFC 3339, or "" while the lease is live
}

// UpsertStandingHold persists a standing hold. A row for hold.IssueID
// already present is replaced entirely.
func (s *Store) UpsertStandingHold(ctx context.Context, hold StandingHold) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO standing_holds (issue_id, identifier, holder, reason, evidence, held_at, renewed_at, held_until, expired_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (issue_id) DO UPDATE SET
			identifier = excluded.identifier,
			holder     = excluded.holder,
			reason     = excluded.reason,
			evidence   = excluded.evidence,
			held_at    = excluded.held_at,
			renewed_at = excluded.renewed_at,
			held_until = excluded.held_until,
			expired_at = excluded.expired_at`,
		hold.IssueID, hold.Identifier, hold.Holder, hold.Reason, hold.Evidence, hold.HeldAt, hold.RenewedAt, hold.Until, hold.ExpiredAt,
	)
	if err != nil {
		return fmt.Errorf("upsert standing hold %q: %w", hold.IssueID, err)
	}
	return nil
}

// DeleteStandingHold removes the standing hold of the given issue ID. It is
// a no-op, not an error, when no row exists for that issue ID.
func (s *Store) DeleteStandingHold(ctx context.Context, issueID string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM standing_holds WHERE issue_id = ?`, issueID)
	if err != nil {
		return fmt.Errorf("delete standing hold %q: %w", issueID, err)
	}
	return nil
}

// ListStandingHolds returns every persisted standing hold. Ordering is
// unspecified. Returns an empty slice (not nil) when no rows exist.
func (s *Store) ListStandingHolds(ctx context.Context) ([]StandingHold, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT issue_id, identifier, holder, reason, evidence, held_at, renewed_at, held_until, expired_at FROM standing_holds`)
	if err != nil {
		return nil, fmt.Errorf("list standing holds: %w", err)
	}
	defer rows.Close() //nolint:errcheck // read-only query; close error is non-actionable

	holds := []StandingHold{}
	for rows.Next() {
		var h StandingHold
		if err := rows.Scan(&h.IssueID, &h.Identifier, &h.Holder, &h.Reason, &h.Evidence, &h.HeldAt, &h.RenewedAt, &h.Until, &h.ExpiredAt); err != nil {
			return nil, fmt.Errorf("scan standing hold: %w", err)
		}
		holds = append(holds, h)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list standing holds: %w", err)
	}
	return holds, nil
}
