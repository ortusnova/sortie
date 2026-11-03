package persistence

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// SessionGrant is sessions an operator granted one issue beyond
// agent.max_sessions.
type SessionGrant struct {
	IssueID    string
	Identifier string
	Sessions   int // positive
	GrantedBy  string
	Reason     string
	GrantedAt  time.Time
}

// AddSessionGrant records g and returns the sessions granted to its issue
// in total, g included.
func (s *Store) AddSessionGrant(ctx context.Context, g SessionGrant) (int, error) {
	if g.IssueID == "" || g.Sessions <= 0 {
		return 0, errors.New("add session grant: an issue and a positive number of sessions are required")
	}
	at := g.GrantedAt
	if at.IsZero() {
		at = time.Now()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("add session grant: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // a no-op after Commit
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO session_grants (issue_id, identifier, sessions, granted_by, reason, granted_at)
		VALUES (?, ?, ?, ?, ?, ?)`,
		g.IssueID, g.Identifier, g.Sessions, g.GrantedBy, g.Reason, at.UTC().Format(time.RFC3339Nano),
	); err != nil {
		return 0, fmt.Errorf("add session grant: %w", err)
	}
	var total int
	if err := tx.QueryRowContext(ctx,
		`SELECT COALESCE(SUM(sessions), 0) FROM session_grants WHERE issue_id = ?`, g.IssueID,
	).Scan(&total); err != nil {
		return 0, fmt.Errorf("sum session grants: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("add session grant: %w", err)
	}
	return total, nil
}

// SessionGrantTotals returns the sessions granted to each issue that has a
// grant, keyed by issue id.
func (s *Store) SessionGrantTotals(ctx context.Context) (map[string]int, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT issue_id, SUM(sessions) FROM session_grants GROUP BY issue_id`)
	if err != nil {
		return nil, fmt.Errorf("query session grants: %w", err)
	}
	defer rows.Close() //nolint:errcheck // read-only query; close error is non-actionable
	out := map[string]int{}
	for rows.Next() {
		var id string
		var total int
		if err := rows.Scan(&id, &total); err != nil {
			return nil, fmt.Errorf("scan session grant: %w", err)
		}
		out[id] = total
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate session grants: %w", err)
	}
	return out, nil
}

// SessionGrantTotal returns the sessions granted to one issue, 0 when it
// has none.
func (s *Store) SessionGrantTotal(ctx context.Context, issueID string) (int, error) {
	var total int
	if err := s.db.QueryRowContext(ctx,
		`SELECT COALESCE(SUM(sessions), 0) FROM session_grants WHERE issue_id = ?`, issueID,
	).Scan(&total); err != nil {
		return 0, fmt.Errorf("sum session grants of %q: %w", issueID, err)
	}
	return total, nil
}
