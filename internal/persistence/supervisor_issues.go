package persistence

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// SupervisorIssue is one issue the supervisor filed: its kind, the stories
// it concerns, and the fault signature it names, by which a later filing
// joins it.
type SupervisorIssue struct {
	IssueID    string
	Identifier string
	Kind       string
	Stories    []string // sorted
	Signature  string   // folded; empty when the filing named none
	FiledAt    time.Time
}

// SaveSupervisorIssue stores issue, replacing the row of the same issue ID.
func (s *Store) SaveSupervisorIssue(ctx context.Context, issue SupervisorIssue) error {
	if issue.IssueID == "" || issue.Kind == "" {
		return errors.New("save supervisor issue: an issue ID and a kind are required")
	}
	at := issue.FiledAt
	if at.IsZero() {
		at = time.Now()
	}
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO supervisor_issues (issue_id, identifier, kind, stories, signature, filed_at)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT (issue_id) DO UPDATE SET
			identifier = excluded.identifier,
			kind       = excluded.kind,
			stories    = excluded.stories,
			signature  = excluded.signature,
			filed_at   = excluded.filed_at`,
		issue.IssueID, issue.Identifier, issue.Kind, strings.Join(issue.Stories, ","), issue.Signature,
		at.UTC().Format(time.RFC3339Nano),
	); err != nil {
		return fmt.Errorf("save supervisor issue %q: %w", issue.IssueID, err)
	}
	return nil
}

// SupervisorIssues returns every issue the supervisor filed, newest first.
// It returns an empty slice, not nil, when there is none.
func (s *Store) SupervisorIssues(ctx context.Context) ([]SupervisorIssue, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT issue_id, identifier, kind, stories, signature, filed_at
		FROM supervisor_issues ORDER BY filed_at DESC, issue_id`)
	if err != nil {
		return nil, fmt.Errorf("query supervisor issues: %w", err)
	}
	defer rows.Close() //nolint:errcheck // read-only query; close error is non-actionable
	out := []SupervisorIssue{}
	for rows.Next() {
		var issue SupervisorIssue
		var stories, at string
		if err := rows.Scan(&issue.IssueID, &issue.Identifier, &issue.Kind, &stories, &issue.Signature, &at); err != nil {
			return nil, fmt.Errorf("scan supervisor issue: %w", err)
		}
		issue.Stories = []string{}
		if stories != "" {
			issue.Stories = strings.Split(stories, ",")
		}
		if issue.FiledAt, err = time.Parse(time.RFC3339Nano, at); err != nil {
			return nil, fmt.Errorf("parse supervisor issue time %q: %w", at, err)
		}
		out = append(out, issue)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate supervisor issues: %w", err)
	}
	return out, nil
}
