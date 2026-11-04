package persistence

import (
	"context"
	"fmt"
	"strings"
)

// ParkRecordRelease is one row of park_record_releases: the latest park of
// a story that a pull request record released. PullRequest is
// owner/repo#number, Comments the ID of each comment that held one of the
// records, and ReleasedAt RFC 3339 in UTC.
type ParkRecordRelease struct {
	IssueID     string
	PullRequest string
	Head        string
	Comments    []string
	ReleasedAt  string
}

// recordParkRecordRelease stores r as the issue's latest record release,
// replacing an earlier one.
func recordParkRecordRelease(ctx context.Context, db statementExecer, r ParkRecordRelease) error {
	_, err := db.ExecContext(ctx,
		`INSERT INTO park_record_releases (issue_id, pull_request, head, comments, released_at)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT (issue_id) DO UPDATE SET
			pull_request = excluded.pull_request,
			head         = excluded.head,
			comments     = excluded.comments,
			released_at  = excluded.released_at`,
		r.IssueID, r.PullRequest, r.Head, strings.Join(r.Comments, ","), r.ReleasedAt,
	)
	if err != nil {
		return fmt.Errorf("record park record release %q: %w", r.IssueID, err)
	}
	return nil
}
