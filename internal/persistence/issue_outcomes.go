package persistence

import (
	"context"
	"fmt"
	"strconv"
	"strings"
)

// Issue outcome kinds.
const (
	IssueOutcomeHandedOff = "handed_off"
	IssueOutcomeMerged    = "merged"
	IssueOutcomeTerminal  = "terminal"
)

// IssueOutcome is how an issue's latest work ended, as the orchestrator
// observed it. ObservedAt is RFC 3339 in UTC. State is empty for a merged
// outcome; MergeReactionKind is set only for a merged outcome, and PRNumber
// and PRRepository for a merged one or for the handoff of a park a pull
// request record released.
//
// ReleaseHead and ReleaseComment (the comment ID of each record,
// comma-separated) name the pull request record that released the issue's
// park. [Store.RecordIssueOutcome] keeps them as the issue's latest record
// release, and [Store.IssueOutcomes] returns them with every later outcome
// of that same pull request, so the merge still names the record.
type IssueOutcome struct {
	IssueID           string
	Kind              string
	ObservedAt        string
	State             string
	MergeReactionKind string
	PRNumber          int
	PRRepository      string
	ReleaseHead       string
	ReleaseComment    string
}

// pullRequestKey is owner/repo#number, empty when number is not set.
func pullRequestKey(repository string, number int) string {
	if number <= 0 {
		return ""
	}
	return repository + "#" + strconv.Itoa(number)
}

// RecordIssueOutcome stores o as the issue's outcome. An existing row is
// replaced only when o is a handoff, when the stored row is a handoff, or
// when a stored terminal outcome meets a merged one; in every other pair the
// stored row stays, so a merge is never replaced by a terminal read and a
// first observation keeps its time. An outcome naming a release head also
// stores it as the issue's latest record release, in the same transaction.
func (s *Store) RecordIssueOutcome(ctx context.Context, o IssueOutcome) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin record issue outcome %q: %w", o.IssueID, err)
	}
	defer tx.Rollback() //nolint:errcheck // rollback on error path; no-op after commit

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO issue_outcomes (issue_id, kind, observed_at, state, merge_reaction_kind, pr_number, pr_repository)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (issue_id) DO UPDATE SET
			kind                = excluded.kind,
			observed_at         = excluded.observed_at,
			state               = excluded.state,
			merge_reaction_kind = excluded.merge_reaction_kind,
			pr_number           = excluded.pr_number,
			pr_repository       = excluded.pr_repository
		WHERE excluded.kind = 'handed_off'
		   OR issue_outcomes.kind = 'handed_off'
		   OR (issue_outcomes.kind = 'terminal' AND excluded.kind = 'merged')`,
		o.IssueID, o.Kind, o.ObservedAt, o.State, o.MergeReactionKind, o.PRNumber, o.PRRepository,
	); err != nil {
		return fmt.Errorf("record issue outcome %q: %w", o.IssueID, err)
	}
	if o.ReleaseHead != "" {
		var comments []string
		if o.ReleaseComment != "" {
			comments = strings.Split(o.ReleaseComment, ",")
		}
		if err := recordParkRecordRelease(ctx, tx, ParkRecordRelease{
			IssueID:     o.IssueID,
			PullRequest: pullRequestKey(o.PRRepository, o.PRNumber),
			Head:        o.ReleaseHead,
			Comments:    comments,
			ReleasedAt:  o.ObservedAt,
		}); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit issue outcome %q: %w", o.IssueID, err)
	}
	return nil
}

// DeleteIssueOutcome removes the issue's outcome. It is a no-op, not an
// error, when no row exists for that issue ID.
func (s *Store) DeleteIssueOutcome(ctx context.Context, issueID string) error {
	_, err := s.db.ExecContext(ctx,
		`DELETE FROM issue_outcomes WHERE issue_id = ?`, issueID)
	if err != nil {
		return fmt.Errorf("delete issue outcome %q: %w", issueID, err)
	}
	return nil
}

// IssueOutcomes returns the stored outcome of each listed issue that has one,
// keyed by issue ID, with the issue's latest record release when the outcome
// is of the pull request that release names. An empty list reads nothing and
// returns an empty, non-nil map.
func (s *Store) IssueOutcomes(ctx context.Context, issueIDs []string) (map[string]IssueOutcome, error) {
	outcomes := make(map[string]IssueOutcome, len(issueIDs))
	if len(issueIDs) == 0 {
		return outcomes, nil
	}

	placeholders := strings.Repeat("?,", len(issueIDs))
	placeholders = placeholders[:len(placeholders)-1]

	args := make([]any, len(issueIDs))
	for i, id := range issueIDs {
		args[i] = id
	}

	query := fmt.Sprintf( //nolint:gosec // placeholders is built only from len(issueIDs); values remain bound parameters
		`SELECT o.issue_id, o.kind, o.observed_at, o.state, o.merge_reaction_kind, o.pr_number, o.pr_repository,
			COALESCE(r.pull_request, ''), COALESCE(r.head, ''), COALESCE(r.comments, '')
		FROM issue_outcomes o
		LEFT JOIN park_record_releases r ON r.issue_id = o.issue_id
		WHERE o.issue_id IN (%s)`,
		placeholders,
	)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query issue outcomes: %w", err)
	}
	defer rows.Close() //nolint:errcheck // read-only query; close error is non-actionable

	for rows.Next() {
		var o IssueOutcome
		var releasePR, releaseHead, releaseComment string
		if err := rows.Scan(&o.IssueID, &o.Kind, &o.ObservedAt, &o.State, &o.MergeReactionKind, &o.PRNumber, &o.PRRepository,
			&releasePR, &releaseHead, &releaseComment); err != nil {
			return nil, fmt.Errorf("scan issue outcome: %w", err)
		}
		if releasePR != "" && releasePR == pullRequestKey(o.PRRepository, o.PRNumber) {
			o.ReleaseHead, o.ReleaseComment = releaseHead, releaseComment
		}
		outcomes[o.IssueID] = o
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("query issue outcomes: %w", err)
	}
	return outcomes, nil
}
