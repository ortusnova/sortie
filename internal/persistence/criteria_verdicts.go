package persistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// CriteriaVerdict is the verdict a story's latest criteria review
// recorded: its outcome, the per-criterion verdicts as the review wrote
// them, and why the outcome is not met when the record itself is at fault.
type CriteriaVerdict struct {
	IssueID    string
	DispatchID string
	Outcome    string
	Criteria   string
	Problem    string
	RecordedAt time.Time
}

// UpsertCriteriaVerdict records v as the story's latest verdict,
// replacing any earlier one.
func (s *Store) UpsertCriteriaVerdict(ctx context.Context, v CriteriaVerdict) error {
	at := v.RecordedAt
	if at.IsZero() {
		at = time.Now()
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO criteria_verdicts (issue_id, dispatch_id, outcome, criteria, problem, recorded_at)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT (issue_id) DO UPDATE SET
			dispatch_id = excluded.dispatch_id, outcome = excluded.outcome, criteria = excluded.criteria,
			problem = excluded.problem, recorded_at = excluded.recorded_at`,
		v.IssueID, v.DispatchID, v.Outcome, v.Criteria, v.Problem, at.UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		return fmt.Errorf("upsert criteria verdict %q: %w", v.IssueID, err)
	}
	return nil
}

// LoadCriteriaVerdict returns the story's latest verdict, and false when
// none was recorded.
func (s *Store) LoadCriteriaVerdict(ctx context.Context, issueID string) (CriteriaVerdict, bool, error) {
	var v CriteriaVerdict
	var at string
	err := s.db.QueryRowContext(ctx, `
		SELECT issue_id, dispatch_id, outcome, criteria, problem, recorded_at
		FROM criteria_verdicts WHERE issue_id = ?`, issueID,
	).Scan(&v.IssueID, &v.DispatchID, &v.Outcome, &v.Criteria, &v.Problem, &at)
	if errors.Is(err, sql.ErrNoRows) {
		return CriteriaVerdict{}, false, nil
	}
	if err != nil {
		return CriteriaVerdict{}, false, fmt.Errorf("load criteria verdict %q: %w", issueID, err)
	}
	v.RecordedAt, _ = time.Parse(time.RFC3339Nano, at)
	return v, true, nil
}
