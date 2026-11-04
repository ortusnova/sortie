package persistence

import (
	"context"
	"fmt"
)

// MergeAndProve is one row of the merge_and_prove table: a story in
// merge-and-prove's queue or in flight. Times are RFC 3339 in UTC.
type MergeAndProve struct {
	IssueID     string
	Identifier  string
	Seq         int64
	Phase       string
	PullRequest string // owner/repo#number
	Branch      string
	Head        string
	Criteria    string // the criteria left to the proof after deploy
	MergeCommit string
	Deployed    string // host=commit pairs, comma-separated
	QueuedAt    string
	PhaseAt     string
	Deadline    string // empty in the queue and while merging
	RevertPR    int
	Reason      string

	AgentKind       string
	ConfiguredModel string
	RuleName        string
	TemplateID      string
}

// SaveMergeAndProve inserts or replaces the row of r's issue.
func (s *Store) SaveMergeAndProve(ctx context.Context, r MergeAndProve) error {
	if _, err := s.db.ExecContext(ctx,
		`INSERT INTO merge_and_prove (issue_id, identifier, seq, phase, pull_request, branch, head, criteria,
			merge_commit, deployed, queued_at, phase_at, deadline, revert_pull_request, reason,
			agent_kind, configured_model, rule_name, template_id)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (issue_id) DO UPDATE SET
			identifier          = excluded.identifier,
			seq                 = excluded.seq,
			phase               = excluded.phase,
			pull_request        = excluded.pull_request,
			branch              = excluded.branch,
			head                = excluded.head,
			criteria            = excluded.criteria,
			merge_commit        = excluded.merge_commit,
			deployed            = excluded.deployed,
			queued_at           = excluded.queued_at,
			phase_at            = excluded.phase_at,
			deadline            = excluded.deadline,
			revert_pull_request = excluded.revert_pull_request,
			reason              = excluded.reason,
			agent_kind          = excluded.agent_kind,
			configured_model    = excluded.configured_model,
			rule_name           = excluded.rule_name,
			template_id         = excluded.template_id`,
		r.IssueID, r.Identifier, r.Seq, r.Phase, r.PullRequest, r.Branch, r.Head, r.Criteria,
		r.MergeCommit, r.Deployed, r.QueuedAt, r.PhaseAt, r.Deadline, r.RevertPR, r.Reason,
		r.AgentKind, r.ConfiguredModel, r.RuleName, r.TemplateID,
	); err != nil {
		return fmt.Errorf("save merge-and-prove of %q: %w", r.IssueID, err)
	}
	return nil
}

// DeleteMergeAndProve removes the row of the issue; a missing row is no
// error.
func (s *Store) DeleteMergeAndProve(ctx context.Context, issueID string) error {
	if _, err := s.db.ExecContext(ctx, `DELETE FROM merge_and_prove WHERE issue_id = ?`, issueID); err != nil {
		return fmt.Errorf("delete merge-and-prove of %q: %w", issueID, err)
	}
	return nil
}

// LoadMergeAndProve returns every row in queue order. It returns an empty
// slice, not nil, when there is none.
func (s *Store) LoadMergeAndProve(ctx context.Context) ([]MergeAndProve, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT issue_id, identifier, seq, phase, pull_request, branch, head, criteria, merge_commit, deployed,
			queued_at, phase_at, deadline, revert_pull_request, reason, agent_kind, configured_model, rule_name,
			template_id
		FROM merge_and_prove ORDER BY seq, issue_id`)
	if err != nil {
		return nil, fmt.Errorf("load merge-and-prove: %w", err)
	}
	defer rows.Close() //nolint:errcheck // read-only query; close error is non-actionable
	out := []MergeAndProve{}
	for rows.Next() {
		var r MergeAndProve
		if err := rows.Scan(&r.IssueID, &r.Identifier, &r.Seq, &r.Phase, &r.PullRequest, &r.Branch, &r.Head,
			&r.Criteria, &r.MergeCommit, &r.Deployed, &r.QueuedAt, &r.PhaseAt, &r.Deadline, &r.RevertPR, &r.Reason,
			&r.AgentKind, &r.ConfiguredModel, &r.RuleName, &r.TemplateID); err != nil {
			return nil, fmt.Errorf("scan merge-and-prove: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("load merge-and-prove: %w", err)
	}
	return out, nil
}
