package persistence

import (
	"context"
	"encoding/json"
	"fmt"
)

// HandoffPartDone is the status of a hand-off part that is closed.
const HandoffPartDone = "done"

// HandoffPart is one row of the handoff_parts table: one part of a story
// a session could not do, and the work Sortie turned it into.
type HandoffPart struct {
	IssueID    string
	PartID     string
	Identifier string
	Kind       string // job, pull_request or person
	Need       string
	Check      string

	Capabilities []string // what a worker must declare to run a job part
	Repository   string   // the repository a pull_request part changes
	Patch        string   // the change a pull_request part carries
	Action       string   // the person action of a person part
	ActionWhere  string

	Status         string
	JobAttempt     int
	WorkIssueID    string // the issue a pull_request part was filed as
	WorkIdentifier string
	FaultID        string // the platform fault that holds the part
	Result         string // what closed the part, or the last failure

	RecordedAt string // RFC 3339 UTC
	UpdatedAt  string
	DoneAt     string // empty while the part is open
}

// SaveHandoffPart inserts or replaces the part keyed by its issue and
// part ID.
func (s *Store) SaveHandoffPart(ctx context.Context, p HandoffPart) error {
	caps := p.Capabilities
	if caps == nil {
		caps = []string{}
	}
	capsJSON, err := json.Marshal(caps)
	if err != nil {
		return fmt.Errorf("encode capabilities of hand-off part %q: %w", p.PartID, err)
	}
	if _, err := s.db.ExecContext(ctx,
		`INSERT INTO handoff_parts (issue_id, part_id, identifier, kind, need, how_to_check, capabilities, repository,
			patch, action, action_where, status, job_attempt, work_issue_id, work_identifier, fault_id, result,
			recorded_at, updated_at, done_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (issue_id, part_id) DO UPDATE SET
			identifier      = excluded.identifier,
			kind            = excluded.kind,
			need            = excluded.need,
			how_to_check    = excluded.how_to_check,
			capabilities    = excluded.capabilities,
			repository      = excluded.repository,
			patch           = excluded.patch,
			action          = excluded.action,
			action_where    = excluded.action_where,
			status          = excluded.status,
			job_attempt     = excluded.job_attempt,
			work_issue_id   = excluded.work_issue_id,
			work_identifier = excluded.work_identifier,
			fault_id        = excluded.fault_id,
			result          = excluded.result,
			recorded_at     = excluded.recorded_at,
			updated_at      = excluded.updated_at,
			done_at         = excluded.done_at`,
		p.IssueID, p.PartID, p.Identifier, p.Kind, p.Need, p.Check, string(capsJSON), p.Repository,
		p.Patch, p.Action, p.ActionWhere, p.Status, p.JobAttempt, p.WorkIssueID, p.WorkIdentifier, p.FaultID, p.Result,
		p.RecordedAt, p.UpdatedAt, p.DoneAt,
	); err != nil {
		return fmt.Errorf("save hand-off part %q of %q: %w", p.PartID, p.IssueID, err)
	}
	return nil
}

// LoadHandoffParts returns every recorded part, open and done, ordered
// by recording time, issue and part ID. It returns an empty slice, not
// nil, when there is none.
func (s *Store) LoadHandoffParts(ctx context.Context) ([]HandoffPart, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT issue_id, part_id, identifier, kind, need, how_to_check, capabilities, repository, patch, action,
			action_where, status, job_attempt, work_issue_id, work_identifier, fault_id, result, recorded_at,
			updated_at, done_at
		FROM handoff_parts ORDER BY recorded_at, issue_id, part_id`)
	if err != nil {
		return nil, fmt.Errorf("load hand-off parts: %w", err)
	}
	defer rows.Close() //nolint:errcheck // read-only query; close error is non-actionable
	parts := []HandoffPart{}
	for rows.Next() {
		var p HandoffPart
		var caps string
		if err := rows.Scan(&p.IssueID, &p.PartID, &p.Identifier, &p.Kind, &p.Need, &p.Check, &caps, &p.Repository,
			&p.Patch, &p.Action, &p.ActionWhere, &p.Status, &p.JobAttempt, &p.WorkIssueID, &p.WorkIdentifier,
			&p.FaultID, &p.Result, &p.RecordedAt, &p.UpdatedAt, &p.DoneAt); err != nil {
			return nil, fmt.Errorf("scan hand-off part: %w", err)
		}
		if err := json.Unmarshal([]byte(caps), &p.Capabilities); err != nil {
			return nil, fmt.Errorf("decode capabilities of hand-off part %q: %w", p.PartID, err)
		}
		parts = append(parts, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("load hand-off parts: %w", err)
	}
	return parts, nil
}
