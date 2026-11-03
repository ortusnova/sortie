package persistence

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// RemoteRun is one running remote attempt as a restarted process needs
// it to re-adopt the run. Record and Entry are opaque to this package.
type RemoteRun struct {
	IssueID  string
	JobID    string
	Attempt  int
	WorkerID string
	// Record is the attempt's job, written at its lease grant.
	Record string
	// Entry is the run as the orchestrator tracks it; empty until first
	// written.
	Entry string
	// SessionID is the agent's session, empty until it started.
	SessionID string
	// TurnsCompleted counts the session's completed turns.
	TurnsCompleted int
	// SessionEnded reports that the session was stopped, so what is left
	// is the worker's returned result.
	SessionEnded bool
	UpdatedAt    time.Time
}

// SaveRemoteRun records r at its lease grant, replacing an earlier row
// of its issue.
func (s *Store) SaveRemoteRun(ctx context.Context, r RemoteRun) error {
	if r.IssueID == "" || r.JobID == "" || r.WorkerID == "" {
		return errors.New("save remote run: an issue, a job and a worker are required")
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT OR REPLACE INTO remote_runs
			(issue_id, job_id, attempt, worker_id, record, entry, session_id, turns_completed, session_ended, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		r.IssueID, r.JobID, r.Attempt, r.WorkerID, r.Record, r.Entry, r.SessionID, r.TurnsCompleted, r.SessionEnded, nowText(),
	)
	if err != nil {
		return fmt.Errorf("save remote run: %w", err)
	}
	return nil
}

// UpdateRemoteRunEntry stores the run as the orchestrator tracks it. An
// issue with no row is left without one.
func (s *Store) UpdateRemoteRunEntry(ctx context.Context, issueID, entry string) error {
	if _, err := s.db.ExecContext(ctx, `UPDATE remote_runs SET entry = ?, updated_at = ? WHERE issue_id = ?`,
		entry, nowText(), issueID); err != nil {
		return fmt.Errorf("update remote run entry: %w", err)
	}
	return nil
}

// UpdateRemoteRunSession stores the progress of the run's agent session.
// An issue with no row is left without one.
func (s *Store) UpdateRemoteRunSession(ctx context.Context, issueID, sessionID string, turnsCompleted int, ended bool) error {
	if _, err := s.db.ExecContext(ctx, `
		UPDATE remote_runs SET session_id = ?, turns_completed = ?, session_ended = ?, updated_at = ?
		WHERE issue_id = ?`,
		sessionID, turnsCompleted, ended, nowText(), issueID); err != nil {
		return fmt.Errorf("update remote run session: %w", err)
	}
	return nil
}

// UpdateRemoteRunRecord replaces the record of the issue's remote run,
// as when its worker resumed the attempt with new job tokens. An issue
// with no row is left without one.
func (s *Store) UpdateRemoteRunRecord(ctx context.Context, issueID, record string) error {
	if _, err := s.db.ExecContext(ctx, `UPDATE remote_runs SET record = ?, updated_at = ? WHERE issue_id = ?`,
		record, nowText(), issueID); err != nil {
		return fmt.Errorf("update remote run record: %w", err)
	}
	return nil
}

// DeleteRemoteRun forgets the issue's remote run, once it ended.
func (s *Store) DeleteRemoteRun(ctx context.Context, issueID string) error {
	if _, err := s.db.ExecContext(ctx, `DELETE FROM remote_runs WHERE issue_id = ?`, issueID); err != nil {
		return fmt.Errorf("delete remote run: %w", err)
	}
	return nil
}

// LoadRemoteRuns returns every recorded remote run, ordered by issue.
func (s *Store) LoadRemoteRuns(ctx context.Context) ([]RemoteRun, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT issue_id, job_id, attempt, worker_id, record, entry, session_id, turns_completed, session_ended, updated_at
		FROM remote_runs ORDER BY issue_id`)
	if err != nil {
		return nil, fmt.Errorf("query remote runs: %w", err)
	}
	defer rows.Close() //nolint:errcheck // read-only query; close error is non-actionable
	var out []RemoteRun
	for rows.Next() {
		var r RemoteRun
		var updated string
		if err := rows.Scan(&r.IssueID, &r.JobID, &r.Attempt, &r.WorkerID, &r.Record, &r.Entry,
			&r.SessionID, &r.TurnsCompleted, &r.SessionEnded, &updated); err != nil {
			return nil, fmt.Errorf("scan remote run: %w", err)
		}
		if t, err := time.Parse(time.RFC3339Nano, updated); err == nil {
			r.UpdatedAt = t
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate remote runs: %w", err)
	}
	return out, nil
}

func nowText() string { return time.Now().UTC().Format(time.RFC3339Nano) }
