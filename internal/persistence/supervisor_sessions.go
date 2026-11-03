package persistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// supervisorTimeLayout stores times at a fixed width, so the text order of
// two stored times is their time order.
const supervisorTimeLayout = "2006-01-02T15:04:05.000000000Z07:00"

// SupervisorSession is one supervisor session Sortie started.
type SupervisorSession struct {
	ID int64

	// TriggerKind, TriggerIssue and TriggerDetail describe the first of
	// the triggers that started the session; Triggers counts them all.
	TriggerKind   string
	TriggerIssue  string
	TriggerDetail string
	Triggers      int

	StartedAt time.Time
	EndedAt   time.Time // zero while the session runs, or when Sortie stopped before it ended
	Outcome   string    // how the session ended; empty while it runs
	Error     string
	Tokens    int64 // tokens, less cache reads, the session spent
}

// SupervisorUsage is what the supervisor sessions started in a window
// spent.
type SupervisorUsage struct {
	Sessions int
	Tokens   int64
}

// StartSupervisorSession stores a started session and returns its row id.
func (s *Store) StartSupervisorSession(ctx context.Context, session SupervisorSession) (int64, error) {
	if session.TriggerKind == "" || session.StartedAt.IsZero() {
		return 0, errors.New("start supervisor session: a trigger and a start time are required")
	}
	res, err := s.db.ExecContext(ctx, `
		INSERT INTO supervisor_sessions (trigger_kind, trigger_issue, trigger_detail, triggers, started_at)
		VALUES (?, ?, ?, ?, ?)`,
		session.TriggerKind, session.TriggerIssue, session.TriggerDetail, max(session.Triggers, 1),
		session.StartedAt.UTC().Format(supervisorTimeLayout),
	)
	if err != nil {
		return 0, fmt.Errorf("start supervisor session: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("start supervisor session: %w", err)
	}
	return id, nil
}

// EndSupervisorSession records how the session id ended.
func (s *Store) EndSupervisorSession(ctx context.Context, id int64, endedAt time.Time, outcome, errText string, tokens int64) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE supervisor_sessions SET ended_at = ?, outcome = ?, error = ?, tokens = ? WHERE id = ?`,
		endedAt.UTC().Format(supervisorTimeLayout), outcome, errText, tokens, id,
	)
	if err != nil {
		return fmt.Errorf("end supervisor session %d: %w", id, err)
	}
	return nil
}

// SupervisorUsageSince returns the sessions started at or after since and
// the tokens they spent.
func (s *Store) SupervisorUsageSince(ctx context.Context, since time.Time) (SupervisorUsage, error) {
	var u SupervisorUsage
	err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*), COALESCE(SUM(tokens), 0) FROM supervisor_sessions WHERE started_at >= ?`,
		since.UTC().Format(supervisorTimeLayout),
	).Scan(&u.Sessions, &u.Tokens)
	if err != nil {
		return SupervisorUsage{}, fmt.Errorf("query supervisor usage: %w", err)
	}
	return u, nil
}

// LatestSupervisorSession returns the newest supervisor session, and false
// when none was started.
func (s *Store) LatestSupervisorSession(ctx context.Context) (SupervisorSession, bool, error) {
	var (
		session            SupervisorSession
		startedAt, endedAt string
	)
	err := s.db.QueryRowContext(ctx, `
		SELECT id, trigger_kind, trigger_issue, trigger_detail, triggers, started_at, ended_at, outcome, error, tokens
		FROM supervisor_sessions ORDER BY id DESC LIMIT 1`,
	).Scan(&session.ID, &session.TriggerKind, &session.TriggerIssue, &session.TriggerDetail, &session.Triggers,
		&startedAt, &endedAt, &session.Outcome, &session.Error, &session.Tokens)
	if errors.Is(err, sql.ErrNoRows) {
		return SupervisorSession{}, false, nil
	}
	if err != nil {
		return SupervisorSession{}, false, fmt.Errorf("query latest supervisor session: %w", err)
	}
	if session.StartedAt, err = time.Parse(supervisorTimeLayout, startedAt); err != nil {
		return SupervisorSession{}, false, fmt.Errorf("parse supervisor session start %q: %w", startedAt, err)
	}
	if endedAt != "" {
		if session.EndedAt, err = time.Parse(supervisorTimeLayout, endedAt); err != nil {
			return SupervisorSession{}, false, fmt.Errorf("parse supervisor session end %q: %w", endedAt, err)
		}
	}
	return session, true, nil
}
