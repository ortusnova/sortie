package persistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"time"
)

// supervisorTimeLayout stores times at a fixed width, so the text order of
// two stored times is their time order.
const supervisorTimeLayout = "2006-01-02T15:04:05.000000000Z07:00"

// SupervisorActionsUnknown is the Actions of a session that runs, or
// whose actions could not be counted.
const SupervisorActionsUnknown = -1

// SupervisorSession is one supervisor session Sortie started.
type SupervisorSession struct {
	ID int64

	// TriggerKind, TriggerIssue and TriggerDetail describe the first of
	// the triggers that started the session; Triggers counts them all.
	TriggerKind   string
	TriggerIssue  string
	TriggerDetail string
	Triggers      int

	// Digest is the state digest Sortie computed when it started the
	// session; empty for a session stored before digests were.
	Digest string

	StartedAt time.Time
	EndedAt   time.Time // zero while the session runs, or when Sortie stopped before it ended
	Outcome   string    // how the session ended; empty while it runs
	Error     string
	Tokens    int64 // tokens, less cache reads, the session spent

	// Actions counts the supervisor actions Sortie took in the session;
	// [SupervisorActionsUnknown] while it runs or when it was not counted.
	Actions int
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
		INSERT INTO supervisor_sessions (trigger_kind, trigger_issue, trigger_detail, triggers, digest, started_at)
		VALUES (?, ?, ?, ?, ?, ?)`,
		session.TriggerKind, session.TriggerIssue, session.TriggerDetail, max(session.Triggers, 1), session.Digest,
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

// EndSupervisorSession records how the session id ended and the actions
// it took, [SupervisorActionsUnknown] when they were not counted.
func (s *Store) EndSupervisorSession(ctx context.Context, id int64, endedAt time.Time, outcome, errText string, tokens int64, actions int) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE supervisor_sessions SET ended_at = ?, outcome = ?, error = ?, tokens = ?, actions = ? WHERE id = ?`,
		endedAt.UTC().Format(supervisorTimeLayout), outcome, errText, tokens, max(actions, SupervisorActionsUnknown), id,
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

const supervisorSessionColumns = `id, trigger_kind, trigger_issue, trigger_detail, triggers, digest, started_at, ended_at, outcome, error, tokens, actions`

func scanSupervisorSession(scan func(dest ...any) error) (SupervisorSession, error) {
	var (
		session            SupervisorSession
		startedAt, endedAt string
	)
	if err := scan(&session.ID, &session.TriggerKind, &session.TriggerIssue, &session.TriggerDetail, &session.Triggers,
		&session.Digest, &startedAt, &endedAt, &session.Outcome, &session.Error, &session.Tokens, &session.Actions); err != nil {
		return SupervisorSession{}, err
	}
	var err error
	if session.StartedAt, err = time.Parse(supervisorTimeLayout, startedAt); err != nil {
		return SupervisorSession{}, fmt.Errorf("parse supervisor session start %q: %w", startedAt, err)
	}
	if endedAt != "" {
		if session.EndedAt, err = time.Parse(supervisorTimeLayout, endedAt); err != nil {
			return SupervisorSession{}, fmt.Errorf("parse supervisor session end %q: %w", endedAt, err)
		}
	}
	return session, nil
}

// LatestSupervisorSession returns the newest supervisor session, and false
// when none was started.
func (s *Store) LatestSupervisorSession(ctx context.Context) (SupervisorSession, bool, error) {
	session, err := scanSupervisorSession(s.db.QueryRowContext(ctx, `
		SELECT `+supervisorSessionColumns+` FROM supervisor_sessions ORDER BY id DESC LIMIT 1`).Scan)
	if errors.Is(err, sql.ErrNoRows) {
		return SupervisorSession{}, false, nil
	}
	if err != nil {
		return SupervisorSession{}, false, fmt.Errorf("query latest supervisor session: %w", err)
	}
	return session, true, nil
}

// RecentSupervisorSessions returns the newest limit supervisor sessions,
// oldest first.
func (s *Store) RecentSupervisorSessions(ctx context.Context, limit int) ([]SupervisorSession, error) {
	if limit <= 0 {
		return []SupervisorSession{}, nil
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+supervisorSessionColumns+` FROM supervisor_sessions ORDER BY id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, fmt.Errorf("query recent supervisor sessions: %w", err)
	}
	defer rows.Close() //nolint:errcheck // read-only query; close error is non-actionable
	out := []SupervisorSession{}
	for rows.Next() {
		session, err := scanSupervisorSession(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("scan supervisor session: %w", err)
		}
		out = append(out, session)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("query recent supervisor sessions: %w", err)
	}
	slices.Reverse(out)
	return out, nil
}
