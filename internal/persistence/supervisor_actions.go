package persistence

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Supervisor action results.
const (
	// SupervisorDone is an action Sortie took.
	SupervisorDone = "done"
	// SupervisorRefused is a request outside the supervisor's scope.
	SupervisorRefused = "refused"
	// SupervisorRejected is an action in scope that Sortie declined: a
	// malformed request, an unknown issue, or a precondition the forge or
	// the runtime state did not meet.
	SupervisorRejected = "rejected"
	// SupervisorFailed is an action that failed on Sortie's side or the
	// tracker's or forge's.
	SupervisorFailed = "failed"
)

// SupervisorAction is one request the supervisor token made to an operator
// route.
type SupervisorAction struct {
	ID              int64
	Action          string // the action's name, or the route of a refused request
	Route           string // method and path of the request
	IssueIdentifier string // empty when the route names no issue
	Evidence        string
	Result          string // one of the Supervisor result values
	Status          int    // the HTTP status Sortie answered
	Detail          string // the error code and message Sortie answered, empty on success
	PullRequest     string // owner/repo#number, when the action named one
	MergeCommit     string // the merge commit the forge gave, when the action named one
	Profile         string // the job profile a fix run or live-proof request named; empty for none
	ActedAt         time.Time
}

// InsertSupervisorAction stores a and returns its row id.
func (s *Store) InsertSupervisorAction(ctx context.Context, a SupervisorAction) (int64, error) {
	if a.Action == "" || a.Result == "" {
		return 0, errors.New("insert supervisor action: an action and a result are required")
	}
	at := a.ActedAt
	if at.IsZero() {
		at = time.Now()
	}
	res, err := s.db.ExecContext(ctx, `
		INSERT INTO supervisor_actions (action, route, issue_identifier, evidence, result, status, detail, pull_request, merge_commit, profile, acted_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		a.Action, a.Route, a.IssueIdentifier, a.Evidence, a.Result, a.Status, a.Detail, a.PullRequest, a.MergeCommit, a.Profile,
		at.UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		return 0, fmt.Errorf("insert supervisor action: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("insert supervisor action: %w", err)
	}
	return id, nil
}

// RecentSupervisorActions returns the newest limit supervisor actions,
// newest first.
func (s *Store) RecentSupervisorActions(ctx context.Context, limit int) ([]SupervisorAction, error) {
	if limit <= 0 {
		return []SupervisorAction{}, nil
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, action, route, issue_identifier, evidence, result, status, detail, pull_request, merge_commit, profile, acted_at
		FROM supervisor_actions ORDER BY id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, fmt.Errorf("query supervisor actions: %w", err)
	}
	defer rows.Close() //nolint:errcheck // read-only query; close error is non-actionable
	out := []SupervisorAction{}
	for rows.Next() {
		var a SupervisorAction
		var at string
		if err := rows.Scan(&a.ID, &a.Action, &a.Route, &a.IssueIdentifier, &a.Evidence, &a.Result, &a.Status,
			&a.Detail, &a.PullRequest, &a.MergeCommit, &a.Profile, &at); err != nil {
			return nil, fmt.Errorf("scan supervisor action: %w", err)
		}
		if a.ActedAt, err = time.Parse(time.RFC3339Nano, at); err != nil {
			return nil, fmt.Errorf("parse supervisor action time %q: %w", at, err)
		}
		out = append(out, a)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate supervisor actions: %w", err)
	}
	return out, nil
}
