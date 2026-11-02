package persistence

import (
	"context"
	"fmt"
)

// PlatformFault is one row of the platform_faults table with its holds: a
// stopped failure no session of a story can fix, recorded once by its
// failure signature, and the stories it holds out of dispatch.
type PlatformFault struct {
	ID        string
	Signature string // the failure signature the fault is recorded by
	Class     string // the fault class of the signature
	Step      string // the failed step, normalized as the signature's
	Cause     string // first line of the error, credentials masked
	Detail    string // the whole cause of the failed step, as error_detail keeps it

	// Action and ActionWhere are the person action that clears the fault
	// and where to take it; both are empty when Sortie's own check clears
	// it.
	Action      string
	ActionWhere string

	RecordedAt string // RFC 3339 UTC

	// ActionDoneAt (RFC 3339 UTC) and ActionDoneBy record that a person
	// marked the action done; both are empty until then.
	ActionDoneAt string
	ActionDoneBy string

	// LastCheckAt (RFC 3339 UTC) and LastCheckError record the latest
	// check of the fault; LastCheckError is empty when it passed or none
	// ran.
	LastCheckAt    string
	LastCheckError string

	// ResolvedAt (RFC 3339 UTC) is when a check passed and released the
	// held stories; empty while the fault is open.
	ResolvedAt string

	Holds []PlatformFaultHold
}

// PlatformFaultHold is one story a platform fault holds.
type PlatformFaultHold struct {
	IssueID    string
	Identifier string
	DisplayID  string // empty when the tracker needs no qualified form
	HeldAt     string // RFC 3339 UTC
}

// SavePlatformFault writes the fault and replaces its holds with
// fault.Holds, in one transaction. A resolved fault keeps its row and
// loses its holds.
func (s *Store) SavePlatformFault(ctx context.Context, fault PlatformFault) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin save platform fault %q: %w", fault.ID, err)
	}
	defer tx.Rollback() //nolint:errcheck // rollback on error path; no-op after commit

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO platform_faults (id, signature, class, step, cause, detail, action, action_where, recorded_at,
			action_done_at, action_done_by, last_check_at, last_check_error, resolved_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (id) DO UPDATE SET
			signature        = excluded.signature,
			class            = excluded.class,
			step             = excluded.step,
			cause            = excluded.cause,
			detail           = excluded.detail,
			action           = excluded.action,
			action_where     = excluded.action_where,
			recorded_at      = excluded.recorded_at,
			action_done_at   = excluded.action_done_at,
			action_done_by   = excluded.action_done_by,
			last_check_at    = excluded.last_check_at,
			last_check_error = excluded.last_check_error,
			resolved_at      = excluded.resolved_at`,
		fault.ID, fault.Signature, fault.Class, fault.Step, fault.Cause, fault.Detail, fault.Action, fault.ActionWhere,
		fault.RecordedAt, fault.ActionDoneAt, fault.ActionDoneBy, fault.LastCheckAt, fault.LastCheckError, fault.ResolvedAt,
	); err != nil {
		return fmt.Errorf("save platform fault %q: %w", fault.ID, err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM platform_fault_holds WHERE fault_id = ?`, fault.ID); err != nil {
		return fmt.Errorf("clear holds of platform fault %q: %w", fault.ID, err)
	}
	if fault.ResolvedAt == "" {
		for _, h := range fault.Holds {
			if _, err := tx.ExecContext(ctx,
				`INSERT INTO platform_fault_holds (fault_id, issue_id, identifier, display_id, held_at) VALUES (?, ?, ?, ?, ?)`,
				fault.ID, h.IssueID, h.Identifier, h.DisplayID, h.HeldAt,
			); err != nil {
				return fmt.Errorf("hold %q under platform fault %q: %w", h.IssueID, fault.ID, err)
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit platform fault %q: %w", fault.ID, err)
	}
	return nil
}

// LoadOpenPlatformFaults returns every fault not yet resolved with its
// holds, oldest first. It returns an empty slice, not nil, when there is
// none.
func (s *Store) LoadOpenPlatformFaults(ctx context.Context) ([]PlatformFault, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, signature, class, step, cause, detail, action, action_where, recorded_at,
			action_done_at, action_done_by, last_check_at, last_check_error, resolved_at
		FROM platform_faults WHERE resolved_at = '' ORDER BY recorded_at, id`)
	if err != nil {
		return nil, fmt.Errorf("load open platform faults: %w", err)
	}
	faults := []PlatformFault{}
	index := map[string]int{}
	for rows.Next() {
		var f PlatformFault
		if err := rows.Scan(&f.ID, &f.Signature, &f.Class, &f.Step, &f.Cause, &f.Detail, &f.Action, &f.ActionWhere,
			&f.RecordedAt, &f.ActionDoneAt, &f.ActionDoneBy, &f.LastCheckAt, &f.LastCheckError, &f.ResolvedAt); err != nil {
			rows.Close() //nolint:errcheck,gosec // the scan error is the one reported
			return nil, fmt.Errorf("scan platform fault: %w", err)
		}
		f.Holds = []PlatformFaultHold{}
		index[f.ID] = len(faults)
		faults = append(faults, f)
	}
	if err := rows.Err(); err != nil {
		rows.Close() //nolint:errcheck,gosec // the iteration error is the one reported
		return nil, fmt.Errorf("load open platform faults: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("load open platform faults: %w", err)
	}

	holds, err := s.db.QueryContext(ctx,
		`SELECT fault_id, issue_id, identifier, display_id, held_at FROM platform_fault_holds ORDER BY held_at, issue_id`)
	if err != nil {
		return nil, fmt.Errorf("load platform fault holds: %w", err)
	}
	defer holds.Close() //nolint:errcheck // read-only query; close error is non-actionable
	for holds.Next() {
		var faultID string
		var h PlatformFaultHold
		if err := holds.Scan(&faultID, &h.IssueID, &h.Identifier, &h.DisplayID, &h.HeldAt); err != nil {
			return nil, fmt.Errorf("scan platform fault hold: %w", err)
		}
		if i, ok := index[faultID]; ok {
			faults[i].Holds = append(faults[i].Holds, h)
		}
	}
	if err := holds.Err(); err != nil {
		return nil, fmt.Errorf("load platform fault holds: %w", err)
	}
	return faults, nil
}
