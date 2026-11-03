package orchestrator

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"log/slog"
	"slices"
	"time"

	"github.com/sortie-ai/sortie/internal/failcause"
	"github.com/sortie-ai/sortie/internal/persistence"
	"github.com/sortie-ai/sortie/internal/redact"
)

// PlatformFaultCheckInterval is how long after a failed check Sortie
// checks an open platform fault again.
const PlatformFaultCheckInterval = 5 * time.Minute

// platformFaultCheckTimeout bounds one check of a platform fault.
const platformFaultCheckTimeout = 2 * time.Minute

// PlatformFault is the runtime view of a stopped failure no session of a
// story can fix: recorded once by its failure signature, with its cause,
// every story it holds out of dispatch, and, when only a person can clear
// it, the action that does. Only the event loop mutates it.
type PlatformFault struct {
	ID        string
	Signature string
	Class     string
	Step      string
	Cause     string
	Detail    string

	// Action is the person action that clears the fault; nil when
	// Sortie's own check clears it.
	Action *failcause.PersonAction

	RecordedAt   time.Time
	ActionDoneAt time.Time
	ActionDoneBy string

	LastCheckAt    time.Time
	LastCheckError string

	// NextCheckAt is when the scheduled check of the fault is due.
	NextCheckAt time.Time

	Holds []PlatformFaultHold

	// checking is set while a check of the fault runs.
	checking bool

	// recheck is set when a person marked the action done while a check
	// that did not know it ran: a failing result of that check makes the
	// next check due at once.
	recheck bool
}

// PlatformFaultHold is one story a platform fault holds.
type PlatformFaultHold struct {
	IssueID    string
	Identifier string
	DisplayID  string
	HeldAt     time.Time
}

// PlatformFaultCheck is what a check of an open platform fault is given.
type PlatformFaultCheck struct {
	FaultID   string
	Signature string
	Class     string
	Step      string

	// PersonAction reports that only a person can clear the fault, and
	// ActionDone that a person marked its action done.
	PersonAction bool
	ActionDone   bool

	Stories []PlatformFaultHold
}

// PlatformFaultChecker checks whether a platform fault still holds. A nil
// error passes the check, which resolves the fault and releases every
// story it holds.
type PlatformFaultChecker func(ctx context.Context, check PlatformFaultCheck) error

// errAwaitingPersonAction fails the check of a fault only a person can
// clear while nobody has marked its action done and no probe could test
// the condition itself.
var errAwaitingPersonAction = errors.New("waiting for a person to take the action and mark it done")

// platformFaultStore persists platform faults.
type platformFaultStore interface {
	SavePlatformFault(ctx context.Context, fault persistence.PlatformFault) error
}

// openPlatformFault returns the open fault recorded under signature, or
// nil. A zero or transient signature has none.
func openPlatformFault(state *State, signature failcause.Signature) *PlatformFault {
	if signature == (failcause.Signature{}) || signature.Transient() {
		return nil
	}
	line := signature.String()
	for _, f := range state.PlatformFaults {
		if f.Signature == line {
			return f
		}
	}
	return nil
}

// platformFaultID derives the ID of a fault recorded under signature at at.
func platformFaultID(signature string, at time.Time) string {
	sum := sha256.Sum256([]byte(signature + "\x00" + at.UTC().Format(time.RFC3339Nano)))
	return "pf-" + hex.EncodeToString(sum[:6])
}

// newPlatformFaultParams describe a platform fault to record.
type newPlatformFaultParams struct {
	Signature failcause.Signature
	Fault     failcause.Fault
	Cause     string
	Detail    string
	Host      string
	Forge     string
	Now       time.Time
}

// recordPlatformFault records a new open fault in state and returns it.
// Its scheduled check is due one [PlatformFaultCheckInterval] after now.
func recordPlatformFault(state *State, p newPlatformFaultParams) *PlatformFault {
	now := p.Now.UTC()
	f := &PlatformFault{
		ID:          platformFaultID(p.Signature.String(), now),
		Signature:   p.Signature.String(),
		Class:       p.Fault.Class,
		Step:        p.Signature.Step,
		Cause:       redact.Mask(p.Cause),
		Detail:      p.Detail,
		RecordedAt:  now,
		NextCheckAt: now.Add(PlatformFaultCheckInterval),
	}
	if p.Fault.Action != nil {
		action := p.Fault.Action.Render(p.Host, p.Forge)
		f.Action = &action
	}
	state.PlatformFaults[f.ID] = f
	return f
}

// holdUnderPlatformFault adds the story to the fault's holds, takes it out
// of dispatch, releases its claim, and persists the fault. A story the
// fault already holds keeps its first hold time.
func holdUnderPlatformFault(ctx context.Context, state *State, store platformFaultStore, f *PlatformFault, hold PlatformFaultHold, log *slog.Logger) {
	if !slices.ContainsFunc(f.Holds, func(h PlatformFaultHold) bool { return h.IssueID == hold.IssueID }) {
		hold.HeldAt = hold.HeldAt.UTC()
		f.Holds = append(f.Holds, hold)
	}
	state.FaultHolds[hold.IssueID] = f.ID
	CancelRetry(state, hold.IssueID)
	delete(state.Claimed, hold.IssueID)
	savePlatformFault(ctx, store, f, time.Time{}, log)
}

// savePlatformFault persists f, resolved at resolvedAt when that is not
// zero. A failed write is logged: the runtime state stays authoritative
// for this process.
func savePlatformFault(ctx context.Context, store platformFaultStore, f *PlatformFault, resolvedAt time.Time, log *slog.Logger) {
	if store == nil {
		return
	}
	if err := store.SavePlatformFault(ctx, platformFaultRow(f, resolvedAt)); err != nil {
		log.Error("failed to persist platform fault",
			slog.String("platform_fault_id", f.ID),
			slog.Any("error", err),
		)
	}
}

func formatFaultTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

func parseFaultTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}
	}
	return t.UTC()
}

// platformFaultRow returns the persisted form of f.
func platformFaultRow(f *PlatformFault, resolvedAt time.Time) persistence.PlatformFault {
	row := persistence.PlatformFault{
		ID:             f.ID,
		Signature:      f.Signature,
		Class:          f.Class,
		Step:           f.Step,
		Cause:          f.Cause,
		Detail:         f.Detail,
		RecordedAt:     formatFaultTime(f.RecordedAt),
		ActionDoneAt:   formatFaultTime(f.ActionDoneAt),
		ActionDoneBy:   f.ActionDoneBy,
		LastCheckAt:    formatFaultTime(f.LastCheckAt),
		LastCheckError: f.LastCheckError,
		ResolvedAt:     formatFaultTime(resolvedAt),
		Holds:          make([]persistence.PlatformFaultHold, 0, len(f.Holds)),
	}
	if f.Action != nil {
		row.Action, row.ActionWhere = f.Action.Action, f.Action.Where
	}
	for _, h := range f.Holds {
		row.Holds = append(row.Holds, persistence.PlatformFaultHold{
			IssueID: h.IssueID, Identifier: h.Identifier, DisplayID: h.DisplayID, HeldAt: formatFaultTime(h.HeldAt),
		})
	}
	return row
}

// PopulatePlatformFaults restores the open platform faults and their holds
// into state at startup. Each restored fault is checked on the first tick.
func PopulatePlatformFaults(state *State, rows []persistence.PlatformFault, log *slog.Logger) {
	if log == nil {
		log = slog.Default()
	}
	for _, row := range rows {
		if row.ID == "" || row.Signature == "" {
			log.Warn("skipping malformed platform fault record")
			continue
		}
		f := &PlatformFault{
			ID:             row.ID,
			Signature:      row.Signature,
			Class:          row.Class,
			Step:           row.Step,
			Cause:          row.Cause,
			Detail:         row.Detail,
			RecordedAt:     parseFaultTime(row.RecordedAt),
			ActionDoneAt:   parseFaultTime(row.ActionDoneAt),
			ActionDoneBy:   row.ActionDoneBy,
			LastCheckAt:    parseFaultTime(row.LastCheckAt),
			LastCheckError: row.LastCheckError,
		}
		if row.Action != "" {
			f.Action = &failcause.PersonAction{Action: row.Action, Where: row.ActionWhere}
		}
		for _, h := range row.Holds {
			f.Holds = append(f.Holds, PlatformFaultHold{
				IssueID: h.IssueID, Identifier: h.Identifier, DisplayID: h.DisplayID, HeldAt: parseFaultTime(h.HeldAt),
			})
			state.FaultHolds[h.IssueID] = f.ID
		}
		state.PlatformFaults[f.ID] = f
	}
}

// checkOf returns the check request of f.
func checkOf(f *PlatformFault) PlatformFaultCheck {
	return PlatformFaultCheck{
		FaultID:      f.ID,
		Signature:    f.Signature,
		Class:        f.Class,
		Step:         f.Step,
		PersonAction: f.Action != nil,
		ActionDone:   !f.ActionDoneAt.IsZero(),
		Stories:      slices.Clone(f.Holds),
	}
}

// dueFaultChecks returns the checks of the open faults whose scheduled
// check is due at now and no check of which runs, and marks each as
// checking. The checks come oldest fault first.
func dueFaultChecks(state *State, now time.Time) []PlatformFaultCheck {
	var due []*PlatformFault
	for _, f := range state.PlatformFaults {
		if !f.checking && !now.Before(f.NextCheckAt) {
			due = append(due, f)
		}
	}
	slices.SortFunc(due, func(a, b *PlatformFault) int { return a.RecordedAt.Compare(b.RecordedAt) })
	checks := make([]PlatformFaultCheck, 0, len(due))
	for _, f := range due {
		f.checking = true
		checks = append(checks, checkOf(f))
	}
	return checks
}

// markPlatformFaultActionDone records that by marked the fault's action
// done at now, persists it, and marks the fault as checking. It returns
// the check to run, and false when no open fault has the ID or a check of
// it already runs.
func markPlatformFaultActionDone(ctx context.Context, state *State, store platformFaultStore, faultID, by string, now time.Time, log *slog.Logger) (PlatformFaultCheck, *PlatformFault, bool) {
	f := state.PlatformFaults[faultID]
	if f == nil {
		return PlatformFaultCheck{}, nil, false
	}
	f.ActionDoneAt, f.ActionDoneBy = now.UTC(), by
	savePlatformFault(ctx, store, f, time.Time{}, log)
	if f.checking {
		f.recheck = true
		return PlatformFaultCheck{}, f, false
	}
	f.checking = true
	return checkOf(f), f, true
}

// applyFaultCheck records the result of a check of the fault. A passing
// check resolves the fault: it leaves the state, its holds are dropped so
// the stories it held dispatch again with the budgets they have, and it
// is persisted as resolved. A failing check keeps every hold and makes
// the next check due one [PlatformFaultCheckInterval] after now, or at
// now when the action was marked done while the check ran. It
// returns the fault and whether the check resolved it; nil when no open
// fault has the ID.
func applyFaultCheck(ctx context.Context, state *State, store platformFaultStore, faultID string, checkErr error, now time.Time, log *slog.Logger) (*PlatformFault, bool) {
	f := state.PlatformFaults[faultID]
	if f == nil {
		return nil, false
	}
	f.checking = false
	f.LastCheckAt = now.UTC()
	log = log.With(slog.String("platform_fault_id", f.ID), slog.String("fault_class", f.Class))
	if checkErr != nil {
		f.LastCheckError = redact.Mask(checkErr.Error())
		f.NextCheckAt = now.Add(PlatformFaultCheckInterval)
		if f.recheck {
			f.NextCheckAt = now
		}
		f.recheck = false
		savePlatformFault(ctx, store, f, time.Time{}, log)
		log.Info("platform fault check failed, stories stay held",
			slog.Int("held", len(f.Holds)),
			slog.String("check_error", f.LastCheckError),
		)
		return f, false
	}
	f.LastCheckError = ""
	delete(state.PlatformFaults, f.ID)
	for _, h := range f.Holds {
		if state.FaultHolds[h.IssueID] == f.ID {
			delete(state.FaultHolds, h.IssueID)
		}
	}
	savePlatformFault(ctx, store, f, now, log)
	log.Info("platform fault check passed, releasing held stories",
		slog.Int("released", len(f.Holds)),
	)
	return f, true
}

// SnapshotPlatformFault is a value copy of an open platform fault for the
// state API, every text masked.
type SnapshotPlatformFault struct {
	ID             string
	Class          string
	Signature      string
	Step           string
	Cause          string
	Detail         string
	Action         *failcause.PersonAction
	RecordedAt     time.Time
	ActionDoneAt   time.Time
	ActionDoneBy   string
	LastCheckAt    time.Time
	LastCheckError string
	NextCheckAt    time.Time
	Checking       bool
	Holds          []PlatformFaultHold
}

// snapshotPlatformFault returns a masked copy of f.
func snapshotPlatformFault(f *PlatformFault) SnapshotPlatformFault {
	s := SnapshotPlatformFault{
		ID:             f.ID,
		Class:          f.Class,
		Signature:      redact.Mask(f.Signature),
		Step:           f.Step,
		Cause:          redact.Mask(f.Cause),
		Detail:         redact.Mask(f.Detail),
		RecordedAt:     f.RecordedAt,
		ActionDoneAt:   f.ActionDoneAt,
		ActionDoneBy:   redact.Mask(f.ActionDoneBy),
		LastCheckAt:    f.LastCheckAt,
		LastCheckError: redact.Mask(f.LastCheckError),
		NextCheckAt:    f.NextCheckAt,
		Checking:       f.checking,
		Holds:          make([]PlatformFaultHold, len(f.Holds)),
	}
	if f.Action != nil {
		s.Action = &failcause.PersonAction{Action: redact.Mask(f.Action.Action), Where: redact.Mask(f.Action.Where)}
	}
	for i, h := range f.Holds {
		h.Identifier, h.DisplayID = redact.Mask(h.Identifier), redact.Mask(h.DisplayID)
		s.Holds[i] = h
	}
	return s
}

// snapshotPlatformFaults returns a copy of every open fault, oldest first;
// an empty slice when there is none.
func snapshotPlatformFaults(state *State) []SnapshotPlatformFault {
	out := make([]SnapshotPlatformFault, 0, len(state.PlatformFaults))
	for _, f := range state.PlatformFaults {
		out = append(out, snapshotPlatformFault(f))
	}
	slices.SortFunc(out, func(a, b SnapshotPlatformFault) int {
		if c := a.RecordedAt.Compare(b.RecordedAt); c != 0 {
			return c
		}
		return cmp.Compare(a.ID, b.ID)
	})
	return out
}
