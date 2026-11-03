package orchestrator

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/logging"
	"github.com/sortie-ai/sortie/internal/notify/route"
	"github.com/sortie-ai/sortie/internal/persistence"
)

// Lease bounds of a standing hold.
const (
	// DefaultHoldLease is the lease a hold request that names no expiry
	// gets.
	DefaultHoldLease = 60 * time.Minute
	// MaxHoldLease is the longest lease one hold or renewal may ask for.
	MaxHoldLease = 4 * time.Hour
)

// HoldRequest is what a hold request asks for. Reason and Holder apply to
// any hold. Evidence, Until, and For apply to a standing hold only: Until
// is when the lease ends, else For after now, else [DefaultHoldLease]
// after now.
type HoldRequest struct {
	Reason   string
	Holder   string
	Evidence string
	Until    time.Time
	For      time.Duration
}

// StandingHold is a lease someone placed on an issue that was not running:
// the issue starts no run, retry, or continuation while the lease holds.
// The holder keeps it by renewing it with new evidence before Until. Once
// Until passes without a renewal Sortie sets ExpiredAt, reports the
// expiry, and keeps the issue out of dispatch for a person to act on. Only
// a release, or a new hold, ends it. Holder, Reason, and Evidence are as
// the requests said; Reason and Holder may be empty.
type StandingHold struct {
	Identifier string
	Holder     string
	Reason     string
	Evidence   string
	Since      time.Time // UTC: when the lease was first placed
	RenewedAt  time.Time // UTC: the latest hold or renewal
	Until      time.Time // UTC: when the lease ends without a renewal
	ExpiredAt  time.Time // UTC: zero while the lease is live
}

// Expired reports whether Sortie found the lease ended without a renewal.
func (h *StandingHold) Expired() bool { return !h.ExpiredAt.IsZero() }

// StandingHoldStore is the persistence a standing hold needs.
type StandingHoldStore interface {
	UpsertStandingHold(ctx context.Context, hold persistence.StandingHold) error
	DeleteStandingHold(ctx context.Context, issueID string) error
}

// standingHoldStoreOf returns store as a [StandingHoldStore], or nil when
// it is not one, so a store without the table keeps holds in memory only.
func standingHoldStoreOf(store any) StandingHoldStore {
	s, _ := store.(StandingHoldStore)
	return s
}

// standingHoldState returns the hold view of a standing hold.
func standingHoldState(issueID string, h *StandingHold, now time.Time) HoldState {
	return HoldState{
		Identifier: h.Identifier,
		IssueID:    issueID,
		Held:       true,
		Reason:     h.Reason,
		Holder:     h.Holder,
		HeldSince:  h.Since,
		HeldTotal:  max(now.Sub(h.Since), 0),
		Lease:      new(*h),
	}
}

// leaseUntil returns when the lease req asks for ends, or an error when it
// ends at or before now or more than [MaxHoldLease] after it.
func leaseUntil(req HoldRequest, now time.Time) (time.Time, error) {
	until := req.Until
	if until.IsZero() {
		until = now.Add(cmp.Or(req.For, DefaultHoldLease))
	}
	switch {
	case !until.After(now):
		return time.Time{}, fmt.Errorf("the hold must end after now, not at %s", until.UTC().Format(time.RFC3339))
	case until.Sub(now) > MaxHoldLease:
		return time.Time{}, fmt.Errorf("the hold may last at most %s; renew it with new evidence instead", MaxHoldLease)
	}
	return until.UTC(), nil
}

// placeStandingHold places or renews the lease on issueID, which is not
// running, at now. A hold needs evidence. A live lease is renewed: the
// renewal needs evidence other than the lease's current one, moves Until,
// and keeps the start and holder; a reason it gives replaces the old one.
// An expired lease is replaced by a new one.
func placeStandingHold(ctx context.Context, state *State, store StandingHoldStore, issueID, identifier string, req HoldRequest, now time.Time, log *slog.Logger) ControlReply {
	evidence := strings.TrimSpace(req.Evidence)
	if evidence == "" {
		return ControlReply{Outcome: ControlInvalid, Message: "a hold on an issue that is not running needs evidence: what is being done for it"}
	}
	until, err := leaseUntil(req, now)
	if err != nil {
		return ControlReply{Outcome: ControlInvalid, Message: err.Error()}
	}
	now = now.UTC()
	h := state.StandingHolds[issueID]
	renewal := h != nil && !h.Expired()
	if renewal {
		if evidence == h.Evidence {
			return ControlReply{Outcome: ControlInvalid, Message: "renewing a hold needs new evidence"}
		}
		h.Evidence, h.RenewedAt, h.Until = evidence, now, until
		h.Reason = cmp.Or(strings.TrimSpace(req.Reason), h.Reason)
	} else {
		h = &StandingHold{
			Identifier: identifier,
			Holder:     strings.TrimSpace(req.Holder),
			Reason:     strings.TrimSpace(req.Reason),
			Evidence:   evidence,
			Since:      now,
			RenewedAt:  now,
			Until:      until,
		}
		state.StandingHolds[issueID] = h
	}
	issueLog := logging.WithIssue(log, issueID, identifier)
	persistStandingHold(ctx, store, issueID, h, issueLog)
	issueLog.Info("issue held",
		slog.Bool("renewal", renewal),
		slog.String("holder", h.Holder),
		slog.String("hold_reason", h.Reason),
		slog.String("evidence", h.Evidence),
		slog.Time("until", h.Until),
	)
	return ControlReply{Outcome: ControlDone, Hold: standingHoldState(issueID, h, now)}
}

func persistStandingHold(ctx context.Context, store StandingHoldStore, issueID string, h *StandingHold, log *slog.Logger) {
	if store == nil {
		return
	}
	row := persistence.StandingHold{
		IssueID:    issueID,
		Identifier: h.Identifier,
		Holder:     h.Holder,
		Reason:     h.Reason,
		Evidence:   h.Evidence,
		HeldAt:     h.Since.Format(time.RFC3339Nano),
		RenewedAt:  h.RenewedAt.Format(time.RFC3339Nano),
		Until:      h.Until.Format(time.RFC3339Nano),
	}
	if h.Expired() {
		row.ExpiredAt = h.ExpiredAt.Format(time.RFC3339Nano)
	}
	if err := store.UpsertStandingHold(ctx, row); err != nil {
		log.Error("failed to persist standing hold", slog.Any("error", err))
	}
}

// removeStandingHold removes the standing hold of issueID, live or
// expired, so the next dispatch pass may start the issue again.
func removeStandingHold(ctx context.Context, state *State, store StandingHoldStore, issueID string, now time.Time, log *slog.Logger) ControlReply {
	h := state.StandingHolds[issueID]
	delete(state.StandingHolds, issueID)
	issueLog := logging.WithIssue(log, issueID, h.Identifier)
	if store != nil {
		if err := store.DeleteStandingHold(ctx, issueID); err != nil {
			issueLog.Error("failed to delete standing hold", slog.Any("error", err))
		}
	}
	issueLog.Info("issue hold released",
		slog.String("holder", h.Holder),
		slog.Bool("expired", h.Expired()),
		slog.Int64("held_ms", max(now.Sub(h.Since), 0).Milliseconds()),
	)
	return ControlReply{Outcome: ControlDone, Hold: HoldState{
		Identifier: h.Identifier,
		IssueID:    issueID,
		HeldTotal:  max(now.Sub(h.Since), 0),
	}}
}

// standingHoldExpiry carries what expiring leases needs besides the state.
type standingHoldExpiry struct {
	Store  StandingHoldStore
	Router *route.Router
	Wg     *sync.WaitGroup
	Ctx    context.Context
	Log    *slog.Logger
}

// expireStandingHolds ends every live lease whose Until is not after now:
// it sets ExpiredAt to Until, persists it, and publishes hold.expired. The
// issue stays out of dispatch. It returns how many leases it ended.
func expireStandingHolds(state *State, now time.Time, p standingHoldExpiry) int {
	expired := 0
	for issueID, h := range state.StandingHolds {
		if h.Expired() || h.Until.After(now) {
			continue
		}
		h.ExpiredAt = h.Until
		expired++
		log := logging.WithIssue(p.Log, issueID, h.Identifier)
		persistStandingHold(p.Ctx, p.Store, issueID, h, log)
		log.Warn("issue hold expired without a renewal",
			slog.String("holder", h.Holder),
			slog.String("hold_reason", h.Reason),
			slog.String("evidence", h.Evidence),
			slog.Time("until", h.Until),
		)
		publishHoldExpired(issueID, h, p, log)
	}
	return expired
}

// holdExpiredNotification builds the hold.expired event of a lease.
func holdExpiredNotification(issueID string, h *StandingHold) domain.Notification {
	return orchestratorNotification(domain.EventHoldExpired, "", domain.NotificationEnvelope{
		IssueID:    issueID,
		Identifier: h.Identifier,
	}, domain.HoldExpiredFacts{
		By:        h.Holder,
		Reason:    h.Reason,
		Evidence:  h.Evidence,
		RenewedAt: h.RenewedAt,
		Until:     h.Until,
	})
}

func publishHoldExpired(issueID string, h *StandingHold, p standingHoldExpiry, log *slog.Logger) {
	if p.Router == nil || p.Wg == nil {
		return
	}
	delivery := p.Router.Route(holdExpiredNotification(issueID, h))
	if delivery.Empty() {
		log.Info("hold expiry had no destination", slog.String("event_type", string(domain.EventHoldExpired)))
		return
	}
	deliverDetached(p.Ctx, p.Wg, delivery, log, func(received bool, err error) {
		if received && err != nil {
			log.Warn("hold expiry tracker comment failed", slog.Any("error", err))
		}
	})
}

// standingHeldEntry returns the dispatch-pass hold a standing hold gives
// its issue.
func standingHeldEntry(h *StandingHold) HeldEntry {
	reason := SkipStandingHold
	if h.Expired() {
		reason = SkipStandingHoldExpired
	}
	return HeldEntry{Reason: reason, Holder: h.Holder, Note: h.Reason}
}

// SnapshotStandingHold is a read-only view of one standing hold. ExpiredAt
// is zero while the lease is live.
type SnapshotStandingHold struct {
	IssueID    string
	Identifier string
	Holder     string
	Reason     string
	Evidence   string
	Since      time.Time
	RenewedAt  time.Time
	Until      time.Time
	ExpiredAt  time.Time
}

// snapshotStandingHolds returns every standing hold, sorted by identifier
// then issue ID. The slice is never nil.
func snapshotStandingHolds(state *State) []SnapshotStandingHold {
	rows := make([]SnapshotStandingHold, 0, len(state.StandingHolds))
	for id, h := range state.StandingHolds {
		rows = append(rows, SnapshotStandingHold{
			IssueID:    id,
			Identifier: h.Identifier,
			Holder:     h.Holder,
			Reason:     h.Reason,
			Evidence:   h.Evidence,
			Since:      h.Since,
			RenewedAt:  h.RenewedAt,
			Until:      h.Until,
			ExpiredAt:  h.ExpiredAt,
		})
	}
	slices.SortFunc(rows, func(a, b SnapshotStandingHold) int {
		return cmp.Or(cmp.Compare(a.Identifier, b.Identifier), cmp.Compare(a.IssueID, b.IssueID))
	})
	return rows
}

// PopulateStandingHolds loads persisted standing holds into
// state.StandingHolds. Called during startup recovery, before the event
// loop starts; the first tick expires a lease that ended while Sortie was
// down.
func PopulateStandingHolds(state *State, rows []persistence.StandingHold, log *slog.Logger) {
	if log == nil {
		log = slog.Default()
	}
	parse := func(s string) time.Time {
		t, err := time.Parse(time.RFC3339Nano, s)
		if err != nil {
			return time.Time{}
		}
		return t.UTC()
	}
	for _, row := range rows {
		if row.IssueID == "" {
			log.Warn("skipping malformed standing hold record")
			continue
		}
		state.StandingHolds[row.IssueID] = &StandingHold{
			Identifier: row.Identifier,
			Holder:     row.Holder,
			Reason:     row.Reason,
			Evidence:   row.Evidence,
			Since:      parse(row.HeldAt),
			RenewedAt:  parse(row.RenewedAt),
			Until:      parse(row.Until),
			ExpiredAt:  parse(row.ExpiredAt),
		}
	}
}
