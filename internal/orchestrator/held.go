package orchestrator

import (
	"cmp"
	"slices"
	"time"

	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/environment"
	"github.com/sortie-ai/sortie/internal/redact"
)

// SlotScope names which agent slot limit held a candidate.
type SlotScope string

const (
	SlotScopeGlobal SlotScope = "global"
	SlotScopeState  SlotScope = "state"
)

// SlotHold is the agent slot limit that held a candidate, as the pass read
// it.
type SlotHold struct {
	Scope SlotScope
	State string // the candidate's state as read; empty for SlotScopeGlobal
	Used  int
	Limit int
}

// HeldEntry is the hold the latest dispatch pass gave one candidate. Blockers
// is set for [SkipBlockedBy] only, Slot for [SkipAgentSlotsFull] only,
// ResetsAt for the spend reasons and [SkipUsageLimit] only, AgentKind,
// ModelOnly, and Model for [SkipUsageLimit] only, and Holder and Note for
// [SkipStandingHold] only.
type HeldEntry struct {
	Identifier string
	DisplayID  string
	Identity   IssueIdentity // unmasked
	Since      time.Time     // UTC; the first pass that held the issue without a break
	Reason     SkipReason
	Blockers   []HeldBlocker
	Slot       *SlotHold
	ResetsAt   time.Time
	AgentKind  string
	ModelOnly  bool
	Model      string
	Holder     string
	Note       string
}

// HeldBlocker is a blocker that holds a candidate, with the reason the
// blocker judge gave: one of the Blocker* reasons.
type HeldBlocker struct {
	domain.BlockerRef
	Reason string
}

// heldPass collects the holds of one dispatch pass. Like [TickResolution] it
// belongs to one pass, is not safe for concurrent use, and is discarded when
// the pass ends.
type heldPass struct {
	prev      map[string]*HeldEntry
	now       time.Time
	spend     *HeldEntry
	usageHold func(domain.Issue) *HeldEntry
	next      map[string]*HeldEntry
}

// newHeldPass returns a recorder that takes each issue's first-held time from
// prev and stamps a new hold with now. spend is the hold every candidate gets
// while a spend window holds, and nil otherwise. usageHold returns the hold a
// usage limit gives a candidate, or nil for one it does not hold; it is nil
// itself while no usage-limit hold is active.
func newHeldPass(prev map[string]*HeldEntry, now time.Time, spend *HeldEntry, usageHold func(domain.Issue) *HeldEntry) *heldPass {
	return &heldPass{prev: prev, now: now, spend: spend, usageHold: usageHold, next: make(map[string]*HeldEntry)}
}

// hold records entry as the hold of issue. A pass holds an issue once; a
// later call for the same issue ID keeps the first record.
func (p *heldPass) hold(issue domain.Issue, entry HeldEntry) {
	if _, held := p.next[issue.ID]; held {
		return
	}
	entry.Identifier = issue.Identifier
	entry.DisplayID = issue.DisplayID
	entry.Identity = issueIdentityOf(issue)
	entry.Since = p.now
	if before := p.prev[issue.ID]; before != nil {
		entry.Since = before.Since
	}
	p.next[issue.ID] = &entry
}

// holdEligible holds every candidate of rest that passes the eligibility
// gates with the pass's spend hold while one applies, else with the hold a
// usage limit gives the candidate, else with entry. It reads no blockers, so a
// capacity break costs the tracker nothing.
func (p *heldPass) holdEligible(rest []domain.Issue, state *State, activeSet, terminalSet map[string]struct{}, entry HeldEntry) {
	if p.spend != nil {
		entry = *p.spend
	}
	for _, candidate := range rest {
		if !passesEligibilityGates(candidate, state, activeSet, terminalSet) {
			continue
		}
		held := entry
		if standing := state.StandingHolds[candidate.ID]; standing != nil {
			p.hold(candidate, standingHeldEntry(standing))
			continue
		}
		if p.spend == nil && p.usageHold != nil {
			if usage := p.usageHold(candidate); usage != nil {
				held = *usage
			}
		}
		p.hold(candidate, held)
	}
}

// holdDecision records the hold a declined candidate decision implies. An
// ineligible candidate is held for nothing and records none. A blocker
// hold names every blocker judge does not accept.
func (p *heldPass) holdDecision(decision CandidateDecision, judge BlockerJudge, terminalSet map[string]struct{}) {
	entry := HeldEntry{Reason: decision.Reason}
	switch {
	case decision.Reason == SkipIneligible || decision.Reason == SkipNone:
		return
	case decision.Reason == SkipBlockedBy:
		entry.Blockers = unsatisfiedBlockers(judge, decision.Issue.BlockedBy, terminalSet)
	case isSpendCeilingReason(decision.Reason) && p.spend != nil:
		entry = *p.spend
	}
	p.hold(decision.Issue, entry)
}

// entries returns the holds the pass recorded.
func (p *heldPass) entries() map[string]*HeldEntry {
	return p.next
}

// spendHoldEntry returns the hold a spend window gives every candidate: reason
// with the reset instant of the window that reason belongs to. It returns nil
// for [SkipNone].
func spendHoldEntry(reason SkipReason, ceilings []SpendCeilingStatus) *HeldEntry {
	if reason == SkipNone {
		return nil
	}
	for _, status := range ceilings {
		if status.Window.SkipReason() == reason {
			return &HeldEntry{Reason: reason, ResetsAt: status.ResetsAt}
		}
	}
	return &HeldEntry{Reason: reason}
}

// listedHeld reports whether the held list shows the issue: it is on no list
// the active issues merge, and its hop record does not put it between stages,
// where its hold is shown instead.
func listedHeld(state *State, issueID string) bool {
	if _, running := state.Running[issueID]; running {
		return false
	}
	if _, retrying := state.RetryAttempts[issueID]; retrying {
		return false
	}
	if _, exhausted := state.BudgetExhausted[issueID]; exhausted {
		return false
	}
	if _, parked := state.Parked[issueID]; parked {
		return false
	}
	for _, reaction := range state.PendingReactions {
		if reaction.IssueID == issueID {
			return false
		}
	}
	hop := state.StageHops[issueID]
	return hop == nil || !listedBetweenStages(state, issueID, hop)
}

// snapshotHeld builds the held rows sorted by issue identifier, then issue
// ID. The slice is never nil and shares nothing with state; identity and
// blocker and slot states are masked.
func snapshotHeld(state *State) []SnapshotHeldEntry {
	rows := make([]SnapshotHeldEntry, 0, len(state.Held))
	for id, entry := range state.Held {
		if !listedHeld(state, id) {
			continue
		}
		identity := maskedIdentity(entry.Identity)
		var instance environment.Instance
		if state.DescribeEnvironment != nil {
			instance = state.DescribeEnvironment(entry.Identifier)
		}
		rows = append(rows, SnapshotHeldEntry{
			IssueID:     id,
			Identifier:  entry.Identifier,
			DisplayID:   entry.DisplayID,
			IssueTitle:  identity.Title,
			IssueURL:    identity.URL,
			IssueType:   identity.IssueType,
			IssueLabels: identity.Labels,
			State:       identity.State,
			Hold:        snapshotHold(entry),
			Environment: snapshotEnvironment(state, instance, entry.Identifier),
		})
	}
	slices.SortFunc(rows, func(a, b SnapshotHeldEntry) int {
		return cmp.Or(cmp.Compare(a.Identifier, b.Identifier), cmp.Compare(a.IssueID, b.IssueID))
	})
	return rows
}

// snapshotHold returns the view of one hold record. Blockers is non-nil
// exactly for [SkipBlockedBy] and Slot exactly for [SkipAgentSlotsFull].
func snapshotHold(e *HeldEntry) SnapshotHold {
	hold := SnapshotHold{
		Reason:    e.Reason,
		Since:     e.Since,
		ResetsAt:  e.ResetsAt,
		AgentKind: e.AgentKind,
		ModelOnly: e.ModelOnly,
		Model:     e.Model,
		Holder:    e.Holder,
		Note:      e.Note,
	}
	if e.Reason == SkipBlockedBy {
		hold.Blockers = make([]SnapshotBlocker, 0, len(e.Blockers))
		for _, blocker := range e.Blockers {
			hold.Blockers = append(hold.Blockers, SnapshotBlocker{
				IssueID:    blocker.ID,
				Identifier: blocker.Identifier,
				DisplayID:  blocker.DisplayID,
				State:      redact.Mask(blocker.State),
				Reason:     blocker.Reason,
			})
		}
	}
	if e.Reason == SkipAgentSlotsFull && e.Slot != nil {
		hold.Slot = &SnapshotSlotHold{
			Scope: e.Slot.Scope,
			State: redact.Mask(e.Slot.State),
			Used:  e.Slot.Used,
			Limit: e.Slot.Limit,
		}
	}
	return hold
}
