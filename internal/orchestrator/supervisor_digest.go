package orchestrator

import (
	"cmp"
	"slices"
	"time"

	"github.com/sortie-ai/sortie/internal/redact"
)

// supervisorDigest is the state a supervisor session's prompt carries:
// what the state API serves of runs, retries, parks, holds, platform
// faults and pull request reactions, and the plan blockers that hold
// stories.
type supervisorDigest struct {
	GeneratedAt    time.Time             `json:"generated_at"`
	DispatchPaused bool                  `json:"dispatch_paused"`
	SlotsUsed      int                   `json:"slots_used"`
	SlotsLimit     int                   `json:"slots_limit"`
	Running        []digestRun           `json:"running"`
	Retrying       []digestRetry         `json:"retrying"`
	Parked         []digestPark          `json:"parked"`
	Held           []digestHold          `json:"held"`
	BudgetHeld     []digestBudgetHold    `json:"budget_held"`
	PlatformFaults []digestPlatformFault `json:"platform_faults"`
	Reactions      []digestReaction      `json:"reactions"`
	PlanBlockers   []digestBlockedStory  `json:"plan_blockers"`
}

type digestRun struct {
	Identifier string    `json:"issue_identifier"`
	Rule       string    `json:"rule,omitempty"`
	StartedAt  time.Time `json:"started_at"`
	Turns      int       `json:"turns"`
}

type digestRetry struct {
	Identifier string    `json:"issue_identifier"`
	Attempt    int       `json:"attempt"`
	DueAt      time.Time `json:"due_at"`
	Error      string    `json:"error,omitempty"`
}

type digestPark struct {
	Identifier string    `json:"issue_identifier"`
	Reason     string    `json:"reason"`
	ParkedAt   time.Time `json:"parked_at"`
	Statement  string    `json:"statement,omitempty"`
}

type digestHold struct {
	Identifier string    `json:"issue_identifier"`
	Reason     string    `json:"reason"`
	Since      time.Time `json:"since"`
}

type digestBudgetHold struct {
	Identifier string `json:"issue_identifier"`
	Reason     string `json:"reason"`
}

type digestPlatformFault struct {
	ID           string   `json:"id"`
	Class        string   `json:"class"`
	Cause        string   `json:"cause"`
	Detail       string   `json:"detail,omitempty"`
	PersonAction string   `json:"person_action,omitempty"`
	Where        string   `json:"where,omitempty"`
	ActionDone   bool     `json:"action_done"`
	Stories      []string `json:"stories"`
}

type digestReaction struct {
	Identifier     string    `json:"issue_identifier"`
	Kind           string    `json:"kind"`
	PullRequest    string    `json:"pull_request,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
	LastProgressAt time.Time `json:"last_progress_at"`
	Checks         int       `json:"checks_without_dispatch"`
	Escalated      bool      `json:"escalated"`
}

type digestBlockedStory struct {
	Identifier string          `json:"issue_identifier"`
	BlockedBy  []digestBlocker `json:"blocked_by"`
}

type digestBlocker struct {
	Identifier string `json:"issue_identifier"`
	State      string `json:"state,omitempty"`
	Reason     string `json:"reason,omitempty"`
}

// buildSupervisorDigest returns the digest of state at now, every list
// sorted by issue identifier and every free text masked.
func buildSupervisorDigest(state *State, now time.Time) supervisorDigest {
	d := supervisorDigest{
		GeneratedAt:    now.UTC(),
		DispatchPaused: state.dispatchPause != nil,
		SlotsUsed:      len(state.Running),
		SlotsLimit:     state.MaxConcurrentAgents,
		Running:        []digestRun{},
		Retrying:       []digestRetry{},
		Parked:         []digestPark{},
		Held:           []digestHold{},
		BudgetHeld:     []digestBudgetHold{},
		PlatformFaults: []digestPlatformFault{},
		Reactions:      []digestReaction{},
		PlanBlockers:   []digestBlockedStory{},
	}
	for _, e := range state.Running {
		d.Running = append(d.Running, digestRun{Identifier: e.Identifier, Rule: e.RuleName, StartedAt: e.StartedAt.UTC(), Turns: e.TurnCount})
	}
	for _, e := range state.RetryAttempts {
		d.Retrying = append(d.Retrying, digestRetry{
			Identifier: e.Identifier, Attempt: e.Attempt, DueAt: time.UnixMilli(e.DueAtMS).UTC(), Error: redact.Mask(e.Error),
		})
	}
	for _, e := range state.Parked {
		d.Parked = append(d.Parked, digestPark{Identifier: e.Identifier, Reason: e.Reason, ParkedAt: e.ParkedAt.UTC(), Statement: redact.Mask(e.Statement)})
	}
	for _, e := range state.Held {
		d.Held = append(d.Held, digestHold{Identifier: e.Identifier, Reason: string(e.Reason), Since: e.Since.UTC()})
		if len(e.Blockers) == 0 {
			continue
		}
		story := digestBlockedStory{Identifier: e.Identifier}
		for _, b := range e.Blockers {
			story.BlockedBy = append(story.BlockedBy, digestBlocker{Identifier: b.Identifier, State: b.State, Reason: b.Reason})
		}
		d.PlanBlockers = append(d.PlanBlockers, story)
	}
	for _, e := range state.BudgetExhausted {
		d.BudgetHeld = append(d.BudgetHeld, digestBudgetHold{Identifier: e.Identifier, Reason: e.Reason})
	}
	for _, f := range snapshotPlatformFaults(state) {
		fault := digestPlatformFault{ID: f.ID, Class: f.Class, Cause: f.Cause, Detail: f.Detail, ActionDone: !f.ActionDoneAt.IsZero(), Stories: []string{}}
		if f.Action != nil {
			fault.PersonAction, fault.Where = f.Action.Action, f.Action.Where
		}
		for _, h := range f.Holds {
			fault.Stories = append(fault.Stories, h.Identifier)
		}
		d.PlatformFaults = append(d.PlatformFaults, fault)
	}
	for _, p := range state.PendingReactions {
		r := digestReaction{
			Identifier: p.Identifier, Kind: p.Kind, CreatedAt: p.CreatedAt.UTC(), LastProgressAt: reactionProgressAt(p).UTC(),
			Checks: p.PendingAttempts, Escalated: !p.EscalatedAt.IsZero(),
		}
		if pr := pullRequestOf(p); pr.Number > 0 {
			r.PullRequest = formatPullRequest(pr)
		}
		d.Reactions = append(d.Reactions, r)
	}

	slices.SortFunc(d.Running, func(a, b digestRun) int { return cmp.Compare(a.Identifier, b.Identifier) })
	slices.SortFunc(d.Retrying, func(a, b digestRetry) int { return cmp.Compare(a.Identifier, b.Identifier) })
	slices.SortFunc(d.Parked, func(a, b digestPark) int { return cmp.Compare(a.Identifier, b.Identifier) })
	slices.SortFunc(d.Held, func(a, b digestHold) int { return cmp.Compare(a.Identifier, b.Identifier) })
	slices.SortFunc(d.BudgetHeld, func(a, b digestBudgetHold) int { return cmp.Compare(a.Identifier, b.Identifier) })
	slices.SortFunc(d.PlanBlockers, func(a, b digestBlockedStory) int { return cmp.Compare(a.Identifier, b.Identifier) })
	slices.SortFunc(d.Reactions, func(a, b digestReaction) int {
		return cmp.Or(cmp.Compare(a.Identifier, b.Identifier), cmp.Compare(a.Kind, b.Kind))
	})
	return d
}
