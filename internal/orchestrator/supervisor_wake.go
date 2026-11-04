package orchestrator

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"slices"
	"time"

	"github.com/sortie-ai/sortie/internal/domain"
)

// Time thresholds the state digest records once crossed.
const (
	// supervisorReadyAge is how long a story pull request waits ready to
	// merge before the digest marks it old.
	supervisorReadyAge = 30 * time.Minute

	// supervisorQuestionAlertAge is how long a parked story's question or
	// a question for a person goes unanswered before the digest marks it
	// old.
	supervisorQuestionAlertAge = 4 * time.Hour
)

// supervisorDigestLen is the hex digits of the stored state digest.
const supervisorDigestLen = 16

// What a watched story pull request waits on, as the state digest
// records it.
const (
	wakePullReady       = "ready"
	wakePullConflicting = "conflicting"
)

// supervisorWake is the state whose digest decides whether a trigger
// starts a supervisor session: what the supervisor could act on, without
// what moves on its own (a slot that frees or fills, a renewed lease, a
// check that ran again). Every list is sorted.
type supervisorWake struct {
	Held      []wakeHold     `json:"held"`
	Leases    []wakeLease    `json:"leases"`
	Parked    []wakePark     `json:"parked"`
	Pulls     []wakePull     `json:"pulls"`
	Faults    []wakeFault    `json:"faults"`
	Questions []wakeQuestion `json:"questions"`
	Jobs      []wakeJob      `json:"jobs"`
}

type wakeHold struct {
	IssueID string `json:"issue_id"`
	Reason  string `json:"reason"`
}

// wakeLease is a standing hold; Expired once it is past its expiry.
type wakeLease struct {
	IssueID string `json:"issue_id"`
	Holder  string `json:"holder"`
	Expired bool   `json:"expired"`
}

// wakePark is a parked story and its question; Old once the question is
// older than its alert age.
type wakePark struct {
	IssueID  string `json:"issue_id"`
	Reason   string `json:"reason"`
	Question string `json:"question"`
	Old      bool   `json:"old"`
}

// wakePull is a story pull request that is ready to merge or conflicts,
// at its head; Old once a ready one waited supervisorReadyAge.
type wakePull struct {
	PullRequest string `json:"pull_request"`
	IssueID     string `json:"issue_id"`
	State       string `json:"state"`
	Head        string `json:"head"`
	Old         bool   `json:"old"`
}

type wakeFault struct {
	ID         string   `json:"id"`
	Class      string   `json:"class"`
	ActionDone bool     `json:"action_done"`
	Stories    []string `json:"stories"`
}

// wakeQuestion is an open platform fault that waits on a person's action;
// Old once it is older than its alert age.
type wakeQuestion struct {
	FaultID string `json:"fault_id"`
	Old     bool   `json:"old"`
}

// wakeJob is a running live-proof or fix job.
type wakeJob struct {
	IssueID string `json:"issue_id"`
	Kind    string `json:"kind"`
}

// digest returns the hex digest of w.
func (w supervisorWake) digest() string {
	body, err := json.Marshal(w)
	if err != nil {
		// Every member is a string, a bool or a list of them.
		panic("supervisor wake state: " + err.Error())
	}
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])[:supervisorDigestLen]
}

// wakeHoldReason is the reason the digest records for a dispatch hold:
// the four blocker reasons are one, since which of them a pass records
// depends on how many blockers it could read.
func wakeHoldReason(r SkipReason) string {
	switch r {
	case SkipBlockedBy, SkipBlockersUnresolved, SkipBlockersNotRead, SkipBlockersIncomplete:
		return "blocked"
	}
	return string(r)
}

// pullState is what a story pull request waits on: wakePullConflicting
// when the last conflict check found it conflicting, wakePullReady when
// every check the reactions made passed, and "" otherwise. It decides as
// the state API's pulls do.
func pullState(reactions []*PendingReaction) string {
	conflict, waits := false, false
	for _, r := range reactions {
		if !r.EscalatedAt.IsZero() {
			return ""
		}
		check := r.LastCheck
		switch r.Kind {
		case ReactionKindMergeConflict:
			if check != nil && check.Mergeability != nil && check.Mergeability.Mergeable != nil && !*check.Mergeability.Mergeable {
				conflict = true
			}
		case ReactionKindCI:
			if check == nil || check.CI == nil || check.CI.Status != string(domain.CIStatusPassing) {
				waits = true
			}
		case ReactionKindReview, ReactionKindBotReview:
			if check == nil || check.Comments == nil || check.Comments.Actionable > 0 || check.Comments.AwaitingApproval {
				waits = true
			}
		}
	}
	switch {
	case conflict:
		return wakePullConflicting
	case waits:
		return ""
	}
	return wakePullReady
}

// pullHead is the head commit of a pull request: the one its newest check
// read, else the one its reactions recorded.
func pullHead(reactions []*PendingReaction) string {
	var head string
	var at time.Time
	for _, r := range reactions {
		if c := r.LastCheck; c != nil && c.HeadSHA != "" && !c.ObservedAt.Before(at) {
			head, at = c.HeadSHA, c.ObservedAt
		}
	}
	if head != "" {
		return head
	}
	for _, r := range reactions {
		if h := pullRequestOf(r).HeadSHA; h != "" {
			return h
		}
	}
	return ""
}

// observeSupervisorWake returns the wake state of state at now. It keeps,
// per ready pull request and head, when it was first seen ready, so a pull
// request crosses supervisorReadyAge once; a restart starts that wait
// again. Must run on the event loop.
func observeSupervisorWake(state *State, now time.Time) supervisorWake {
	w := supervisorWake{
		Held: []wakeHold{}, Leases: []wakeLease{}, Parked: []wakePark{}, Pulls: []wakePull{},
		Faults: []wakeFault{}, Questions: []wakeQuestion{}, Jobs: []wakeJob{},
	}
	for id, h := range state.Held {
		if h.Reason == SkipAgentSlotsFull || h.Reason == SkipSSHHostsFull {
			continue
		}
		w.Held = append(w.Held, wakeHold{IssueID: id, Reason: wakeHoldReason(h.Reason)})
	}
	for id, e := range state.BudgetExhausted {
		w.Held = append(w.Held, wakeHold{IssueID: id, Reason: "budget: " + e.Reason})
	}
	for id, fault := range state.FaultHolds {
		w.Held = append(w.Held, wakeHold{IssueID: id, Reason: "platform_fault: " + fault})
	}
	for id, h := range state.StandingHolds {
		w.Leases = append(w.Leases, wakeLease{IssueID: id, Holder: h.Holder, Expired: h.Expired() || !h.Until.After(now)})
	}
	for id, p := range state.Parked {
		w.Parked = append(w.Parked, wakePark{
			IssueID: id, Reason: p.Reason, Question: p.Statement, Old: now.Sub(p.ParkedAt) >= supervisorQuestionAlertAge,
		})
	}
	for _, f := range state.PlatformFaults {
		fault := wakeFault{ID: f.ID, Class: f.Class, ActionDone: !f.ActionDoneAt.IsZero(), Stories: []string{}}
		for _, h := range f.Holds {
			fault.Stories = append(fault.Stories, h.IssueID)
		}
		slices.Sort(fault.Stories)
		w.Faults = append(w.Faults, fault)
		if f.Action != nil && f.ActionDoneAt.IsZero() {
			w.Questions = append(w.Questions, wakeQuestion{FaultID: f.ID, Old: now.Sub(f.RecordedAt) >= supervisorQuestionAlertAge})
		}
	}
	for id, e := range state.Running {
		if e.ReactionKind != "" {
			w.Jobs = append(w.Jobs, wakeJob{IssueID: id, Kind: e.ReactionKind})
		}
	}

	pulls := map[string][]*PendingReaction{}
	for _, p := range state.PendingReactions {
		pr := pullRequestOf(p)
		if p.Kind == ReactionKindMergeCompletion || pr.Number <= 0 {
			continue
		}
		key := formatPullRequest(pr)
		pulls[key] = append(pulls[key], p)
	}
	var readySince map[string]time.Time
	if s := state.supervisor; s != nil {
		readySince = make(map[string]time.Time, len(s.readySince))
	}
	for key, reactions := range pulls {
		st := pullState(reactions)
		if st == "" {
			continue
		}
		pull := wakePull{PullRequest: key, IssueID: reactions[0].IssueID, State: st, Head: pullHead(reactions)}
		if st == wakePullReady && readySince != nil {
			mark := key + "@" + pull.Head
			since, seen := state.supervisor.readySince[mark]
			if !seen {
				since = now
			}
			readySince[mark] = since
			pull.Old = now.Sub(since) >= supervisorReadyAge
		}
		w.Pulls = append(w.Pulls, pull)
	}
	if readySince != nil {
		state.supervisor.readySince = readySince
	}

	slices.SortFunc(w.Held, func(a, b wakeHold) int {
		return cmp.Or(cmp.Compare(a.IssueID, b.IssueID), cmp.Compare(a.Reason, b.Reason))
	})
	slices.SortFunc(w.Leases, func(a, b wakeLease) int { return cmp.Compare(a.IssueID, b.IssueID) })
	slices.SortFunc(w.Parked, func(a, b wakePark) int { return cmp.Compare(a.IssueID, b.IssueID) })
	slices.SortFunc(w.Pulls, func(a, b wakePull) int { return cmp.Compare(a.PullRequest, b.PullRequest) })
	slices.SortFunc(w.Faults, func(a, b wakeFault) int { return cmp.Compare(a.ID, b.ID) })
	slices.SortFunc(w.Questions, func(a, b wakeQuestion) int { return cmp.Compare(a.FaultID, b.FaultID) })
	slices.SortFunc(w.Jobs, func(a, b wakeJob) int { return cmp.Or(cmp.Compare(a.IssueID, b.IssueID), cmp.Compare(a.Kind, b.Kind)) })
	return w
}

// supervisorStateDigest returns the state digest of state at now. Must run
// on the event loop.
func supervisorStateDigest(state *State, now time.Time) string {
	return observeSupervisorWake(state, now).digest()
}
