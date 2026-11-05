package orchestrator

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"maps"
	"slices"
	"time"

	"github.com/sortie-ai/sortie/internal/domain"
)

// supervisorReadyAge is how long a story pull request waits ready to
// merge before the digest marks it old: Sortie's own merge has had its
// chance, and the supervisor's merge rule applies.
const supervisorReadyAge = 30 * time.Minute

// supervisorDigestLen is the hex digits of the stored state digest.
const supervisorDigestLen = 16

// What a watched story pull request waits on, as the state digest
// records it.
const (
	wakePullReady       = "ready"
	wakePullConflicting = "conflicting"
)

// supervisorWake is the state whose digest decides whether a trigger
// starts a supervisor session: what a supervisor rule acts on, and nothing
// that changes while no rule's action becomes possible. Every list is
// sorted. Left out, each because no rule acts on it:
//   - a story held only by its blockers: the rules act on the blocker, a
//     parked or held blocker counts on its own, and a blocker's pull
//     request that conflicts or waits ready counts in Pulls;
//   - a full slot or host, a hold's since, a lease's renewal;
//   - a running job, story run or fix job, starting or ending: a fix that
//     lands moves its pull request (Pulls), one that fails stops on a
//     failure, which triggers its own session; the end of a parked story's
//     live-proof job is the one end a rule acts on (wakePark.ProofsEnded);
//   - how long a question waits on a person: only a person answers it,
//     and the park or the question itself is in the digest.
type supervisorWake struct {
	Held      []wakeHold     `json:"held"`
	Leases    []wakeLease    `json:"leases"`
	Parked    []wakePark     `json:"parked"`
	Pulls     []wakePull     `json:"pulls"`
	Faults    []wakeFault    `json:"faults"`
	Questions []wakeQuestion `json:"questions"`
}

// wakeHold is a story held for a reason the supervisor can act on: a hold
// of the dispatch pass other than a blocker or a full slot (release a
// lease, ask a person or file an issue), a budget (grant a session) or a
// platform fault (ask, or withdraw its own stale question).
type wakeHold struct {
	IssueID string `json:"issue_id"`
	Reason  string `json:"reason"`
}

// wakeLease is a standing hold; Expired once it is past its expiry, when
// the supervisor renews or releases it.
type wakeLease struct {
	IssueID string `json:"issue_id"`
	Holder  string `json:"holder"`
	Expired bool   `json:"expired"`
}

// wakePark is a parked story and its question, which the supervisor
// records a merge of, answers or proves. ProofsEnded counts the
// live-proof jobs of the story that ended while this process ran, since
// the supervisor decides the park on each one's result.
type wakePark struct {
	IssueID     string `json:"issue_id"`
	Reason      string `json:"reason"`
	Question    string `json:"question"`
	ProofsEnded int    `json:"proofs_ended"`
}

// wakePull is a story pull request that is ready to merge or conflicts,
// at its head; Old once a ready one waited supervisorReadyAge. The
// supervisor merges a ready one, or starts the fix of a conflicting one.
type wakePull struct {
	PullRequest string `json:"pull_request"`
	IssueID     string `json:"issue_id"`
	State       string `json:"state"`
	Head        string `json:"head"`
	Old         bool   `json:"old"`
}

// wakeFault is an open platform fault: whether a person marked its action
// done, and the stories it holds, which a question it asks may gain.
type wakeFault struct {
	ID         string   `json:"id"`
	Class      string   `json:"class"`
	ActionDone bool     `json:"action_done"`
	Stories    []string `json:"stories"`
}

// wakeQuestion is an open platform fault that waits on a person's action.
type wakeQuestion struct {
	FaultID string `json:"fault_id"`
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

// blockerHold reports whether r holds a story for its blockers.
func blockerHold(r SkipReason) bool {
	switch r {
	case SkipBlockedBy, SkipBlockersUnresolved, SkipBlockersNotRead, SkipBlockersIncomplete:
		return true
	}
	return false
}

// noteLiveProofEnded records that a live-proof job of the story issueID
// ended. Must run on the event loop.
func noteLiveProofEnded(state *State, issueID string) {
	if s := state.supervisor; s != nil {
		s.proofsEnded[issueID]++
	}
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
		Faults: []wakeFault{}, Questions: []wakeQuestion{},
	}
	for id, h := range state.Held {
		if h.Reason == SkipAgentSlotsFull || h.Reason == SkipSSHHostsFull || blockerHold(h.Reason) {
			continue
		}
		w.Held = append(w.Held, wakeHold{IssueID: id, Reason: string(h.Reason)})
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
	var proofsEnded map[string]int
	if s := state.supervisor; s != nil {
		maps.DeleteFunc(s.proofsEnded, func(id string, _ int) bool { return state.Parked[id] == nil })
		proofsEnded = s.proofsEnded
	}
	for id, p := range state.Parked {
		w.Parked = append(w.Parked, wakePark{IssueID: id, Reason: p.Reason, Question: p.Statement, ProofsEnded: proofsEnded[id]})
	}
	for _, f := range state.PlatformFaults {
		fault := wakeFault{ID: f.ID, Class: f.Class, ActionDone: !f.ActionDoneAt.IsZero(), Stories: []string{}}
		for _, h := range f.Holds {
			fault.Stories = append(fault.Stories, h.IssueID)
		}
		slices.Sort(fault.Stories)
		w.Faults = append(w.Faults, fault)
		if f.Action != nil && f.ActionDoneAt.IsZero() {
			w.Questions = append(w.Questions, wakeQuestion{FaultID: f.ID})
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
	return w
}

// supervisorStateDigest returns the state digest of state at now. Must run
// on the event loop.
func supervisorStateDigest(state *State, now time.Time) string {
	return observeSupervisorWake(state, now).digest()
}
