package domain

import "time"

// MessageFacts is the content of an orchestrator event: the values its
// title and body report. Its implementations are the types in this file;
// agent.message carries none.
type MessageFacts interface{ messageFacts() }

// Budget hold reasons carried by [BudgetHeldFacts.Reason].
const (
	BudgetHoldReasonMoney   = "money_budget"
	BudgetHoldReasonToken   = "token_budget"
	BudgetHoldReasonSession = "session_budget"
)

// StageReasonCeiling is the [StageTransition.Reason] of a hop stopped at
// the hop ceiling.
const StageReasonCeiling = "ceiling"

// SessionStartedFacts is the content of session.started.
type SessionStartedFacts struct{}

// SessionCompletedFacts is the content of session.completed.
type SessionCompletedFacts struct {
	Elapsed        time.Duration
	Turns          int
	RetryScheduled bool
}

// SessionStoppedFacts is the content of session.stopped.
type SessionStoppedFacts struct {
	Elapsed time.Duration
	Turns   int

	// Reason is the soft-stop token.
	Reason string
}

// SessionFailedFacts is the content of session.failed.
type SessionFailedFacts struct {
	Elapsed        time.Duration
	RetryScheduled bool
	NextAttempt    int
}

// CIFailureFacts is the content of escalation.ci_failure with a check
// summary.
type CIFailureFacts struct {
	Ref          string
	Attempts     int
	FailingCount int

	// Checks lists every check run, in result order.
	Checks []CICheckFacts
}

// CICheckFacts is one check run of a [CIFailureFacts].
type CICheckFacts struct {
	Name       string
	Conclusion string
	DetailsURL string
	Failing    bool
}

// TriageEscalationFacts is the content of a triage escalation:
// escalation.ci_failure, escalation.review_comments,
// escalation.bot_review, or escalation.merge_conflicts.
type TriageEscalationFacts struct {
	// Ref is set on escalation.ci_failure.
	Ref string

	// PRNumber is set on every other triage event.
	PRNumber int
}

// ReviewEscalationFacts is the content of escalation.review_comments.
type ReviewEscalationFacts struct {
	PRNumber int
	Branch   string
	Turns    int
}

// BotReviewEscalationFacts is the content of escalation.bot_review.
type BotReviewEscalationFacts struct {
	PRNumber int
	Turns    int
}

// MergeConflictFacts is the content of escalation.merge_conflicts.
type MergeConflictFacts struct {
	PRNumber int
	Attempts int
}

// AutoMergeEscalationFacts is the content of escalation.auto_merge.
type AutoMergeEscalationFacts struct {
	PRNumber int
	Turns    int
}

// AutoMergedFacts is the content of auto_merge.merged.
type AutoMergedFacts struct {
	Owner    string
	Repo     string
	PRNumber int
	Strategy MergeStrategy

	// SHA is empty when none was reported.
	SHA string
}

// MergeCompletionFacts is the content of escalation.merge_completion when
// the tracker transition did not complete.
type MergeCompletionFacts struct {
	Owner       string
	Repo        string
	PRNumber    int
	TargetState string
	Attempts    int
}

// MergeCompletionMissingSHAFacts is the content of
// escalation.merge_completion when the merge commit SHA never arrived.
type MergeCompletionMissingSHAFacts struct {
	Owner       string
	Repo        string
	PRNumber    int
	TargetState string
	Waited      time.Duration
}

// StageFacts is the content of stage.advanced and stage.not_advanced. The
// content is the envelope's Stage.
type StageFacts struct{}

// BudgetHeldFacts carries the budget-hold entry's reported fields under
// their entry names and types.
type BudgetHeldFacts struct {
	// Reason is a BudgetHoldReason value.
	Reason string

	// CeilingSetting is the setting that bounds Reason; empty when none
	// is known.
	CeilingSetting string

	UsedSessions   int
	BudgetSessions int
	UsedTokens     *int64
	BudgetTokens   int64

	// TokensComplete is false when a session's or a turn's token spend
	// is unknown.
	TokensComplete bool

	StoppedInFlight *int
	UsedUSD         *float64
	UsedUSDComplete *bool
	BudgetUSD       float64
	ExhaustedAt     time.Time
}

// SessionEscalationCriteriaFailed is the [SessionEscalatedFacts.Kind] of
// a plan story whose acceptance-criteria check did not pass.
const SessionEscalationCriteriaFailed = "criteria_failed"

// SessionEscalatedFacts is the content of session.escalated: the values of
// the event's [SessionEscalation].
type SessionEscalatedFacts struct {
	Kind string
	Rule string
	URL  string
}

// HoldExpiredFacts is the content of hold.expired: the lease that ran out
// without a renewal. By, Reason, and Evidence are as the last hold or
// renewal request said.
type HoldExpiredFacts struct {
	By        string
	Reason    string
	Evidence  string
	RenewedAt time.Time
	Until     time.Time
}

func (SessionStartedFacts) messageFacts()            {}
func (SessionCompletedFacts) messageFacts()          {}
func (SessionStoppedFacts) messageFacts()            {}
func (SessionFailedFacts) messageFacts()             {}
func (CIFailureFacts) messageFacts()                 {}
func (TriageEscalationFacts) messageFacts()          {}
func (ReviewEscalationFacts) messageFacts()          {}
func (BotReviewEscalationFacts) messageFacts()       {}
func (MergeConflictFacts) messageFacts()             {}
func (AutoMergeEscalationFacts) messageFacts()       {}
func (AutoMergedFacts) messageFacts()                {}
func (MergeCompletionFacts) messageFacts()           {}
func (MergeCompletionMissingSHAFacts) messageFacts() {}
func (StageFacts) messageFacts()                     {}
func (BudgetHeldFacts) messageFacts()                {}
func (SessionEscalatedFacts) messageFacts()          {}
func (HoldExpiredFacts) messageFacts()               {}

// StoryRevertedFacts is the content of story.reverted: a story's merge
// that a merged pull request reverted, so the story is no longer done.
// BySortie reports that Sortie opened and merged the revert because the
// merged change was not proven live by its deadline; otherwise someone
// merged a revert on the forge. Reason says why, and State is where the
// story went, empty when Sortie left its state as it was.
type StoryRevertedFacts struct {
	PullRequest       string
	MergeCommit       string
	RevertPullRequest string
	BySortie          bool
	Reason            string
	State             string
}

func (StoryRevertedFacts) messageFacts() {}
