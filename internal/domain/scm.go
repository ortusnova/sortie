package domain

import (
	"context"
	"fmt"
	"time"
)

// MergeStrategy enumerates the supported pull-request merge strategies.
// Implementations map these to the provider's merge-method values.
type MergeStrategy string

const (
	// StrategyMerge produces a merge commit on the base branch.
	StrategyMerge MergeStrategy = "merge"

	// StrategySquash squashes the PR commits into a single commit on
	// the base branch.
	StrategySquash MergeStrategy = "squash"

	// StrategyRebase rebases the PR commits onto the base branch.
	StrategyRebase MergeStrategy = "rebase"
)

// ReviewDecision summarizes the aggregated review state for a pull request.
type ReviewDecision string

const (
	// ReviewDecisionApproved indicates no changes request stands, no
	// approval rule is unmet, and the forge reports approval or its
	// approval rules met. GitLab Enterprise reports approval when no rule
	// applies, so the value does not prove a person approved.
	ReviewDecisionApproved ReviewDecision = "APPROVED"

	// ReviewDecisionChangesRequested indicates at least one reviewer
	// has requested changes and the request is the most recent review.
	ReviewDecisionChangesRequested ReviewDecision = "CHANGES_REQUESTED"

	// ReviewDecisionReviewRequired indicates no changes request stands
	// and an approval rule is unmet, counted as the forge's merge gate
	// counts approvals.
	ReviewDecisionReviewRequired ReviewDecision = "REVIEW_REQUIRED"

	// ReviewDecisionReviewRequested indicates none of the other decisions
	// holds and a requested or assigned reviewer has not approved. Only
	// adapters that fold reviewer requests return it.
	ReviewDecisionReviewRequested ReviewDecision = "REVIEW_REQUESTED"

	// ReviewDecisionNotRequired indicates none of the other decisions
	// holds.
	ReviewDecisionNotRequired ReviewDecision = "NOT_REQUIRED"
)

// MergeabilityState summarizes whether a pull request is ready to merge.
// Implementations map provider-specific status strings to this enum.
type MergeabilityState string

const (
	// MergeabilityClean indicates all required checks pass and the PR
	// is ready to merge.
	MergeabilityClean MergeabilityState = "clean"

	// MergeabilityUnstable indicates the PR is mergeable but some
	// non-required checks are failing.
	MergeabilityUnstable MergeabilityState = "unstable"

	// MergeabilityBlocked indicates branch protection prevents the
	// merge (missing review, behind base, draft state).
	MergeabilityBlocked MergeabilityState = "blocked"

	// MergeabilityDirty indicates the PR has merge conflicts.
	MergeabilityDirty MergeabilityState = "dirty"

	// MergeabilityUnknown indicates the provider is still computing
	// mergeability.
	MergeabilityUnknown MergeabilityState = "unknown"
)

// PRMergeStatus carries the merge precondition state for a pull request.
// ReviewDecision and CIConclusion are populated by their dedicated reads
// so a single status read can answer one question at a time.
type PRMergeStatus struct {
	// ReviewDecision is the aggregated review decision. Unset when the
	// caller obtains the value from a separate read.
	ReviewDecision ReviewDecision

	// CIConclusion is the aggregated CI conclusion string. Unset when
	// the caller obtains the value from a separate read.
	CIConclusion string

	// Draft reports whether the PR is in draft state.
	Draft bool

	// Mergeability is the normalized mergeability classification.
	Mergeability MergeabilityState

	// HeadSHA is the latest commit SHA on the PR head branch.
	HeadSHA string

	// BranchName is the source branch name (PR head).
	BranchName string

	// BaseBranch is the PR target (base) branch name, e.g. "main" or
	// "develop". Populated by GetMergeability from the same PR object it
	// already fetches; empty only when the provider omits the base ref.
	BaseBranch string

	// Merged reports whether the pull request has been merged.
	Merged bool

	// MergeCommitSHA is the merge commit identifier reported by the
	// provider. Set only when Merged is true.
	MergeCommitSHA string

	// Closed reports whether the provider considers the pull request
	// no longer open. It is true for a pull request closed without
	// merging and, on providers whose closed state subsumes merging,
	// for a merged one as well, so a caller that wants the
	// closed-without-merge condition tests Closed && !Merged rather
	// than Closed alone.
	Closed bool

	// URL is the pull request's web page as the provider reports it; empty
	// when the provider reports none.
	URL string
}

// MergeResult carries the outcome of a successful merge call.
type MergeResult struct {
	// SHA is the merge commit SHA returned by the provider.
	SHA string

	// Merged reports whether the merge completed.
	Merged bool

	// Message is the provider-supplied status text, if any.
	Message string
}

// LabelEvent is one normalized entry from a pull request's label-event
// journal. Adapters map provider-specific event shapes to this type.
type LabelEvent struct {
	// ID is the provider journal entry id: opaque to the orchestrator and
	// unique per PR. Adapters normalize it to a lexically-sortable form so
	// that ordering entries by (At, ID) as strings matches their journal
	// (chronological) order.
	ID string

	// Label is the normalized (lowercased) label name.
	Label string

	// Actor is the login of the acting user.
	Actor string

	// Added is true for a labeled event and false for an unlabeled event.
	Added bool

	// At is the entry's own timestamp, normalized to UTC. It is never
	// synthesized: At is half of the (At, ID) high-water-mark position,
	// and the zero time sorts before every recorded position, so a
	// substituted value would make the entry unreachable for detection.
	At time.Time
}

// SCMAdapter exposes read and write operations against the SCM platform.
// Implementations must be safe for concurrent use.
type SCMAdapter interface {
	// FetchPendingReviews returns review comments from CHANGES_REQUESTED
	// reviews on the given PR. An implementation excludes an author only
	// when its platform reports that author as an automated bot account;
	// it MUST NOT consult an operator-configured allowlist, because the
	// caller applies that half of bot classification itself. Comments
	// marked as outdated by the platform are included with
	// Outdated=true; the caller is responsible for filtering.
	//
	// Returns an empty non-nil slice when no pending review comments
	// exist. Returns a [*SCMError] on failure.
	FetchPendingReviews(ctx context.Context, prNumber int, owner, repo string) ([]ReviewComment, error)

	// FetchBotReviewComments returns review comments authored by
	// automated review bots on the given PR. A comment is bot-authored
	// when the platform reports its author as an automated bot account
	// OR when the author login matches an entry in botUsernames under a
	// case-insensitive comparison. The two conditions form a union:
	// either one qualifies. botUsernames may be nil or empty, in which
	// case only the platform's bot-account signal selects comments.
	// FetchBotReviewComments applies both arms itself; the caller applies
	// only the allowlist arm on top of [SCMAdapter.FetchPendingReviews],
	// which already carries the platform-marker arm.
	//
	// Unlike [SCMAdapter.FetchPendingReviews], selection does not require
	// any particular review state. Comments marked as outdated by the
	// platform are included with Outdated=true; the caller is responsible
	// for filtering.
	//
	// Returns an empty non-nil slice when no bot review comments exist.
	// Returns a [*SCMError] on failure.
	FetchBotReviewComments(ctx context.Context, prNumber int, owner, repo string, botUsernames []string) ([]ReviewComment, error)

	// GetReviewDecision returns the aggregated review decision for the
	// given PR. Returns a [*SCMError] on failure.
	GetReviewDecision(ctx context.Context, prNumber int, owner, repo string) (ReviewDecision, error)

	// GetCIStatus returns the aggregated CI conclusion string for the
	// PR head ref. Empty string means no required checks exist on the
	// PR. Returns a [*SCMError] on failure.
	GetCIStatus(ctx context.Context, prNumber int, owner, repo string) (string, error)

	// GetMergeability returns the merge precondition state for the
	// given PR. The ReviewDecision and CIConclusion fields are left
	// unset; callers obtain those from the dedicated reads.
	//
	// The platform may need additional time to compute mergeability
	// after a recent push; callers must treat [MergeabilityUnknown]
	// as a deferral condition. Returns a [*SCMError] on failure.
	GetMergeability(ctx context.Context, prNumber int, owner, repo string) (PRMergeStatus, error)

	// MergePR merges the given PR with the requested strategy. Empty
	// commitTitle and commitMessage defer to the provider's
	// strategy-specific defaults. expectedHeadSHA, when non-empty, is
	// sent as the precondition SHA so the provider rejects the call
	// when the head has moved between the precondition check and the
	// merge request.
	//
	// Every merge-endpoint rejection that the provider signals with
	// HTTP 405 (method not allowed) or HTTP 409 (conflict) is returned
	// as a [*SCMError] with Kind [ErrSCMConflict]. This single kind
	// collapses three distinct semantic conditions that the caller
	// must disambiguate before dispatching the outcome:
	//
	//   - the PR was already merged between the precondition check
	//     and this call, by a prior Sortie attempt or any external
	//     actor;
	//   - the head SHA has moved since the precondition check, so the
	//     provider refused the precondition;
	//   - branch protection rejected the merge for a reason other
	//     than the precondition.
	//
	// Callers disambiguate the already-merged subcase by inspecting
	// the [SCMError] Message for the substring "already merged"
	// (case-insensitive); implementations must surface that phrase in
	// Message when they have confirmed, by re-reading the pull request,
	// that the pull request is merged, rather than by matching the
	// provider's response wording. Without that marker the caller
	// treats the conflict as a transient precondition failure and
	// reattempts after re-reading the merge state.
	//
	// Returns a [*SCMError] on any other failure.
	MergePR(ctx context.Context, prNumber int, owner, repo string, strategy MergeStrategy, commitTitle, commitMessage, expectedHeadSHA string) (MergeResult, error)

	// DeleteBranch deletes the branch reference. Returns nil on
	// success. Returns a [*SCMError] with Kind [ErrSCMNotFound] when
	// the branch is already gone; callers treat this as a successful
	// no-op. Returns a [*SCMError] on any other failure.
	DeleteBranch(ctx context.Context, owner, repo, branch string) error

	// ListLabelEvents returns the label-event journal for the given PR,
	// oldest first. Returns an empty non-nil slice when the PR has no
	// label events. Returns a [*SCMError] on failure.
	//
	// An entry whose timestamp is absent or is not RFC 3339 MUST fail the
	// read with a [*SCMError] of Kind [ErrSCMPayload]; an implementation
	// MUST NOT substitute the zero time or any other synthesized value.
	// The timestamp is parsed only for entries the implementation
	// retains, so an entry skipped for its kind or its missing label
	// name cannot fail the read.
	ListLabelEvents(ctx context.Context, prNumber int, owner, repo string) ([]LabelEvent, error)

	// RemoveLabel removes the named label from the given PR. Returns nil
	// on success and treats an already-absent label as a successful
	// no-op: implementations map a [*SCMError] with Kind [ErrSCMNotFound]
	// to nil rather than returning it. Returns a [*SCMError] on any
	// other failure.
	RemoveLabel(ctx context.Context, prNumber int, owner, repo, label string) error
}

// PullRequest identifies a pull request (a merge request on GitLab)
// that [PullRequestOpener.EnsurePullRequest] found or created.
type PullRequest struct {
	// Number is the repository-scoped pull request number.
	Number int

	// URL is the pull request's web page.
	URL string

	// Created reports whether the call created the pull request rather
	// than finding an open one.
	Created bool
}

// PullRequestCommentReader is an optional capability of an [SCMAdapter]:
// an adapter that implements it reads the conversation comments of a pull
// request, the ones left on the pull request itself rather than in a
// review. Implementations must be safe for concurrent use.
type PullRequestCommentReader interface {
	// FetchPullRequestComments returns the conversation comments of the
	// given PR, oldest first. An implementation excludes an author only
	// when its platform reports that author as an automated bot account.
	// Each comment's ID is distinct from the IDs of the adapter's review
	// comments, FilePath is empty, and the lines are zero.
	//
	// Returns an empty non-nil slice when the PR has no such comments.
	// Returns a [*SCMError] on failure.
	FetchPullRequestComments(ctx context.Context, prNumber int, owner, repo string) ([]ReviewComment, error)
}

// PullRequestOpener is an optional capability of an [SCMAdapter]: an
// adapter that implements it can open a pull request for a branch.
// Implementations must be safe for concurrent use.
type PullRequestOpener interface {
	// EnsurePullRequest returns the open pull request in owner/repo
	// whose head is the branch head of that same repository, whatever
	// its base, leaving its title and body unchanged. When there is
	// none it creates one from head to base with title and body and
	// reports it as created. A create that loses a race with another
	// creating the same pull request returns the one that won.
	//
	// A permission or scope failure is a [*SCMError] of Kind
	// [ErrSCMAuth] whose Message names the scope the provider
	// requires. Any other failure is a [*SCMError] of its own Kind.
	EnsurePullRequest(ctx context.Context, owner, repo, head, base, title, body string) (PullRequest, error)
}

// PullRequestFinder is an optional capability of an [SCMAdapter]: an
// adapter that implements it finds the open pull request of a branch
// without opening one. Implementations must be safe for concurrent use.
type PullRequestFinder interface {
	// FindOpenPullRequest returns the open pull request in owner/repo
	// whose head is the branch head of that same repository, whatever
	// its base, and false when there is none. Returns a [*SCMError] on
	// failure.
	FindOpenPullRequest(ctx context.Context, owner, repo, head string) (PullRequest, bool, error)
}

// PullRequestDrafter is an optional capability of an [SCMAdapter]: an
// adapter that implements it can turn an open pull request back into a
// draft. Implementations must be safe for concurrent use.
type PullRequestDrafter interface {
	// MarkPullRequestDraft makes the given open PR a draft. A PR that is
	// already a draft succeeds and changes nothing. Returns a [*SCMError]
	// on failure.
	MarkPullRequestDraft(ctx context.Context, prNumber int, owner, repo string) error
}

// PullRequestReverter is an optional capability of an [SCMAdapter]: an
// adapter that implements it opens a pull request that reverts a merged
// pull request's merge commit. Implementations must be safe for
// concurrent use.
type PullRequestReverter interface {
	// RevertPullRequest opens a pull request against the merged PR's
	// base that reverts its merge, with title and body, and returns it.
	// Returns a [*SCMError] on failure.
	RevertPullRequest(ctx context.Context, prNumber int, owner, repo, title, body string) (PullRequest, error)
}

// CommitComparer is an optional capability of an [SCMAdapter]: an adapter
// that implements it says whether one commit holds another.
// Implementations must be safe for concurrent use.
type CommitComparer interface {
	// CommitContains reports whether ancestor is commit itself or one of
	// its ancestors in owner/repo. A commit the forge does not know is a
	// [*SCMError] of Kind [ErrSCMNotFound].
	CommitContains(ctx context.Context, owner, repo, commit, ancestor string) (bool, error)
}

// MergedRevert is a merged pull request that reverts earlier work, as
// [RevertFinder] reports it.
type MergedRevert struct {
	// Number is the reverting pull request's number.
	Number int

	// MergedAt is when it merged.
	MergedAt time.Time

	// MergeCommitSHA is the commit its merge made, empty when the forge
	// does not say.
	MergeCommitSHA string

	// Commits are the commits it says it reverts ("This reverts commit
	// <sha>"), lower-cased, possibly abbreviated.
	Commits []string

	// PullRequests are the pull requests of the same repository it says
	// it reverts ("Reverts owner/repo#<n>").
	PullRequests []int
}

// RevertFinder is an optional capability of an [SCMAdapter]: an adapter
// that implements it lists the merged pull requests of a repository that
// revert earlier work, whoever merged them. Implementations must be safe
// for concurrent use.
type RevertFinder interface {
	// FindMergedReverts returns the pull requests of owner/repo merged at
	// or after since that revert a commit or a pull request, newest
	// first. Returns a [*SCMError] on failure.
	FindMergedReverts(ctx context.Context, owner, repo string, since time.Time) ([]MergedRevert, error)
}

// SCMTokenReport is what a forge says of the token an [SCMAdapter] calls
// it with.
type SCMTokenReport struct {
	// Scopes lists the scopes the token is granted, nil when the forge
	// does not say, as for a token whose permissions it does not list.
	Scopes []string

	// ExpiresAt is when the token expires; zero when it does not or the
	// forge does not say.
	ExpiresAt time.Time
}

// SCMTokenInspector is an optional capability of an [SCMAdapter]: an
// adapter that implements it reports what the forge says of its token,
// and which scopes a change needs. Implementations must be safe for
// concurrent use.
type SCMTokenInspector interface {
	// InspectToken asks the forge about the token. A token the forge
	// refuses is a [*SCMError] of Kind [ErrSCMAuth].
	InspectToken(ctx context.Context) (SCMTokenReport, error)

	// ChangeScopes returns the scopes, as [SCMTokenReport.Scopes] names
	// them, that a push of a change touching paths needs beyond write
	// access to the repository's contents, sorted; nil when it needs
	// none. A path is relative to the repository root, with slashes.
	ChangeScopes(paths []string) []string
}

// SCMErrorKind enumerates the normalized error categories that SCM
// adapter implementations map their native errors to. The orchestrator
// uses these categories for logging and observability; all SCM errors
// are non-fatal.
type SCMErrorKind string

const (
	// ErrSCMTransport indicates a network or transport failure
	// (connection error, DNS failure, TLS handshake failure).
	ErrSCMTransport SCMErrorKind = "scm_transport_error"

	// ErrSCMAuth indicates an authentication or authorization failure
	// (expired token, insufficient scopes).
	ErrSCMAuth SCMErrorKind = "scm_auth_error"

	// ErrSCMAPI indicates a non-success HTTP status or API-level error
	// (rate limiting, server error, unexpected status code).
	ErrSCMAPI SCMErrorKind = "scm_api_error"

	// ErrSCMNotFound indicates the requested resource does not exist
	// on the SCM platform (HTTP 404).
	ErrSCMNotFound SCMErrorKind = "scm_not_found"

	// ErrSCMPayload indicates a malformed or unexpected response
	// structure from the SCM platform.
	ErrSCMPayload SCMErrorKind = "scm_payload_error"

	// ErrSCMConflict indicates the merge endpoint rejected the call
	// with HTTP 405 (method not allowed) or HTTP 409 (conflict). The
	// kind collapses every such rejection into one category; callers
	// disambiguate the underlying subcase by inspecting the [SCMError]
	// Message rather than the Kind alone.
	//
	// The already-merged subcase is signaled by the substring
	// "already merged" (case-insensitive) in Message and is dispatched
	// as success. Every other ErrSCMConflict is treated as a transient
	// precondition failure (head SHA drift, branch protection refusal)
	// and reattempted after re-reading the merge state. See [MergePR]
	// for the full disambiguation contract that implementations honor.
	ErrSCMConflict SCMErrorKind = "scm_conflict_error"
)

// SCMError is a structured error returned by [SCMAdapter]
// implementations. The Kind field enables structured logging and
// observability without inspecting error messages.
type SCMError struct {
	// Kind is the normalized error category.
	Kind SCMErrorKind

	// Message is an operator-friendly description of the failure.
	Message string

	// Err is the underlying error, if any.
	Err error
}

// Error returns a human-readable diagnostic including the error
// category and message.
func (e *SCMError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("scm: %s: %s: %v", e.Kind, e.Message, e.Err)
	}
	return fmt.Sprintf("scm: %s: %s", e.Kind, e.Message)
}

// Unwrap returns the underlying error for use with [errors.Is] and
// [errors.As].
func (e *SCMError) Unwrap() error {
	return e.Err
}
