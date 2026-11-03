package orchestrator

import (
	"context"
	"errors"
	"time"

	"github.com/sortie-ai/sortie/internal/domain"
)

// canceled reports whether err stems from ctx being canceled, which only
// shutdown does; such a run says nothing about the provider or the store.
func canceled(ctx context.Context, err error) bool {
	return err != nil && errors.Is(err, context.Canceled) && ctx.Err() != nil
}

func observeRead(ctx context.Context, rec *healthRecord, err error) {
	if canceled(ctx, err) {
		return
	}
	rec.record(time.Now(), err)
}

// observeSCM wraps inner so every read it serves updates rec; writes pass
// through unrecorded because a provider words a refused write with its own
// status and a refusal that ends the reaction leaves no later read to
// clear the failure.
//
// Every method is written out and neither wrapper embeds the interface, so
// a method added to [domain.SCMAdapter] fails to compile until it is
// classified here. An optional interface the orchestrator asserts on its
// adapter MUST be forwarded by the wrapper as well; otherwise the assertion
// fails silently and the auto-merge preflight retry passes without
// checking the token.
func observeSCM(inner domain.SCMAdapter, rec *healthRecord) domain.SCMAdapter {
	base := observedSCM{inner: inner, rec: rec}
	if verifier, ok := inner.(AutoMergeScopeVerifier); ok {
		return observedVerifyingSCM{observedSCM: base, verifier: verifier}
	}
	return base
}

// observeCI wraps inner so every status read it serves updates rec. The
// wrapper reads another repository through inner when inner can.
func observeCI(inner domain.CIStatusProvider, rec *healthRecord) domain.RepositoryCIStatusProvider {
	return observedCI{inner: inner, rec: rec}
}

type observedSCM struct {
	inner domain.SCMAdapter
	rec   *healthRecord
}

func (a observedSCM) FetchPendingReviews(ctx context.Context, prNumber int, owner, repo string) ([]domain.ReviewComment, error) {
	comments, err := a.inner.FetchPendingReviews(ctx, prNumber, owner, repo)
	observeRead(ctx, a.rec, err)
	return comments, err
}

func (a observedSCM) FetchBotReviewComments(ctx context.Context, prNumber int, owner, repo string, botUsernames []string) ([]domain.ReviewComment, error) {
	comments, err := a.inner.FetchBotReviewComments(ctx, prNumber, owner, repo, botUsernames)
	observeRead(ctx, a.rec, err)
	return comments, err
}

func (a observedSCM) GetReviewDecision(ctx context.Context, prNumber int, owner, repo string) (domain.ReviewDecision, error) {
	decision, err := a.inner.GetReviewDecision(ctx, prNumber, owner, repo)
	observeRead(ctx, a.rec, err)
	return decision, err
}

func (a observedSCM) GetCIStatus(ctx context.Context, prNumber int, owner, repo string) (string, error) {
	status, err := a.inner.GetCIStatus(ctx, prNumber, owner, repo)
	observeRead(ctx, a.rec, err)
	return status, err
}

func (a observedSCM) GetMergeability(ctx context.Context, prNumber int, owner, repo string) (domain.PRMergeStatus, error) {
	mergeability, err := a.inner.GetMergeability(ctx, prNumber, owner, repo)
	observeRead(ctx, a.rec, err)
	return mergeability, err
}

func (a observedSCM) ListLabelEvents(ctx context.Context, prNumber int, owner, repo string) ([]domain.LabelEvent, error) {
	events, err := a.inner.ListLabelEvents(ctx, prNumber, owner, repo)
	observeRead(ctx, a.rec, err)
	return events, err
}

func (a observedSCM) MergePR(ctx context.Context, prNumber int, owner, repo string, strategy domain.MergeStrategy, commitTitle, commitMessage, expectedHeadSHA string) (domain.MergeResult, error) {
	return a.inner.MergePR(ctx, prNumber, owner, repo, strategy, commitTitle, commitMessage, expectedHeadSHA)
}

func (a observedSCM) DeleteBranch(ctx context.Context, owner, repo, branch string) error {
	return a.inner.DeleteBranch(ctx, owner, repo, branch)
}

func (a observedSCM) RemoveLabel(ctx context.Context, prNumber int, owner, repo, label string) error {
	return a.inner.RemoveLabel(ctx, prNumber, owner, repo, label)
}

type observedVerifyingSCM struct {
	observedSCM
	verifier AutoMergeScopeVerifier
}

func (a observedVerifyingSCM) VerifyAutoMergeScopes(ctx context.Context, requireContents bool) ([]string, []string, error) {
	scopes, missing, err := a.verifier.VerifyAutoMergeScopes(ctx, requireContents)
	observeRead(ctx, a.rec, err)
	return scopes, missing, err
}

type observedCI struct {
	inner domain.CIStatusProvider
	rec   *healthRecord
}

func (p observedCI) FetchCIStatus(ctx context.Context, ref string) (domain.CIResult, error) {
	result, err := p.inner.FetchCIStatus(ctx, ref)
	observeRead(ctx, p.rec, err)
	return result, err
}

func (p observedCI) FetchRepositoryCIStatus(ctx context.Context, owner, repo, ref string) (domain.CIResult, error) {
	result, err := domain.FetchPullRequestCIStatus(ctx, p.inner, owner, repo, ref)
	observeRead(ctx, p.rec, err)
	return result, err
}

// observeCommentReader returns the conversation-comment reader of inner,
// wrapped so its reads update rec, or nil when inner has none.
func observeCommentReader(inner domain.SCMAdapter, rec *healthRecord) domain.PullRequestCommentReader {
	reader, ok := inner.(domain.PullRequestCommentReader)
	if !ok {
		return nil
	}
	return observedCommentReader{inner: reader, rec: rec}
}

type observedCommentReader struct {
	inner domain.PullRequestCommentReader
	rec   *healthRecord
}

func (a observedCommentReader) FetchPullRequestComments(ctx context.Context, prNumber int, owner, repo string) ([]domain.ReviewComment, error) {
	comments, err := a.inner.FetchPullRequestComments(ctx, prNumber, owner, repo)
	observeRead(ctx, a.rec, err)
	return comments, err
}
