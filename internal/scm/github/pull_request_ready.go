package github

import (
	"context"

	"github.com/sortie-ai/sortie/internal/domain"
)

var _ domain.PullRequestReadier = (*GitHubSCMAdapter)(nil)

// markReadyForReviewMutation marks a draft pull request ready for review.
// GitHub offers it only through GraphQL.
const markReadyForReviewMutation = `mutation($id: ID!) {
  markPullRequestReadyForReview(input: {pullRequestId: $id}) {
    pullRequest {
      isDraft
    }
  }
}`

type markReadyForReviewData struct {
	MarkPullRequestReadyForReview *struct {
		PullRequest struct {
			IsDraft bool `json:"isDraft"`
		} `json:"pullRequest"`
	} `json:"markPullRequestReadyForReview"`
}

// MarkPullRequestReadyForReview marks the given draft PR ready for
// review. A PR that is not a draft is left as it is.
func (a *GitHubSCMAdapter) MarkPullRequestReadyForReview(ctx context.Context, prNumber int, owner, repo string) error {
	node := graphqlResponseEnvelope[pullRequestNodeData]{}
	if err := a.postGraphQL(ctx, pullRequestNodeQuery, map[string]any{
		"owner": owner, "repo": repo, "number": prNumber,
	}, &node); err != nil {
		return err
	}
	pr := node.Data.Repository.PullRequest
	if pr == nil || pr.ID == "" {
		return &domain.SCMError{Kind: domain.ErrSCMPayload, Message: "graphql response missing pullRequest payload"}
	}
	if !pr.IsDraft {
		return nil
	}
	marked := graphqlResponseEnvelope[markReadyForReviewData]{}
	if err := a.postGraphQL(ctx, markReadyForReviewMutation, map[string]any{"id": pr.ID}, &marked); err != nil {
		return err
	}
	if m := marked.Data.MarkPullRequestReadyForReview; m == nil || m.PullRequest.IsDraft {
		return &domain.SCMError{Kind: domain.ErrSCMPayload, Message: "graphql markPullRequestReadyForReview did not report the pull request ready"}
	}
	return nil
}
