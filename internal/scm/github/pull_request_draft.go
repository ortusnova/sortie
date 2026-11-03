package github

import (
	"context"

	"github.com/sortie-ai/sortie/internal/domain"
)

var _ domain.PullRequestDrafter = (*GitHubSCMAdapter)(nil)

// pullRequestNodeQuery reads the node id and draft flag of a pull request,
// which the draft mutation needs.
const pullRequestNodeQuery = `query($owner: String!, $repo: String!, $number: Int!) {
  repository(owner: $owner, name: $repo) {
    pullRequest(number: $number) {
      id
      isDraft
    }
  }
}`

// convertToDraftMutation turns an open pull request into a draft. GitHub
// offers it only through GraphQL.
const convertToDraftMutation = `mutation($id: ID!) {
  convertPullRequestToDraft(input: {pullRequestId: $id}) {
    pullRequest {
      isDraft
    }
  }
}`

type pullRequestNodeData struct {
	Repository struct {
		PullRequest *struct {
			ID      string `json:"id"`
			IsDraft bool   `json:"isDraft"`
		} `json:"pullRequest"`
	} `json:"repository"`
}

type convertToDraftData struct {
	ConvertPullRequestToDraft *struct {
		PullRequest struct {
			IsDraft bool `json:"isDraft"`
		} `json:"pullRequest"`
	} `json:"convertPullRequestToDraft"`
}

// MarkPullRequestDraft makes the given open PR a draft. A PR that is
// already a draft is left as it is.
func (a *GitHubSCMAdapter) MarkPullRequestDraft(ctx context.Context, prNumber int, owner, repo string) error {
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
	if pr.IsDraft {
		return nil
	}
	converted := graphqlResponseEnvelope[convertToDraftData]{}
	if err := a.postGraphQL(ctx, convertToDraftMutation, map[string]any{"id": pr.ID}, &converted); err != nil {
		return err
	}
	if c := converted.Data.ConvertPullRequestToDraft; c == nil || !c.PullRequest.IsDraft {
		return &domain.SCMError{Kind: domain.ErrSCMPayload, Message: "graphql convertPullRequestToDraft did not report a draft"}
	}
	return nil
}
