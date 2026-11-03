package github

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"strings"

	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/httpkit"
	"github.com/sortie-ai/sortie/internal/scm/scmcore"
)

var _ domain.PullRequestCommentReader = (*GitHubSCMAdapter)(nil)

// issueCommentIDPrefix starts the ID of a conversation comment, so it can
// never equal the ID of a review comment, which GitHub numbers apart.
const issueCommentIDPrefix = "issuecomment-"

type githubIssueComment struct {
	ID   int64  `json:"id"`
	Body string `json:"body"`
	User struct {
		Login string `json:"login"`
		Type  string `json:"type"`
	} `json:"user"`
	CreatedAt string `json:"created_at"`
}

// FetchPullRequestComments returns the conversation comments of the given
// PR, oldest first, leaving out authors GitHub reports as bots and empty
// bodies. A conversation comment is an issue comment on GitHub.
//
// SubmittedAt is parsed via [scmcore.ParseTimestampOrZero]: it drives
// debounce only, so a malformed timestamp does not fail the read.
func (a *GitHubSCMAdapter) FetchPullRequestComments(ctx context.Context, prNumber int, owner, repo string) ([]domain.ReviewComment, error) {
	path := fmt.Sprintf("/repos/%s/%s/issues/%d/comments", url.PathEscape(owner), url.PathEscape(repo), prNumber)
	params := url.Values{"per_page": {"100"}}

	paginator := httpkit.NewLinkPaginator(a.client, path, params, func(body []byte) ([]githubIssueComment, error) {
		var batch []githubIssueComment
		if err := json.Unmarshal(body, &batch); err != nil {
			return nil, &domain.SCMError{
				Kind:    domain.ErrSCMPayload,
				Message: "failed to parse pull request comments response",
				Err:     err,
			}
		}
		return batch, nil
	}, httpkit.PaginatorOptions{
		MaxPages: maxReviewPages,
		OnLimitReached: func(limit int) {
			slog.WarnContext(ctx, "pull request comments response truncated at page limit",
				slog.String("owner", owner),
				slog.String("repo", repo),
				slog.Int("pr_number", prNumber),
				slog.Int("max_pages", limit))
		},
	})

	all, err := paginator.All(ctx)
	if err != nil {
		return nil, scmcore.AsSCMError(err)
	}

	result := make([]domain.ReviewComment, 0, len(all))
	for _, c := range all {
		body := strings.TrimSpace(c.Body)
		if body == "" || strings.EqualFold(c.User.Type, "Bot") {
			continue
		}
		result = append(result, domain.ReviewComment{
			ID:          fmt.Sprintf("%s%d", issueCommentIDPrefix, c.ID),
			Reviewer:    c.User.Login,
			Body:        body,
			SubmittedAt: scmcore.ParseTimestampOrZero(c.CreatedAt),
		})
	}
	return result, nil
}
