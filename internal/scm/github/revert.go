package github

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/scm/scmcore"
)

var (
	_ domain.PullRequestReverter = (*GitHubSCMAdapter)(nil)
	_ domain.CommitComparer      = (*GitHubSCMAdapter)(nil)
	_ domain.RevertFinder        = (*GitHubSCMAdapter)(nil)
)

// revertPullRequestMutation opens a pull request that reverts a merged
// one. GitHub offers it only through GraphQL.
const revertPullRequestMutation = `mutation($id: ID!, $title: String, $body: String) {
  revertPullRequest(input: {pullRequestId: $id, title: $title, body: $body}) {
    revertPullRequest {
      number
      url
    }
  }
}`

type revertPullRequestData struct {
	RevertPullRequest *struct {
		RevertPullRequest *struct {
			Number int    `json:"number"`
			URL    string `json:"url"`
		} `json:"revertPullRequest"`
	} `json:"revertPullRequest"`
}

// RevertPullRequest opens a pull request that reverts the merged PR with
// GitHub's revertPullRequest mutation, which makes the revert branch and
// commit itself.
func (a *GitHubSCMAdapter) RevertPullRequest(ctx context.Context, prNumber int, owner, repo, title, body string) (domain.PullRequest, error) {
	node := graphqlResponseEnvelope[pullRequestNodeData]{}
	if err := a.postGraphQL(ctx, pullRequestNodeQuery, map[string]any{
		"owner": owner, "repo": repo, "number": prNumber,
	}, &node); err != nil {
		return domain.PullRequest{}, err
	}
	pr := node.Data.Repository.PullRequest
	if pr == nil || pr.ID == "" {
		return domain.PullRequest{}, &domain.SCMError{Kind: domain.ErrSCMPayload, Message: "graphql response missing pullRequest payload"}
	}
	reverted := graphqlResponseEnvelope[revertPullRequestData]{}
	if err := a.postGraphQL(ctx, revertPullRequestMutation, map[string]any{
		"id": pr.ID, "title": title, "body": body,
	}, &reverted); err != nil {
		return domain.PullRequest{}, err
	}
	r := reverted.Data.RevertPullRequest
	if r == nil || r.RevertPullRequest == nil || r.RevertPullRequest.Number <= 0 {
		return domain.PullRequest{}, &domain.SCMError{Kind: domain.ErrSCMPayload, Message: "graphql revertPullRequest returned no pull request"}
	}
	return domain.PullRequest{Number: r.RevertPullRequest.Number, URL: r.RevertPullRequest.URL, Created: true}, nil
}

// compareResponse is the subset of GET /repos/{owner}/{repo}/compare.
type compareResponse struct {
	Status string `json:"status"`
}

// CommitContains reads GET /repos/{owner}/{repo}/compare/{ancestor}...{commit}:
// commit holds ancestor when it is identical to it or ahead of it.
func (a *GitHubSCMAdapter) CommitContains(ctx context.Context, owner, repo, commit, ancestor string) (bool, error) {
	path := fmt.Sprintf("/repos/%s/%s/compare/%s...%s", url.PathEscape(owner), url.PathEscape(repo),
		url.PathEscape(ancestor), url.PathEscape(commit))
	body, _, err := a.client.Get(ctx, path, url.Values{"per_page": {"1"}})
	if err != nil {
		return false, scmcore.ToSCMError(err)
	}
	var cmp compareResponse
	if err := json.Unmarshal(body, &cmp); err != nil {
		return false, &domain.SCMError{Kind: domain.ErrSCMPayload, Message: "failed to parse compare response", Err: err}
	}
	switch cmp.Status {
	case "identical", "ahead":
		return true, nil
	case "behind", "diverged":
		return false, nil
	}
	return false, &domain.SCMError{Kind: domain.ErrSCMPayload, Message: fmt.Sprintf("compare status %q is not identical, ahead, behind or diverged", cmp.Status)}
}

// maxRevertListPages bounds the pages of closed pull requests one
// FindMergedReverts call reads.
const maxRevertListPages = 10

type closedPullRequest struct {
	Number         int     `json:"number"`
	Title          string  `json:"title"`
	Body           *string `json:"body"`
	MergedAt       *string `json:"merged_at"`
	UpdatedAt      string  `json:"updated_at"`
	MergeCommitSHA *string `json:"merge_commit_sha"`
}

type pullRequestCommit struct {
	Commit struct {
		Message string `json:"message"`
	} `json:"commit"`
}

var (
	revertsCommitPattern = regexp.MustCompile(`(?i)this reverts commit ([0-9a-f]{7,40})`)
	revertsPullPattern   = regexp.MustCompile(`(?i)\breverts\s+([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+)?#(\d+)`)
)

// FindMergedReverts lists the closed pull requests of owner/repo, most
// recently updated first, until one was last updated before since, and
// returns each merged at or after since that says it reverts something:
// GitHub's revert button writes "Reverts owner/repo#N" in the body, and
// git revert writes "This reverts commit <sha>" in each commit message,
// which is read from the pull request's commits when its title or body
// mentions a revert.
func (a *GitHubSCMAdapter) FindMergedReverts(ctx context.Context, owner, repo string, since time.Time) ([]domain.MergedRevert, error) {
	path := fmt.Sprintf("/repos/%s/%s/pulls", url.PathEscape(owner), url.PathEscape(repo))
	var out []domain.MergedRevert
	for page := 1; page <= maxRevertListPages; page++ {
		body, _, err := a.client.Get(ctx, path, url.Values{
			"state": {"closed"}, "sort": {"updated"}, "direction": {"desc"},
			"per_page": {"100"}, "page": {strconv.Itoa(page)},
		})
		if err != nil {
			return nil, scmcore.ToSCMError(err)
		}
		var pulls []closedPullRequest
		if err := json.Unmarshal(body, &pulls); err != nil {
			return nil, &domain.SCMError{Kind: domain.ErrSCMPayload, Message: "failed to parse pull request list response", Err: err}
		}
		older := false
		for _, p := range pulls {
			if updated, err := time.Parse(time.RFC3339, p.UpdatedAt); err == nil && updated.Before(since) {
				older = true
				continue
			}
			if p.MergedAt == nil {
				continue
			}
			merged, err := time.Parse(time.RFC3339, *p.MergedAt)
			if err != nil || merged.Before(since) {
				continue
			}
			r, ok, err := a.revertOf(ctx, owner, repo, p, merged)
			if err != nil {
				return nil, err
			}
			if ok {
				out = append(out, r)
			}
		}
		if older || len(pulls) < 100 {
			break
		}
	}
	return out, nil
}

// revertOf reads what the merged pull request p reverts, and false when it
// reverts nothing.
func (a *GitHubSCMAdapter) revertOf(ctx context.Context, owner, repo string, p closedPullRequest, merged time.Time) (domain.MergedRevert, bool, error) {
	text := p.Title
	if p.Body != nil {
		text += "\n" + *p.Body
	}
	if !strings.Contains(strings.ToLower(text), "revert") {
		return domain.MergedRevert{}, false, nil
	}
	r := domain.MergedRevert{Number: p.Number, MergedAt: merged}
	if p.MergeCommitSHA != nil {
		r.MergeCommitSHA = strings.ToLower(*p.MergeCommitSHA)
	}
	messages := []string{text}
	path := fmt.Sprintf("/repos/%s/%s/pulls/%d/commits", url.PathEscape(owner), url.PathEscape(repo), p.Number)
	body, _, err := a.client.Get(ctx, path, url.Values{"per_page": {"100"}})
	switch {
	case err != nil && httpStatus(err) != http.StatusNotFound:
		return domain.MergedRevert{}, false, scmcore.ToSCMError(err)
	case err == nil:
		var commits []pullRequestCommit
		if err := json.Unmarshal(body, &commits); err != nil {
			return domain.MergedRevert{}, false, &domain.SCMError{Kind: domain.ErrSCMPayload, Message: "failed to parse pull request commits response", Err: err}
		}
		for _, c := range commits {
			messages = append(messages, c.Commit.Message)
		}
	}
	self := strings.ToLower(owner + "/" + repo)
	for _, m := range messages {
		for _, match := range revertsCommitPattern.FindAllStringSubmatch(m, -1) {
			if sha := strings.ToLower(match[1]); !slices.Contains(r.Commits, sha) {
				r.Commits = append(r.Commits, sha)
			}
		}
		for _, match := range revertsPullPattern.FindAllStringSubmatch(m, -1) {
			if match[1] != "" && strings.ToLower(match[1]) != self {
				continue
			}
			if n, err := strconv.Atoi(match[2]); err == nil && n > 0 && n != p.Number && !slices.Contains(r.PullRequests, n) {
				r.PullRequests = append(r.PullRequests, n)
			}
		}
	}
	if len(r.Commits) == 0 && len(r.PullRequests) == 0 {
		return domain.MergedRevert{}, false, nil
	}
	return r, true, nil
}
