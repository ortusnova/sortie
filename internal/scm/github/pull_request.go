package github

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"

	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/scm/scmcore"
)

var _ domain.PullRequestOpener = (*GitHubSCMAdapter)(nil)

var _ domain.PullRequestFinder = (*GitHubSCMAdapter)(nil)

// pullRequestScopeHint names what a token needs to open a pull request.
const pullRequestScopeHint = "the token needs the classic scope repo, or the fine-grained permission Pull requests: write"

// pullRequestSummary is the subset of a GitHub pull request that
// identifies it.
type pullRequestSummary struct {
	Number  int    `json:"number"`
	HTMLURL string `json:"html_url"`
	Head    struct {
		Ref string `json:"ref"`
	} `json:"head"`
}

// createPullRequestBody is the JSON body of POST /repos/{owner}/{repo}/pulls.
type createPullRequestBody struct {
	Title string `json:"title"`
	Head  string `json:"head"`
	Base  string `json:"base"`
	Body  string `json:"body"`
}

// EnsurePullRequest returns the open pull request whose head is the
// branch head of owner/repo, read from GET /repos/{owner}/{repo}/pulls
// filtered by head, or creates it with POST /repos/{owner}/{repo}/pulls.
//
// GitHub rejects a second pull request for the same head and base with
// 422, so a 422 on create re-reads the open pull requests and returns
// the one another caller created, or the original error when there is
// none. A permission 403 or a 404 on create, after the read succeeded,
// means the token can read the repository but not open pull requests
// there; both map to [domain.ErrSCMAuth] naming the scope. A
// rate-limited 403 stays a [domain.ErrSCMAPI].
func (a *GitHubSCMAdapter) EnsurePullRequest(ctx context.Context, owner, repo, head, base, title, body string) (domain.PullRequest, error) {
	found, ok, err := a.findOpenPullRequest(ctx, owner, repo, head)
	if err != nil {
		return domain.PullRequest{}, err
	}
	if ok {
		return found, nil
	}

	payload, marshalErr := json.Marshal(createPullRequestBody{Title: title, Head: head, Base: base, Body: body})
	if marshalErr != nil {
		return domain.PullRequest{}, &domain.SCMError{
			Kind:    domain.ErrSCMPayload,
			Message: "failed to encode pull request body",
			Err:     marshalErr,
		}
	}
	path := fmt.Sprintf("/repos/%s/%s/pulls", url.PathEscape(owner), url.PathEscape(repo))
	respBody, sendErr := a.client.Send(ctx, http.MethodPost, path, bytes.NewReader(payload))
	if sendErr != nil {
		scm := scmcore.ToSCMError(sendErr)
		switch httpStatus(sendErr) {
		case http.StatusUnprocessableEntity:
			if raced, ok, err := a.findOpenPullRequest(ctx, owner, repo, head); err == nil && ok {
				return raced, nil
			}
			return domain.PullRequest{}, scm
		case http.StatusNotFound:
			return domain.PullRequest{}, pullRequestAuthError(owner, repo, scm)
		}
		// A rate-limited 403 is classified as an API error, not an
		// authentication one, and stays what it is.
		if scm.Kind == domain.ErrSCMAuth {
			return domain.PullRequest{}, pullRequestAuthError(owner, repo, scm)
		}
		return domain.PullRequest{}, scm
	}

	var created pullRequestSummary
	if jsonErr := json.Unmarshal(respBody, &created); jsonErr != nil || created.Number <= 0 {
		return domain.PullRequest{}, &domain.SCMError{
			Kind:    domain.ErrSCMPayload,
			Message: "failed to parse created pull request response",
			Err:     jsonErr,
		}
	}
	return domain.PullRequest{Number: created.Number, URL: created.HTMLURL, Created: true}, nil
}

// FindOpenPullRequest returns the open pull request of owner/repo whose
// head is its own branch head, and false when there is none.
func (a *GitHubSCMAdapter) FindOpenPullRequest(ctx context.Context, owner, repo, head string) (domain.PullRequest, bool, error) {
	return a.findOpenPullRequest(ctx, owner, repo, head)
}

// findOpenPullRequest returns the open pull request whose head is the
// branch head of owner/repo, and false when there is none.
func (a *GitHubSCMAdapter) findOpenPullRequest(ctx context.Context, owner, repo, head string) (domain.PullRequest, bool, error) {
	path := fmt.Sprintf("/repos/%s/%s/pulls", url.PathEscape(owner), url.PathEscape(repo))
	params := url.Values{
		"state":    {"open"},
		"head":     {owner + ":" + head},
		"per_page": {"100"},
	}
	respBody, _, err := a.client.Get(ctx, path, params)
	if err != nil {
		scm := scmcore.ToSCMError(err)
		if scm.Kind == domain.ErrSCMAuth {
			return domain.PullRequest{}, false, pullRequestAuthError(owner, repo, scm)
		}
		return domain.PullRequest{}, false, scm
	}
	var pulls []pullRequestSummary
	if jsonErr := json.Unmarshal(respBody, &pulls); jsonErr != nil {
		return domain.PullRequest{}, false, &domain.SCMError{
			Kind:    domain.ErrSCMPayload,
			Message: "failed to parse pull request list response",
			Err:     jsonErr,
		}
	}
	for _, pr := range pulls {
		if pr.Head.Ref == head && pr.Number > 0 {
			return domain.PullRequest{Number: pr.Number, URL: pr.HTMLURL}, true, nil
		}
	}
	return domain.PullRequest{}, false, nil
}

// pullRequestAuthError returns an [domain.ErrSCMAuth] for a refused
// pull request call that names the scope the token needs.
func pullRequestAuthError(owner, repo string, scm *domain.SCMError) *domain.SCMError {
	return &domain.SCMError{
		Kind:    domain.ErrSCMAuth,
		Message: fmt.Sprintf("github refused to open a pull request in %s/%s: %s: %s", owner, repo, pullRequestScopeHint, scm.Message),
		Err:     scm.Err,
	}
}

// httpStatus returns the HTTP status of a classified response error,
// or zero for any other error.
func httpStatus(err error) int {
	if te, ok := errors.AsType[*domain.TrackerError](err); ok {
		return te.Status
	}
	return 0
}
