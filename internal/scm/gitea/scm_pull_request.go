package gitea

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/scm/scmcore"
)

var _ domain.PullRequestOpener = (*GiteaSCMAdapter)(nil)

var _ domain.PullRequestFinder = (*GiteaSCMAdapter)(nil)

// giteaPullRequestSummary is the subset of a Gitea pull request that
// identifies it and its head.
type giteaPullRequestSummary struct {
	Number  int    `json:"number"`
	HTMLURL string `json:"html_url"`
	Head    struct {
		Ref  string `json:"ref"`
		Repo *struct {
			FullName string `json:"full_name"`
		} `json:"repo"`
	} `json:"head"`
}

// giteaCreatePullRequest is the JSON body of POST
// /repos/{owner}/{repo}/pulls.
type giteaCreatePullRequest struct {
	Head  string `json:"head"`
	Base  string `json:"base"`
	Title string `json:"title"`
	Body  string `json:"body"`
}

// EnsurePullRequest returns the open pull request of owner/repo whose
// head is its own branch head, found by reading GET
// /repos/{owner}/{repo}/pulls?state=open, most recently updated first,
// or creates one with POST /repos/{owner}/{repo}/pulls. Gitea's list
// route has no head filter, so the head is matched here; a pull request
// from a fork's branch of the same name is never returned.
//
// Gitea rejects a second open pull request for the same head and base
// with 409, so a 409 on create re-reads the open pull requests and
// returns the one another caller created, or the original error when
// there is none. An authentication failure maps to [domain.ErrSCMAuth]
// naming write:repository.
func (a *GiteaSCMAdapter) EnsurePullRequest(ctx context.Context, owner, repo, head, base, title, body string) (domain.PullRequest, error) {
	found, ok, err := a.findOpenPullRequest(ctx, owner, repo, head)
	if err != nil {
		return domain.PullRequest{}, err
	}
	if ok {
		return found, nil
	}

	payload, marshalErr := json.Marshal(giteaCreatePullRequest{Head: head, Base: base, Title: title, Body: body})
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
		if te, ok := errors.AsType[*domain.TrackerError](sendErr); ok && te.Status == http.StatusConflict {
			if raced, ok, err := a.findOpenPullRequest(ctx, owner, repo, head); err == nil && ok {
				return raced, nil
			}
		}
		if scm.Kind == domain.ErrSCMAuth {
			return domain.PullRequest{}, pullRequestAuthError(owner, repo, scm)
		}
		return domain.PullRequest{}, scm
	}

	var created giteaPullRequestSummary
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
func (a *GiteaSCMAdapter) FindOpenPullRequest(ctx context.Context, owner, repo, head string) (domain.PullRequest, bool, error) {
	return a.findOpenPullRequest(ctx, owner, repo, head)
}

// findOpenPullRequest returns the open pull request of owner/repo whose
// head is its own branch head, and false when there is none.
func (a *GiteaSCMAdapter) findOpenPullRequest(ctx context.Context, owner, repo, head string) (domain.PullRequest, bool, error) {
	path := fmt.Sprintf("/repos/%s/%s/pulls?state=open&sort=recentupdate", url.PathEscape(owner), url.PathEscape(repo))
	pulls, err := paginateSCM(ctx, a.client, path, func(body []byte) ([]giteaPullRequestSummary, error) {
		var page []giteaPullRequestSummary
		if jsonErr := json.Unmarshal(body, &page); jsonErr != nil {
			return nil, &domain.SCMError{
				Kind:    domain.ErrSCMPayload,
				Message: "failed to parse pull request list response",
				Err:     jsonErr,
			}
		}
		return page, nil
	})
	if err != nil {
		scm := scmcore.AsSCMError(err)
		if scm.Kind == domain.ErrSCMAuth {
			return domain.PullRequest{}, false, pullRequestAuthError(owner, repo, scm)
		}
		return domain.PullRequest{}, false, scm
	}
	for _, pr := range pulls {
		if pr.Head.Ref != head || pr.Number <= 0 {
			continue
		}
		if pr.Head.Repo != nil && !strings.EqualFold(pr.Head.Repo.FullName, owner+"/"+repo) {
			continue
		}
		return domain.PullRequest{Number: pr.Number, URL: pr.HTMLURL}, true, nil
	}
	return domain.PullRequest{}, false, nil
}

// pullRequestAuthError returns an [domain.ErrSCMAuth] for a refused
// pull request call that names write:repository, the scope a token
// needs, whatever the rejection said.
func pullRequestAuthError(owner, repo string, scm *domain.SCMError) *domain.SCMError {
	return &domain.SCMError{
		Kind:    domain.ErrSCMAuth,
		Message: fmt.Sprintf("gitea refused to open a pull request in %s/%s: the token needs the %s scope and write access to the repository: %s", owner, repo, writeRepositoryScope, scm.Message),
		Err:     scm.Err,
	}
}
