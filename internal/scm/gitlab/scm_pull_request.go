package gitlab

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

var _ domain.PullRequestOpener = (*GitLabSCMAdapter)(nil)

var _ domain.PullRequestFinder = (*GitLabSCMAdapter)(nil)

// mergeRequestScopeHint names what a token needs to open a merge
// request.
const mergeRequestScopeHint = "the token needs the api scope and at least the Developer role in the project"

// gitlabMergeRequestSummary is the subset of a GitLab merge request
// that identifies it and its source.
type gitlabMergeRequestSummary struct {
	IID             int    `json:"iid"`
	WebURL          string `json:"web_url"`
	SourceBranch    string `json:"source_branch"`
	SourceProjectID int64  `json:"source_project_id"`
	TargetProjectID int64  `json:"target_project_id"`
}

// gitlabCreateMergeRequest is the JSON body of POST
// /projects/{project}/merge_requests.
type gitlabCreateMergeRequest struct {
	SourceBranch string `json:"source_branch"`
	TargetBranch string `json:"target_branch"`
	Title        string `json:"title"`
	Description  string `json:"description"`
}

// EnsurePullRequest returns the open merge request of owner/repo whose
// source is its own branch head, read from GET
// /projects/{project}/merge_requests filtered by source branch, or
// creates one with POST /projects/{project}/merge_requests. A merge
// request from a fork's branch of the same name is never returned.
//
// GitLab rejects a second open merge request for one source branch with
// 409, so a 409 on create re-reads the open merge requests and returns
// the one another caller created, or the original error when there is
// none. An authentication failure maps to [domain.ErrSCMAuth] naming the
// api scope.
func (a *GitLabSCMAdapter) EnsurePullRequest(ctx context.Context, owner, repo, head, base, title, body string) (domain.PullRequest, error) {
	found, ok, err := a.findOpenMergeRequest(ctx, owner, repo, head)
	if err != nil {
		return domain.PullRequest{}, err
	}
	if ok {
		return found, nil
	}

	payload, marshalErr := json.Marshal(gitlabCreateMergeRequest{SourceBranch: head, TargetBranch: base, Title: title, Description: body})
	if marshalErr != nil {
		return domain.PullRequest{}, &domain.SCMError{
			Kind:    domain.ErrSCMPayload,
			Message: "failed to encode merge request body",
			Err:     marshalErr,
		}
	}
	path := "/projects/" + projectPath(owner, repo) + "/merge_requests"
	respBody, sendErr := a.client.Send(ctx, http.MethodPost, path, bytes.NewReader(payload))
	if sendErr != nil {
		scm := scmcore.AsSCMError(sendErr)
		if te, ok := errors.AsType[*domain.TrackerError](sendErr); ok && te.Status == http.StatusConflict {
			if raced, ok, err := a.findOpenMergeRequest(ctx, owner, repo, head); err == nil && ok {
				return raced, nil
			}
		}
		if scm.Kind == domain.ErrSCMAuth {
			return domain.PullRequest{}, mergeRequestAuthError(owner, repo, scm)
		}
		return domain.PullRequest{}, scm
	}

	var created gitlabMergeRequestSummary
	if jsonErr := json.Unmarshal(respBody, &created); jsonErr != nil || created.IID <= 0 {
		return domain.PullRequest{}, &domain.SCMError{
			Kind:    domain.ErrSCMPayload,
			Message: "failed to parse created merge request response",
			Err:     jsonErr,
		}
	}
	return domain.PullRequest{Number: created.IID, URL: created.WebURL, Created: true}, nil
}

// FindOpenPullRequest returns the open merge request of owner/repo whose
// head is its own branch head, and false when there is none.
func (a *GitLabSCMAdapter) FindOpenPullRequest(ctx context.Context, owner, repo, head string) (domain.PullRequest, bool, error) {
	return a.findOpenMergeRequest(ctx, owner, repo, head)
}

// findOpenMergeRequest returns the open merge request of owner/repo
// whose source is its own branch head, and false when there is none.
func (a *GitLabSCMAdapter) findOpenMergeRequest(ctx context.Context, owner, repo, head string) (domain.PullRequest, bool, error) {
	path := "/projects/" + projectPath(owner, repo) + "/merge_requests"
	params := url.Values{"state": {"opened"}, "source_branch": {head}}
	mrs, err := paginateSCM(ctx, a.client, a.log, path, params, func(body []byte) ([]gitlabMergeRequestSummary, error) {
		var page []gitlabMergeRequestSummary
		if jsonErr := json.Unmarshal(body, &page); jsonErr != nil {
			return nil, &domain.SCMError{
				Kind:    domain.ErrSCMPayload,
				Message: "failed to parse merge request list response",
				Err:     jsonErr,
			}
		}
		return page, nil
	})
	if err != nil {
		scm := scmcore.AsSCMError(err)
		if scm.Kind == domain.ErrSCMAuth {
			return domain.PullRequest{}, false, mergeRequestAuthError(owner, repo, scm)
		}
		return domain.PullRequest{}, false, scm
	}
	for _, mr := range mrs {
		if mr.SourceBranch == head && mr.SourceProjectID == mr.TargetProjectID && mr.IID > 0 {
			return domain.PullRequest{Number: mr.IID, URL: mr.WebURL}, true, nil
		}
	}
	return domain.PullRequest{}, false, nil
}

// mergeRequestAuthError returns an [domain.ErrSCMAuth] for a refused
// merge request call that names the scope the token needs.
func mergeRequestAuthError(owner, repo string, scm *domain.SCMError) *domain.SCMError {
	return &domain.SCMError{
		Kind:    domain.ErrSCMAuth,
		Message: fmt.Sprintf("gitlab refused to open a merge request in %s/%s: %s: %s", owner, repo, mergeRequestScopeHint, scm.Message),
		Err:     scm.Err,
	}
}
