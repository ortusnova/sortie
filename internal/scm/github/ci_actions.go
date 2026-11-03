package github

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/httpkit"
)

// checkRunsRefusalTTL is how long a repository whose token may not read
// check runs is read through the Actions API before check runs are tried
// again.
const checkRunsRefusalTTL = time.Hour

// actionsAppSlug is the app slug GitHub gives the check runs of Actions
// jobs. A job read through the Actions API carries it too, so the log
// excerpt treats both the same.
const actionsAppSlug = "github-actions"

// checkRunsRefusals remembers, per repository, that the token was refused
// the check-runs endpoint. The zero value is ready to use and safe for
// concurrent use.
type checkRunsRefusals struct {
	now func() time.Time

	mu     sync.Mutex
	until  map[string]time.Time
	warned map[string]bool
}

func (r *checkRunsRefusals) clock() time.Time {
	if r.now != nil {
		return r.now()
	}
	return time.Now()
}

// refused reports whether repo's check runs were refused less than
// checkRunsRefusalTTL ago.
func (r *checkRunsRefusals) refused(repo string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	until, ok := r.until[repo]
	return ok && r.clock().Before(until)
}

// record remembers the refusal of repo's check runs and logs the first one
// at warn level.
func (r *checkRunsRefusals) record(ctx context.Context, repo string, cause error) {
	r.mu.Lock()
	if r.until == nil {
		r.until = make(map[string]time.Time)
		r.warned = make(map[string]bool)
	}
	r.until[repo] = r.clock().Add(checkRunsRefusalTTL)
	first := !r.warned[repo]
	r.warned[repo] = true
	r.mu.Unlock()

	level := slog.LevelDebug
	if first {
		level = slog.LevelWarn
	}
	slog.Log(ctx, level, "github token may not read check runs, reading ci through the actions api",
		slog.String("repository", repo),
		slog.Duration("retry_check_runs_after", checkRunsRefusalTTL),
		slog.String("hint", "fine-grained tokens cannot read check runs; grant Actions: Read-only"),
		slog.Any("error", cause))
}

// isCheckRunsRefusal reports whether err is GitHub refusing the token the
// check-runs endpoint: a 403 that is not a rate limit.
func isCheckRunsRefusal(err error) bool {
	var te *domain.TrackerError
	return errors.As(err, &te) && te.Kind == domain.ErrTrackerAuth && te.Status == http.StatusForbidden
}

// readCheckRuns returns the check runs on ref from checks, the check-runs
// endpoint. When the token is refused that endpoint, it reads the GitHub
// Actions jobs for ref instead and remembers the refusal for repo.
func readCheckRuns(ctx context.Context, client *httpkit.Client, refusals *checkRunsRefusals, owner, repo, ref string, checks func() ([]githubCheckRun, error)) ([]githubCheckRun, error) {
	key := owner + "/" + repo
	if !refusals.refused(key) {
		runs, err := checks()
		if err == nil || !isCheckRunsRefusal(err) {
			return runs, err
		}
		refusals.record(ctx, key, err)
	}
	return fetchActionsCheckRuns(ctx, client, owner, repo, ref)
}

type workflowRunsResponse struct {
	WorkflowRuns []workflowRun `json:"workflow_runs"`
}

type workflowRun struct {
	ID         int64  `json:"id"`
	WorkflowID int64  `json:"workflow_id"`
	Event      string `json:"event"`
}

type workflowJobsResponse struct {
	Jobs []workflowJob `json:"jobs"`
}

type workflowJob struct {
	ID         int64   `json:"id"`
	Name       string  `json:"name"`
	Status     string  `json:"status"`
	Conclusion *string `json:"conclusion"`
	HTMLURL    string  `json:"html_url"`
}

// fetchActionsCheckRuns reads the GitHub Actions jobs for the commit sha as
// the check runs GitHub would list for them. Only the newest workflow run
// per workflow and event counts, and only the latest attempt of its jobs,
// as the check-runs endpoint lists only the latest check runs.
func fetchActionsCheckRuns(ctx context.Context, client *httpkit.Client, owner, repo, sha string) ([]githubCheckRun, error) {
	runsPath := fmt.Sprintf("/repos/%s/%s/actions/runs", url.PathEscape(owner), url.PathEscape(repo))
	runs, err := httpkit.NewLinkPaginator(client, runsPath,
		url.Values{"head_sha": {sha}, "per_page": {"100"}},
		func(body []byte) ([]workflowRun, error) {
			var resp workflowRunsResponse
			if err := json.Unmarshal(body, &resp); err != nil {
				return nil, &domain.TrackerError{
					Kind:    domain.ErrTrackerPayload,
					Message: "failed to parse workflow runs response",
					Err:     err,
				}
			}
			return resp.WorkflowRuns, nil
		}, actionsPageOptions(ctx, runsPath)).All(ctx)
	if err != nil {
		return nil, err
	}

	checkRuns := make([]githubCheckRun, 0)
	for _, run := range latestWorkflowRuns(runs) {
		jobsPath := fmt.Sprintf("/repos/%s/%s/actions/runs/%d/jobs", url.PathEscape(owner), url.PathEscape(repo), run.ID)
		jobs, err := httpkit.NewLinkPaginator(client, jobsPath,
			url.Values{"filter": {"latest"}, "per_page": {"100"}},
			func(body []byte) ([]workflowJob, error) {
				var resp workflowJobsResponse
				if err := json.Unmarshal(body, &resp); err != nil {
					return nil, &domain.TrackerError{
						Kind:    domain.ErrTrackerPayload,
						Message: "failed to parse workflow jobs response",
						Err:     err,
					}
				}
				return resp.Jobs, nil
			}, actionsPageOptions(ctx, jobsPath)).All(ctx)
		if err != nil {
			return nil, err
		}
		for _, job := range jobs {
			checkRuns = append(checkRuns, githubCheckRun{
				ID:         job.ID,
				Name:       job.Name,
				Status:     job.Status,
				Conclusion: job.Conclusion,
				HTMLURL:    job.HTMLURL,
				App:        &checkRunApp{Slug: actionsAppSlug},
			})
		}
	}
	return checkRuns, nil
}

// latestWorkflowRuns keeps the newest run of each workflow and event, in
// the order they were listed. A later run of the same workflow on the same
// commit and event supersedes the earlier one, as its check suite does.
func latestWorkflowRuns(runs []workflowRun) []workflowRun {
	type key struct {
		workflow int64
		event    string
	}
	newest := make(map[key]int64, len(runs))
	for _, run := range runs {
		k := key{run.WorkflowID, run.Event}
		if run.ID > newest[k] {
			newest[k] = run.ID
		}
	}
	kept := make([]workflowRun, 0, len(newest))
	for _, run := range runs {
		k := key{run.WorkflowID, run.Event}
		if newest[k] == run.ID {
			kept = append(kept, run)
			newest[k] = 0
		}
	}
	return kept
}

func actionsPageOptions(ctx context.Context, path string) httpkit.PaginatorOptions {
	return httpkit.PaginatorOptions{
		MaxPages: maxCIPages,
		OnLimitReached: func(limit int) {
			slog.WarnContext(ctx, "response truncated at page limit",
				slog.String("path", path),
				slog.Int("max_pages", limit))
		},
	}
}
