package github

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sortie-ai/sortie/internal/domain"
)

const fallbackSHA = "abc123"

type fallbackJob struct {
	id         int64
	name       string
	status     string
	conclusion string // empty encodes null
}

type fallbackRun struct {
	id       int64
	workflow int64
	event    string
	jobs     []fallbackJob
}

// fallbackFake serves a repository whose token is refused check runs, or
// not, and whose CI is read through the Actions API.
type fallbackFake struct {
	refuseChecks bool
	rateLimited  bool
	checkRuns    []byte
	combined     string
	jobObjects   map[int64][]byte
	logs         map[int64][]byte

	mu       sync.Mutex
	runs     []fallbackRun
	requests []string
}

func (f *fallbackFake) setRuns(runs ...fallbackRun) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.runs = runs
}

func (f *fallbackFake) requested(path string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	count := 0
	for _, requested := range f.requests {
		if requested == path {
			count++
		}
	}
	return count
}

func (f *fallbackFake) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.requests = append(f.requests, r.URL.Path)
	runs := f.runs
	f.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	const prefix = "/repos/owner/repo"
	path := r.URL.Path
	switch {
	case path == prefix+"/pulls/1":
		writeBody(w, []byte(`{"head":{"sha":"`+fallbackSHA+`","ref":"feature/x"}}`))
	case path == prefix+"/commits/"+fallbackSHA+"/status":
		writeBody(w, []byte(f.combined))
	case path == prefix+"/commits/"+fallbackSHA+"/check-runs":
		switch {
		case f.rateLimited:
			w.Header().Set("X-Ratelimit-Remaining", "0")
			w.WriteHeader(http.StatusForbidden)
			writeBody(w, []byte(`{"message":"API rate limit exceeded"}`))
		case f.refuseChecks:
			w.WriteHeader(http.StatusForbidden)
			writeBody(w, []byte(`{"message":"Resource not accessible by personal access token","status":"403"}`))
		default:
			writeBody(w, f.checkRuns)
		}
	case path == prefix+"/actions/runs":
		listed := []fallbackRun{}
		if r.URL.Query().Get("head_sha") == fallbackSHA {
			listed = runs
		}
		writeJSON(w, map[string]any{"total_count": len(listed), "workflow_runs": runsJSON(listed)})
	case strings.HasPrefix(path, prefix+"/actions/runs/") && strings.HasSuffix(path, "/jobs"):
		id, _ := strconv.ParseInt(strings.TrimSuffix(strings.TrimPrefix(path, prefix+"/actions/runs/"), "/jobs"), 10, 64)
		for _, run := range runs {
			if run.id == id {
				writeJSON(w, map[string]any{"total_count": len(run.jobs), "jobs": jobsJSON(run.jobs)})
				return
			}
		}
		w.WriteHeader(http.StatusNotFound)
	case strings.HasPrefix(path, prefix+"/actions/jobs/"):
		idText, isLog := strings.CutSuffix(strings.TrimPrefix(path, prefix+"/actions/jobs/"), "/logs")
		id, _ := strconv.ParseInt(idText, 10, 64)
		table := f.jobObjects
		if isLog {
			table = f.logs
		}
		body, ok := table[id]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		writeBody(w, body)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func runsJSON(runs []fallbackRun) []map[string]any {
	out := make([]map[string]any, len(runs))
	for i, run := range runs {
		out[i] = map[string]any{"id": run.id, "workflow_id": run.workflow, "event": run.event, "head_sha": fallbackSHA}
	}
	return out
}

func jobsJSON(jobs []fallbackJob) []map[string]any {
	out := make([]map[string]any, len(jobs))
	for i, job := range jobs {
		var conclusion any
		if job.conclusion != "" {
			conclusion = job.conclusion
		}
		out[i] = map[string]any{
			"id":         job.id,
			"name":       job.name,
			"status":     job.status,
			"conclusion": conclusion,
			"html_url":   "https://github.com/owner/repo/actions/runs/1/job/" + strconv.FormatInt(job.id, 10),
		}
	}
	return out
}

func writeJSON(w http.ResponseWriter, v any) {
	body, err := json.Marshal(v)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	writeBody(w, body)
}

func serveFallback(t *testing.T, f *fallbackFake) string {
	t.Helper()
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	return srv.URL
}

func fetchFallback(t *testing.T, p *GitHubCIProvider) domain.CIResult {
	t.Helper()
	result, err := p.FetchCIStatus(t.Context(), fallbackSHA)
	if err != nil {
		t.Fatalf("FetchCIStatus: unexpected error: %v", err)
	}
	return result
}

const (
	checkRunsPath = "/repos/owner/repo/commits/" + fallbackSHA + "/check-runs"
	runsPath      = "/repos/owner/repo/actions/runs"
)

func TestFetchCIStatus_ActionsFallback_Passing(t *testing.T) {
	t.Parallel()

	f := &fallbackFake{refuseChecks: true}
	f.setRuns(fallbackRun{id: 10, workflow: 1, event: "pull_request", jobs: []fallbackJob{
		{id: 101, name: "lint", status: "completed", conclusion: "success"},
		{id: 102, name: "test", status: "completed", conclusion: "success"},
	}})
	p := newTestCIProvider(t, serveFallback(t, f), 50)

	result := fetchFallback(t, p)

	if result.Status != domain.CIStatusPassing {
		t.Errorf("Status = %q, want %q", result.Status, domain.CIStatusPassing)
	}
	if len(result.CheckRuns) != 2 {
		t.Fatalf("len(CheckRuns) = %d, want 2", len(result.CheckRuns))
	}
	want := domain.CheckRun{
		Name:       "lint",
		Status:     domain.CheckRunStatusCompleted,
		Conclusion: domain.CheckConclusionSuccess,
		DetailsURL: "https://github.com/owner/repo/actions/runs/1/job/101",
	}
	if result.CheckRuns[0] != want {
		t.Errorf("CheckRuns[0] = %+v, want %+v", result.CheckRuns[0], want)
	}
}

func TestFetchCIStatus_ActionsFallback_FailingNamesJobWithExcerpt(t *testing.T) {
	t.Parallel()

	f := &fallbackFake{
		refuseChecks: true,
		jobObjects:   map[int64][]byte{2001: loadFixture(t, "job_101566349874_steps.json")},
		logs:         map[int64][]byte{2001: loadFixture(t, "job_101566349874.log")},
	}
	f.setRuns(fallbackRun{id: 10, workflow: 1, event: "pull_request", jobs: []fallbackJob{
		{id: 2002, name: "lint", status: "completed", conclusion: "success"},
		{id: 2001, name: "test", status: "completed", conclusion: "failure"},
	}})
	p := newTestCIProvider(t, serveFallback(t, f), 50)

	result := fetchFallback(t, p)

	if result.Status != domain.CIStatusFailing {
		t.Errorf("Status = %q, want %q", result.Status, domain.CIStatusFailing)
	}
	if result.FailingCount != 1 {
		t.Errorf("FailingCount = %d, want 1", result.FailingCount)
	}
	if got := result.CheckRuns[1]; got.Name != "test" || got.Conclusion != domain.CheckConclusionFailure {
		t.Errorf("CheckRuns[1] = %+v, want the failing job named test", got)
	}
	if first, _, _ := strings.Cut(result.LogExcerpt, "\n"); first != locatedNote("Run tests") {
		t.Errorf("LogExcerpt first line = %q, want %q", first, locatedNote("Run tests"))
	}
	if n := f.requested("/repos/owner/repo/actions/jobs/2001/logs"); n != 1 {
		t.Errorf("failing job log requested %d times, want 1", n)
	}
}

func TestFetchCIStatus_ActionsFallback_Pending(t *testing.T) {
	t.Parallel()

	f := &fallbackFake{refuseChecks: true}
	f.setRuns(fallbackRun{id: 10, workflow: 1, event: "pull_request", jobs: []fallbackJob{
		{id: 101, name: "lint", status: "completed", conclusion: "success"},
		{id: 102, name: "test", status: "queued"},
	}})
	p := newTestCIProvider(t, serveFallback(t, f), 50)

	result := fetchFallback(t, p)

	if result.Status != domain.CIStatusPending {
		t.Errorf("Status = %q, want %q", result.Status, domain.CIStatusPending)
	}
	if result.CheckRuns[1].Status != domain.CheckRunStatusQueued {
		t.Errorf("CheckRuns[1].Status = %q, want %q", result.CheckRuns[1].Status, domain.CheckRunStatusQueued)
	}
}

func TestFetchCIStatus_ActionsFallback_NewestRunPerWorkflowCounts(t *testing.T) {
	t.Parallel()

	f := &fallbackFake{refuseChecks: true}
	f.setRuns(
		fallbackRun{id: 12, workflow: 1, event: "pull_request", jobs: []fallbackJob{
			{id: 121, name: "test", status: "completed", conclusion: "success"},
		}},
		fallbackRun{id: 11, workflow: 1, event: "pull_request", jobs: []fallbackJob{
			{id: 111, name: "test", status: "completed", conclusion: "failure"},
		}},
		fallbackRun{id: 9, workflow: 1, event: "push", jobs: []fallbackJob{
			{id: 91, name: "test", status: "completed", conclusion: "success"},
		}},
	)
	p := newTestCIProvider(t, serveFallback(t, f), 0)

	result := fetchFallback(t, p)

	if result.Status != domain.CIStatusPassing {
		t.Errorf("Status = %q, want %q: a newer run of the same workflow supersedes the failed one", result.Status, domain.CIStatusPassing)
	}
	if len(result.CheckRuns) != 2 {
		t.Errorf("len(CheckRuns) = %d, want 2 (one per workflow and event)", len(result.CheckRuns))
	}
	if n := f.requested(runsPath + "/11/jobs"); n != 0 {
		t.Errorf("superseded run's jobs requested %d times, want 0", n)
	}
}

func TestFetchCIStatus_ActionsFallback_RemembersRefusal(t *testing.T) {
	t.Parallel()

	f := &fallbackFake{refuseChecks: true}
	f.setRuns(fallbackRun{id: 10, workflow: 1, event: "pull_request", jobs: []fallbackJob{
		{id: 101, name: "test", status: "completed", conclusion: "success"},
	}})
	p := newTestCIProvider(t, serveFallback(t, f), 0)
	now := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	p.refusals.now = func() time.Time { return now }

	fetchFallback(t, p)
	fetchFallback(t, p)
	if n := f.requested(checkRunsPath); n != 1 {
		t.Errorf("check runs requested %d times over two polls, want 1", n)
	}
	if n := f.requested(runsPath); n != 2 {
		t.Errorf("workflow runs requested %d times over two polls, want 2", n)
	}

	now = now.Add(checkRunsRefusalTTL)
	fetchFallback(t, p)
	if n := f.requested(checkRunsPath); n != 2 {
		t.Errorf("check runs requested %d times once the refusal expired, want 2", n)
	}
}

func TestFetchCIStatus_NoActionsFallbackWhenCheckRunsWork(t *testing.T) {
	t.Parallel()

	f := &fallbackFake{checkRuns: loadFixture(t, "check_runs_passing.json")}
	p := newTestCIProvider(t, serveFallback(t, f), 50)

	result := fetchFallback(t, p)

	if result.Status != domain.CIStatusPassing {
		t.Errorf("Status = %q, want %q", result.Status, domain.CIStatusPassing)
	}
	if n := f.requested(runsPath); n != 0 {
		t.Errorf("workflow runs requested %d times, want 0", n)
	}
}

func TestFetchCIStatus_NoActionsFallbackOnRateLimit(t *testing.T) {
	t.Parallel()

	f := &fallbackFake{rateLimited: true}
	p := newTestCIProvider(t, serveFallback(t, f), 50)

	_, err := p.FetchCIStatus(t.Context(), fallbackSHA)

	assertCIErrorKind(t, err, domain.ErrCIAPI)
	if n := f.requested(runsPath); n != 0 {
		t.Errorf("workflow runs requested %d times on a rate limit, want 0", n)
	}
}

func TestGetCIStatus_ActionsFallback_WaitsThenSucceeds(t *testing.T) {
	t.Parallel()

	f := &fallbackFake{refuseChecks: true, combined: `{"state":"","statuses":[]}`}
	f.setRuns(fallbackRun{id: 10, workflow: 1, event: "pull_request", jobs: []fallbackJob{
		{id: 101, name: "lint", status: "completed", conclusion: "success"},
		{id: 102, name: "test", status: "in_progress"},
	}})
	a := newTestSCMAdapter(t, serveFallback(t, f))

	status, err := a.GetCIStatus(t.Context(), 1, "owner", "repo")
	if err != nil {
		t.Fatalf("GetCIStatus: %v", err)
	}
	if status != "pending" {
		t.Errorf("GetCIStatus() with a running job = %q, want %q", status, "pending")
	}

	f.setRuns(fallbackRun{id: 10, workflow: 1, event: "pull_request", jobs: []fallbackJob{
		{id: 101, name: "lint", status: "completed", conclusion: "success"},
		{id: 102, name: "test", status: "completed", conclusion: "success"},
	}})
	status, err = a.GetCIStatus(t.Context(), 1, "owner", "repo")
	if err != nil {
		t.Fatalf("GetCIStatus: %v", err)
	}
	if status != "success" {
		t.Errorf("GetCIStatus() once every job passed = %q, want %q", status, "success")
	}
	if n := f.requested(checkRunsPath); n != 1 {
		t.Errorf("check runs requested %d times over two polls, want 1", n)
	}
}

func TestGetCIStatus_ActionsFallback_CombinesCommitStatuses(t *testing.T) {
	t.Parallel()

	f := &fallbackFake{refuseChecks: true, combined: `{"state":"failure","statuses":[{"state":"failure"}]}`}
	f.setRuns(fallbackRun{id: 10, workflow: 1, event: "pull_request", jobs: []fallbackJob{
		{id: 101, name: "test", status: "completed", conclusion: "success"},
	}})
	a := newTestSCMAdapter(t, serveFallback(t, f))

	status, err := a.GetCIStatus(t.Context(), 1, "owner", "repo")
	if err != nil {
		t.Fatalf("GetCIStatus: %v", err)
	}
	if status != "failing" {
		t.Errorf("GetCIStatus() = %q, want %q", status, "failing")
	}
}

func TestGetCIStatus_ActionsFallback_FailingJob(t *testing.T) {
	t.Parallel()

	f := &fallbackFake{refuseChecks: true, combined: `{"state":"","statuses":[]}`}
	f.setRuns(fallbackRun{id: 10, workflow: 1, event: "pull_request", jobs: []fallbackJob{
		{id: 101, name: "test", status: "completed", conclusion: "failure"},
	}})
	a := newTestSCMAdapter(t, serveFallback(t, f))

	status, err := a.GetCIStatus(t.Context(), 1, "owner", "repo")
	if err != nil {
		t.Fatalf("GetCIStatus: %v", err)
	}
	if status != "failing" {
		t.Errorf("GetCIStatus() = %q, want %q", status, "failing")
	}
}

func TestGetCIStatus_NoActionsFallbackWhenCheckRunsWork(t *testing.T) {
	t.Parallel()

	f := &fallbackFake{
		combined:  `{"state":"","statuses":[]}`,
		checkRuns: []byte(`{"total_count":1,"check_runs":[{"status":"completed","conclusion":"success"}]}`),
	}
	a := newTestSCMAdapter(t, serveFallback(t, f))

	status, err := a.GetCIStatus(t.Context(), 1, "owner", "repo")
	if err != nil {
		t.Fatalf("GetCIStatus: %v", err)
	}
	if status != "success" {
		t.Errorf("GetCIStatus() = %q, want %q", status, "success")
	}
	if n := f.requested(runsPath); n != 0 {
		t.Errorf("workflow runs requested %d times, want 0", n)
	}
}
