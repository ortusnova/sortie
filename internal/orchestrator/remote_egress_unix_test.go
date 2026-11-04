//go:build unix

package orchestrator

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/egress"
	"github.com/sortie-ai/sortie/internal/worker"
)

// commitOn commits content as the egress policy file on branch of the
// fixture's origin, branching from main when branch is new, and returns
// the commit.
func (f *gitFixture) commitOn(branch, content string) string {
	path := egress.File
	f.t.Helper()
	work := filepath.Join(f.t.TempDir(), "work")
	f.git(filepath.Dir(work), "clone", "--quiet", f.origin, work)
	if branch != "main" {
		start := "main"
		if f.branch(f.origin, branch) != "" {
			start = "origin/" + branch
		}
		f.git(work, "checkout", "--quiet", "-B", branch, start)
	}
	file := filepath.Join(work, path)
	if err := os.MkdirAll(filepath.Dir(file), 0o750); err != nil {
		f.t.Fatal(err)
	}
	if err := os.WriteFile(file, []byte(content), 0o600); err != nil {
		f.t.Fatal(err)
	}
	f.git(work, "add", path)
	f.git(work, "commit", "--quiet", "-m", "change "+path)
	f.git(work, "push", "--quiet", "origin", "HEAD:refs/heads/"+branch)
	return f.git(work, "rev-parse", "HEAD")
}

const baseEgress = `version: 1
hosts:
  - host: deb.debian.org
    reason: apt installs the build's packages
udp:
  - cidr: 0.0.0.0/0
    port: 123
    reason: NTP keeps the machine's clock
`

func mustParseEgress(t *testing.T, doc string) egress.Policy {
	t.Helper()
	p, err := egress.Parse([]byte(doc))
	if err != nil {
		t.Fatalf("egress.Parse: %v", err)
	}
	return p
}

// harnessEgress is the workflow's environment.remote.egress.
func harnessEgress() egress.Policy {
	return egress.Policy{Hosts: []egress.Host{{Host: "models.opencode.ai", Reason: "the agent harness's models"}},
		UDP: []egress.UDP{}, Private: []egress.Private{}}
}

// withHarnessAndAnthropic declares the harness's hosts and binds the
// anthropic credential for api.anthropic.com.
func withHarnessAndAnthropic(cfg *config.ServiceConfig) {
	cfg.Environment.Remote.Egress = harnessEgress()
	cfg.Environment.Remote.Credentials = []config.RemoteCredential{
		{Name: "anthropic", Env: "ANTHROPIC_API_KEY", Hosts: []string{"api.anthropic.com"}, Header: "x-api-key"},
	}
}

// job-egress-policy: An agent edits its own policy, and A homelab offer.
// The issue branch holds a wider file, with pastebin.com; the offer
// carries the base branch's file and names its commit, with the
// workflow's harness hosts and the credential binding's hosts, and no
// credential value.
func TestRemoteOfferCarriesTheBaseBranchsPolicy(t *testing.T) {
	f := newGitFixture(t)
	base := f.commitOn("main", baseEgress)
	f.commitOn("sortie/ONS-30", baseEgress+"  - host: pastebin.com\n    reason: the agent's own idea\n")
	const secret = "sk-ant-api03-not-for-the-offer"
	t.Setenv("ANTHROPIC_API_KEY", secret)

	run := runRemoteONS30With(t, f, withHarnessAndAnthropic, nil, nil)

	if run.result.ExitKind != WorkerExitNormal || run.result.Error != nil {
		t.Fatalf("result %v %v", run.result.ExitKind, run.result.Error)
	}
	want := worker.Egress{
		Source:          worker.EgressSource{Repository: "org/app", Branch: "main", Commit: base, File: egress.File},
		Harness:         harnessEgress(),
		Repository:      mustParseEgress(t, baseEgress),
		CredentialHosts: []string{"api.anthropic.com"},
	}
	if got := run.worker.offer.Run.Egress; !reflect.DeepEqual(got, want) {
		t.Errorf("run.egress = %+v\nwant %+v", got, want)
	}
	for _, leak := range []string{"pastebin.com", secret} {
		if bytes.Contains(run.offerRaw, []byte(leak)) {
			t.Errorf("the offer holds %q: %s", leak, run.offerRaw)
		}
	}
	// The worker's clone of the seed is the issue branch, which does hold
	// the wider file: only the offer's policy is the base branch's.
	if data, err := os.ReadFile(filepath.Join(run.worker.clone, egress.File)); err != nil || !strings.Contains(string(data), "pastebin.com") {
		t.Errorf("the seed's %s = %q, %v; want the issue branch's wider file", egress.File, data, err)
	}
}

// job-egress-policy: A merged host. Once a pull request that adds a host
// merges to the base branch, the next offer carries the host and names
// that commit.
func TestRemoteOfferCarriesAMergedHost(t *testing.T) {
	f := newGitFixture(t)
	before := f.commitOn("main", baseEgress)
	first := runRemoteONS30(t, f, nil, nil)
	if got := first.worker.offer.Run.Egress; got.Source.Commit != before || slices.Contains(got.Repository.HostNames(), "ghcr.io") {
		t.Fatalf("first offer egress = %+v, want %s without ghcr.io", got, before)
	}

	merged := f.commitOn("main", baseEgress[:strings.Index(baseEgress, "udp:")]+
		"  - host: ghcr.io\n    reason: the job pulls its test images\n"+baseEgress[strings.Index(baseEgress, "udp:"):])
	second := runRemoteONS30(t, f, nil, nil)
	if got := second.worker.offer.Run.Egress; got.Source.Commit != merged || !reflect.DeepEqual(got.Repository.HostNames(), []string{"deb.debian.org", "ghcr.io"}) {
		t.Errorf("offer egress after the merge = %+v, want ghcr.io at %s", got, merged)
	}
}

// job-egress-policy: No file. A repository with no .sortie/egress.yaml
// gets the empty repository policy; the offer still names the commit
// read.
func TestRemoteOfferWithNoPolicyFile(t *testing.T) {
	f := newGitFixture(t)
	run := runRemoteONS30(t, f, nil, nil)
	got := run.worker.offer.Run.Egress
	if !reflect.DeepEqual(got.Repository, egress.Empty()) || got.Source.Commit != f.git(f.origin, "rev-parse", "main") {
		t.Errorf("run.egress = %+v, want the empty policy at main", got)
	}
	if !bytes.Contains(run.offerRaw, []byte(`"repository":{"hosts":[],"udp":[],"private":[]}`)) {
		t.Errorf("the offer's repository policy is not three empty lists: %s", run.offerRaw)
	}
}

// An invalid file the dispatch check did not see, merged between the
// check and the seed, is still never offered: the attempt fails naming
// the hold, and no worker is offered the job.
func TestRemoteAttemptRefusesAnInvalidPolicyAtTheSeed(t *testing.T) {
	f := newGitFixture(t)
	f.commitOn("main", "version: 1\nhosts:\n  - {host: \"ghcr.io:443\", reason: images}\n")
	run := runRemoteONS30(t, f, nil, func(*fakeWorker, context.CancelCauseFunc) {})
	if run.result.Error == nil || !strings.Contains(run.result.Error.Error(), "egress policy invalid") ||
		!strings.Contains(run.result.Error.Error(), "hosts[0].host") {
		t.Fatalf("result %v %v, want a failure naming the invalid entry", run.result.ExitKind, run.result.Error)
	}
	if run.worker.offer.Job.ID != "" {
		t.Errorf("offered %+v, want no offer", run.worker.offer.Job)
	}
}

// egressHoldHarness is the remote kind with homelab, whose upstream is a
// copy of homelab's fixture, and sortie, the default, whose upstream has
// no policy file. Policies are read from the upstreams through mirrors
// under a temporary state directory, as Sortie reads them.
func egressHoldHarness(t *testing.T, homelab, sortie *gitFixture, candidates ...domain.Issue) *runHarness {
	t.Helper()
	cfg := remoteHoldConfig(t, 5)
	repos := cfg.Environment.Remote.Repositories
	repos[0].URL, repos[1].URL = homelab.origin, sortie.origin
	repos[0].Requires = nil
	cfg.Environment.Remote.Repository = &repos[1]
	h := newRunHarness(t, cfg, "kind-a")
	h.state.MaxConcurrentAgents = 5
	h.tracker.candidates = candidates
	h.build()
	h.o.readEgress = readBaseEgress(t.TempDir(), discardLogger())
	h.o.remote.matcher.CheckIn("taichi", remoteHost())
	return h
}

func homelabIssue(id, identifier string) domain.Issue {
	return candidate(id, identifier, "sortie-repository:homelab")
}

func (h *runHarness) commentsOn(id string) []string {
	h.state.TrackerOpsWg.Wait()
	h.tracker.commentMu.Lock()
	defer h.tracker.commentMu.Unlock()
	var out []string
	for _, c := range h.tracker.commentCalls {
		if c.IssueID == id {
			out = append(out, c.Text)
		}
	}
	return out
}

// job-egress-policy: A typo merged, and The fix merges. A host with a
// port on homelab's base branch holds every homelab issue with egress
// policy invalid, the commit and the entry, in GET /api/v1/state and in
// one comment on each issue; no session starts and no attempt is used,
// and the other repository's issue is offered. A valid file merged to
// the base branch releases them at the next poll.
func TestHandleTick_AnInvalidPolicyHoldsTheRepositorysJobs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	homelab, sortie := newGitFixture(t), newGitFixture(t)
	typo := homelab.commitOn("main", "version: 1\nhosts:\n  - {host: \"ghcr.io:443\", reason: the job pulls its images}\n")
	h := egressHoldHarness(t, homelab, sortie, homelabIssue("issue-30", "ONS-30"), homelabIssue("issue-31", "ONS-31"), candidate("issue-60", "ONS-60"))

	h.o.handleTick(ctx)
	h.o.handleTick(ctx)

	for _, id := range []string{"issue-30", "issue-31"} {
		got := requireHeld(t, h, id, SkipEgressInvalid)
		want := EgressHold{Repository: "homelab", Branch: "main", Commit: typo, File: egress.File, Entry: "hosts[0].host",
			Error: `"ghcr.io:443" has a port: write the DNS name alone`}
		if got.Egress == nil || *got.Egress != want {
			t.Errorf("State.Held[%s].Egress = %+v, want %+v", id, got.Egress, want)
		}
		if _, running := h.state.Running[id]; running {
			t.Errorf("%s is running, want it held", id)
		}
		if comments := h.commentsOn(id); len(comments) != 1 || !strings.Contains(comments[0], "egress policy invalid") ||
			!strings.Contains(comments[0], typo) || !strings.Contains(comments[0], "hosts[0].host") {
			t.Errorf("comments on %s = %q, want one naming the hold, the commit and the entry", id, comments)
		}
	}
	if _, running := h.state.Running["issue-60"]; !running {
		t.Errorf("ONS-60 of the sortie repository not running; held: %+v", h.state.Held["issue-60"])
	}
	snap := RuntimeSnapshot(h.state, time.Now())
	var served []string
	for _, e := range snap.Held {
		if e.Hold.Reason == SkipEgressInvalid && e.Hold.Egress != nil && e.Hold.Egress.Commit == typo {
			served = append(served, e.Identifier)
		}
	}
	if slices.Sort(served); !slices.Equal(served, []string{"ONS-30", "ONS-31"}) {
		t.Errorf("snapshot egress holds = %v, want ONS-30 and ONS-31", served)
	}
	h.store.mu.Lock()
	histories := len(h.store.runHistories)
	h.store.mu.Unlock()
	if histories != 0 || len(h.state.RetryAttempts) != 0 || h.o.remote.matcher.Queued("ONS-30") {
		t.Errorf("run histories %d, retries %d, ONS-30 queued %v; want no session and no attempt used",
			histories, len(h.state.RetryAttempts), h.o.remote.matcher.Queued("ONS-30"))
	}

	homelab.commitOn("main", baseEgress)
	h.o.handleTick(ctx)

	for _, id := range []string{"issue-30", "issue-31"} {
		if _, running := h.state.Running[id]; !running {
			t.Errorf("%s not running after the fix merged; held: %+v", id, h.state.Held[id])
		}
		if _, noticed := h.state.EgressHoldNoticed[id]; noticed {
			t.Errorf("EgressHoldNoticed[%s] kept after the release", id)
		}
		if comments := h.commentsOn(id); len(comments) != 1 {
			t.Errorf("comments on %s = %d after the release, want the one", id, len(comments))
		}
	}
}

// A retry whose repository's egress policy is invalid keeps its attempt
// and takes no slot: it is re-armed with the hold, and dispatched once
// the policy is valid.
func TestHandleRetryTimer_HeldByAnInvalidPolicy(t *testing.T) {
	store := &mockRetryStore{}
	tracker := &mockRetryTracker{fetchedIssue: candidateIssue("ISS-30", "ONS-30", "To Do")}
	state := retryState(t, "ISS-30", "ONS-30", 2)
	params := defaultRetryParams(t, store, tracker)
	params.PollIntervalMS = 60_000
	hold := &EgressHold{Repository: "homelab", Branch: "main", Commit: "4b825dc642cb6eb9a060e54bf8d69288fbee4904",
		File: egress.File, Entry: "hosts[0].host", Error: "has a port"}
	params.EgressHold = func(domain.Issue) *EgressHold { return hold }
	dispatched := 0
	params.MakeWorkerFn = func(_, _, _, _, _ string, _ domain.AgentAdapter, _ AttemptSettings) WorkerFunc {
		dispatched++
		return func(context.Context, domain.Issue, *int) {}
	}

	HandleRetryTimer(state, "ISS-30", params)

	if _, running := state.Running["ISS-30"]; running || dispatched != 0 {
		t.Fatalf("running = %v, dispatches = %d; want the retry held without a slot", running, dispatched)
	}
	entry := state.RetryAttempts["ISS-30"]
	if entry == nil {
		t.Fatal("RetryAttempts[ISS-30] missing, want it re-armed")
	}
	if entry.TimerHandle != nil {
		entry.TimerHandle.Stop()
	}
	if entry.Attempt != 2 || entry.EgressHold == nil || *entry.EgressHold != *hold {
		t.Errorf("re-armed attempt %d, hold %+v; want attempt 2 with the hold", entry.Attempt, entry.EgressHold)
	}

	hold = nil
	entry.scheduledAt = time.Time{}
	HandleRetryTimer(state, "ISS-30", params)
	if _, running := state.Running["ISS-30"]; !running || dispatched != 1 {
		t.Errorf("running = %v, dispatches = %d once the policy is valid; want 1 dispatch", running, dispatched)
	}
}

// job-egress-policy: A harness host. The workflow's harness hosts reach
// every job's offer, whatever its repository, with the credential
// bindings' hosts once each, and an empty repository policy until the
// seed reads it.
func TestJobEgressCarriesTheHarnessForEveryRepository(t *testing.T) {
	cfg := remoteHoldConfig(t, 1)
	cfg.Environment.Remote.Egress = harnessEgress()
	cfg.Environment.Remote.Credentials = []config.RemoteCredential{
		{Name: "anthropic", Env: "ANTHROPIC_API_KEY", Hosts: []string{"api.anthropic.com"}},
		{Name: "opencode", Env: "OPENCODE_API_KEY", Hosts: []string{"opencode.ai", "api.anthropic.com"}},
	}
	for _, issue := range []domain.Issue{homelabIssue("issue-30", "ONS-30"), candidate("issue-60", "ONS-60")} {
		job := remoteJob(context.Background(), cfg, issue, nil, "kind-a", "a-cmd")
		got := job.Run.Egress
		if !reflect.DeepEqual(got.Harness, harnessEgress()) || !reflect.DeepEqual(got.CredentialHosts, []string{"api.anthropic.com", "opencode.ai"}) ||
			!reflect.DeepEqual(got.Repository, egress.Empty()) {
			t.Errorf("%s run.egress = %+v", issue.Identifier, got)
		}
	}
}
