package orchestrator

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/waittest"
	"github.com/sortie-ai/sortie/internal/worker"
)

// nestedProfile is a job profile for a proof that runs smolvm in the job's
// machine: its own image, and a worker with nested virtualization.
var nestedProfile = config.JobProfile{
	Image:    "live-proof.smolmachine",
	Requires: config.RemoteRequirements{Capabilities: []string{"nested"}, CPU: 4, MemGiB: 8, DiskGiB: 40},
}

func profileCfg() config.ServiceConfig {
	cfg := remoteCfg()
	cfg.Environment.Remote.Profiles = map[string]config.JobProfile{"nested": nestedProfile}
	return cfg
}

// A job whose dispatch names a job profile runs in the profile's image and
// requires what the profile requires, with the backend; a job that names
// none, or one the workflow does not declare, keeps its repository's.
func TestRemoteJobRunsWithItsJobProfile(t *testing.T) {
	cfg := profileCfg()
	issue := domain.Issue{ID: "1", Identifier: "ONS-134"}

	j := remoteJob(withJobProfile(context.Background(), "nested"), cfg, issue, nil, "claude-code", "claude")
	if j.Run.Image != "live-proof.smolmachine" {
		t.Errorf("image = %q, want the profile's", j.Run.Image)
	}
	want := worker.Requires{Capabilities: []string{"nested", "smolvm"}, CPU: 4, MemGiB: 8, DiskGiB: 40}
	if !slices.Equal(j.Requires.Capabilities, want.Capabilities) || j.Requires.CPU != want.CPU || j.Requires.MemGiB != want.MemGiB || j.Requires.DiskGiB != want.DiskGiB {
		t.Errorf("requires = %+v, want %+v", j.Requires, want)
	}

	for name, ctx := range map[string]context.Context{
		"none":     context.Background(),
		"unknown":  withJobProfile(context.Background(), "gpu"),
		"no image": withJobProfile(context.Background(), "bare"),
	} {
		c := profileCfg()
		c.Environment.Remote.Profiles["bare"] = config.JobProfile{Requires: config.RemoteRequirements{Capabilities: []string{"linux"}, CPU: 2, MemGiB: 4, DiskGiB: 10}}
		j := remoteJob(ctx, c, issue, nil, "claude-code", "claude")
		if j.Run.Image != "ghcr.io/ortusnova/agent:1" {
			t.Errorf("%s: image = %q, want the repository's", name, j.Run.Image)
		}
		if slices.Contains(j.Requires.Capabilities, "nested") {
			t.Errorf("%s: requires = %+v, want the repository's", name, j.Requires)
		}
	}
}

// The matcher never offers a job that names the nested profile to a worker
// without nested virtualization, and offers it to the one with it.
func TestAJobProfileThatRequiresNestedSkipsAWorkerWithout(t *testing.T) {
	cfg := profileCfg()
	job := remoteJob(withJobProfile(context.Background(), "nested"), cfg, domain.Issue{ID: "1", Identifier: "ONS-134"}, nil, "claude-code", "claude")
	m := worker.NewMatcher(worker.NewTTLLeaser(time.Minute), time.Now, nil)
	p1 := remoteHost()
	p1.Nested = false
	m.CheckIn("p1", p1)
	m.CheckIn("taichi", remoteHost())
	m.Submit(job)
	if offer, ok, err := m.Poll("p1"); ok || err != nil {
		t.Fatalf("p1 was offered the nested job: %+v, %v", offer.Job, err)
	}
	offer, ok, err := m.Poll("taichi")
	if !ok || err != nil {
		t.Fatalf("taichi was not offered the nested job: %v", err)
	}
	if offer.Run.Image != "live-proof.smolmachine" {
		t.Errorf("image = %q, want the profile's", offer.Run.Image)
	}
}

// The dispatch check asks for the requirements of the job's profile: a
// retry that names the nested profile waits while only a worker without
// nested virtualization is checked in.
func TestWorkerWaitReadsTheJobProfile(t *testing.T) {
	h := remoteHoldHarness(t, 1)
	cfg := remoteHoldConfig(t, 1)
	cfg.Environment.Remote.Profiles = map[string]config.JobProfile{"nested": nestedProfile}
	h.o.remote.matcher.CheckIn("p1", p1Worker())
	issue := candidate("issue-134", "ONS-134")
	if wait := h.o.workerWait(cfg, issue, ""); wait != nil {
		t.Fatalf("the repository's job waits: %+v", wait)
	}
	wait := h.o.workerWait(cfg, issue, "nested")
	if wait == nil || !slices.Contains(wait.Capabilities, "nested") {
		t.Fatalf("the nested profile's job = %+v, want it held for nested", wait)
	}
	h.o.remote.matcher.CheckIn("taichi", taichiWorker(200))
	if wait := h.o.workerWait(cfg, issue, "nested"); wait != nil {
		t.Errorf("the nested profile's job waits with taichi checked in: %+v", wait)
	}
}

// A supervisor fix run names its job profile: the scheduled run carries
// it. A profile the workflow does not declare is refused before anything
// is scheduled.
func TestSupervisor_FixRunCarriesItsJobProfile(t *testing.T) {
	t.Parallel()
	h := newSupervisorHarness(t, stateWithReviewReaction(t, "id-15", 61))
	h.o.workflowManager = &stubWorkflowManager{config: profileCfg()}
	if got := h.o.Supervisor().FixRun(t.Context(), "id-15-ident", "prove it", "gpu"); got.Code != "unknown_profile" {
		t.Fatalf("fix run with an undeclared profile = %+v, want unknown_profile", got)
	}
	var retry *RetryEntry
	onLoop(t, h.o, func() { retry = h.o.state.RetryAttempts["id-15"] })
	if retry != nil {
		t.Fatalf("a refused fix run scheduled %+v", retry)
	}
	if got := h.o.Supervisor().FixRun(t.Context(), "id-15-ident", "prove it", "nested"); got.Status != SupervisorDone {
		t.Fatalf("fix run = %+v, want done", got)
	}
	onLoop(t, h.o, func() { retry = h.o.state.RetryAttempts["id-15"] })
	if retry == nil || retry.JobProfile != "nested" || retry.ReactionKind != ReactionKindReview {
		t.Fatalf("retry = %+v, want a review fix run with the nested profile", retry)
	}
}

// A live-proof request names its job profile. A watched story's request
// is only checked; a parked story's job, which Sortie starts itself, runs
// with it.
func TestSupervisor_RequestLiveProofCarriesItsJobProfile(t *testing.T) {
	t.Parallel()
	h := newSupervisorHarness(t, stateWithReviewReaction(t, "id-16", 71))
	h.o.workflowManager = &stubWorkflowManager{config: profileCfg()}
	if got := h.o.Supervisor().RequestLiveProof(t.Context(), "id-16-ident", "criterion", "nested"); got.Status != SupervisorDone {
		t.Errorf("live proof = %+v, want done", got)
	}
	if got := h.o.Supervisor().RequestLiveProof(t.Context(), "id-16-ident", "criterion", "gpu"); got.Code != "unknown_profile" {
		t.Errorf("live proof with an undeclared profile = %+v, want unknown_profile", got)
	}

	p := parkedStoryHarness(t, supervisorFinder{"o/homelab:sortie/ONS-39": {Number: 155}})
	cfg := p.o.workflowManager.Config()
	cfg.Environment.Remote.Profiles = map[string]config.JobProfile{"nested": nestedProfile}
	p.o.workflowManager = &stubWorkflowManager{config: cfg}
	if got := p.o.Supervisor().RequestLiveProof(t.Context(), "ONS-39", "the proof boots smolvm", "nested"); got.Status != SupervisorDone {
		t.Fatalf("live proof of a parked story = %+v, want done", got)
	}
	var retry *RetryEntry
	onLoop(t, p.o, func() { retry = p.o.state.RetryAttempts["id-39"] })
	if retry == nil || retry.ReactionKind != ReactionKindLiveProof || retry.JobProfile != "nested" {
		t.Fatalf("retry = %+v, want the live-proof job with the nested profile", retry)
	}
}

// A retry that names a job profile dispatches its worker with that profile,
// the running entry keeps it, and the worker-wait check is asked for it.
func TestHandleRetryTimer_JobProfileReachesTheWorker(t *testing.T) {
	t.Parallel()

	const id = "ISS-PROFILE"
	state := frozenRetryState(id, RetryEntry{Attempt: 1, JobProfile: "nested"})
	params := defaultRetryParams(t, &mockRetryStore{}, &mockRetryTracker{fetchedIssue: candidateIssue(id, id, "To Do")})
	var asked []string
	params.WorkerWait = func(_ domain.Issue, profile string) *WorkerHold {
		asked = append(asked, profile)
		return nil
	}
	profiles := make(chan string, 1)
	params.MakeWorkerFn = func(_, _, _, _, _ string, _ domain.AgentAdapter, _ AttemptSettings) WorkerFunc {
		return func(ctx context.Context, _ domain.Issue, _ *int) {
			profiles <- jobProfileFrom(ctx)
		}
	}

	HandleRetryTimer(state, id, params)

	if got := waittest.Receive(t, profiles, "worker goroutine execution"); got != "nested" {
		t.Errorf("the worker's job profile = %q, want nested", got)
	}
	if !slices.Equal(asked, []string{"nested"}) {
		t.Errorf("worker wait asked for %v, want the nested profile", asked)
	}
	if running := state.Running[id]; running == nil || running.JobProfile != "nested" {
		t.Errorf("Running[%s] = %+v, want the nested profile", id, running)
	}
}
