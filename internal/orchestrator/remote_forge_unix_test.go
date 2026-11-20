//go:build unix

package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/forge"
	"github.com/sortie-ai/sortie/internal/worker"
)

const baseForge = `version: 1
access:
  - repository: ortusnova/fleet-vm
    permissions: {contents: write}
    reason: "fleet mirror: push this seed's branch"
`

// widerForge adds the metal fleet, as an agent would on its own branch.
const widerForge = baseForge + `  - repository: ortusnova/fleet
    permissions: {contents: write}
    reason: the agent's own idea
`

// appCeiling lets org/app's jobs, the fixture's repository, reach both
// fleets with contents write, so only the base-branch read keeps the
// metal fleet out of an offer.
func appCeiling(cfg *config.ServiceConfig) {
	cfg.Environment.Remote.ForgeCeiling = forge.Ceiling{"org/app": {
		{Repository: "ortusnova/fleet-vm", Permissions: map[string]string{"contents": "write"}},
		{Repository: "ortusnova/fleet", Permissions: map[string]string{"contents": "write"}},
		{Repository: "ortusnova/homelab", Permissions: map[string]string{"contents": "read", "pull_requests": "write"}},
		{Repository: "briancappello/personal-site", Permissions: map[string]string{"contents": "write"}},
	}}
}

// tokenShaped matches GitHub's token prefixes, whatever their length.
var tokenShaped = regexp.MustCompile(`\b(ghs|ghp|gho|ghu|ghr|github_pat)_[A-Za-z0-9_]+`)

// requireNoForgeToken fails when the offer holds a GitHub token: one of
// the host's own, any token-shaped value, or a key of run.forge other
// than the ones that name repositories and permissions.
func requireNoForgeToken(t *testing.T, raw []byte, hostTokens ...string) {
	t.Helper()
	for _, tok := range hostTokens {
		if bytes.Contains(raw, []byte(tok)) {
			t.Errorf("the offer holds the host's token %q", tok)
		}
	}
	if m := tokenShaped.Find(raw); m != nil {
		t.Errorf("the offer holds a token-shaped value %q", m)
	}
	var offer struct {
		Run struct {
			Forge map[string]any `json:"forge"`
		} `json:"run"`
	}
	if err := json.Unmarshal(raw, &offer); err != nil {
		t.Fatal(err)
	}
	keys := func(m map[string]any) []string { return slices.Sorted(maps.Keys(m)) }
	if got := keys(offer.Run.Forge); !slices.Equal(got, []string{"owners", "source"}) {
		t.Errorf("run.forge keys = %v, want owners and source", got)
	}
	source, _ := offer.Run.Forge["source"].(map[string]any)
	if got := keys(source); !slices.Equal(got, []string{"branch", "commit", "declared", "file", "repository"}) {
		t.Errorf("run.forge.source keys = %v", got)
	}
	owners, _ := offer.Run.Forge["owners"].([]any)
	for _, o := range owners {
		om := o.(map[string]any)
		if got := keys(om); !slices.Equal(got, []string{"owner", "permissions", "repositories"}) {
			t.Errorf("run.forge owner keys = %v", got)
		}
		for _, r := range om["repositories"].([]any) {
			if got := keys(r.(map[string]any)); !slices.Equal(got, []string{"permissions", "repository"}) {
				t.Errorf("run.forge repository keys = %v", got)
			}
		}
	}
}

// forge-access: An agent widens its own access. The issue branch's file
// adds ortusnova/fleet; the offer carries the base branch's file and
// names its commit, and holds no token.
func TestRemoteOfferCarriesTheBaseBranchsForgeAccess(t *testing.T) {
	f := newGitFixture(t)
	base := f.commitFileOn("main", forge.File, baseForge)
	f.commitFileOn("sortie/ONS-30", forge.File, widerForge)
	const hostToken = "ghp_theHostsOwnTokenNeverForAJob0123456789"
	t.Setenv("SORTIE_GITHUB_TOKEN", hostToken)
	t.Setenv("GITHUB_TOKEN", hostToken)

	run := runRemoteONS30With(t, f, appCeiling, nil, nil)

	if run.result.ExitKind != WorkerExitNormal || run.result.Error != nil {
		t.Fatalf("result %v %v", run.result.ExitKind, run.result.Error)
	}
	want := worker.Forge{
		Source: worker.ForgeSource{Repository: "org/app", Branch: "main", Commit: base, File: forge.File, Declared: true},
		Owners: []forge.OwnerGrant{{Owner: "ortusnova", Permissions: map[string]string{"contents": "write"},
			Repositories: []forge.RepositoryGrant{{Repository: "ortusnova/fleet-vm", Permissions: map[string]string{"contents": "write"}}}}},
	}
	if got := run.worker.offer.Run.Forge; !reflect.DeepEqual(got, want) {
		t.Errorf("run.forge = %+v\nwant %+v", got, want)
	}
	if bytes.Contains(run.offerRaw, []byte(`"ortusnova/fleet"`)) {
		t.Errorf("the offer grants the issue branch's ortusnova/fleet: %s", run.offerRaw)
	}
	requireNoForgeToken(t, run.offerRaw, hostToken)
	if data, err := os.ReadFile(filepath.Join(run.worker.clone, forge.File)); err != nil || !strings.Contains(string(data), "ortusnova/fleet\n") {
		t.Errorf("the seed's %s = %q, %v; want the issue branch's wider file", forge.File, data, err)
	}
}

// forge-access: No file. The offer grants contents and metadata read on
// the repository itself, and no other repository.
func TestRemoteOfferWithNoForgeFile(t *testing.T) {
	f := newGitFixture(t)
	run := runRemoteONS30(t, f, nil, nil)
	want := worker.Forge{
		Source: worker.ForgeSource{Repository: "org/app", Branch: "main", Commit: f.git(f.origin, "rev-parse", "main"), File: forge.File},
		Owners: []forge.OwnerGrant{{Owner: "org", Permissions: map[string]string{"contents": "read", "metadata": "read"},
			Repositories: []forge.RepositoryGrant{{Repository: "org/app", Permissions: map[string]string{"contents": "read", "metadata": "read"}}}}},
	}
	if got := run.worker.offer.Run.Forge; !reflect.DeepEqual(got, want) {
		t.Errorf("run.forge = %+v\nwant %+v", got, want)
	}
	requireNoForgeToken(t, run.offerRaw)
}

// forge-access: Two owners. The offer's run.forge has one entry per
// owner, each with its one repository and its permissions, and no token.
func TestRemoteOfferGroupsTheGrantByOwner(t *testing.T) {
	f := newGitFixture(t)
	f.commitFileOn("main", forge.File, `version: 1
access:
  - {repository: ortusnova/homelab, permissions: {contents: read, pull_requests: write}, reason: comment on the story's pull request}
  - {repository: briancappello/personal-site, permissions: {contents: write}, reason: push the site's branch}
`)
	run := runRemoteONS30With(t, f, appCeiling, nil, nil)
	got := run.worker.offer.Run.Forge.Owners
	want := []forge.OwnerGrant{
		{Owner: "briancappello", Permissions: map[string]string{"contents": "write"},
			Repositories: []forge.RepositoryGrant{{Repository: "briancappello/personal-site", Permissions: map[string]string{"contents": "write"}}}},
		{Owner: "ortusnova", Permissions: map[string]string{"contents": "read", "pull_requests": "write"},
			Repositories: []forge.RepositoryGrant{{Repository: "ortusnova/homelab", Permissions: map[string]string{"contents": "read", "pull_requests": "write"}}}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("run.forge.owners = %+v\nwant %+v", got, want)
	}
	requireNoForgeToken(t, run.offerRaw)
}

// A declaration above the ceiling that the dispatch check did not see,
// merged between the check and the seed, is never offered: the attempt
// fails naming the hold, and no worker is offered the job.
func TestRemoteAttemptRefusesAForgeAboveTheCeilingAtTheSeed(t *testing.T) {
	f := newGitFixture(t)
	f.commitFileOn("main", forge.File, widerForge)
	narrow := func(cfg *config.ServiceConfig) {
		cfg.Environment.Remote.ForgeCeiling = forge.Ceiling{"org/app": {
			{Repository: "ortusnova/fleet-vm", Permissions: map[string]string{"contents": "write"}}}}
	}
	run := runRemoteONS30With(t, f, narrow, nil, func(*fakeWorker, context.CancelCauseFunc) {})
	if run.result.Error == nil || !strings.Contains(run.result.Error.Error(), "GitHub access refused") ||
		!strings.Contains(run.result.Error.Error(), "access[1].repository") || !strings.Contains(run.result.Error.Error(), "ortusnova/fleet is outside the ceiling") {
		t.Fatalf("result %v %v, want a failure naming the refused entry", run.result.ExitKind, run.result.Error)
	}
	if run.worker.offer.Job.ID != "" {
		t.Errorf("offered %+v, want no offer", run.worker.offer.Job)
	}
}

// forgeHoldHarness is egressHoldHarness with homelab's forge ceiling:
// ortusnova/fleet-vm contents write, and never ortusnova/fleet. Both
// files are read through the same mirrors, as Sortie reads them.
func forgeHoldHarness(t *testing.T, homelab, sortie *gitFixture, candidates ...domain.Issue) *runHarness {
	t.Helper()
	cfg := remoteHoldConfig(t, 5)
	repos := cfg.Environment.Remote.Repositories
	repos[0].URL, repos[1].URL = homelab.origin, sortie.origin
	repos[0].Requires = nil
	cfg.Environment.Remote.Repository = &repos[1]
	cfg.Environment.Remote.ForgeCeiling = forge.Ceiling{"homelab": {
		{Repository: "ortusnova/fleet-vm", Permissions: map[string]string{"contents": "write"}}}}
	h := newRunHarness(t, cfg, "kind-a")
	h.state.MaxConcurrentAgents = 5
	h.tracker.candidates = candidates
	h.build()
	stateDir := t.TempDir()
	h.o.readEgress = readBaseEgress(stateDir, discardLogger())
	h.o.readForge = readBaseForge(stateDir, discardLogger())
	h.o.remote.matcher.CheckIn("taichi", remoteHost())
	return h
}

// forge-access: Above the ceiling, and The fix merges. homelab's base
// branch adds ortusnova/fleet; every homelab issue is held naming it as
// outside the ceiling, in the state and in one comment on each issue; no
// session starts and no attempt is used, and the other repository's
// issue is offered. A declaration within the ceiling merged to the base
// branch releases them at the next poll.
func TestHandleTick_AForgeAboveTheCeilingHoldsTheRepositorysJobs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	homelab, sortie := newGitFixture(t), newGitFixture(t)
	above := homelab.commitFileOn("main", forge.File, widerForge)
	h := forgeHoldHarness(t, homelab, sortie, homelabIssue("issue-30", "ONS-30"), homelabIssue("issue-31", "ONS-31"), candidate("issue-60", "ONS-60"))

	h.o.handleTick(ctx)
	h.o.handleTick(ctx)

	for _, id := range []string{"issue-30", "issue-31"} {
		got := requireHeld(t, h, id, SkipForgeRefused)
		want := ForgeHold{Repository: "homelab", Branch: "main", Commit: above, File: forge.File, Entry: "access[1].repository",
			Error: "ortusnova/fleet is outside the ceiling: environment.remote.forge_ceiling.homelab does not list it"}
		if got.Forge == nil || *got.Forge != want {
			t.Errorf("State.Held[%s].Forge = %+v, want %+v", id, got.Forge, want)
		}
		if _, running := h.state.Running[id]; running {
			t.Errorf("%s is running, want it held", id)
		}
		if comments := h.commentsOn(id); len(comments) != 1 || !strings.Contains(comments[0], "GitHub access refused") ||
			!strings.Contains(comments[0], above) || !strings.Contains(comments[0], "ortusnova/fleet is outside the ceiling") {
			t.Errorf("comments on %s = %q, want one naming the hold, the commit and ortusnova/fleet", id, comments)
		}
	}
	if _, running := h.state.Running["issue-60"]; !running {
		t.Errorf("ONS-60 of the sortie repository not running; held: %+v", h.state.Held["issue-60"])
	}
	snap := RuntimeSnapshot(h.state, time.Now())
	var served []string
	for _, e := range snap.Held {
		if e.Hold.Reason == SkipForgeRefused && e.Hold.Forge != nil && e.Hold.Forge.Commit == above {
			served = append(served, e.Identifier)
		}
	}
	if slices.Sort(served); !slices.Equal(served, []string{"ONS-30", "ONS-31"}) {
		t.Errorf("snapshot forge holds = %v, want ONS-30 and ONS-31", served)
	}
	h.store.mu.Lock()
	histories := len(h.store.runHistories)
	h.store.mu.Unlock()
	if histories != 0 || len(h.state.RetryAttempts) != 0 || h.o.remote.matcher.Queued("ONS-30") {
		t.Errorf("run histories %d, retries %d, ONS-30 queued %v; want no session and no attempt used",
			histories, len(h.state.RetryAttempts), h.o.remote.matcher.Queued("ONS-30"))
	}

	homelab.commitFileOn("main", forge.File, baseForge)
	h.o.handleTick(ctx)

	for _, id := range []string{"issue-30", "issue-31"} {
		if _, running := h.state.Running[id]; !running {
			t.Errorf("%s not running after the fix merged; held: %+v", id, h.state.Held[id])
		}
		if _, noticed := h.state.ForgeHoldNoticed[id]; noticed {
			t.Errorf("ForgeHoldNoticed[%s] kept after the release", id)
		}
		if comments := h.commentsOn(id); len(comments) != 1 {
			t.Errorf("comments on %s = %d after the release, want the one", id, len(comments))
		}
	}
}

// forge-access: Invalid entries hold the job. A file with an entry that
// has no reason holds homelab's issue naming the entry.
func TestHandleTick_AnInvalidForgeFileHoldsTheJob(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	homelab, sortie := newGitFixture(t), newGitFixture(t)
	bad := homelab.commitFileOn("main", forge.File, "version: 1\naccess:\n  - {repository: ortusnova/fleet-vm, permissions: {contents: write}}\n")
	h := forgeHoldHarness(t, homelab, sortie, homelabIssue("issue-30", "ONS-30"))

	h.o.handleTick(ctx)

	got := requireHeld(t, h, "issue-30", SkipForgeRefused)
	if got.Forge == nil || got.Forge.Commit != bad || got.Forge.Entry != "access[0].reason" || !strings.Contains(got.Forge.Error, "is required") {
		t.Errorf("State.Held[issue-30].Forge = %+v, want access[0].reason at %s", got.Forge, bad)
	}
}

// A retry whose repository's declaration is refused keeps its attempt
// and takes no slot: it is re-armed with the hold, and dispatched once
// the declaration is within the ceiling.
func TestHandleRetryTimer_HeldByAForgeHold(t *testing.T) {
	store := &mockRetryStore{}
	tracker := &mockRetryTracker{fetchedIssue: candidateIssue("ISS-30", "ONS-30", "To Do")}
	state := retryState(t, "ISS-30", "ONS-30", 2)
	params := defaultRetryParams(t, store, tracker)
	params.PollIntervalMS = 60_000
	hold := &ForgeHold{Repository: "homelab", Branch: "main", Commit: "4b825dc642cb6eb9a060e54bf8d69288fbee4904",
		File: forge.File, Entry: "access[1].repository", Error: "ortusnova/fleet is outside the ceiling"}
	params.ForgeHold = func(domain.Issue) *ForgeHold { return hold }
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
	if entry.Attempt != 2 || entry.ForgeHold == nil || *entry.ForgeHold != *hold {
		t.Errorf("re-armed attempt %d, hold %+v; want attempt 2 with the hold", entry.Attempt, entry.ForgeHold)
	}

	hold = nil
	entry.scheduledAt = time.Time{}
	HandleRetryTimer(state, "ISS-30", params)
	if _, running := state.Running["ISS-30"]; !running || dispatched != 1 {
		t.Errorf("running = %v, dispatches = %d once the declaration is within the ceiling; want 1 dispatch", running, dispatched)
	}
}

// Before the seed reads the declaration, every job's offer grants
// nothing, its owners an empty list and never null.
func TestRemoteJobGrantsNothingBeforeTheSeed(t *testing.T) {
	cfg := remoteHoldConfig(t, 1)
	job := remoteJob(context.Background(), cfg, homelabIssue("issue-30", "ONS-30"), nil, "kind-a", "a-cmd")
	raw, err := json.Marshal(job.Run.Forge)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(raw, []byte(`"owners":[]`)) {
		t.Errorf("run.forge before the seed = %s, want owners: []", raw)
	}
}
