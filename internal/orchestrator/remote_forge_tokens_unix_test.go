//go:build unix

package orchestrator

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/failcause"
	"github.com/sortie-ai/sortie/internal/forge"
	"github.com/sortie-ai/sortie/internal/forgeapp"
	"github.com/sortie-ai/sortie/internal/forgeapp/forgeapptest"
	"github.com/sortie-ai/sortie/internal/forgetoken"
	"github.com/sortie-ai/sortie/internal/persistence"
	"github.com/sortie-ai/sortie/internal/waittest"
)

// forgeGate is the egress gate's registration endpoint, recording each
// registration and whether the attempt's job was queued for workers yet
// when it came.
type forgeGate struct {
	mu      sync.Mutex
	queued  func(job string) bool
	regs    []forgetoken.Registration
	early   []bool
	removed []string
}

func (g *forgeGate) Register(_ context.Context, reg forgetoken.Registration) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.regs = append(g.regs, reg)
	if g.queued != nil {
		g.early = append(g.early, !g.queued(reg.Job))
	}
	return nil
}

func (g *forgeGate) Unregister(_ context.Context, job string, attempt int) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.removed = append(g.removed, job)
	return nil
}

func (g *forgeGate) snapshot() ([]forgetoken.Registration, []bool, []string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return slices.Clone(g.regs), slices.Clone(g.early), slices.Clone(g.removed)
}

var forgeAppPermissions = map[string]string{"contents": "write", "pull_requests": "write", "metadata": "read"}

func newForgeFake(t *testing.T) *forgeapptest.Server {
	t.Helper()
	return forgeapptest.New(t, forgeAppPermissions,
		forgeapptest.Installation{ID: 11, Owner: "ortusnova", Repositories: []string{"fleet-vm", "homelab"}})
}

// newForgeBroker returns a broker of fake's App over store, nil for none,
// logging to logs.
func newForgeBroker(t *testing.T, fake *forgeapptest.Server, gate forgetoken.Gate, store forgetoken.Store, logs *bytes.Buffer) *forgetoken.Broker {
	t.Helper()
	client, err := forgeapp.New(forgeapp.Config{AppID: forgeapptest.AppID, Key: fake.Key, APIURL: fake.URL})
	if err != nil {
		t.Fatal(err)
	}
	b, err := forgetoken.New(forgetoken.Config{Minter: client, Gate: gate, Store: store, SealKey: forgeapp.KeyDigest(fake.Key),
		Logger: slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

const fleetVMForge = "version: 1\naccess:\n  - {repository: ortusnova/fleet-vm, permissions: {contents: write}, reason: mirror}\n"

func withFleetVMCeiling(cfg *config.ServiceConfig) {
	cfg.Environment.Remote.ForgeCeiling = forge.Ceiling{"org/app": {
		{Repository: "ortusnova/fleet-vm", Permissions: map[string]string{"contents": "write"}},
		{Repository: "zz-not-installed/thing", Permissions: map[string]string{"contents": "read"}},
	}}
}

// forge-tokens: The request, and The attempt ends. The attempt's token is
// minted and handed to the gate before its job is queued for any worker,
// narrowed to its grant, and revoked when the attempt ends; the gate
// forgets it. Neither the logs nor the offer hold it.
func TestRemoteAttemptMintsBeforeTheJobIsQueuedAndRevokesAtItsEnd(t *testing.T) {
	f := newGitFixture(t)
	f.commitFileOn("main", forge.File, fleetVMForge)
	fake := newForgeFake(t)
	gate := &forgeGate{}
	var logs bytes.Buffer
	run := runRemoteONS30Opts(t, f, remoteRunOpts{adjust: withFleetVMCeiling, forgeTokens: func(rw *remoteWorkers) *forgetoken.Broker {
		gate.queued = rw.matcher.Queued
		return newForgeBroker(t, fake, gate, nil, &logs)
	}})

	if run.result.ExitKind != WorkerExitNormal || run.result.Error != nil {
		t.Fatalf("result %v %v", run.result.ExitKind, run.result.Error)
	}
	mints := fake.Mints()
	if len(mints) != 1 || mints[0].Owner != "ortusnova" || !slices.Equal(mints[0].Repositories, []string{"fleet-vm"}) ||
		len(mints[0].Permissions) != 1 || mints[0].Permissions["contents"] != "write" {
		t.Fatalf("mints = %+v, want one for ortusnova naming only fleet-vm, contents write", mints)
	}
	regs, early, removed := gate.snapshot()
	if len(regs) != 1 || regs[0].Job != "ONS-30" || regs[0].Tokens["ortusnova"] != mints[0].Token {
		t.Fatalf("gate registrations = %d, want ONS-30's token", len(regs))
	}
	if !slices.Equal(early, []bool{true}) {
		t.Errorf("the gate got the token after the job was queued: %v", early)
	}
	if fake.Valid(mints[0].Token) {
		t.Error("the token is still valid after the attempt ended")
	}
	if !slices.Equal(removed, []string{"ONS-30"}) {
		t.Errorf("the gate forgot %v, want ONS-30", removed)
	}
	for _, where := range []struct{ name, text string }{{"the offer", string(run.offerRaw)}, {"the logs", logs.String()}} {
		if strings.Contains(where.text, mints[0].Token) {
			t.Errorf("%s holds the token", where.name)
		}
	}
}

// forge-tokens: No installation. A grant naming an owner the App is not
// installed on mints nothing, offers no job and starts no session; the
// failure names the owner, and its class is the GitHub App's, which the
// platform fault's person action clears.
func TestRemoteAttemptForAnOwnerWithoutTheApp(t *testing.T) {
	f := newGitFixture(t)
	f.commitFileOn("main", forge.File, fleetVMForge+"  - {repository: zz-not-installed/thing, permissions: {contents: read}, reason: other}\n")
	fake := newForgeFake(t)
	var logs bytes.Buffer
	run := runRemoteONS30Opts(t, f, remoteRunOpts{adjust: withFleetVMCeiling, drive: func(*fakeWorker, context.CancelCauseFunc) {},
		forgeTokens: func(*remoteWorkers) *forgetoken.Broker { return newForgeBroker(t, fake, nil, nil, &logs) }})

	var appErr *forgeapp.Error
	if run.result.Error == nil || !errors.As(run.result.Error, &appErr) || appErr.Owner != "zz-not-installed" {
		t.Fatalf("result %v, want the owner refused", run.result.Error)
	}
	if fault := failcause.RouteOf(failcause.Sign("workspace preparation", run.result.Error)); fault.Class != "github_app" || !fault.PersonOnly() {
		t.Errorf("the failure routes to %s, person only %v; want the GitHub App's class", fault.Class, fault.PersonOnly())
	}
	for _, tok := range fake.Tokens() {
		if fake.Valid(tok) {
			t.Error("a token minted for the other owner is still valid")
		}
	}
	if run.worker.offer.Job.ID != "" || len(run.launches) != 0 {
		t.Errorf("offered %q, %d session launches; want neither", run.worker.offer.Job.ID, len(run.launches))
	}
}

// forge-tokens: Sortie restarts. Sortie stops with two attempts holding
// tokens: ONS-30, whose worker keeps it, and ONS-99, which no worker
// held. The restarted Sortie revokes ONS-99's token from its records,
// hands the gate ONS-30's again, and revokes that one once the run ends.
func TestRestartRevokesTheTokensOfAttemptsNoLongerLeased(t *testing.T) {
	h := newRestartHarness(t, 1)
	h.git.commitFileOn("main", forge.File, fleetVMForge)
	// The harness's upstream is a copy of the origin made before the
	// commit: make it again, and point the workflow at it.
	h.upstream = h.git.upstream()
	h.cfg.Environment.Remote.Repository = runRepository(h.upstream)
	withFleetVMCeiling(&h.cfg)
	fake := newForgeFake(t)
	gate := &forgeGate{}
	var logs bytes.Buffer
	h.prepare = func(o *Orchestrator) {
		store, ok := o.store.(*persistence.Store)
		if !ok {
			t.Fatal("the process's store is not a persistence.Store")
		}
		o.remote.forgeTokens = newForgeBroker(t, fake, gate, store, &logs)
	}

	w := newRestartWorker(h)
	w.hold = func(start int, _ string) bool { return start == 1 }
	first := h.start(w.checkIn)
	// ONS-99's attempt held tokens too, and no worker took its job.
	orphan, err := first.o.remote.forgeTokens.Acquire(context.Background(), "ONS-99", 1,
		forge.Grant{Owners: []forge.OwnerGrant{{Owner: "ortusnova", Permissions: map[string]string{"contents": "write"},
			Repositories: []forge.RepositoryGrant{{Repository: "ortusnova/homelab", Permissions: map[string]string{"contents": "write"}}}}}})
	if err != nil {
		t.Fatal(err)
	}
	worked := make(chan struct{})
	go func() {
		defer close(worked)
		w.run()
	}()
	waittest.Receive(t, w.holding, "the agent in the middle of its turn")
	h.recorded(first, func(r persistence.RemoteRun) error { return nil })
	var ons30 string
	for _, m := range fake.Mints() {
		if slices.Equal(m.Repositories, []string{"fleet-vm"}) {
			ons30 = m.Token
		}
	}
	if ons30 == "" {
		t.Fatal("ONS-30's attempt holds no token")
	}
	h.stopAndGo(first)
	if !fake.Valid(ons30) || !fake.Valid(orphan.Tokens[0].Value) {
		t.Fatal("a token was revoked by the stop; the restart must find both")
	}

	second := h.start()
	if fake.Valid(orphan.Tokens[0].Value) {
		t.Error("the token of ONS-99, no longer leased, is still valid after the restart")
	}
	if !fake.Valid(ons30) {
		t.Error("ONS-30's token was revoked, though its worker keeps the attempt")
	}
	regs, _, removed := gate.snapshot()
	if last := regs[len(regs)-1]; last.Job != "ONS-30" || last.Tokens["ortusnova"] != ons30 {
		t.Errorf("the last registration is %s's; want ONS-30's token handed to the gate again", last.Job)
	}
	if !slices.Contains(removed, "ONS-99") {
		t.Errorf("the gate forgot %v, want ONS-99", removed)
	}
	h.handedOff(second)
	waittest.Receive(t, worked, "the worker's report")
	second.stop()
	if fake.Valid(ons30) {
		t.Error("ONS-30's token is still valid after its run ended")
	}
	for _, tok := range fake.Tokens() {
		if strings.Contains(logs.String(), tok) {
			t.Error("the logs hold a token")
		}
	}
}

// forge-tokens: homelab sandboxes take tokens from the same broker. A
// sandbox's set follows the repository's base-branch declaration and
// its ceiling, as a job's does: within it, one token per owner that the
// end revokes; above it, a refusal naming the entry and no mint; a
// declaration that grants nothing, no token.
func TestMintForgeSandbox(t *testing.T) {
	homelab, sortie := newGitFixture(t), newGitFixture(t)
	fake := newForgeFake(t)
	var logs bytes.Buffer
	h := forgeHoldHarness(t, homelab, sortie)
	h.o.remote.forgeTokens = newForgeBroker(t, fake, &forgeGate{}, nil, &logs)
	h.o.dbPath = filepath.Join(t.TempDir(), "sortie.db")
	mint, end := h.o.MintForgeSandboxFunc(), h.o.EndForgeSandboxFunc()
	ctx := context.Background()

	homelab.commitFileOn("main", forge.File, fleetVMForge)
	box, err := mint(ctx, "homelab")
	if err != nil {
		t.Fatal(err)
	}
	if box.ID == "" || len(box.Set.Tokens) != 1 || !fake.Valid(box.Set.Tokens[0].Value) || box.Source.Commit == "" ||
		!slices.Equal(box.Set.Tokens[0].Repositories, []string{"ortusnova/fleet-vm"}) {
		t.Fatalf("sandbox %q with %d tokens from commit %q", box.ID, len(box.Set.Tokens), box.Source.Commit)
	}
	if err := end(ctx, box.ID); err != nil || fake.Valid(box.Set.Tokens[0].Value) {
		t.Fatalf("end = %v; token still valid %v", err, fake.Valid(box.Set.Tokens[0].Value))
	}
	if err := end(ctx, box.ID); !errors.Is(err, forgetoken.ErrNotFound) {
		t.Fatalf("a second end = %v, want ErrNotFound", err)
	}

	homelab.commitFileOn("main", forge.File, fleetVMForge+"  - {repository: ortusnova/fleet, permissions: {contents: read}, reason: metal}\n")
	mints := len(fake.Mints())
	_, err = mint(ctx, "homelab")
	var refusal *ForgeRefusal
	if !errors.As(err, &refusal) || refusal.Hold.Entry != "access[1].repository" || !strings.Contains(err.Error(), "ortusnova/fleet is outside the ceiling") {
		t.Fatalf("mint above the ceiling = %v, want a refusal naming ortusnova/fleet", err)
	}
	if len(fake.Mints()) != mints {
		t.Fatal("a sandbox above the ceiling was minted a token")
	}

	homelab.commitFileOn("main", forge.File, "version: 1\naccess: []\n")
	box, err = mint(ctx, "homelab")
	if err != nil || box.ID != "" || len(box.Set.Tokens) != 0 || len(fake.Mints()) != mints {
		t.Fatalf("a sandbox granted nothing = %q with %d tokens, %v", box.ID, len(box.Set.Tokens), err)
	}

	if _, err := mint(ctx, "fleet"); !errors.Is(err, ErrUnknownRepository) {
		t.Fatalf("mint for a repository the workflow does not list = %v", err)
	}
	for _, tok := range fake.Tokens() {
		if strings.Contains(logs.String(), tok) {
			t.Error("the logs hold a token")
		}
	}
}

// forge-tokens: Tokens outlive no attempt. An attempt whose lease
// expires ends as failed, and its token is revoked as on any other end.
func TestLeaseExpiryRevokesTheAttemptsTokens(t *testing.T) {
	f := newGitFixture(t)
	f.commitFileOn("main", forge.File, fleetVMForge)
	fake := newForgeFake(t)
	gate := &forgeGate{}
	var rw *remoteWorkers
	run := runRemoteONS30Opts(t, f, remoteRunOpts{
		adjust: withFleetVMCeiling,
		forgeTokens: func(r *remoteWorkers) *forgetoken.Broker {
			rw = r
			return newForgeBroker(t, fake, gate, nil, &bytes.Buffer{})
		},
		drive: func(w *fakeWorker, _ context.CancelCauseFunc) {
			w.take()
			// The worker's heartbeats stop: its last renewal leaves the
			// lease a millisecond, and the sweep finds it expired.
			rw.leaser.SetTTL(time.Millisecond)
			if _, err := rw.leaser.Renew("ONS-30", "taichi", time.Now()); err != nil {
				t.Fatal(err)
			}
			waittest.Until(t, func() error {
				if len(rw.matcher.ExpireLeases()) == 1 {
					return nil
				}
				return errors.New("the lease has not expired")
			})
		},
	})
	if run.result.Error == nil || !strings.Contains(run.result.Error.Error(), "expired") {
		t.Fatalf("result %v, want the expired lease", run.result.Error)
	}
	mints := fake.Mints()
	if len(mints) != 1 || fake.Valid(mints[0].Token) {
		t.Fatalf("mints %d; the expired attempt's token is still valid", len(mints))
	}
	if _, _, removed := gate.snapshot(); !slices.Equal(removed, []string{"ONS-30"}) {
		t.Errorf("the gate forgot %v, want ONS-30", removed)
	}
}
