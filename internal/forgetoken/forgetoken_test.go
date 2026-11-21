package forgetoken_test

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

	"github.com/sortie-ai/sortie/internal/forge"
	"github.com/sortie-ai/sortie/internal/forgeapp"
	"github.com/sortie-ai/sortie/internal/forgeapp/forgeapptest"
	"github.com/sortie-ai/sortie/internal/forgetoken"
	"github.com/sortie-ai/sortie/internal/persistence"
)

var appPermissions = map[string]string{"contents": "write", "pull_requests": "write", "issues": "write", "metadata": "read"}

// clock is a fake clock both the broker and the fake GitHub read.
type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// gate records every registration and unregistration, in order, with
// the events of the fake GitHub interleaved.
type gate struct {
	mu     sync.Mutex
	events []string
	regs   []forgetoken.Registration
	fail   int
}

func (g *gate) Register(_ context.Context, reg forgetoken.Registration) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.fail > 0 {
		g.fail--
		return errors.New("gate down")
	}
	g.regs = append(g.regs, reg)
	g.events = append(g.events, "register "+reg.Job)
	return nil
}

func (g *gate) Unregister(_ context.Context, job string, attempt int) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.events = append(g.events, "unregister "+job)
	return nil
}

func (g *gate) last() forgetoken.Registration {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.regs[len(g.regs)-1]
}

// recordingMinter wraps the App's client and logs each revocation as a
// gate event, so a test sees what came first.
type recordingMinter struct {
	*forgeapp.Client
	g *gate
}

func (m recordingMinter) Revoke(ctx context.Context, token string) error {
	err := m.Client.Revoke(ctx, token)
	if err == nil {
		m.g.mu.Lock()
		m.g.events = append(m.g.events, "revoke")
		m.g.mu.Unlock()
	}
	return err
}

type harness struct {
	t     *testing.T
	fake  *forgeapptest.Server
	clock *clock
	gate  *gate
	db    string
	logs  *bytes.Buffer
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	fake := forgeapptest.New(t, appPermissions,
		forgeapptest.Installation{ID: 11, Owner: "ortusnova", Repositories: []string{"fleet-vm", "homelab", "fleet"}},
		forgeapptest.Installation{ID: 22, Owner: "briancappello", Repositories: []string{"personal-site"}},
	)
	c := &clock{now: time.Now().UTC().Truncate(time.Second)}
	fake.SetNow(c.Now)
	return &harness{t: t, fake: fake, clock: c, gate: &gate{}, db: filepath.Join(t.TempDir(), "sortie.db"), logs: &bytes.Buffer{}}
}

func (h *harness) store() *persistence.Store {
	h.t.Helper()
	s, err := persistence.Open(context.Background(), h.db)
	if err != nil {
		h.t.Fatal(err)
	}
	if err := s.Migrate(context.Background()); err != nil {
		h.t.Fatal(err)
	}
	h.t.Cleanup(func() { _ = s.Close() })
	return s
}

// broker returns a broker on a fresh store connection: a new process.
func (h *harness) broker() *forgetoken.Broker {
	h.t.Helper()
	client, err := forgeapp.New(forgeapp.Config{AppID: forgeapptest.AppID, Key: h.fake.Key, APIURL: h.fake.URL, Now: h.clock.Now})
	if err != nil {
		h.t.Fatal(err)
	}
	b, err := forgetoken.New(forgetoken.Config{Minter: recordingMinter{client, h.gate}, Gate: h.gate, Store: h.store(),
		SealKey: forgeapp.KeyDigest(h.fake.Key), Now: h.clock.Now,
		Logger: slog.New(slog.NewTextHandler(h.logs, &slog.HandlerOptions{Level: slog.LevelDebug}))})
	if err != nil {
		h.t.Fatal(err)
	}
	if err := b.Load(context.Background()); err != nil {
		h.t.Fatal(err)
	}
	return b
}

// noSecretLogged fails when the logs hold a token or the App's key.
func (h *harness) noSecretLogged() {
	h.t.Helper()
	logs := h.logs.String()
	for _, tok := range h.fake.Tokens() {
		if strings.Contains(logs, tok) {
			h.t.Errorf("the logs hold a token")
		}
	}
	key := h.fake.KeyPEM()
	for _, line := range strings.Split(strings.TrimSpace(key), "\n")[1:3] {
		if strings.Contains(logs, line) {
			h.t.Errorf("the logs hold the App's private key")
		}
	}
	if !strings.Contains(logs, "GitHub tokens") {
		h.t.Errorf("the logs say nothing of the tokens; the check proves nothing: %s", logs)
	}
}

func grantOf(t *testing.T, doc string) forge.Grant {
	t.Helper()
	d, err := forge.Parse([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	return d.Grant()
}

const twoOwners = `version: 1
access:
  - {repository: ortusnova/fleet-vm, permissions: {contents: write}, reason: mirror}
  - {repository: ortusnova/homelab, permissions: {contents: read, pull_requests: write}, reason: comment}
  - {repository: briancappello/personal-site, permissions: {issues: write}, reason: issue}
`

// forge-tokens: The request, and One token per owner per attempt. One
// mint per owner, each naming exactly that owner's repositories and the
// permissions its token carries; none for an owner the grant does not
// name; both handed to the gate.
func TestAcquireMintsOneTokenPerOwner(t *testing.T) {
	h := newHarness(t)
	b := h.broker()
	set, err := b.Acquire(context.Background(), "ONS-40", 1, grantOf(t, twoOwners))
	if err != nil {
		t.Fatal(err)
	}
	mints := h.fake.Mints()
	if len(mints) != 2 {
		t.Fatalf("mints = %+v, want one per owner", mints)
	}
	type req struct {
		owner string
		repos string
		perms string
	}
	var got []req
	for _, m := range mints {
		var perms []string
		for p, l := range m.Permissions {
			perms = append(perms, p+"="+l)
		}
		slices.Sort(perms)
		got = append(got, req{m.Owner, strings.Join(m.Repositories, ","), strings.Join(perms, ",")})
	}
	want := []req{
		{"briancappello", "personal-site", "issues=write"},
		{"ortusnova", "fleet-vm,homelab", "contents=write,pull_requests=write"},
	}
	if !slices.Equal(got, want) {
		t.Fatalf("mint requests = %+v\nwant %+v", got, want)
	}
	reg := h.gate.last()
	if reg.Job != "ONS-40" || reg.Attempt != 1 || len(reg.Tokens) != 2 || reg.Tokens["ortusnova"] != mints[1].Token || reg.Tokens["briancappello"] != mints[0].Token {
		t.Fatalf("gate registration = job %s attempt %d owners %d", reg.Job, reg.Attempt, len(reg.Tokens))
	}
	if len(set.Tokens) != 2 || set.ID != forgetoken.AttemptID("ONS-40", 1) {
		t.Fatalf("set = %s with %d tokens", set.ID, len(set.Tokens))
	}
	// Acquire again, as an attempt adopted again: the same set, no mint.
	if _, err := b.Acquire(context.Background(), "ONS-40", 1, grantOf(t, twoOwners)); err != nil || len(h.fake.Mints()) != 2 {
		t.Fatalf("a second Acquire minted again: %v, %d mints", err, len(h.fake.Mints()))
	}
	h.noSecretLogged()
}

// A grant with no owner mints nothing and tells the gate nothing.
func TestAcquireOfNoGrant(t *testing.T) {
	h := newHarness(t)
	b := h.broker()
	if _, err := b.Acquire(context.Background(), "ONS-41", 1, forge.Empty().Grant()); err != nil {
		t.Fatal(err)
	}
	if len(h.fake.Mints()) != 0 || len(h.gate.regs) != 0 {
		t.Fatalf("mints %d, registrations %d; want none", len(h.fake.Mints()), len(h.gate.regs))
	}
}

// forge-tokens: No installation. A grant naming an owner the App is not
// installed on mints nothing that survives: the other owner's token is
// revoked, the gate is not told, and nothing is recorded.
func TestAcquireRefusedForOneOwner(t *testing.T) {
	h := newHarness(t)
	b := h.broker()
	grant := grantOf(t, `version: 1
access:
  - {repository: ortusnova/fleet-vm, permissions: {contents: write}, reason: mirror}
  - {repository: zz-not-installed/thing, permissions: {contents: read}, reason: other}
`)
	_, err := b.Acquire(context.Background(), "ONS-42", 1, grant)
	var fe *forgeapp.Error
	if !errors.As(err, &fe) || fe.Owner != "zz-not-installed" || fe.FailureClass() != "forge_app_not_installed" {
		t.Fatalf("Acquire = %v, want the owner refused", err)
	}
	for _, tok := range h.fake.Tokens() {
		if h.fake.Valid(tok) {
			t.Errorf("a token minted before the refusal is still valid")
		}
	}
	if len(h.gate.regs) != 0 || len(b.Sets()) != 0 {
		t.Fatalf("registrations %d, sets %d; want none", len(h.gate.regs), len(b.Sets()))
	}
	if sets, _ := h.store().LoadForgeTokenSets(context.Background()); len(sets) != 0 {
		t.Fatalf("recorded %+v, want nothing", sets)
	}
}

// forge-tokens: The attempt ends. Both tokens are revoked and refused
// 401 by GitHub, the gate forgets the attempt, and the record goes.
func TestEndRevokesEveryToken(t *testing.T) {
	h := newHarness(t)
	b := h.broker()
	if _, err := b.Acquire(context.Background(), "ONS-43", 2, grantOf(t, twoOwners)); err != nil {
		t.Fatal(err)
	}
	b.End(context.Background(), "ONS-43", 2)
	for _, tok := range h.fake.Tokens() {
		if h.fake.Valid(tok) {
			t.Error("a token is still valid after the attempt ended")
		}
	}
	if !slices.Contains(h.gate.events, "unregister ONS-43") {
		t.Errorf("gate events %v, want the attempt unregistered", h.gate.events)
	}
	if sets, _ := h.store().LoadForgeTokenSets(context.Background()); len(sets) != 0 {
		t.Fatalf("recorded %+v after the end, want nothing", sets)
	}
	h.noSecretLogged()
}

// forge-tokens: A long attempt. 50 minutes in, a new token with the same
// grant is in force: the gate has it before the old one is revoked.
// Nothing is refreshed earlier than 10 minutes before the expiry.
func TestTickRefreshesTenMinutesBeforeExpiry(t *testing.T) {
	h := newHarness(t)
	b := h.broker()
	if _, err := b.Acquire(context.Background(), "ONS-44", 1, grantOf(t, "version: 1\naccess:\n  - {repository: ortusnova/fleet-vm, permissions: {contents: write}, reason: mirror}\n")); err != nil {
		t.Fatal(err)
	}
	first := h.gate.last().Tokens["ortusnova"]

	h.clock.Advance(49 * time.Minute)
	b.Tick(context.Background())
	if len(h.fake.Mints()) != 1 {
		t.Fatalf("refreshed with 11 minutes left; mints = %d", len(h.fake.Mints()))
	}

	h.clock.Advance(time.Minute)
	h.gate.events = nil
	b.Tick(context.Background())
	mints := h.fake.Mints()
	if len(mints) != 2 || !slices.Equal(mints[1].Repositories, mints[0].Repositories) || mints[1].Permissions["contents"] != "write" {
		t.Fatalf("mints = %+v, want one replacement with the same grant", mints)
	}
	second := h.gate.last().Tokens["ortusnova"]
	if second == first || second != mints[1].Token {
		t.Fatal("the gate does not hold the replacement")
	}
	if !slices.Equal(h.gate.events, []string{"register ONS-44", "revoke"}) {
		t.Fatalf("events = %v, want the gate given the new token, then the old one revoked", h.gate.events)
	}
	if h.fake.Valid(first) || !h.fake.Valid(second) {
		t.Fatalf("first valid %v, second valid %v; want only the replacement", h.fake.Valid(first), h.fake.Valid(second))
	}
	// The replacement is refreshed in its turn: the tokens stay valid for
	// as long as the attempt lasts.
	h.clock.Advance(50 * time.Minute)
	b.Tick(context.Background())
	if third := h.gate.last().Tokens["ortusnova"]; third == second || !h.fake.Valid(third) || h.fake.Valid(second) {
		t.Fatal("the replacement was not refreshed in its turn")
	}
	h.noSecretLogged()
}

// A refresh the gate refuses keeps the old token in force, revokes the
// unused new one, and succeeds at the next tick.
func TestRefreshTheGateRefuses(t *testing.T) {
	h := newHarness(t)
	b := h.broker()
	if _, err := b.Acquire(context.Background(), "ONS-45", 1, grantOf(t, "version: 1\naccess:\n  - {repository: ortusnova/homelab, permissions: {contents: read}, reason: read}\n")); err != nil {
		t.Fatal(err)
	}
	first := h.gate.last().Tokens["ortusnova"]
	h.clock.Advance(51 * time.Minute)
	h.gate.fail = 1
	b.Tick(context.Background())
	if !h.fake.Valid(first) || h.gate.last().Tokens["ortusnova"] != first {
		t.Fatal("the old token is not in force after the gate refused its replacement")
	}
	if unused := h.fake.Mints()[1].Token; h.fake.Valid(unused) {
		t.Fatal("the replacement the gate refused is still valid")
	}
	b.Tick(context.Background())
	if now := h.gate.last().Tokens["ortusnova"]; now == first || !h.fake.Valid(now) || h.fake.Valid(first) {
		t.Fatal("the next tick did not refresh the token")
	}
}

// A revocation that fails is recorded with the set and retried, and the
// record goes once it succeeds.
func TestFailedRevocationIsRetried(t *testing.T) {
	h := newHarness(t)
	b := h.broker()
	if _, err := b.Acquire(context.Background(), "ONS-46", 1, grantOf(t, twoOwners)); err != nil {
		t.Fatal(err)
	}
	h.fake.FailRevokes(2)
	b.End(context.Background(), "ONS-46", 1)
	sets, _ := h.store().LoadForgeTokenSets(context.Background())
	if len(sets) != 1 || !sets[0].Ended || sets[0].RevokeError == "" || sets[0].RevokeAttempts != 1 {
		t.Fatalf("recorded %+v, want the ended set with its failed revocation", sets)
	}
	b.Tick(context.Background())
	for _, tok := range h.fake.Tokens() {
		if h.fake.Valid(tok) {
			t.Error("a token is still valid after the retry")
		}
	}
	if sets, _ := h.store().LoadForgeTokenSets(context.Background()); len(sets) != 0 {
		t.Fatalf("recorded %+v after the retry, want nothing", sets)
	}
}

// forge-tokens: Sortie restarts. Two attempts hold tokens when the
// process dies; the next process revokes those of the attempt that is no
// longer leased, from its records, and gives the gate the tokens of the
// one it adopts again.
func TestRestartRevokesTheTokensOfEndedAttempts(t *testing.T) {
	h := newHarness(t)
	before := h.broker()
	for _, job := range []string{"ONS-47", "ONS-48"} {
		if _, err := before.Acquire(context.Background(), job, 1, grantOf(t, "version: 1\naccess:\n  - {repository: ortusnova/fleet-vm, permissions: {contents: write}, reason: mirror}\n")); err != nil {
			t.Fatal(err)
		}
	}
	tokens := map[string]string{}
	for _, r := range h.gate.regs {
		tokens[r.Job] = r.Tokens["ortusnova"]
	}
	// The process dies without ending either attempt.

	after := h.broker()
	h.gate.events = nil
	after.Reconcile(context.Background(), func(job string, attempt int) bool { return job == "ONS-47" })
	if !h.fake.Valid(tokens["ONS-47"]) {
		t.Error("the adopted attempt's token was revoked")
	}
	if h.fake.Valid(tokens["ONS-48"]) {
		t.Error("the token of the attempt no longer leased is still valid")
	}
	if !slices.Contains(h.gate.events, "register ONS-47") || !slices.Contains(h.gate.events, "unregister ONS-48") {
		t.Errorf("gate events %v, want ONS-47 registered again and ONS-48 forgotten", h.gate.events)
	}
	sets, _ := h.store().LoadForgeTokenSets(context.Background())
	if len(sets) != 1 || sets[0].JobID != "ONS-47" {
		t.Fatalf("recorded %+v, want only ONS-47's set", sets)
	}
	// The recorded row holds no token in the clear.
	for _, tok := range tokens {
		if strings.Contains(sets[0].Sealed, tok) || strings.Contains(sets[0].Grant, tok) {
			t.Error("the record holds a token in the clear")
		}
	}
	h.noSecretLogged()
}

// A record sealed with another App key is not read: it is dropped with
// a warning.
func TestRecordOfAnotherKeyIsDropped(t *testing.T) {
	h := newHarness(t)
	if _, err := h.broker().Acquire(context.Background(), "ONS-49", 1, grantOf(t, twoOwners)); err != nil {
		t.Fatal(err)
	}
	client, _ := forgeapp.New(forgeapp.Config{AppID: forgeapptest.AppID, Key: h.fake.Key, APIURL: h.fake.URL})
	other, err := forgetoken.New(forgetoken.Config{Minter: client, Store: h.store(), SealKey: [32]byte{1}, Logger: slog.New(slog.NewTextHandler(h.logs, nil))})
	if err != nil {
		t.Fatal(err)
	}
	if err := other.Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(other.Sets()) != 0 || !strings.Contains(h.logs.String(), "do not open with this App's key") {
		t.Fatalf("sets %d; logs %s", len(other.Sets()), h.logs.String())
	}
}

// forge-tokens: homelab sandboxes take tokens from the same broker. A
// sandbox's set is revoked by its end, and one never ended is dropped
// from the records within the hour.
func TestSandboxSets(t *testing.T) {
	h := newHarness(t)
	b := h.broker()
	grant := grantOf(t, "version: 1\naccess:\n  - {repository: ortusnova/fleet-vm, permissions: {contents: write}, reason: mirror}\n")
	ended, err := b.MintSandbox(context.Background(), grant)
	if err != nil {
		t.Fatal(err)
	}
	if len(ended.Tokens) != 1 || ended.Tokens[0].Value == "" || !slices.Equal(ended.Tokens[0].Repositories, []string{"ortusnova/fleet-vm"}) {
		t.Fatalf("sandbox set %s has %d tokens", ended.ID, len(ended.Tokens))
	}
	if len(h.gate.regs) != 0 {
		t.Fatal("a sandbox's tokens were handed to the gate")
	}
	if err := b.EndSandbox(context.Background(), ended.ID); err != nil || h.fake.Valid(ended.Tokens[0].Value) {
		t.Fatalf("EndSandbox = %v; token still valid %v", err, h.fake.Valid(ended.Tokens[0].Value))
	}
	if err := b.EndSandbox(context.Background(), ended.ID); !errors.Is(err, forgetoken.ErrNotFound) {
		t.Fatalf("a second EndSandbox = %v, want ErrNotFound", err)
	}
	if err := b.EndSandbox(context.Background(), forgetoken.AttemptID("ONS-1", 1)); !errors.Is(err, forgetoken.ErrNotFound) {
		t.Fatalf("EndSandbox of an attempt's set = %v, want ErrNotFound", err)
	}

	if _, err := b.MintSandbox(context.Background(), grant); err != nil {
		t.Fatal(err)
	}
	for _, s := range b.Sets() {
		for _, tok := range s.Tokens {
			if tok.Value != "" {
				t.Error("Sets shows a token value")
			}
		}
	}
	h.clock.Advance(59 * time.Minute)
	b.Tick(context.Background())
	if len(b.Sets()) != 1 || len(h.fake.Mints()) != 2 {
		t.Fatalf("sets %d, mints %d at 59 minutes: a sandbox's set is never refreshed, and lives until it expires", len(b.Sets()), len(h.fake.Mints()))
	}
	h.clock.Advance(time.Minute)
	b.Tick(context.Background())
	if len(b.Sets()) != 0 {
		t.Fatalf("a sandbox set never ended is still recorded after its expiry: %+v", b.Sets())
	}
	if sets, _ := h.store().LoadForgeTokenSets(context.Background()); len(sets) != 0 {
		t.Fatalf("recorded %+v, want nothing", sets)
	}
	h.noSecretLogged()
}
