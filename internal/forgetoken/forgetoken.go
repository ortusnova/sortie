// Package forgetoken keeps the GitHub tokens Sortie mints for remote
// attempts and the operator's sandboxes: one installation token per
// repository owner of a grant, minted before the attempt's job is queued,
// handed to the egress gate, refreshed before it expires for as long as
// the attempt lasts, and revoked when the attempt ends. Every set is
// recorded, its tokens sealed, so a restarted process revokes the tokens
// of an attempt that no longer runs. No token value is ever logged: each
// is registered with the log masker for as long as it lives.
package forgetoken

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sortie-ai/sortie/internal/forge"
	"github.com/sortie-ai/sortie/internal/forgeapp"
	"github.com/sortie-ai/sortie/internal/persistence"
	"github.com/sortie-ai/sortie/internal/redact"
)

// RefreshBefore is how long before a token expires Sortie mints its
// replacement.
const RefreshBefore = 10 * time.Minute

// TickInterval is how often [Broker.Run] refreshes tokens and retries
// revocations.
const TickInterval = 30 * time.Second

// Kinds of set.
const (
	KindAttempt = "attempt"
	KindSandbox = "sandbox"
)

// redactSource names the masker's entries of this package.
const redactSource = "forge token"

// ErrNotFound is a set that is not recorded, or already ended.
var ErrNotFound = errors.New("no such GitHub token set")

// Minter mints and revokes installation tokens: [*forgeapp.Client].
type Minter interface {
	Mint(ctx context.Context, owner string, repositories []string, permissions map[string]string) (forgeapp.Token, error)
	Revoke(ctx context.Context, token string) error
}

// Gate is the egress gate's registration of an attempt's tokens, which
// it puts in place of the attempt's placeholders.
type Gate interface {
	Register(ctx context.Context, reg Registration) error
	Unregister(ctx context.Context, job string, attempt int) error
}

// Registration is an attempt's tokens as the egress gate takes them: by
// owner, the token in force.
type Registration struct {
	Job     string            `json:"job"`
	Attempt int               `json:"attempt"`
	Tokens  map[string]string `json:"tokens"`
}

// Store records sets: [*persistence.Store].
type Store interface {
	SaveForgeTokenSet(ctx context.Context, set persistence.ForgeTokenSet) error
	DeleteForgeTokenSet(ctx context.Context, id string) error
	LoadForgeTokenSets(ctx context.Context) ([]persistence.ForgeTokenSet, error)
}

// Config is what a [Broker] needs. Gate and Store may be nil: then no
// gate is told, and nothing outlives the process.
type Config struct {
	Minter Minter
	Gate   Gate
	Store  Store
	// SealKey encrypts the recorded tokens: [forgeapp.KeyDigest] of the
	// App's key.
	SealKey [32]byte
	Now     func() time.Time
	Logger  *slog.Logger
}

// Token is one token of a set, as a sandbox's caller is given it.
type Token struct {
	Owner        string
	Value        string
	ExpiresAt    time.Time
	Repositories []string
	Permissions  map[string]string
}

// Set is a snapshot of one set: its ID, its grant and its tokens in
// force.
type Set struct {
	ID        string
	Kind      string
	Job       string
	Attempt   int
	Grant     forge.Grant
	Tokens    []Token
	ExpiresAt time.Time
}

// held is one minted token.
type held struct {
	Owner     string    `json:"owner"`
	Value     string    `json:"value"`
	ExpiresAt time.Time `json:"expires_at"`
}

// set is a recorded set. Only the broker's lock guards its fields; its
// own lock serializes the calls to GitHub and the gate on it.
type set struct {
	op sync.Mutex

	id      string
	kind    string
	job     string
	attempt int
	grant   forge.Grant
	created time.Time
	// current holds the token in force per owner.
	current []held
	// retired holds the tokens replaced or ended, until revoked.
	retired        []held
	ended          bool
	revokeError    string
	revokeAttempts int
}

// Broker keeps the sets. It is safe for concurrent use.
type Broker struct {
	minter Minter
	gate   Gate
	store  Store
	aead   cipher.AEAD
	now    func() time.Time
	log    *slog.Logger

	mu   sync.Mutex
	sets map[string]*set
}

// New returns a broker with no set; [Broker.Load] reads the recorded
// ones.
func New(cfg Config) (*Broker, error) {
	if cfg.Minter == nil {
		return nil, errors.New("forgetoken: a minter is required")
	}
	block, err := aes.NewCipher(cfg.SealKey[:])
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	b := &Broker{minter: cfg.Minter, gate: cfg.Gate, store: cfg.Store, aead: aead, now: cfg.Now, log: cfg.Logger, sets: map[string]*set{}}
	if b.now == nil {
		b.now = time.Now
	}
	if b.log == nil {
		b.log = slog.Default()
	}
	return b, nil
}

// AttemptID is the ID of an attempt's set.
func AttemptID(job string, attempt int) string {
	return KindAttempt + ":" + job + "#" + strconv.Itoa(attempt)
}

// Load reads every recorded set. A set it cannot read, as one sealed with
// another App key, is dropped with a warning: its tokens die within the
// hour.
func (b *Broker) Load(ctx context.Context) error {
	if b.store == nil {
		return nil
	}
	rows, err := b.store.LoadForgeTokenSets(ctx)
	if err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, row := range rows {
		s, err := b.decode(row)
		if err != nil {
			b.log.Warn("GitHub token set not read; its tokens expire within the hour", slog.String("set", row.ID), slog.Any("error", err))
			_ = b.store.DeleteForgeTokenSet(ctx, row.ID) //nolint:errcheck // an unreadable row is only dropped
			continue
		}
		for _, h := range append(slices.Clone(s.current), s.retired...) {
			redact.Add(redactSource, h.Value)
		}
		b.sets[s.id] = s
	}
	return nil
}

// Acquire returns the set of the attempt, minting it when there is none:
// one token per owner of grant, narrowed to that owner's repositories
// and permissions, registered with the gate. A mint refused for one owner
// revokes the tokens minted for the others and returns the refusal. A
// grant with no owner mints nothing.
func (b *Broker) Acquire(ctx context.Context, job string, attempt int, grant forge.Grant) (Set, error) {
	id := AttemptID(job, attempt)
	b.mu.Lock()
	s, ok := b.sets[id]
	b.mu.Unlock()
	if ok {
		s.op.Lock()
		defer s.op.Unlock()
		if !b.isEnded(s) {
			return b.snapshot(s), nil
		}
	}
	if len(grant.Owners) == 0 {
		return Set{ID: id, Kind: KindAttempt, Job: job, Attempt: attempt, Grant: grant.Clone()}, nil
	}
	s = &set{id: id, kind: KindAttempt, job: job, attempt: attempt, grant: grant.Clone(), created: b.now()}
	s.op.Lock()
	defer s.op.Unlock()
	if err := b.mintAll(ctx, s); err != nil {
		return Set{}, err
	}
	b.mu.Lock()
	b.sets[id] = s
	b.mu.Unlock()
	b.save(ctx, s)
	if err := b.register(ctx, s); err != nil {
		b.endLocked(ctx, s)
		return Set{}, fmt.Errorf("hand the attempt's GitHub tokens to the egress gate: %w", err)
	}
	b.log.Info("GitHub tokens minted for the attempt", slog.String("job", job), slog.Int("attempt", attempt),
		slog.String("owners", ownersOf(s.current)))
	return b.snapshot(s), nil
}

// MintSandbox mints a set for a sandbox, by grant, with no gate: the
// caller hands its tokens to the sandbox. It is never refreshed, and is
// dropped from the records once its tokens expire.
func (b *Broker) MintSandbox(ctx context.Context, grant forge.Grant) (Set, error) {
	raw := make([]byte, 12)
	if _, err := rand.Read(raw); err != nil {
		return Set{}, err
	}
	s := &set{id: KindSandbox + ":fts-" + hex.EncodeToString(raw), kind: KindSandbox, grant: grant.Clone(), created: b.now()}
	s.op.Lock()
	defer s.op.Unlock()
	if err := b.mintAll(ctx, s); err != nil {
		return Set{}, err
	}
	b.mu.Lock()
	b.sets[s.id] = s
	b.mu.Unlock()
	b.save(ctx, s)
	b.log.Info("GitHub tokens minted for a sandbox", slog.String("set", s.id), slog.String("owners", ownersOf(s.current)))
	return b.snapshot(s), nil
}

// mintAll mints a token per owner of s's grant into s.current. On a
// refusal it revokes what it minted and returns the refusal.
func (b *Broker) mintAll(ctx context.Context, s *set) error {
	for _, o := range s.grant.Owners {
		tok, err := b.mintOwner(ctx, o)
		if err != nil {
			for _, h := range s.current {
				b.revokeOne(ctx, h)
			}
			s.current = nil
			return err
		}
		s.current = append(s.current, held{Owner: o.Owner, Value: tok.Value, ExpiresAt: tok.ExpiresAt})
	}
	return nil
}

func (b *Broker) mintOwner(ctx context.Context, o forge.OwnerGrant) (forgeapp.Token, error) {
	repos := make([]string, 0, len(o.Repositories))
	for _, r := range o.Repositories {
		_, name, _ := strings.Cut(r.Repository, "/")
		repos = append(repos, name)
	}
	tok, err := b.minter.Mint(ctx, o.Owner, repos, o.Permissions)
	if err != nil {
		return forgeapp.Token{}, err
	}
	redact.Add(redactSource, tok.Value)
	return tok, nil
}

// revokeOne revokes h and reports whether it is gone: revoked, or past
// its expiry.
func (b *Broker) revokeOne(ctx context.Context, h held) bool {
	if !b.now().Before(h.ExpiresAt) {
		redact.Remove(h.Value)
		return true
	}
	if err := b.minter.Revoke(ctx, h.Value); err != nil {
		b.log.Warn("GitHub token not revoked; retried", slog.String("owner", h.Owner), slog.Any("error", err))
		return false
	}
	redact.Remove(h.Value)
	return true
}

// End ends the attempt's set: the gate forgets it and every token is
// revoked. A revocation that fails is recorded and retried by
// [Broker.Tick]. Ending a set that does not exist does nothing.
func (b *Broker) End(ctx context.Context, job string, attempt int) {
	b.mu.Lock()
	s, ok := b.sets[AttemptID(job, attempt)]
	b.mu.Unlock()
	if !ok {
		return
	}
	s.op.Lock()
	defer s.op.Unlock()
	b.endLocked(ctx, s)
}

// EndSandbox ends a sandbox's set as [Broker.End] ends an attempt's.
func (b *Broker) EndSandbox(ctx context.Context, id string) error {
	b.mu.Lock()
	s, ok := b.sets[id]
	b.mu.Unlock()
	if !ok || s.kind != KindSandbox {
		return ErrNotFound
	}
	s.op.Lock()
	defer s.op.Unlock()
	if b.isEnded(s) {
		return ErrNotFound
	}
	b.endLocked(ctx, s)
	return nil
}

// endLocked ends s; s.op is held.
func (b *Broker) endLocked(ctx context.Context, s *set) {
	b.mu.Lock()
	already := s.ended
	s.ended = true
	s.retired = append(s.retired, s.current...)
	s.current = nil
	b.mu.Unlock()
	if !already && s.kind == KindAttempt && b.gate != nil {
		if err := b.gate.Unregister(ctx, s.job, s.attempt); err != nil {
			b.log.Warn("egress gate not told the attempt ended; its tokens are revoked anyway",
				slog.String("job", s.job), slog.Int("attempt", s.attempt), slog.Any("error", err))
		}
	}
	b.revokeRetired(ctx, s)
	if !already {
		b.log.Info("GitHub tokens of the set ended", slog.String("set", s.id), slog.Int("pending_revocations", b.pending(s)))
	}
}

// revokeRetired revokes every retired token of s, keeps those that fail,
// and records s, or forgets it once it holds nothing. s.op is held.
func (b *Broker) revokeRetired(ctx context.Context, s *set) {
	b.mu.Lock()
	retired := slices.Clone(s.retired)
	b.mu.Unlock()
	var kept []held
	var lastErr string
	for _, h := range retired {
		if !b.revokeOne(ctx, h) {
			kept = append(kept, h)
			lastErr = "a revocation failed; Sortie retries it until the token expires"
		}
	}
	b.mu.Lock()
	s.retired = kept
	if lastErr != "" {
		s.revokeError = lastErr
		s.revokeAttempts++
	} else {
		s.revokeError = ""
	}
	done := s.ended && len(s.retired) == 0
	if done {
		delete(b.sets, s.id)
	}
	b.mu.Unlock()
	if done {
		if b.store != nil {
			if err := b.store.DeleteForgeTokenSet(ctx, s.id); err != nil {
				b.log.Warn("GitHub token set record not deleted", slog.String("set", s.id), slog.Any("error", err))
			}
		}
		return
	}
	b.save(ctx, s)
}

// Reconcile, after a restart, ends the set of every attempt live does
// not report as still running, and registers again with the gate the
// tokens of every attempt that is.
func (b *Broker) Reconcile(ctx context.Context, live func(job string, attempt int) bool) {
	for _, s := range b.all() {
		if s.kind != KindAttempt {
			continue
		}
		s.op.Lock()
		switch {
		case b.isEnded(s):
		case live(s.job, s.attempt):
			if err := b.register(ctx, s); err != nil {
				b.log.Warn("egress gate not given an adopted attempt's GitHub tokens again", slog.String("set", s.id), slog.Any("error", err))
			}
		default:
			b.log.Info("revoking the GitHub tokens of an attempt that no longer runs", slog.String("job", s.job), slog.Int("attempt", s.attempt))
			b.endLocked(ctx, s)
		}
		s.op.Unlock()
	}
}

// Tick refreshes every token of a live attempt's set that expires within
// [RefreshBefore]: the replacement is minted with the same grant and
// handed to the gate before the old token is revoked. It retries failed
// revocations, and forgets what can no longer be used: a sandbox's
// expired tokens and an ended set's expired ones.
func (b *Broker) Tick(ctx context.Context) {
	for _, s := range b.all() {
		s.op.Lock()
		b.tickLocked(ctx, s)
		s.op.Unlock()
	}
}

func (b *Broker) tickLocked(ctx context.Context, s *set) {
	now := b.now()
	b.mu.Lock()
	ended, kind := s.ended, s.kind
	current := slices.Clone(s.current)
	b.mu.Unlock()
	if !ended && kind == KindSandbox {
		expired := true
		for _, h := range current {
			if now.Before(h.ExpiresAt) {
				expired = false
			}
		}
		if expired {
			b.endLocked(ctx, s)
			return
		}
	}
	if !ended && kind == KindAttempt {
		b.refreshLocked(ctx, s, now)
	}
	b.revokeRetired(ctx, s)
}

// refreshLocked replaces each token of s that expires within
// [RefreshBefore]. s.op is held.
func (b *Broker) refreshLocked(ctx context.Context, s *set, now time.Time) {
	b.mu.Lock()
	current := slices.Clone(s.current)
	b.mu.Unlock()
	next := slices.Clone(current)
	var old []held
	for i, h := range current {
		if h.ExpiresAt.Sub(now) > RefreshBefore {
			continue
		}
		o, ok := ownerGrant(s.grant, h.Owner)
		if !ok {
			continue
		}
		tok, err := b.mintOwner(ctx, o)
		if err != nil {
			b.log.Warn("GitHub token not refreshed; retried at the next tick", slog.String("set", s.id), slog.String("owner", h.Owner), slog.Any("error", err))
			continue
		}
		next[i] = held{Owner: h.Owner, Value: tok.Value, ExpiresAt: tok.ExpiresAt}
		old = append(old, h)
	}
	if len(old) == 0 {
		return
	}
	b.mu.Lock()
	s.current = next
	b.mu.Unlock()
	if err := b.register(ctx, s); err != nil {
		// The gate keeps the old tokens: keep them in force too, and
		// revoke the new ones; the next tick tries again.
		b.log.Warn("egress gate not given the refreshed GitHub tokens; the old ones stay in force", slog.String("set", s.id), slog.Any("error", err))
		b.mu.Lock()
		for i := range s.current {
			for _, h := range old {
				if s.current[i].Owner == h.Owner {
					s.retired = append(s.retired, s.current[i])
					s.current[i] = h
				}
			}
		}
		b.mu.Unlock()
		b.save(ctx, s)
		return
	}
	b.mu.Lock()
	s.retired = append(s.retired, old...)
	b.mu.Unlock()
	b.save(ctx, s)
	b.log.Info("GitHub tokens refreshed", slog.String("set", s.id), slog.String("owners", ownersOf(old)))
}

// Run calls [Broker.Tick] every [TickInterval] until ctx ends.
func (b *Broker) Run(ctx context.Context) {
	t := time.NewTicker(TickInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			b.Tick(ctx)
		}
	}
}

// Sets returns a snapshot of every set, for the operator's view: no
// token value.
func (b *Broker) Sets() []Set {
	var out []Set
	for _, s := range b.all() {
		snap := b.snapshot(s)
		for i := range snap.Tokens {
			snap.Tokens[i].Value = ""
		}
		out = append(out, snap)
	}
	return out
}

func (b *Broker) all() []*set {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]*set, 0, len(b.sets))
	for _, s := range b.sets {
		out = append(out, s)
	}
	slices.SortFunc(out, func(x, y *set) int { return strings.Compare(x.id, y.id) })
	return out
}

func (b *Broker) isEnded(s *set) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return s.ended
}

func (b *Broker) pending(s *set) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(s.retired)
}

func (b *Broker) register(ctx context.Context, s *set) error {
	if b.gate == nil || s.kind != KindAttempt {
		return nil
	}
	b.mu.Lock()
	reg := Registration{Job: s.job, Attempt: s.attempt, Tokens: map[string]string{}}
	for _, h := range s.current {
		reg.Tokens[h.Owner] = h.Value
	}
	b.mu.Unlock()
	return b.gate.Register(ctx, reg)
}

func (b *Broker) snapshot(s *set) Set {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := Set{ID: s.id, Kind: s.kind, Job: s.job, Attempt: s.attempt, Grant: s.grant.Clone()}
	for _, h := range s.current {
		t := Token{Owner: h.Owner, Value: h.Value, ExpiresAt: h.ExpiresAt}
		if o, ok := ownerGrant(s.grant, h.Owner); ok {
			for _, r := range o.Repositories {
				t.Repositories = append(t.Repositories, r.Repository)
			}
			t.Permissions = o.Permissions
		}
		if out.ExpiresAt.IsZero() || h.ExpiresAt.Before(out.ExpiresAt) {
			out.ExpiresAt = h.ExpiresAt
		}
		out.Tokens = append(out.Tokens, t)
	}
	return out
}

func ownerGrant(g forge.Grant, owner string) (forge.OwnerGrant, bool) {
	for _, o := range g.Owners {
		if strings.EqualFold(o.Owner, owner) {
			return o, true
		}
	}
	return forge.OwnerGrant{}, false
}

func ownersOf(hs []held) string {
	owners := make([]string, 0, len(hs))
	for _, h := range hs {
		owners = append(owners, h.Owner)
	}
	return strings.Join(owners, ",")
}

// sealed is what a record's sealed column holds, before encryption.
type sealed struct {
	Current []held `json:"current"`
	Retired []held `json:"retired"`
}

// save records s; a failed write is logged, the set stays in memory.
func (b *Broker) save(ctx context.Context, s *set) {
	if b.store == nil {
		return
	}
	b.mu.Lock()
	plain, err := json.Marshal(sealed{Current: s.current, Retired: s.retired})
	row := persistence.ForgeTokenSet{ID: s.id, Kind: s.kind, JobID: s.job, Attempt: s.attempt, Ended: s.ended,
		RevokeError: s.revokeError, RevokeAttempts: s.revokeAttempts, CreatedAt: s.created}
	grant, gerr := json.Marshal(s.grant)
	b.mu.Unlock()
	if err == nil {
		err = gerr
	}
	if err == nil {
		row.Grant = string(grant)
		row.Sealed, err = b.seal(plain)
	}
	if err == nil {
		err = b.store.SaveForgeTokenSet(ctx, row)
	}
	if err != nil {
		b.log.Error("GitHub token set not recorded; a restart cannot revoke its tokens, which expire within the hour",
			slog.String("set", s.id), slog.Any("error", err))
	}
}

func (b *Broker) seal(plain []byte) (string, error) {
	nonce := make([]byte, b.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(b.aead.Seal(nonce, nonce, plain, nil)), nil
}

func (b *Broker) unseal(text string) ([]byte, error) {
	raw, err := base64.StdEncoding.DecodeString(text)
	if err != nil || len(raw) < b.aead.NonceSize() {
		return nil, errors.New("the sealed tokens are not readable")
	}
	plain, err := b.aead.Open(nil, raw[:b.aead.NonceSize()], raw[b.aead.NonceSize():], nil)
	if err != nil {
		return nil, errors.New("the sealed tokens do not open with this App's key")
	}
	return plain, nil
}

func (b *Broker) decode(row persistence.ForgeTokenSet) (*set, error) {
	plain, err := b.unseal(row.Sealed)
	if err != nil {
		return nil, err
	}
	var tokens sealed
	if err := json.Unmarshal(plain, &tokens); err != nil {
		return nil, errors.New("the sealed tokens are not a token set")
	}
	var grant forge.Grant
	if err := json.Unmarshal([]byte(row.Grant), &grant); err != nil {
		return nil, fmt.Errorf("the set's grant: %w", err)
	}
	return &set{id: row.ID, kind: row.Kind, job: row.JobID, attempt: row.Attempt, grant: grant, created: row.CreatedAt,
		current: tokens.Current, retired: tokens.Retired, ended: row.Ended, revokeError: row.RevokeError,
		revokeAttempts: row.RevokeAttempts}, nil
}
