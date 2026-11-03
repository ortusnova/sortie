// Package jobtoken mints, checks and revokes the per-job, per-flow
// bearer tokens a remote job uses on Sortie's data plane.
//
// Taking a job mints one token for each data-plane flow the job may use.
// Every data-plane request presents one of them; Sortie accepts it only
// for the job and the flow it was minted for, and only until the job's
// attempt ends. Tokens are kept as SHA-256 digests, never in clear, and
// a [Token] formats as a mask wherever it is printed or logged.
//
// The package knows nothing of the matcher: a matcher reaches it only
// through [Issuer.OnLeaseGrant] and [Issuer.OnLeaseEnd].
package jobtoken

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"sync"
)

// Flow names one data-plane flow a job uses, such as its git traffic or
// its model traffic.
type Flow string

// Token is a bearer token in clear, as handed to a worker once. Its
// String, GoString, LogValue and MarshalText forms are masked; [Token.Reveal]
// is the only way to read the secret.
type Token struct{ secret string }

const mask = "[job-token]"

// Reveal returns the token in clear, for the one handoff to the worker.
func (t Token) Reveal() string { return t.secret }

// String implements [fmt.Stringer] with a mask.
func (t Token) String() string { return mask }

// GoString implements [fmt.GoStringer] with a mask.
func (t Token) GoString() string { return mask }

// LogValue implements [slog.LogValuer] with a mask.
func (t Token) LogValue() slog.Value { return slog.StringValue(mask) }

// MarshalText implements [encoding.TextMarshaler] with a mask, so JSON
// encoding never carries the secret by accident.
func (t Token) MarshalText() ([]byte, error) { return []byte(mask), nil }

// Grant is what a token allows: one flow of one job's attempt.
type Grant struct {
	JobID   string `json:"job_id"`
	Attempt uint64 `json:"attempt"`
	Flow    Flow   `json:"flow"`
}

var (
	// ErrInvalid reports a token Sortie never minted or has revoked.
	ErrInvalid = errors.New("jobtoken: invalid or ended token")
	// ErrWrongJob reports a token presented for another job.
	ErrWrongJob = errors.New("jobtoken: token belongs to another job")
	// ErrWrongFlow reports a token presented for another flow.
	ErrWrongFlow = errors.New("jobtoken: token belongs to another flow")
)

type digest [sha256.Size]byte

func hash(secret string) digest { return sha256.Sum256([]byte(secret)) }

// Issuer holds the live tokens. It is safe for concurrent use.
type Issuer struct {
	flows []Flow

	mu       sync.Mutex
	grants   map[digest]Grant
	byJob    map[string][]digest
	attempts map[string]uint64
}

// NewIssuer returns an issuer that mints one token for each of flows.
func NewIssuer(flows ...Flow) *Issuer {
	return &Issuer{
		flows:    append([]Flow(nil), flows...),
		grants:   map[digest]Grant{},
		byJob:    map[string][]digest{},
		attempts: map[string]uint64{},
	}
}

// OnLeaseGrant is the matcher's lease-grant hook. It starts a new
// attempt of jobID, ending any earlier one, and mints one token per flow.
func (i *Issuer) OnLeaseGrant(jobID string) (map[Flow]Token, error) {
	return i.mint(jobID, 0, i.flows)
}

// Mint starts the given attempt of jobID, ending any earlier one, and
// mints one token for each of flows. It serves a caller that numbers
// attempts and names a job's flows itself.
func (i *Issuer) Mint(jobID string, attempt uint64, flows []Flow) (map[Flow]Token, error) {
	return i.mint(jobID, attempt, flows)
}

// mint numbers the attempt one past the last it numbered when attempt
// is zero.
func (i *Issuer) mint(jobID string, attempt uint64, flows []Flow) (map[Flow]Token, error) {
	out := make(map[Flow]Token, len(flows))
	for _, f := range flows {
		s, err := newSecret()
		if err != nil {
			return nil, err
		}
		out[f] = Token{secret: s}
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	i.revokeLocked(jobID)
	if attempt == 0 {
		i.attempts[jobID]++
		attempt = i.attempts[jobID]
	}
	n := attempt
	for f, t := range out {
		d := hash(t.secret)
		i.grants[d] = Grant{JobID: jobID, Attempt: n, Flow: f}
		i.byJob[jobID] = append(i.byJob[jobID], d)
	}
	return out, nil
}

// OnLeaseEnd is the matcher's lease-end hook: the attempt of jobID ends
// and every token minted for it is refused from now on.
func (i *Issuer) OnLeaseEnd(jobID string) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.revokeLocked(jobID)
}

func (i *Issuer) revokeLocked(jobID string) {
	for _, d := range i.byJob[jobID] {
		delete(i.grants, d)
	}
	delete(i.byJob, jobID)
}

// Digests returns the attempt of jobID's live tokens and the SHA-256
// digest of each, hex-encoded, by flow; nil when it has none. A digest
// admits nothing by itself: a process that restarts hands the digests
// to [Issuer.Restore], so the tokens the worker holds stay valid.
func (i *Issuer) Digests(jobID string) (uint64, map[Flow]string) {
	i.mu.Lock()
	defer i.mu.Unlock()
	var attempt uint64
	var out map[Flow]string
	for _, d := range i.byJob[jobID] {
		g := i.grants[d]
		if out == nil {
			out = map[Flow]string{}
		}
		attempt = g.Attempt
		out[g.Flow] = hex.EncodeToString(d[:])
	}
	return attempt, out
}

// Restore starts the given attempt of jobID again with the tokens whose
// digests [Issuer.Digests] returned, ending any other attempt of it.
func (i *Issuer) Restore(jobID string, attempt uint64, digests map[Flow]string) error {
	parsed := make(map[Flow]digest, len(digests))
	for f, s := range digests {
		raw, err := hex.DecodeString(s)
		if err != nil || len(raw) != sha256.Size {
			return fmt.Errorf("jobtoken: the %s digest of %s is not a SHA-256 digest", f, jobID)
		}
		parsed[f] = digest(raw)
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	i.revokeLocked(jobID)
	i.attempts[jobID] = max(i.attempts[jobID], attempt)
	for f, d := range parsed {
		i.grants[d] = Grant{JobID: jobID, Attempt: attempt, Flow: f}
		i.byJob[jobID] = append(i.byJob[jobID], d)
	}
	return nil
}

// Lookup returns what the token grants, or [ErrInvalid].
func (i *Issuer) Lookup(token string) (Grant, error) {
	if token == "" {
		return Grant{}, ErrInvalid
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	g, ok := i.grants[hash(token)]
	if !ok {
		return Grant{}, ErrInvalid
	}
	return g, nil
}

// Check accepts token only for jobID's live attempt and flow.
func (i *Issuer) Check(token, jobID string, flow Flow) (Grant, error) {
	g, err := i.Lookup(token)
	if err != nil {
		return Grant{}, err
	}
	if g.JobID != jobID {
		return Grant{}, ErrWrongJob
	}
	if g.Flow != flow {
		return Grant{}, ErrWrongFlow
	}
	return g, nil
}

func newSecret() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}
