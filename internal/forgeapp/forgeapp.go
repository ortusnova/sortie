// Package forgeapp is a GitHub App's side of the GitHub REST API: the
// App's JSON Web Token, its installations, and the installation access
// tokens it mints and revokes. It holds the App's private key, read once
// from a file only its owner may read, and never logs a key or a token.
// A token is an opaque string of any length: GitHub's formats differ in
// length and change.
package forgeapp

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

// DefaultAPIURL is GitHub's REST API.
const DefaultAPIURL = "https://api.github.com"

// apiVersion is the REST API version every request names.
const apiVersion = "2022-11-28"

// maxBody bounds a response body Sortie reads.
const maxBody = 4 << 20

// installationsRefresh bounds how often an unknown owner re-reads the
// App's installations, as when the operator installs the App on it.
const installationsRefresh = time.Minute

// ErrNotInstalled is a mint for an owner the App has no installation on.
var ErrNotInstalled = errors.New("the GitHub App is not installed on the owner")

// ErrNotGranted is a mint naming a repository or a permission the
// owner's installation does not grant the App.
var ErrNotGranted = errors.New("the GitHub App's installation does not grant every repository and permission of the grant")

// Error is a refusal of a mint, named for the owner. It carries its
// failure class, so a platform fault names the owner and what clears it.
type Error struct {
	Owner string
	Err   error
	// Detail is GitHub's message, when it gave one; it holds no token.
	Detail string
}

func (e *Error) Error() string {
	msg := fmt.Sprintf("GitHub token for owner %s: %v", e.Owner, e.Err)
	if e.Detail != "" {
		msg += ": " + e.Detail
	}
	return msg
}

func (e *Error) Unwrap() error { return e.Err }

// FailureClass names the class of a refusal only a person can clear.
func (e *Error) FailureClass() string {
	switch {
	case errors.Is(e.Err, ErrNotInstalled):
		return "forge_app_not_installed"
	case errors.Is(e.Err, ErrNotGranted):
		return "forge_app_not_granted"
	}
	return ""
}

// Token is an installation access token and when GitHub expires it.
type Token struct {
	Value     string
	ExpiresAt time.Time
}

// String hides the value, so a token printed by mistake shows none.
func (t Token) String() string {
	return "forgeapp.Token{expires " + t.ExpiresAt.UTC().Format(time.RFC3339) + "}"
}

// GoString hides the value from %#v as well.
func (t Token) GoString() string { return t.String() }

// Config is what a [Client] needs.
type Config struct {
	// AppID is the App's ID, the issuer of its JSON Web Tokens.
	AppID string
	// Key is the App's private key.
	Key *rsa.PrivateKey
	// APIURL is the REST API's base URL; empty is [DefaultAPIURL].
	APIURL string
	// HTTP is the client requests go through; nil is a client with a
	// 30-second timeout.
	HTTP *http.Client
	// Now is the clock; nil is time.Now.
	Now func() time.Time
}

// Client is the App's REST client. It is safe for concurrent use.
type Client struct {
	appID string
	key   *rsa.PrivateKey
	api   *url.URL
	http  *http.Client
	now   func() time.Time

	mu            sync.Mutex
	installations map[string]int64
	readAt        time.Time
}

// New returns the App's client.
func New(cfg Config) (*Client, error) {
	if strings.TrimSpace(cfg.AppID) == "" {
		return nil, errors.New("forgeapp: the App's ID is required")
	}
	if cfg.Key == nil {
		return nil, errors.New("forgeapp: the App's private key is required")
	}
	raw := cfg.APIURL
	if raw == "" {
		raw = DefaultAPIURL
	}
	api, err := url.Parse(strings.TrimRight(raw, "/"))
	if err != nil || (api.Scheme != "https" && api.Scheme != "http") || api.Host == "" {
		return nil, fmt.Errorf("forgeapp: the API URL %q is not an absolute http or https URL", raw)
	}
	c := &Client{appID: cfg.AppID, key: cfg.Key, api: api, http: cfg.HTTP, now: cfg.Now}
	if c.http == nil {
		c.http = &http.Client{Timeout: 30 * time.Second}
	}
	if c.now == nil {
		c.now = time.Now
	}
	return c, nil
}

// LoadKey reads the App's private key from path, a PEM file of a PKCS1
// or PKCS8 RSA key. A file a group or other may read, write or execute
// is refused, so the key never sits where another account reads it.
func LoadKey(path string) (*rsa.PrivateKey, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("the GitHub App's private key: %w", err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("the GitHub App's private key %s is not a regular file", path)
	}
	if perm := info.Mode().Perm(); perm&0o077 != 0 {
		return nil, fmt.Errorf("the GitHub App's private key %s has mode %04o: only its owner may read it (chmod 600)", path, perm)
	}
	data, err := os.ReadFile(path) //nolint:gosec // G304: the operator's workflow names the key file
	if err != nil {
		return nil, fmt.Errorf("the GitHub App's private key: %w", err)
	}
	return ParseKey(data)
}

// ParseKey reads a PEM PKCS1 or PKCS8 RSA private key. Its errors
// never quote the key.
func ParseKey(data []byte) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode(data)
	if block == nil {
		return nil, errors.New("the GitHub App's private key is not PEM")
	}
	switch block.Type {
	case "RSA PRIVATE KEY":
		k, err := x509.ParsePKCS1PrivateKey(block.Bytes)
		if err != nil {
			return nil, errors.New("the GitHub App's private key is not a valid PKCS #1 RSA key")
		}
		return k, nil
	case "PRIVATE KEY":
		k, err := x509.ParsePKCS8PrivateKey(block.Bytes)
		if err != nil {
			return nil, errors.New("the GitHub App's private key is not a valid PKCS #8 key")
		}
		rk, ok := k.(*rsa.PrivateKey)
		if !ok {
			return nil, errors.New("the GitHub App's private key is not an RSA key")
		}
		return rk, nil
	}
	return nil, fmt.Errorf("the GitHub App's private key is a PEM %q block, not an RSA private key", block.Type)
}

// KeyDigest is a digest of the key that seals stored tokens: anyone who
// holds the key can mint tokens anyway, so a copy of the store alone
// gives none.
func KeyDigest(k *rsa.PrivateKey) [32]byte {
	return sha256.Sum256(append([]byte("sortie forge token store\x00"), x509.MarshalPKCS1PrivateKey(k)...))
}

// jwt returns the App's JSON Web Token: issued a minute in the past, for
// clock drift, and valid for nine minutes, under GitHub's ten.
func (c *Client) jwt() (string, error) {
	now := c.now().Unix()
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"RS256","typ":"JWT"}`))
	claims, err := json.Marshal(map[string]any{"iat": now - 60, "exp": now + 540, "iss": c.appID})
	if err != nil {
		return "", err
	}
	signing := header + "." + base64.RawURLEncoding.EncodeToString(claims)
	sum := sha256.Sum256([]byte(signing))
	sig, err := rsa.SignPKCS1v15(rand.Reader, c.key, crypto.SHA256, sum[:])
	if err != nil {
		return "", fmt.Errorf("sign the GitHub App's token: %w", err)
	}
	return signing + "." + base64.RawURLEncoding.EncodeToString(sig), nil
}

// do sends a request with bearer and returns the status and body.
func (c *Client) do(ctx context.Context, method, path, bearer string, body any) (int, []byte, error) {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.api.String()+path, rd)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", apiVersion)
	req.Header.Set("Authorization", "Bearer "+bearer)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, nil, fmt.Errorf("%s %s: %w", method, path, scrub(err))
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxBody))
	if err != nil {
		return resp.StatusCode, nil, fmt.Errorf("%s %s: read the answer: %w", method, path, err)
	}
	return resp.StatusCode, data, nil
}

// scrub drops the request URL from a transport error, which never holds
// a token here but may hold the API's userinfo.
func scrub(err error) error {
	if ue, ok := errors.AsType[*url.Error](err); ok {
		return ue.Err
	}
	return err
}

// message returns GitHub's message of an error answer, cut short.
func message(data []byte) string {
	var m struct {
		Message string `json:"message"`
	}
	if json.Unmarshal(data, &m) != nil || m.Message == "" {
		return ""
	}
	if len(m.Message) > 200 {
		return m.Message[:200]
	}
	return m.Message
}

// Installation returns the ID of the App's installation on owner, from
// the installations read once and kept. An owner not among them reads
// them again, at most once a minute, before it is refused with
// [ErrNotInstalled].
func (c *Client) Installation(ctx context.Context, owner string) (int64, error) {
	key := strings.ToLower(owner)
	c.mu.Lock()
	id, ok := c.installations[key]
	stale := c.installations == nil || c.now().Sub(c.readAt) >= installationsRefresh
	c.mu.Unlock()
	if ok {
		return id, nil
	}
	if !stale {
		return 0, &Error{Owner: owner, Err: ErrNotInstalled}
	}
	found, err := c.readInstallations(ctx)
	if err != nil {
		return 0, err
	}
	c.mu.Lock()
	c.installations, c.readAt = found, c.now()
	id, ok = found[key]
	c.mu.Unlock()
	if !ok {
		return 0, &Error{Owner: owner, Err: ErrNotInstalled}
	}
	return id, nil
}

// readInstallations reads every installation of the App, by account.
func (c *Client) readInstallations(ctx context.Context) (map[string]int64, error) {
	jwt, err := c.jwt()
	if err != nil {
		return nil, err
	}
	out := map[string]int64{}
	for page := 1; ; page++ {
		status, data, err := c.do(ctx, http.MethodGet, "/app/installations?per_page=100&page="+strconv.Itoa(page), jwt, nil)
		if err != nil {
			return nil, err
		}
		if status != http.StatusOK {
			return nil, fmt.Errorf("GET /app/installations: HTTP %d %s", status, message(data))
		}
		var list []struct {
			ID      int64 `json:"id"`
			Account struct {
				Login string `json:"login"`
			} `json:"account"`
		}
		if err := json.Unmarshal(data, &list); err != nil {
			return nil, fmt.Errorf("GET /app/installations: %w", err)
		}
		for _, inst := range list {
			if inst.Account.Login != "" {
				out[strings.ToLower(inst.Account.Login)] = inst.ID
			}
		}
		if len(list) < 100 {
			return out, nil
		}
	}
}

// Mint mints an installation access token on owner's installation,
// narrowed to exactly repositories, each a name without its owner, and
// permissions. GitHub refuses a repository or a permission the
// installation does not grant; Mint returns that as [ErrNotGranted].
func (c *Client) Mint(ctx context.Context, owner string, repositories []string, permissions map[string]string) (Token, error) {
	if len(repositories) == 0 || len(permissions) == 0 {
		return Token{}, fmt.Errorf("GitHub token for owner %s: a mint needs at least one repository and one permission", owner)
	}
	id, err := c.Installation(ctx, owner)
	if err != nil {
		return Token{}, err
	}
	jwt, err := c.jwt()
	if err != nil {
		return Token{}, err
	}
	repos := slices.Clone(repositories)
	slices.Sort(repos)
	path := "/app/installations/" + strconv.FormatInt(id, 10) + "/access_tokens"
	status, data, err := c.do(ctx, http.MethodPost, path, jwt, map[string]any{"repositories": repos, "permissions": permissions})
	if err != nil {
		return Token{}, err
	}
	switch status {
	case http.StatusCreated:
	case http.StatusUnprocessableEntity, http.StatusNotFound:
		if status == http.StatusNotFound {
			// The installation is gone: read the installations again.
			c.mu.Lock()
			c.installations = nil
			c.mu.Unlock()
		}
		return Token{}, &Error{Owner: owner, Err: ErrNotGranted, Detail: message(data)}
	default:
		return Token{}, fmt.Errorf("POST %s: HTTP %d %s", path, status, message(data))
	}
	var tok struct {
		Token     string    `json:"token"`
		ExpiresAt time.Time `json:"expires_at"`
	}
	if err := json.Unmarshal(data, &tok); err != nil {
		return Token{}, fmt.Errorf("POST %s: the answer is not a token", path)
	}
	if tok.Token == "" || tok.ExpiresAt.IsZero() {
		return Token{}, fmt.Errorf("POST %s: the answer names no token or no expiry", path)
	}
	return Token{Value: tok.Token, ExpiresAt: tok.ExpiresAt}, nil
}

// Revoke revokes token. A token GitHub no longer accepts, revoked or
// expired, is already what Revoke makes it, and returns nil.
func (c *Client) Revoke(ctx context.Context, token string) error {
	status, data, err := c.do(ctx, http.MethodDelete, "/installation/token", token, nil)
	if err != nil {
		return err
	}
	switch status {
	case http.StatusNoContent, http.StatusUnauthorized:
		return nil
	}
	return fmt.Errorf("DELETE /installation/token: HTTP %d %s", status, message(data))
}
