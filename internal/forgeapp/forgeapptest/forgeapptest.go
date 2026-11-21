// Package forgeapptest is a fake of GitHub's REST API for a GitHub App:
// GET /app/installations, POST /app/installations/{id}/access_tokens and
// DELETE /installation/token. It checks each App JSON Web Token's RS256
// signature, issuer and lifetime, refuses a repository or a permission an
// installation does not grant as GitHub does, and records every mint and
// revocation. Its tokens are long, in the stateless ghs_APPID_JWT form, so
// a test fails on code that assumes a token's length.
package forgeapptest

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// AppID is the fake App's ID.
const AppID = "424242"

// Installation is one account the App is installed on.
type Installation struct {
	ID    int64
	Owner string
	// Repositories are the names, without the owner, the installation
	// grants.
	Repositories []string
}

// Mint is one access token request the fake answered.
type Mint struct {
	Installation int64
	Owner        string
	Repositories []string
	Permissions  map[string]string
	Token        string
	Status       int
}

// Server is the fake API. Its zero value is not usable; call [New].
type Server struct {
	*httptest.Server
	Key *rsa.PrivateKey

	mu            sync.Mutex
	now           func() time.Time
	installations []Installation
	permissions   map[string]string
	tokens        map[string]token
	mints         []Mint
	revoked       []string
	jwtErrors     []string
	failRevokes   int
}

type token struct {
	owner   string
	expires time.Time
	revoked bool
}

// New starts the fake with the App's permissions and its installations,
// stopped at the test's end.
func New(t testing.TB, permissions map[string]string, installations ...Installation) *Server {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{Key: key, now: time.Now, installations: installations, permissions: permissions, tokens: map[string]token{}}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /app/installations", s.listInstallations)
	mux.HandleFunc("POST /app/installations/{id}/access_tokens", s.mint)
	mux.HandleFunc("DELETE /installation/token", s.revoke)
	mux.HandleFunc("GET /repos/{owner}/{repo}", s.repo)
	s.Server = httptest.NewServer(mux)
	t.Cleanup(s.Close)
	return s
}

// SetNow sets the fake's clock, for token expiry.
func (s *Server) SetNow(now func() time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.now = now
}

// Install adds an installation, as an operator installing the App.
func (s *Server) Install(inst Installation) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.installations = append(s.installations, inst)
}

// KeyFile writes the App's private key, PKCS1 PEM, to a file of mode
// mode in dir and returns its path.
func (s *Server) KeyFile(t testing.TB, dir string, mode os.FileMode) string {
	t.Helper()
	path := filepath.Join(dir, "github-app.pem")
	data := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(s.Key)})
	if err := os.WriteFile(path, data, mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
	return path
}

// KeyPEM is the App's private key, PKCS1 PEM, as a test greps logs for.
func (s *Server) KeyPEM() string {
	return string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(s.Key)}))
}

// FailRevokes makes the next n revocations answer 502, as an outage of
// GitHub's API.
func (s *Server) FailRevokes(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failRevokes = n
}

// Mints returns every access token request, in order.
func (s *Server) Mints() []Mint {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.mints)
}

// Revoked returns every token revoked, in order.
func (s *Server) Revoked() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.revoked)
}

// Tokens returns every token the fake minted.
func (s *Server) Tokens() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.tokens))
	for v := range s.tokens {
		out = append(out, v)
	}
	slices.Sort(out)
	return out
}

// Valid reports whether GitHub would accept token now: minted, not
// revoked, not expired.
func (s *Server) Valid(tok string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tokens[tok]
	return ok && !t.revoked && s.now().Before(t.expires)
}

// JWTErrors returns why each refused App token was refused.
func (s *Server) JWTErrors() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.jwtErrors)
}

func reply(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// appJWT checks the request's App token and reports whether it passes.
func (s *Server) appJWT(w http.ResponseWriter, r *http.Request) bool {
	why := s.checkJWT(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
	if why == "" {
		return true
	}
	s.mu.Lock()
	s.jwtErrors = append(s.jwtErrors, why)
	s.mu.Unlock()
	reply(w, http.StatusUnauthorized, map[string]string{"message": "A JSON web token could not be decoded"})
	return false
}

func (s *Server) checkJWT(jwt string) string {
	parts := strings.Split(jwt, ".")
	if len(parts) != 3 {
		return "not three parts"
	}
	head, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil || string(head) != `{"alg":"RS256","typ":"JWT"}` {
		return "bad header " + string(head)
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return "bad signature encoding"
	}
	sum := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if rsa.VerifyPKCS1v15(&s.Key.PublicKey, crypto.SHA256, sum[:], sig) != nil {
		return "signature does not verify"
	}
	body, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "bad claims encoding"
	}
	var c struct {
		Iat int64  `json:"iat"`
		Exp int64  `json:"exp"`
		Iss string `json:"iss"`
	}
	if json.Unmarshal(body, &c) != nil {
		return "bad claims"
	}
	s.mu.Lock()
	now := s.now().Unix()
	s.mu.Unlock()
	switch {
	case c.Iss != AppID:
		return "issuer " + c.Iss
	case c.Exp-c.Iat > 600:
		return "lifetime over ten minutes"
	case c.Iat > now+60 || c.Exp < now:
		return "not valid now"
	}
	return ""
}

func (s *Server) listInstallations(w http.ResponseWriter, r *http.Request) {
	if !s.appJWT(w, r) {
		return
	}
	perPage, _ := strconv.Atoi(r.URL.Query().Get("per_page"))
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if perPage <= 0 {
		perPage = 30
	}
	if page <= 0 {
		page = 1
	}
	s.mu.Lock()
	all := slices.Clone(s.installations)
	s.mu.Unlock()
	out := []map[string]any{}
	for i, inst := range all {
		if i >= (page-1)*perPage && i < page*perPage {
			out = append(out, map[string]any{"id": inst.ID, "account": map[string]any{"login": inst.Owner}})
		}
	}
	reply(w, http.StatusOK, out)
}

func (s *Server) mint(w http.ResponseWriter, r *http.Request) {
	if !s.appJWT(w, r) {
		return
	}
	id, _ := strconv.ParseInt(r.PathValue("id"), 10, 64)
	var body struct {
		Repositories []string          `json:"repositories"`
		Permissions  map[string]string `json:"permissions"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		reply(w, http.StatusBadRequest, map[string]string{"message": "Problems parsing JSON"})
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	m := Mint{Installation: id, Repositories: body.Repositories, Permissions: body.Permissions}
	var inst *Installation
	for i := range s.installations {
		if s.installations[i].ID == id {
			inst = &s.installations[i]
		}
	}
	if inst == nil {
		m.Status = http.StatusNotFound
		s.mints = append(s.mints, m)
		reply(w, m.Status, map[string]string{"message": "Not Found"})
		return
	}
	m.Owner = inst.Owner
	for _, repo := range body.Repositories {
		if !slices.Contains(inst.Repositories, repo) {
			m.Status = http.StatusUnprocessableEntity
			s.mints = append(s.mints, m)
			reply(w, m.Status, map[string]string{"message": "There is at least one repository that does not exist or is not accessible to the parent installation."})
			return
		}
	}
	for p, level := range body.Permissions {
		if have, ok := s.permissions[p]; !ok || (have == "read" && level == "write") {
			m.Status = http.StatusUnprocessableEntity
			s.mints = append(s.mints, m)
			reply(w, m.Status, map[string]string{"message": "The permissions requested are not granted to this installation."})
			return
		}
	}
	raw := make([]byte, 96)
	_, _ = rand.Read(raw)
	value := "ghs_" + AppID + "_" + base64.RawURLEncoding.EncodeToString(raw) + "." + hex.EncodeToString(raw[:24])
	expires := s.now().Add(time.Hour).UTC().Truncate(time.Second)
	s.tokens[value] = token{owner: inst.Owner, expires: expires}
	m.Token, m.Status = value, http.StatusCreated
	s.mints = append(s.mints, m)
	reply(w, http.StatusCreated, map[string]any{"token": value, "expires_at": expires.Format(time.RFC3339),
		"permissions": body.Permissions, "repository_selection": "selected"})
}

func (s *Server) revoke(w http.ResponseWriter, r *http.Request) {
	value := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failRevokes > 0 {
		s.failRevokes--
		reply(w, http.StatusBadGateway, map[string]string{"message": "Server Error"})
		return
	}
	t, ok := s.tokens[value]
	if !ok || t.revoked || !s.now().Before(t.expires) {
		reply(w, http.StatusUnauthorized, map[string]string{"message": "Bad credentials"})
		return
	}
	t.revoked = true
	s.tokens[value] = t
	s.revoked = append(s.revoked, value)
	w.WriteHeader(http.StatusNoContent)
}

// repo answers a repository read with a token, 401 for one GitHub would
// refuse: what a job's call with a revoked token gets.
func (s *Server) repo(w http.ResponseWriter, r *http.Request) {
	value := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	s.mu.Lock()
	t, ok := s.tokens[value]
	valid := ok && !t.revoked && s.now().Before(t.expires)
	s.mu.Unlock()
	if !valid {
		reply(w, http.StatusUnauthorized, map[string]string{"message": "Bad credentials"})
		return
	}
	reply(w, http.StatusOK, map[string]string{"full_name": r.PathValue("owner") + "/" + r.PathValue("repo")})
}
