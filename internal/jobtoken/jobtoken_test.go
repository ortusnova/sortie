package jobtoken

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

const (
	flowGit Flow = "git"
	flowLLM Flow = "llm"
)

func mint(t *testing.T, i *Issuer, job string) map[Flow]Token {
	t.Helper()
	toks, err := i.OnLeaseGrant(job)
	if err != nil {
		t.Fatalf("OnLeaseGrant(%q): %v", job, err)
	}
	return toks
}

func TestTakingAJobMintsOneTokenPerFlow(t *testing.T) {
	t.Parallel()
	i := NewIssuer(flowGit, flowLLM)
	toks := mint(t, i, "job-a")
	if len(toks) != 2 {
		t.Fatalf("got %d tokens, want 2", len(toks))
	}
	if toks[flowGit].Reveal() == "" || toks[flowGit].Reveal() == toks[flowLLM].Reveal() {
		t.Fatal("flows must get distinct non-empty tokens")
	}
	for f, tok := range toks {
		g, err := i.Check(tok.Reveal(), "job-a", f)
		if err != nil || g.Flow != f || g.JobID != "job-a" || g.Attempt != 1 {
			t.Fatalf("Check(%s) = %+v, %v", f, g, err)
		}
	}
	other := mint(t, i, "job-b")
	if other[flowGit].Reveal() == toks[flowGit].Reveal() {
		t.Fatal("two jobs share a token")
	}
}

func TestDataPlaneRequestsAreChecked(t *testing.T) {
	t.Parallel()
	i := NewIssuer(flowGit, flowLLM)
	a := mint(t, i, "job-a")
	mint(t, i, "job-b")

	reached := 0
	h := i.Require(flowGit, func(r *http.Request) string { return r.PathValue("job") },
		http.HandlerFunc(func(http.ResponseWriter, *http.Request) { reached++ }))
	mux := http.NewServeMux()
	mux.Handle("/git/{job}", h)

	cases := []struct {
		name, job, auth string
		want            int
	}{
		{"own job and flow", "job-a", "Bearer " + a[flowGit].Reveal(), http.StatusOK},
		{"refused for another job", "job-b", "Bearer " + a[flowGit].Reveal(), http.StatusForbidden},
		{"refused for another flow", "job-a", "Bearer " + a[flowLLM].Reveal(), http.StatusForbidden},
		{"missing token", "job-a", "", http.StatusUnauthorized},
		{"unknown token", "job-a", "Bearer nope", http.StatusUnauthorized},
	}
	for _, c := range cases {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/git/"+c.job, nil)
		if c.auth != "" {
			req.Header.Set("Authorization", c.auth)
		}
		mux.ServeHTTP(rec, req)
		if rec.Code != c.want {
			t.Errorf("%s: status %d, want %d", c.name, rec.Code, c.want)
		}
	}
	if reached != 1 {
		t.Fatalf("handler reached %d times, want 1", reached)
	}
	if _, err := i.Check(a[flowGit].Reveal(), "job-b", flowGit); !errors.Is(err, ErrWrongJob) {
		t.Fatalf("Check for another job = %v, want ErrWrongJob", err)
	}
}

func TestTokensEndWithTheAttempt(t *testing.T) {
	t.Parallel()
	i := NewIssuer(flowGit)
	first := mint(t, i, "job-a")
	i.OnLeaseEnd("job-a")
	if _, err := i.Check(first[flowGit].Reveal(), "job-a", flowGit); !errors.Is(err, ErrInvalid) {
		t.Fatalf("after end: %v, want ErrInvalid", err)
	}

	second := mint(t, i, "job-a")
	third := mint(t, i, "job-a")
	if _, err := i.Check(second[flowGit].Reveal(), "job-a", flowGit); !errors.Is(err, ErrInvalid) {
		t.Fatalf("earlier attempt token: %v, want ErrInvalid", err)
	}
	g, err := i.Check(third[flowGit].Reveal(), "job-a", flowGit)
	if err != nil || g.Attempt != 3 {
		t.Fatalf("live attempt: %+v, %v", g, err)
	}
}

// A token of an attempt that ended is known as ended, with its grant,
// until the job's next attempt ends; one never minted is not.
func TestEndedTokensAreKnown(t *testing.T) {
	t.Parallel()
	i := NewIssuer(flowGit)
	first := mint(t, i, "job-a")
	if _, ok := i.Ended(first[flowGit].Reveal()); ok {
		t.Fatal("a live token is known as ended")
	}
	i.OnLeaseEnd("job-a")
	g, ok := i.Ended(first[flowGit].Reveal())
	if !ok || g.JobID != "job-a" || g.Attempt != 1 || g.Flow != flowGit {
		t.Fatalf("ended token: %+v %v, want job-a attempt 1", g, ok)
	}
	second := mint(t, i, "job-a")
	i.OnLeaseEnd("job-a")
	if _, ok := i.Ended(first[flowGit].Reveal()); ok {
		t.Error("the first attempt's token is still kept once the second ended")
	}
	if g, ok := i.Ended(second[flowGit].Reveal()); !ok || g.Attempt != 2 {
		t.Errorf("second attempt's token: %+v %v", g, ok)
	}
	if _, ok := i.Ended("never-minted"); ok {
		t.Error("a token never minted is known as ended")
	}
}

func TestMintBindsTheCallersAttemptAndFlows(t *testing.T) {
	t.Parallel()
	i := NewIssuer(flowGit)
	first, err := i.Mint("job-a", 4, []Flow{flowLLM, "mcp"})
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 2 || first[flowLLM].Reveal() == "" || first[flowLLM].Reveal() == first["mcp"].Reveal() {
		t.Fatalf("tokens %v", first)
	}
	g, err := i.Check(first["mcp"].Reveal(), "job-a", "mcp")
	if err != nil || g != (Grant{JobID: "job-a", Attempt: 4, Flow: "mcp"}) {
		t.Fatalf("grant %+v, %v", g, err)
	}
	second, err := i.Mint("job-a", 5, []Flow{flowLLM})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := i.Lookup(first[flowLLM].Reveal()); !errors.Is(err, ErrInvalid) {
		t.Fatalf("earlier attempt token: %v, want ErrInvalid", err)
	}
	if g, err := i.Lookup(second[flowLLM].Reveal()); err != nil || g.Attempt != 5 {
		t.Fatalf("live attempt %+v, %v", g, err)
	}
	i.OnLeaseEnd("job-a")
	if _, err := i.Lookup(second[flowLLM].Reveal()); !errors.Is(err, ErrInvalid) {
		t.Fatalf("ended attempt token: %v, want ErrInvalid", err)
	}
}

func TestTokensAreNotKeptOrShownInClear(t *testing.T) {
	t.Parallel()
	i := NewIssuer(flowGit)
	tok := mint(t, i, "job-a")[flowGit]
	secret := tok.Reveal()

	var buf bytes.Buffer
	slog.New(slog.NewJSONHandler(&buf, nil)).Info("minted", slog.Any("token", tok))
	js, _ := json.Marshal(map[string]Token{"t": tok})
	shown := []string{
		fmt.Sprint(tok), fmt.Sprintf("%v %+v %#v %s", tok, tok, tok, tok),
		buf.String(), string(js),
	}
	for _, s := range shown {
		if strings.Contains(s, secret) {
			t.Fatalf("secret shown in clear: %q", s)
		}
	}

	i.mu.Lock()
	defer i.mu.Unlock()
	if strings.Contains(fmt.Sprintf("%#v", i.grants), secret) {
		t.Fatal("issuer keeps the secret in clear")
	}
	if _, ok := i.grants[hash(secret)]; !ok {
		t.Fatal("issuer does not keep the digest")
	}
}

func TestProxiesAskWhatATokenGrants(t *testing.T) {
	t.Parallel()
	i := NewIssuer(flowGit, flowLLM)
	tok := mint(t, i, "job-a")[flowLLM]
	srv := httptest.NewServer(i.IntrospectHandler())
	defer srv.Close()

	ask := func(body string) (int, string) {
		req, _ := http.NewRequest(http.MethodPost, srv.URL, strings.NewReader(body))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		var b bytes.Buffer
		_, _ = b.ReadFrom(resp.Body)
		return resp.StatusCode, b.String()
	}

	code, body := ask(`{"token":"` + tok.Reveal() + `"}`)
	if code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
	if strings.Contains(body, tok.Reveal()) {
		t.Fatal("introspection echoes the token")
	}
	var g map[string]any
	if err := json.Unmarshal([]byte(body), &g); err != nil {
		t.Fatal(err)
	}
	if want := map[string]any{"job": "job-a", "attempt": float64(1), "flow": "llm"}; !reflect.DeepEqual(g, want) {
		t.Fatalf("grant %v, want %v", g, want)
	}

	for _, b := range []string{`{"token":"bogus"}`, `{}`, `not json`} {
		if code, _ := ask(b); code != http.StatusNotFound {
			t.Fatalf("%s: status %d, want 404", b, code)
		}
	}
	i.OnLeaseEnd("job-a")
	if code, _ := ask(`{"token":"` + tok.Reveal() + `"}`); code != http.StatusNotFound {
		t.Fatalf("ended token status %d", code)
	}
}

// A restarted process given an attempt's digests admits the tokens the
// worker already holds, for that attempt only, and nothing it never
// minted.
func TestRestoredDigestsAdmitTheWorkersTokens(t *testing.T) {
	t.Parallel()
	before := NewIssuer()
	toks, err := before.Mint("ONS-30", 2, []Flow{flowGit, flowLLM})
	if err != nil {
		t.Fatal(err)
	}
	attempt, digests := before.Digests("ONS-30")
	if attempt != 2 || len(digests) != 2 {
		t.Fatalf("Digests = %d %v, want attempt 2 and two flows", attempt, digests)
	}
	for _, d := range digests {
		if strings.Contains(d, toks[flowGit].Reveal()) || strings.Contains(d, toks[flowLLM].Reveal()) {
			t.Fatalf("a digest holds a token in clear: %q", d)
		}
	}

	after := NewIssuer()
	if err := after.Restore("ONS-30", attempt, digests); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	for f, tok := range toks {
		g, err := after.Check(tok.Reveal(), "ONS-30", f)
		if err != nil || g.Attempt != 2 || g.Flow != f {
			t.Fatalf("Check(%s) after Restore = %+v, %v", f, g, err)
		}
	}
	if _, err := after.Lookup("never-minted"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("Lookup of an unknown token = %v, want ErrInvalid", err)
	}
	after.OnLeaseEnd("ONS-30")
	if _, err := after.Lookup(toks[flowGit].Reveal()); !errors.Is(err, ErrInvalid) {
		t.Fatalf("Lookup after the lease ended = %v, want ErrInvalid", err)
	}
	if err := after.Restore("ONS-30", 1, map[Flow]string{flowGit: "not-hex"}); err == nil {
		t.Fatal("Restore accepted a malformed digest")
	}
}
