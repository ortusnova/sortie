package forgetoken

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// GatePath is the egress gate's registration endpoint, under its base
// URL. POST registers an attempt's tokens, replacing any it held, with a
// JSON [Registration]; DELETE with the query job and attempt forgets
// them. Both carry Sortie's control token as a bearer token, the token
// the gate already holds to ask Sortie what a job token grants.
const GatePath = "/v1/forge-tokens"

// HTTPGate is the egress gate's registration endpoint over HTTP.
type HTTPGate struct {
	base  string
	token string
	http  *http.Client
}

// NewHTTPGate returns the client of the gate at base, authenticated with
// token.
func NewHTTPGate(base, token string, client *http.Client) (*HTTPGate, error) {
	u, err := url.Parse(base)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil {
		return nil, fmt.Errorf("the egress gate's URL %q is not an absolute http or https URL without user information", base)
	}
	if token == "" {
		return nil, errors.New("the egress gate needs Sortie's control token")
	}
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	return &HTTPGate{base: strings.TrimRight(base, "/"), token: token, http: client}, nil
}

// Register implements [Gate].
func (g *HTTPGate) Register(ctx context.Context, reg Registration) error {
	body, err := json.Marshal(reg)
	if err != nil {
		return err
	}
	return g.send(ctx, http.MethodPost, g.base+GatePath, body)
}

// Unregister implements [Gate].
func (g *HTTPGate) Unregister(ctx context.Context, job string, attempt int) error {
	q := url.Values{"job": {job}, "attempt": {strconv.Itoa(attempt)}}
	return g.send(ctx, http.MethodDelete, g.base+GatePath+"?"+q.Encode(), nil)
}

func (g *HTTPGate) send(ctx context.Context, method, target string, body []byte) error {
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, target, rd)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+g.token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := g.http.Do(req)
	if err != nil {
		if ue, ok := errors.AsType[*url.Error](err); ok {
			err = ue.Err
		}
		return fmt.Errorf("%s %s: %w", method, GatePath, err)
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("%s %s: HTTP %d", method, GatePath, resp.StatusCode)
	}
	return nil
}
