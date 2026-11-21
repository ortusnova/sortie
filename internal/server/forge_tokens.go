package server

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/sortie-ai/sortie/internal/forgeapp"
	"github.com/sortie-ai/sortie/internal/forgetoken"
	"github.com/sortie-ai/sortie/internal/orchestrator"
)

// ForgeSandboxMintFunc mints a sandbox's GitHub tokens for the named
// repository: [orchestrator.Orchestrator.MintForgeSandboxFunc].
type ForgeSandboxMintFunc func(ctx context.Context, repository string) (orchestrator.ForgeSandbox, error)

// ForgeSandboxEndFunc revokes a sandbox's set by its ID:
// [orchestrator.Orchestrator.EndForgeSandboxFunc].
type ForgeSandboxEndFunc func(ctx context.Context, id string) error

// forgeTokensRequest is the body of POST /api/v1/forge-tokens.
type forgeTokensRequest struct {
	Repository string `json:"repository"`
}

// forgeTokenResponse is one token of a sandbox set: its owner, the
// token, when GitHub expires it, and what it reaches.
type forgeTokenResponse struct {
	Owner        string            `json:"owner"`
	Token        string            `json:"token"`
	ExpiresAt    time.Time         `json:"expires_at"`
	Repositories []string          `json:"repositories"`
	Permissions  map[string]string `json:"permissions"`
}

// forgeTokensSourceResponse is where the declaration was read.
type forgeTokensSourceResponse struct {
	Repository string `json:"repository"`
	Branch     string `json:"branch"`
	Commit     string `json:"commit"`
	File       string `json:"file"`
	Declared   bool   `json:"declared"`
}

// forgeTokensResponse answers POST /api/v1/forge-tokens. ID is empty,
// and Tokens empty, when the declaration grants nothing.
type forgeTokensResponse struct {
	ID        string                    `json:"id"`
	Source    forgeTokensSourceResponse `json:"source"`
	ExpiresAt *time.Time                `json:"expires_at"`
	Tokens    []forgeTokenResponse      `json:"tokens"`
}

// handleForgeTokens serves /api/v1/forge-tokens, for the operator only:
// POST mints a sandbox's GitHub tokens, and DELETE with the query id
// revokes them.
func (s *Server) handleForgeTokens(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		s.forgeTokensGuard(s.serveForgeTokensMint)(w, r)
	case http.MethodDelete:
		s.forgeTokensGuard(s.serveForgeTokensEnd)(w, r)
	default:
		s.methodNotAllowed(w, r, http.MethodPost, http.MethodDelete)
	}
}

// forgeTokensGuard admits the control token only. A Sortie with no
// control token refuses every caller here, unlike its other control
// routes: this one hands out credentials.
func (s *Server) forgeTokensGuard(next http.HandlerFunc) http.HandlerFunc {
	guarded := s.requireControl(scopeOperator, next)
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		if s.controlToken == "" {
			s.rejectControl(w, r, http.StatusForbidden, "forbidden", "GitHub tokens are served only with server.control_token set")
			return
		}
		guarded(w, r)
	}
}

func (s *Server) serveForgeTokensMint(w http.ResponseWriter, r *http.Request) {
	if s.forgeSandboxMintFn == nil {
		writeErrorJSON(w, s.logger, http.StatusServiceUnavailable, "orchestrator_unavailable", "GitHub tokens are not available")
		return
	}
	var req forgeTokensRequest
	if err := decodeControlBody(r, &req); err != nil || strings.TrimSpace(req.Repository) == "" {
		writeErrorJSON(w, s.logger, http.StatusBadRequest, "invalid_request", "request body must be a JSON object naming the repository")
		return
	}
	box, err := s.forgeSandboxMintFn(r.Context(), req.Repository)
	var refusal *orchestrator.ForgeRefusal
	var appErr *forgeapp.Error
	switch {
	case err == nil:
	case errors.Is(err, orchestrator.ErrForgeTokensOff):
		writeErrorJSON(w, s.logger, http.StatusServiceUnavailable, "forge_tokens_off", err.Error())
		return
	case errors.Is(err, orchestrator.ErrUnknownRepository):
		writeErrorJSON(w, s.logger, http.StatusNotFound, "unknown_repository", err.Error())
		return
	case errors.As(err, &refusal):
		writeErrorJSON(w, s.logger, http.StatusConflict, "forge_refused", err.Error())
		return
	case errors.As(err, &appErr):
		writeErrorJSON(w, s.logger, http.StatusUnprocessableEntity, appErr.FailureClass(), err.Error())
		return
	default:
		writeErrorJSON(w, s.logger, http.StatusBadGateway, "forge_unavailable", err.Error())
		return
	}
	resp := forgeTokensResponse{ID: box.ID, Tokens: []forgeTokenResponse{},
		Source: forgeTokensSourceResponse{Repository: box.Source.Repository, Branch: box.Source.Branch, Commit: box.Source.Commit,
			File: box.Source.File, Declared: box.Source.Declared}}
	if !box.Set.ExpiresAt.IsZero() {
		expires := box.Set.ExpiresAt.UTC()
		resp.ExpiresAt = &expires
	}
	for _, t := range box.Set.Tokens {
		resp.Tokens = append(resp.Tokens, forgeTokenResponse{Owner: t.Owner, Token: t.Value, ExpiresAt: t.ExpiresAt.UTC(),
			Repositories: nonNil(t.Repositories), Permissions: t.Permissions})
	}
	writeJSON(w, s.logger, http.StatusCreated, resp)
	s.logger.Info("sandbox GitHub tokens served", slog.String("repository", req.Repository), slog.String("set", box.ID),
		slog.Int("tokens", len(resp.Tokens)))
}

func (s *Server) serveForgeTokensEnd(w http.ResponseWriter, r *http.Request) {
	if s.forgeSandboxEndFn == nil {
		writeErrorJSON(w, s.logger, http.StatusServiceUnavailable, "orchestrator_unavailable", "GitHub tokens are not available")
		return
	}
	id := r.URL.Query().Get("id")
	err := s.forgeSandboxEndFn(r.Context(), id)
	switch {
	case err == nil:
		w.WriteHeader(http.StatusNoContent)
		s.logger.Info("sandbox GitHub tokens revoked", slog.String("set", id))
	case errors.Is(err, forgetoken.ErrNotFound):
		writeErrorJSON(w, s.logger, http.StatusNotFound, "not_found", "no such sandbox token set")
	case errors.Is(err, orchestrator.ErrForgeTokensOff):
		writeErrorJSON(w, s.logger, http.StatusServiceUnavailable, "forge_tokens_off", err.Error())
	default:
		writeErrorJSON(w, s.logger, http.StatusBadGateway, "forge_unavailable", err.Error())
	}
}
