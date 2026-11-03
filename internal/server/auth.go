package server

import (
	"context"
	"crypto/subtle"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// SessionTokenFunc reports the issue identifier of the running session
// a session token was minted for, and false when no running session
// holds it. It must be safe to call from any goroutine.
type SessionTokenFunc func(token string) (identifier string, ok bool)

// controlScope says which callers a state-changing route admits besides
// the holder of the control token.
type controlScope int

const (
	// scopeOperator admits only the control token, and every caller when
	// none is configured.
	scopeOperator controlScope = iota

	// scopeSession also admits the session token minted for the issue
	// the request path names.
	scopeSession

	// scopeSupervisor also admits the supervisor token.
	scopeSupervisor
)

func constantTimeEqual(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

// bearerToken returns the credential of an "Authorization: Bearer"
// header, and false when the request carries none.
func bearerToken(r *http.Request) (string, bool) {
	header := r.Header.Get("Authorization")
	if header == "" {
		return "", false
	}
	scheme, token, found := strings.Cut(header, " ")
	if !found || !strings.EqualFold(scheme, "Bearer") {
		return "", true
	}
	return strings.TrimSpace(token), true
}

// requireControl wraps a state-changing handler with Sortie's control
// authentication:
//
//   - The control token is accepted on every wrapped route.
//   - A session token is accepted only on a [scopeSession] route whose
//     path names the issue it was minted for, and answered 403 elsewhere.
//   - The supervisor token is accepted only on a [scopeSupervisor] route,
//     and answered 403 elsewhere; either way the request is recorded.
//   - Any other presented credential is answered 401.
//   - A request with no credential is accepted only when no control
//     token is configured, which startup allows only on a loopback bind.
//
// A rejected request never reaches next.
func (s *Server) requireControl(scope controlScope, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token, presented := bearerToken(r)
		switch {
		case !presented:
			if s.controlToken == "" {
				next(w, r)
				return
			}
			s.rejectControl(w, r, http.StatusUnauthorized, "unauthorized", "a bearer token is required")
			return
		case token == "":
			s.rejectControl(w, r, http.StatusUnauthorized, "unauthorized", "the Authorization header is not a bearer token")
			return
		case s.controlToken != "" && constantTimeEqual(token, s.controlToken):
			next(w, r)
			return
		case s.isSupervisorToken(token):
			if scope != scopeSupervisor {
				s.refuseSupervisor(w, r)
				return
			}
			next(w, r.WithContext(context.WithValue(r.Context(), supervisorCtxKey{}, &supervisorNote{})))
			return
		}

		identifier, ok := s.lookupSessionToken(token)
		if !ok {
			s.rejectControl(w, r, http.StatusUnauthorized, "unauthorized", "unknown or expired token")
			return
		}
		if scope != scopeSession || r.PathValue("identifier") != identifier {
			s.rejectControl(w, r, http.StatusForbidden, "forbidden", "a session token may only hold, release, or escalate its own issue, or report its broker")
			return
		}
		next(w, r)
	}
}

func (s *Server) lookupSessionToken(token string) (string, bool) {
	if s.sessionTokenFn == nil {
		return "", false
	}
	return s.sessionTokenFn(token)
}

func (s *Server) rejectControl(w http.ResponseWriter, r *http.Request, status int, code, message string) {
	start := time.Now()
	writeErrorJSON(w, s.logger, status, code, message)
	s.logger.Info("control request rejected",
		slog.String("method", r.Method),
		slog.String("path", r.URL.Path),
		slog.Int("status", status),
		slog.Duration("duration", time.Since(start)),
	)
}
