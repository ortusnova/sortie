package server

import (
	"net/http"

	"github.com/sortie-ai/sortie/internal/jobtoken"
	"github.com/sortie-ai/sortie/internal/worker"
)

// workerRoutes are the worker API routes of protocol 1, all served by
// [Server.routeWorker].
var workerRoutes = []string{
	"/api/v1/workers/checkin",
	"/api/v1/workers/poll",
	"/api/v1/workers/jobs/{job}/attempts/{attempt}/heartbeat",
	"/api/v1/workers/jobs/{job}/attempts/{attempt}/report",
	"/api/v1/workers/jobs/{job}/attempts/{attempt}/resume",
}

// routeWorker serves the worker API while the remote environment kind
// is selected, and 404 otherwise.
func (s *Server) routeWorker(w http.ResponseWriter, r *http.Request) {
	var m *worker.Matcher
	if s.workersFn != nil {
		m = s.workersFn()
	}
	if m == nil {
		writeErrorJSON(w, s.logger, http.StatusNotFound, "not_found",
			"the worker API is served only under environment.kind remote")
		return
	}
	header := ""
	if s.workerHeaderFn != nil {
		header = s.workerHeaderFn()
	}
	worker.Handler(m, header).ServeHTTP(w, r)
}

// workersState returns the workers and leases GET /api/v1/state lists,
// or nil while the remote environment kind is not selected.
func (s *Server) workersState() *worker.State {
	if s.workersFn == nil {
		return nil
	}
	m := s.workersFn()
	if m == nil {
		return nil
	}
	return new(m.Snapshot())
}

// handleIntrospect tells a proxy what a job token grants. Its route
// takes the control token, so a worker cannot ask.
func (s *Server) handleIntrospect(w http.ResponseWriter, r *http.Request) {
	var issuer *jobtoken.Issuer
	if s.jobTokensFn != nil {
		issuer = s.jobTokensFn()
	}
	if issuer == nil {
		writeErrorJSON(w, s.logger, http.StatusNotFound, "not_found", "unknown or ended job token")
		return
	}
	issuer.IntrospectHandler().ServeHTTP(w, r)
}

// routeDataPlane serves the data-plane flows Sortie answers itself while
// the remote environment kind is selected, and 404 otherwise. Each
// request authenticates with its flow's job token, not the control
// token.
func (s *Server) routeDataPlane(w http.ResponseWriter, r *http.Request) {
	var h http.Handler
	if s.dataPlaneFn != nil {
		h = s.dataPlaneFn()
	}
	if h == nil {
		writeErrorJSON(w, s.logger, http.StatusNotFound, "not_found",
			"the data plane is served only under environment.kind remote")
		return
	}
	h.ServeHTTP(w, r)
}
