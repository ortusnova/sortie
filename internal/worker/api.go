package worker

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
)

// DefaultIdentityHeader is the request header that names the worker
// when worker.identity_header is unset.
const DefaultIdentityHeader = "X-Sortie-Worker-Identity"

// ProtocolHeader is the request header every worker API request
// carries with the protocol version it speaks.
const ProtocolHeader = "X-Sortie-Worker-Protocol"

// Prefix is the path every worker API route sits under.
const Prefix = "/api/v1/workers/"

// Handler serves worker API protocol 1. Workers dial Sortie; Sortie
// never dials a worker. Every request carries [ProtocolHeader]; the
// worker's identity is the value of the identityHeader request header,
// never a path segment or body field. A refused request changes no
// state.
//
//	POST /api/v1/workers/checkin                                  body Declaration; 200 {heartbeat_ms, lease_ms}
//	POST /api/v1/workers/poll                                     200 Offer or 204
//	POST /api/v1/workers/jobs/{job}/attempts/{attempt}/heartbeat  optional body Step; 200 {expires}
//	POST /api/v1/workers/jobs/{job}/attempts/{attempt}/report     body {outcome, reason, machine}; 204
//	POST /api/v1/workers/jobs/{job}/attempts/{attempt}/resume     200 Offer, with new tokens
//
// A heartbeat, report or resume answers 409 while the attempt is held by
// another worker or under a lease not yet swept, 410 for an attempt that
// ended or that Sortie does not hold, and 503 for a job Sortie does not
// know while it still adopts the runs it restored.
func Handler(m *Matcher, identityHeader string) http.Handler {
	if identityHeader == "" {
		identityHeader = DefaultIdentityHeader
	}
	mux := http.NewServeMux()
	identified := func(h func(w http.ResponseWriter, r *http.Request, id string)) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			id := r.Header.Get(identityHeader)
			if id == "" {
				problem(w, http.StatusUnauthorized, "missing_identity", "the "+identityHeader+" header names no worker")
				return
			}
			h(w, r, id)
		}
	}
	mux.HandleFunc("POST /api/v1/workers/checkin", identified(func(w http.ResponseWriter, r *http.Request, id string) {
		d, missing, invalid, err := decodeDeclaration(r)
		switch {
		case err != nil:
			problem(w, http.StatusBadRequest, "invalid_checkin", err.Error())
			return
		case len(missing) > 0 || len(invalid) > 0:
			msg := "check-in declaration"
			if len(missing) > 0 {
				msg += " is missing " + strings.Join(missing, ", ")
			}
			if len(invalid) > 0 {
				if len(missing) > 0 {
					msg += ";"
				}
				msg += " has invalid " + strings.Join(invalid, ", ")
			}
			body := errorBody("invalid_checkin", msg)
			body["missing"] = nonNil(missing)
			body["invalid"] = nonNil(invalid)
			reply(w, http.StatusBadRequest, body)
			return
		}
		m.CheckIn(id, d)
		heartbeat, lease := m.Timings()
		reply(w, http.StatusOK, map[string]int64{"heartbeat_ms": heartbeat.Milliseconds(), "lease_ms": lease.Milliseconds()})
	}))
	mux.HandleFunc("POST /api/v1/workers/poll", identified(func(w http.ResponseWriter, r *http.Request, id string) {
		offer, ok, err := m.Poll(id)
		if err != nil {
			fail(w, err)
			return
		}
		if !ok {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		reply(w, http.StatusOK, offer)
	}))
	mux.HandleFunc("POST /api/v1/workers/jobs/{job}/attempts/{attempt}/heartbeat", identified(func(w http.ResponseWriter, r *http.Request, id string) {
		attempt, err := strconv.Atoi(r.PathValue("attempt"))
		if err != nil {
			problem(w, http.StatusBadRequest, "invalid_attempt", "attempt must be an integer")
			return
		}
		exp, err := m.HeartbeatStep(id, r.PathValue("job"), attempt, decodeStep(r.Body))
		if err != nil {
			fail(w, err)
			return
		}
		reply(w, http.StatusOK, exp)
	}))
	mux.HandleFunc("POST /api/v1/workers/jobs/{job}/attempts/{attempt}/resume", identified(func(w http.ResponseWriter, r *http.Request, id string) {
		attempt, err := strconv.Atoi(r.PathValue("attempt"))
		if err != nil {
			problem(w, http.StatusBadRequest, "invalid_attempt", "attempt must be an integer")
			return
		}
		offer, err := m.Resume(id, r.PathValue("job"), attempt)
		if err != nil {
			fail(w, err)
			return
		}
		reply(w, http.StatusOK, offer)
	}))
	mux.HandleFunc("POST /api/v1/workers/jobs/{job}/attempts/{attempt}/report", identified(func(w http.ResponseWriter, r *http.Request, id string) {
		attempt, err := strconv.Atoi(r.PathValue("attempt"))
		if err != nil {
			problem(w, http.StatusBadRequest, "invalid_attempt", "attempt must be an integer")
			return
		}
		out, err := decodeReport(r)
		if err != nil {
			problem(w, http.StatusBadRequest, "invalid_report", err.Error())
			return
		}
		if err := m.Report(id, r.PathValue("job"), attempt, out); err != nil {
			fail(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if v := r.Header.Get(ProtocolHeader); v != strconv.Itoa(Protocol) {
			body := errorBody("unsupported_protocol", "worker API protocol "+strconv.Quote(v)+" is not served; supported versions: "+strconv.Itoa(Protocol)+"; send "+ProtocolHeader+": "+strconv.Itoa(Protocol))
			body["supported"] = []int{Protocol}
			reply(w, http.StatusBadRequest, body)
			return
		}
		mux.ServeHTTP(w, r)
	})
}

// decodeDeclaration reads a check-in body. It names, by dotted path,
// each field that is absent or null and each whose value is invalid.
func decodeDeclaration(r *http.Request) (Declaration, []string, []string, error) {
	var raw map[string]json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil || raw == nil {
		return Declaration{}, nil, nil, errors.New("check-in body is not a JSON object")
	}
	var d Declaration
	var missing, invalid []string
	field := func(obj map[string]json.RawMessage, key, path string, dst any) {
		v, ok := obj[key]
		if !ok || bytes.Equal(bytes.TrimSpace(v), []byte("null")) {
			missing = append(missing, path)
			return
		}
		if err := json.Unmarshal(v, dst); err != nil {
			invalid = append(invalid, path)
		}
	}
	field(raw, "os", "os", &d.OS)
	field(raw, "arch", "arch", &d.Arch)
	field(raw, "kvm", "kvm", &d.KVM)
	field(raw, "nested", "nested", &d.Nested)
	field(raw, "backends", "backends", &d.Backends)
	field(raw, "run_slots", "run_slots", &d.RunSlots)
	field(raw, "running", "running", &d.Running)
	var free map[string]json.RawMessage
	field(raw, "free", "free", &free)
	if free != nil {
		field(free, "cpu", "free.cpu", &d.Free.CPU)
		field(free, "mem_gib", "free.mem_gib", &d.Free.MemGiB)
		field(free, "disk_gib", "free.disk_gib", &d.Free.DiskGiB)
	}
	for _, n := range []struct {
		path string
		v    float64
	}{
		{"run_slots", float64(d.RunSlots)}, {"running", float64(d.Running)},
		{"free.cpu", d.Free.CPU}, {"free.mem_gib", d.Free.MemGiB}, {"free.disk_gib", d.Free.DiskGiB},
	} {
		if n.v < 0 {
			invalid = append(invalid, n.path)
		}
	}
	if d.Backends == nil {
		d.Backends = []string{}
	}
	decodeActivity(raw, &d)
	return d, missing, invalid, nil
}

// decodeReport reads a report body: outcome and machine are required,
// reason is free text.
func decodeReport(r *http.Request) (Outcome, error) {
	var body struct {
		Outcome *string `json:"outcome"`
		Reason  *string `json:"reason"`
		Machine *string `json:"machine"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		return Outcome{}, errors.New(`report body must be {"outcome", "reason", "machine"}`)
	}
	if body.Outcome == nil || (*body.Outcome != "succeeded" && *body.Outcome != "failed") {
		return Outcome{}, errors.New(`outcome must be "succeeded" or "failed"`)
	}
	if body.Machine == nil {
		return Outcome{}, errors.New(`machine must be "running", "stopped" or "deleted"`)
	}
	switch m := Machine(*body.Machine); m {
	case MachineRunning, MachineStopped, MachineDeleted:
	default:
		return Outcome{}, errors.New(`machine must be "running", "stopped" or "deleted"`)
	}
	out := Outcome{Succeeded: *body.Outcome == "succeeded", Machine: Machine(*body.Machine)}
	if body.Reason != nil {
		out.Reason = *body.Reason
	}
	return out, nil
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

func reply(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func errorBody(code, msg string) map[string]any {
	return map[string]any{"error": map[string]any{"code": code, "message": msg}}
}

func problem(w http.ResponseWriter, code int, kind, msg string) {
	reply(w, code, errorBody(kind, msg))
}

func fail(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrUnknownWorker):
		problem(w, http.StatusConflict, "not_checked_in", "check in before polling")
	case errors.Is(err, ErrLeaseLost), errors.Is(err, ErrUnknownLease):
		problem(w, http.StatusConflict, "lease_lost", err.Error())
	case errors.Is(err, ErrJobGone):
		problem(w, http.StatusGone, "job_gone", err.Error())
	case errors.Is(err, ErrRecovering):
		w.Header().Set("Retry-After", "1")
		problem(w, http.StatusServiceUnavailable, "recovering", err.Error())
	default:
		problem(w, http.StatusInternalServerError, "internal", err.Error())
	}
}
