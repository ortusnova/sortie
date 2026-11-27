package worker

import (
	"sort"
	"strconv"
	"time"
)

// RefusedWorker is a worker whose last check-in Sortie refused because
// it serves another deployment environment, or names none, while
// Sortie's workflow names one.
type RefusedWorker struct {
	ID string `json:"id"`
	// Expected is the deployment environment Sortie serves.
	Expected string `json:"expected"`
	// Declared is the one the worker declared; empty when it declared
	// none.
	Declared string `json:"declared"`
	// LastRefused is when Sortie last refused it.
	LastRefused time.Time `json:"last_refused"`
}

// DeployEnvError refuses a check-in whose deploy_env is not the one
// Sortie serves.
type DeployEnvError struct {
	Expected string
	Declared string
}

func (e *DeployEnvError) Error() string {
	if e.Declared == "" {
		return "worker: check-in declares no deploy_env; this Sortie serves deploy_env " + strconv.Quote(e.Expected)
	}
	return "worker: check-in declares deploy_env " + strconv.Quote(e.Declared) +
		"; this Sortie serves deploy_env " + strconv.Quote(e.Expected)
}

// SetDeployEnv sets the deployment environment Sortie serves; empty
// admits a worker whatever it declares. A change refuses, from now on,
// each checked-in worker that declared another, as its check-in would
// be, and forgets the workers refused under the previous value, which
// their next check-in judges again. The leases those workers hold are
// kept.
func (m *Matcher) SetDeployEnv(env string) {
	m.mu.Lock()
	var refused []RefusedWorker
	if env != m.deployEnv {
		m.deployEnv = env
		m.refused = map[string]RefusedWorker{}
		if env != "" {
			ids := make([]string, 0, len(m.workers))
			for id := range m.workers {
				ids = append(ids, id)
			}
			sort.Strings(ids)
			for _, id := range ids {
				if d := m.workers[id].Declaration.DeployEnv; d != env {
					refused = append(refused, m.refuseLocked(id, d))
				}
			}
		}
	}
	f := m.onRefuse
	m.mu.Unlock()
	if f != nil {
		for _, r := range refused {
			f(r)
		}
	}
}

// OnRefuse sets f to be told, outside the matcher's lock, of each
// worker newly refused for its deployment environment: one not refused
// before, or refused before for another declared value.
func (m *Matcher) OnRefuse(f func(RefusedWorker)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onRefuse = f
}

// Admit records a worker's declaration as [Matcher.CheckIn] does, or
// refuses it with a [*DeployEnvError] when Sortie serves a deployment
// environment and the declaration names none or another. A refused
// worker leaves the checked-in workers, is offered no job and is listed
// in the state's refused workers; the leases it holds are kept until
// they end.
func (m *Matcher) Admit(id string, d Declaration) error {
	m.mu.Lock()
	if m.deployEnv == "" || d.DeployEnv == m.deployEnv {
		delete(m.refused, id)
		m.checkInLocked(id, d)
		m.mu.Unlock()
		return nil
	}
	prev, was := m.refused[id]
	r := m.refuseLocked(id, d.DeployEnv)
	f := m.onRefuse
	m.mu.Unlock()
	if f != nil && (!was || prev.Declared != r.Declared) {
		f(r)
	}
	return &DeployEnvError{Expected: r.Expected, Declared: r.Declared}
}

// refuseLocked drops worker id from the checked-in workers and records
// it as refused now for declaring declared.
func (m *Matcher) refuseLocked(id, declared string) RefusedWorker {
	delete(m.workers, id)
	r := RefusedWorker{ID: id, Expected: m.deployEnv, Declared: declared, LastRefused: m.now().UTC()}
	m.refused[id] = r
	return r
}

// refusedLocked returns the refused workers ordered by ID.
func (m *Matcher) refusedLocked() []RefusedWorker {
	out := make([]RefusedWorker, 0, len(m.refused))
	for _, r := range m.refused {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
