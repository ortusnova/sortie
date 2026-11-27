package worker

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
)

// checkInAs is completeCheckIn declaring deploy_env env.
func checkInAs(env string) string {
	return strings.Replace(completeCheckIn, `{`, `{"deploy_env":`+jsonString(env)+`,`, 1)
}

func jsonString(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

type refusalBody struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
	Expected *string  `json:"expected"`
	Declared *string  `json:"declared"`
	Missing  []string `json:"missing"`
}

func refusalOf(t *testing.T, r *httptest.ResponseRecorder) refusalBody {
	t.Helper()
	if r.Code != http.StatusForbidden {
		t.Fatalf("check-in: %d %s, want 403", r.Code, r.Body)
	}
	var body refusalBody
	if err := json.Unmarshal(r.Body.Bytes(), &body); err != nil {
		t.Fatalf("refusal body %s: %v", r.Body, err)
	}
	if body.Error.Code != "deploy_env_mismatch" {
		t.Fatalf("refusal code %q, want deploy_env_mismatch", body.Error.Code)
	}
	return body
}

// environment-isolation: A worker of another environment.
func TestAWorkerOfAnotherEnvironmentIsRefused(t *testing.T) {
	t.Parallel()
	c := &clock{t: time.Unix(100, 0)}
	m := newM(c)
	m.SetDeployEnv("dev")
	m.Submit(job("ONS-7", 1))
	a := newAPI(t, m)
	body := refusalOf(t, a.post("p1", "/api/v1/workers/checkin", checkInAs("test-b")))
	if body.Expected == nil || *body.Expected != "dev" || body.Declared == nil || *body.Declared != "test-b" {
		t.Fatalf("refusal names expected %v declared %v, want dev and test-b", body.Expected, body.Declared)
	}
	if !strings.Contains(body.Error.Message, "dev") || !strings.Contains(body.Error.Message, "test-b") {
		t.Fatalf("refusal message %q does not name dev and test-b", body.Error.Message)
	}
	s := m.Snapshot()
	want := []RefusedWorker{{ID: "p1", Expected: "dev", Declared: "test-b", LastRefused: c.now().UTC()}}
	if !reflect.DeepEqual(s.RefusedWorkers, want) {
		t.Fatalf("refused workers %+v, want %+v", s.RefusedWorkers, want)
	}
	if len(s.Workers) != 0 {
		t.Fatalf("a refused worker is checked in: %+v", s.Workers)
	}
	for range 3 {
		if r := a.post("p1", "/api/v1/workers/poll", ""); r.Code != http.StatusNoContent {
			t.Fatalf("poll of a refused worker: %d %s, want 204", r.Code, r.Body)
		}
	}
	if !m.Queued("ONS-7") || len(m.Snapshot().Leases) != 0 {
		t.Fatal("a refused worker was offered the job")
	}
	c.add(time.Minute)
	refusalOf(t, a.post("p1", "/api/v1/workers/checkin", checkInAs("test-b")))
	if got := m.Snapshot().RefusedWorkers[0].LastRefused; !got.Equal(c.now()) {
		t.Fatalf("last refusal %v, want %v", got, c.now())
	}
}

// environment-isolation: A worker with no declaration.
func TestAWorkerWithNoDeployEnvIsRefused(t *testing.T) {
	t.Parallel()
	c := &clock{t: time.Unix(100, 0)}
	m := newM(c)
	m.SetDeployEnv("dev")
	a := newAPI(t, m)
	for name, decl := range map[string]string{
		"absent": completeCheckIn,
		"null":   strings.Replace(completeCheckIn, `{`, `{"deploy_env":null,`, 1),
		"empty":  checkInAs(""),
	} {
		body := refusalOf(t, a.post("taichi", "/api/v1/workers/checkin", decl))
		if body.Expected == nil || *body.Expected != "dev" || body.Declared != nil ||
			!reflect.DeepEqual(body.Missing, []string{"deploy_env"}) {
			t.Fatalf("%s: refusal %+v, want expected dev, declared null, missing [deploy_env]", name, body)
		}
		if !strings.Contains(body.Error.Message, "dev") || !strings.Contains(body.Error.Message, "deploy_env") {
			t.Fatalf("%s: refusal message %q does not name dev and deploy_env", name, body.Error.Message)
		}
	}
	if s := m.Snapshot(); len(s.Workers) != 0 || len(s.RefusedWorkers) != 1 || s.RefusedWorkers[0].Declared != "" {
		t.Fatalf("state %+v", s)
	}
}

// environment-isolation: A workflow without it. Today's check-in
// fixtures are admitted unchanged, and so is one that declares an
// environment.
func TestAWorkflowWithoutDeployEnvAdmitsEveryWorker(t *testing.T) {
	t.Parallel()
	c := &clock{t: time.Unix(100, 0)}
	m := newM(c)
	a := newAPI(t, m)
	for id, decl := range map[string]string{"p1": completeCheckIn, "taichi": checkInAs("dev")} {
		if r := a.post(id, "/api/v1/workers/checkin", decl); r.Code != http.StatusOK {
			t.Fatalf("%s: check-in %d %s", id, r.Code, r.Body)
		}
	}
	s := m.Snapshot()
	if len(s.Workers) != 2 || len(s.RefusedWorkers) != 0 {
		t.Fatalf("state %+v", s)
	}
	if s.Workers[0].Declaration.DeployEnv != "" || s.Workers[1].Declaration.DeployEnv != "dev" {
		t.Fatalf("declared environments %q %q", s.Workers[0].Declaration.DeployEnv, s.Workers[1].Declaration.DeployEnv)
	}
	m.Submit(job("ONS-7", 1))
	if offer, _ := decodeOffer(t, a.post("p1", "/api/v1/workers/poll", "")); offer.Job.ID != "ONS-7" {
		t.Fatalf("offer %+v", offer)
	}
}

// A worker of Sortie's own environment is admitted and offered jobs, and
// a refused worker is admitted once it declares it.
func TestAWorkerOfTheSameEnvironmentIsAdmitted(t *testing.T) {
	t.Parallel()
	c := &clock{t: time.Unix(100, 0)}
	m := newM(c)
	m.SetDeployEnv("dev")
	a := newAPI(t, m)
	refusalOf(t, a.post("taichi", "/api/v1/workers/checkin", completeCheckIn))
	if r := a.post("taichi", "/api/v1/workers/checkin", checkInAs("dev")); r.Code != http.StatusOK {
		t.Fatalf("check-in: %d %s", r.Code, r.Body)
	}
	if s := m.Snapshot(); len(s.RefusedWorkers) != 0 || len(s.Workers) != 1 {
		t.Fatalf("state %+v", s)
	}
	m.Submit(job("ONS-7", 1))
	if offer, _ := decodeOffer(t, a.post("taichi", "/api/v1/workers/poll", "")); offer.Job.ID != "ONS-7" {
		t.Fatalf("offer %+v", offer)
	}
}

// A refused worker never takes a job another worker could: the job goes
// to the admitted worker even though the refused one polls first.
func TestARefusedWorkerHoldsNoPlacement(t *testing.T) {
	t.Parallel()
	c := &clock{t: time.Unix(100, 0)}
	m := newM(c)
	m.SetDeployEnv("dev")
	a := newAPI(t, m)
	refusalOf(t, a.post("p1", "/api/v1/workers/checkin", checkInAs("test-b")))
	a.post("taichi", "/api/v1/workers/checkin", checkInAs("dev"))
	m.Submit(job("ONS-7", 1))
	if r := a.post("p1", "/api/v1/workers/poll", ""); r.Code != http.StatusNoContent {
		t.Fatalf("refused worker poll: %d", r.Code)
	}
	if offer, _ := decodeOffer(t, a.post("taichi", "/api/v1/workers/poll", "")); offer.Job.ID != "ONS-7" {
		t.Fatalf("offer %+v", offer)
	}
}

// A worker that holds a lease when the workflow starts naming an
// environment it does not declare is refused from then on, takes no new
// job, and keeps renewing its lease until it reports.
func TestARefusedWorkerKeepsItsLease(t *testing.T) {
	t.Parallel()
	c := &clock{t: time.Unix(0, 0)}
	m := newM(c)
	a := newAPI(t, m)
	a.post("taichi", "/api/v1/workers/checkin", completeCheckIn)
	ended := m.Submit(job("ONS-7", 1))
	decodeOffer(t, a.post("taichi", "/api/v1/workers/poll", ""))

	m.SetDeployEnv("dev")
	want := []RefusedWorker{{ID: "taichi", Expected: "dev", Declared: "", LastRefused: c.now().UTC()}}
	if s := m.Snapshot(); len(s.Workers) != 0 || !reflect.DeepEqual(s.RefusedWorkers, want) {
		t.Fatalf("after the workflow names dev: %+v", s)
	}
	m.Submit(job("ONS-8", 1))
	for range 3 {
		c.add(50 * time.Second)
		refusalOf(t, a.post("taichi", "/api/v1/workers/checkin", completeCheckIn))
		if r := a.post("taichi", "/api/v1/workers/poll", ""); r.Code != http.StatusNoContent {
			t.Fatalf("poll: %d %s, want 204", r.Code, r.Body)
		}
		r := a.post("taichi", "/api/v1/workers/jobs/ONS-7/attempts/1/heartbeat", "")
		if r.Code != http.StatusOK {
			t.Fatalf("heartbeat: %d %s", r.Code, r.Body)
		}
		if ls := m.Snapshot().Leases; len(ls) != 1 || ls[0].Job != "ONS-7" || !ls[0].Expires.Equal(c.now().Add(time.Minute)) {
			t.Fatalf("lease %+v", ls)
		}
		if got := m.ExpireLeases(); len(got) != 0 {
			t.Fatalf("renewed lease expired: %v", got)
		}
	}
	if !m.Queued("ONS-8") {
		t.Fatal("the refused worker took a new job")
	}
	if r := a.post("taichi", "/api/v1/workers/jobs/ONS-7/attempts/1/report", `{"outcome":"succeeded","reason":"","machine":"deleted"}`); r.Code != http.StatusNoContent {
		t.Fatalf("report: %d %s", r.Code, r.Body)
	}
	if o := outcome(t, ended); !o.Succeeded {
		t.Fatalf("outcome %+v", o)
	}
	if s := m.Snapshot(); len(s.Leases) != 0 || !m.Queued("ONS-8") {
		t.Fatalf("after the report: %+v", s)
	}
}

// A workflow that stops naming an environment forgets its refusals, and
// one that names another judges the checked-in workers again.
func TestAChangedDeployEnvJudgesAgain(t *testing.T) {
	t.Parallel()
	c := &clock{t: time.Unix(100, 0)}
	m := newM(c)
	m.SetDeployEnv("dev")
	a := newAPI(t, m)
	a.post("taichi", "/api/v1/workers/checkin", checkInAs("dev"))
	refusalOf(t, a.post("p1", "/api/v1/workers/checkin", checkInAs("test-b")))
	m.SetDeployEnv("dev")
	if s := m.Snapshot(); len(s.Workers) != 1 || len(s.RefusedWorkers) != 1 {
		t.Fatalf("an unchanged value changed the state: %+v", s)
	}
	m.SetDeployEnv("test-b")
	if s := m.Snapshot(); len(s.Workers) != 0 || len(s.RefusedWorkers) != 1 || s.RefusedWorkers[0].ID != "taichi" ||
		s.RefusedWorkers[0].Expected != "test-b" || s.RefusedWorkers[0].Declared != "dev" {
		t.Fatalf("after test-b: %+v", s)
	}
	m.SetDeployEnv("")
	if s := m.Snapshot(); len(s.RefusedWorkers) != 0 {
		t.Fatalf("after unset: %+v", s)
	}
	if r := a.post("p1", "/api/v1/workers/checkin", checkInAs("test-b")); r.Code != http.StatusOK {
		t.Fatalf("check-in after unset: %d %s", r.Code, r.Body)
	}
}

// OnRefuse is told of a refusal once, not at every check-in that
// repeats it, and again when the declared value changes.
func TestOnRefuseToldOfEachNewRefusal(t *testing.T) {
	t.Parallel()
	c := &clock{t: time.Unix(100, 0)}
	m := newM(c)
	var told []RefusedWorker
	m.OnRefuse(func(r RefusedWorker) { told = append(told, r) })
	m.SetDeployEnv("dev")
	a := newAPI(t, m)
	for _, env := range []string{"test-b", "test-b", "test-c"} {
		refusalOf(t, a.post("p1", "/api/v1/workers/checkin", checkInAs(env)))
	}
	if len(told) != 2 || told[0].Declared != "test-b" || told[1].Declared != "test-c" || told[1].Expected != "dev" {
		t.Fatalf("told %+v", told)
	}
}

// A deploy_env that is not a string is refused like any invalid field.
func TestADeployEnvOfTheWrongType(t *testing.T) {
	t.Parallel()
	c := &clock{t: time.Unix(100, 0)}
	m := newM(c)
	r := newAPI(t, m).post("p1", "/api/v1/workers/checkin", strings.Replace(completeCheckIn, `{`, `{"deploy_env":7,`, 1))
	if r.Code != http.StatusBadRequest || !strings.Contains(r.Body.String(), `"invalid":["deploy_env"]`) {
		t.Fatalf("%d %s", r.Code, r.Body)
	}
	if s := m.Snapshot(); len(s.Workers) != 0 || len(s.RefusedWorkers) != 0 {
		t.Fatalf("state %+v", s)
	}
}
