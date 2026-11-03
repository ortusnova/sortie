package worker

import (
	"encoding/json"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"
)

// A heartbeat that carries the job's step is kept on the lease, a
// heartbeat without one keeps it, and a new attempt starts with none.
func TestHeartbeatKeepsTheReportedStep(t *testing.T) {
	c := &clock{t: time.Unix(0, 0)}
	m := newM(c)
	a := newAPI(t, m)
	a.post("taichi", "/api/v1/workers/checkin", completeCheckIn)
	ended := m.Submit(job("ONS-34", 1))
	a.post("taichi", "/api/v1/workers/poll", "")

	beat := `{"step":"seed-vm-recreate\u0007","phase":"live_proof","step_at":"2026-11-02T09:30:00+01:00"}`
	if r := a.post("taichi", "/api/v1/workers/jobs/ONS-34/attempts/1/heartbeat", beat); r.Code != http.StatusOK {
		t.Fatalf("heartbeat: %d %s", r.Code, r.Body)
	}
	at := time.Date(2026, 11, 2, 8, 30, 0, 0, time.UTC)
	want := Step{Step: "seed-vm-recreate", Phase: "live_proof", StepAt: &at}
	if got := m.Snapshot().Leases[0].Step; !reflect.DeepEqual(got, want) {
		t.Fatalf("step %+v, want %+v", got, want)
	}
	for _, body := range []string{"", "{}", "not json"} {
		if r := a.post("taichi", "/api/v1/workers/jobs/ONS-34/attempts/1/heartbeat", body); r.Code != http.StatusOK {
			t.Fatalf("heartbeat %q: %d", body, r.Code)
		}
		if got := m.Snapshot().Leases[0].Step; !reflect.DeepEqual(got, want) {
			t.Fatalf("after %q: step %+v", body, got)
		}
	}
	b, _ := json.Marshal(m.Snapshot().Leases[0])
	if !strings.Contains(string(b), `"step":"seed-vm-recreate","phase":"live_proof","step_at":"2026-11-02T08:30:00Z"`) {
		t.Fatalf("lease JSON %s", b)
	}

	if r := a.post("taichi", "/api/v1/workers/jobs/ONS-34/attempts/1/report", `{"outcome":"failed","machine":"deleted"}`); r.Code != http.StatusNoContent {
		t.Fatalf("report: %d", r.Code)
	}
	outcome(t, ended)
	m.Submit(job("ONS-34", 2))
	a.post("taichi", "/api/v1/workers/checkin", completeCheckIn)
	a.post("taichi", "/api/v1/workers/poll", "")
	if got := m.Snapshot().Leases[0]; got.Attempt != 2 || got.Step != (Step{}) {
		t.Fatalf("new attempt %+v", got)
	}
}

// A long step is cut to MaxStep characters.
func TestHeartbeatStepIsBounded(t *testing.T) {
	got := decodeStep(strings.NewReader(`{"step":"` + strings.Repeat("é", MaxStep+50) + `"}`))
	if n := len([]rune(got.Step)); n != MaxStep {
		t.Fatalf("step of %d characters", n)
	}
}

// A check-in carries what the jobs were promised and the machines no
// Sortie started; a malformed member is dropped, never refused.
func TestCheckInCarriesUsedAndUnmanaged(t *testing.T) {
	c := &clock{t: time.Unix(0, 0)}
	m := newM(c)
	a := newAPI(t, m)
	body := strings.TrimSuffix(completeCheckIn, "}") +
		`,"used":{"cpu":4,"mem_gib":8,"disk_gib":20},"unmanaged":[{"name":"sandbox-brian","since":"2026-11-02T07:00:00Z","state":"running"},{"name":""}]}`
	if r := a.post("taichi", "/api/v1/workers/checkin", body); r.Code != http.StatusOK {
		t.Fatalf("check-in: %d %s", r.Code, r.Body)
	}
	d := m.Snapshot().Workers[0].Declaration
	if d.Used == nil || *d.Used != (Free{CPU: 4, MemGiB: 8, DiskGiB: 20}) {
		t.Fatalf("used %+v", d.Used)
	}
	want := []Unmanaged{{Name: "sandbox-brian", Since: time.Date(2026, 11, 2, 7, 0, 0, 0, time.UTC), State: "running"}}
	if !reflect.DeepEqual(d.Unmanaged, want) {
		t.Fatalf("unmanaged %+v", d.Unmanaged)
	}

	for _, extra := range []string{`,"used":{"cpu":-1},"unmanaged":"none"`, `,"used":null,"unmanaged":null`} {
		body := strings.TrimSuffix(completeCheckIn, "}") + extra + "}"
		if r := a.post("taichi", "/api/v1/workers/checkin", body); r.Code != http.StatusOK {
			t.Fatalf("check-in %s: %d %s", extra, r.Code, r.Body)
		}
		d := m.Snapshot().Workers[0].Declaration
		if d.Used != nil || d.Unmanaged != nil {
			t.Fatalf("%s kept %+v %+v", extra, d.Used, d.Unmanaged)
		}
	}

	a.post("taichi", "/api/v1/workers/checkin", completeCheckIn)
	b, _ := json.Marshal(m.Snapshot().Workers[0].Declaration)
	if strings.Contains(string(b), "used") || strings.Contains(string(b), "unmanaged") {
		t.Fatalf("a worker that says nothing shows %s", b)
	}
}
