package worker

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) add(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func newM(c *clock) *Matcher {
	return NewMatcher(NewTTLLeaser(time.Minute), c.now, nil)
}

// host is a worker with room for anything the tests ask of it.
func host() Declaration {
	return Declaration{OS: "linux", Arch: "x86_64", KVM: true, Nested: true, Backends: []string{"smolvm"},
		RunSlots: 4, Running: 0, Free: Free{CPU: 16, MemGiB: 52, DiskGiB: 210}}
}

func outcome(t *testing.T, ch <-chan Outcome) Outcome {
	t.Helper()
	select {
	case o := <-ch:
		return o
	default:
		t.Fatal("job has not ended")
		return Outcome{}
	}
}

func notEnded(t *testing.T, ch <-chan Outcome) {
	t.Helper()
	select {
	case o := <-ch:
		t.Fatalf("job ended: %+v", o)
	default:
	}
}

func job(id string, attempt int) Job {
	return Job{ID: id, Attempt: attempt, Issue: Issue{ID: "i-" + id, Identifier: id, Title: "story " + id}}
}

type api struct {
	t *testing.T
	h http.Handler
}

// post sends a worker API request as worker id, with protocol 1.
func (a api) post(id, path, body string) *httptest.ResponseRecorder {
	return a.send(id, "1", path, body)
}

func (a api) send(id, protocol, path, body string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	if id != "" {
		req.Header.Set(DefaultIdentityHeader, id)
	}
	if protocol != "" {
		req.Header.Set(ProtocolHeader, protocol)
	}
	a.h.ServeHTTP(rec, req)
	return rec
}

const completeCheckIn = `{"os":"linux","arch":"x86_64","kvm":true,"nested":true,"backends":["smolvm"],"run_slots":2,"running":1,"free":{"cpu":16,"mem_gib":52,"disk_gib":210}}`

func newAPI(t *testing.T, m *Matcher) api { return api{t, Handler(m, "")} }

// worker-api: Unknown protocol version.
func TestUnknownProtocolVersion(t *testing.T) {
	c := &clock{t: time.Unix(0, 0)}
	m := newM(c)
	m.Submit(job("ONS-7", 1))
	a := newAPI(t, m)
	for _, protocol := range []string{"2", ""} {
		for _, path := range []string{"/api/v1/workers/checkin", "/api/v1/workers/poll",
			"/api/v1/workers/jobs/ONS-7/attempts/1/heartbeat", "/api/v1/workers/jobs/ONS-7/attempts/1/report"} {
			r := a.send("taichi", protocol, path, completeCheckIn)
			if r.Code != http.StatusBadRequest {
				t.Fatalf("protocol %q %s: %d", protocol, path, r.Code)
			}
			var body struct {
				Supported []int `json:"supported"`
				Error     struct {
					Message string `json:"message"`
				} `json:"error"`
			}
			_ = json.Unmarshal(r.Body.Bytes(), &body)
			if !reflect.DeepEqual(body.Supported, []int{1}) || !strings.Contains(body.Error.Message, "1") {
				t.Fatalf("body does not name version 1: %s", r.Body)
			}
		}
	}
	s := m.Snapshot()
	if len(s.Workers) != 0 || len(s.Leases) != 0 || len(s.Queue) != 1 {
		t.Fatalf("state changed: %+v", s)
	}
}

// worker-api: Identity in the header.
func TestIdentityInTheHeader(t *testing.T) {
	c := &clock{t: time.Unix(0, 0)}
	m := newM(c)
	a := newAPI(t, m)
	if r := a.post("taichi", "/api/v1/workers/checkin", completeCheckIn); r.Code != http.StatusOK {
		t.Fatal(r.Code, r.Body)
	}
	if ws := m.Snapshot().Workers; len(ws) != 1 || ws[0].ID != "taichi" {
		t.Fatal(ws)
	}
	if r := a.post("taichi", "/api/v1/workers/taichi/poll", ""); r.Code != http.StatusNotFound {
		t.Fatalf("a path naming a worker is served: %d", r.Code)
	}
	other := api{t, Handler(m, "X-Forwarded-User")}
	if r := other.post("p1", "/api/v1/workers/checkin", completeCheckIn); r.Code != http.StatusUnauthorized {
		t.Fatalf("a header other than the configured one named a worker: %d", r.Code)
	}
}

// worker-api: No identity header; worker-dispatch: A request without a
// worker identity.
func TestNoIdentityHeader(t *testing.T) {
	c := &clock{t: time.Unix(0, 0)}
	m := newM(c)
	m.CheckIn("taichi", host())
	m.Submit(job("ONS-7", 1))
	a := newAPI(t, m)
	for _, path := range []string{"/api/v1/workers/checkin", "/api/v1/workers/poll",
		"/api/v1/workers/jobs/ONS-7/attempts/1/heartbeat", "/api/v1/workers/jobs/ONS-7/attempts/1/report"} {
		if r := a.post("", path, completeCheckIn); r.Code != http.StatusUnauthorized {
			t.Fatalf("%s: %d", path, r.Code)
		}
	}
	s := m.Snapshot()
	if len(s.Leases) != 0 || len(s.Queue) != 1 || len(s.Workers) != 1 || s.Workers[0].Declaration.Running != 0 {
		t.Fatalf("state changed: %+v", s)
	}
}

// worker-api: Timings in the answer.
func TestTimingsInTheAnswer(t *testing.T) {
	c := &clock{t: time.Unix(0, 0)}
	a := newAPI(t, newM(c))
	r := a.post("taichi", "/api/v1/workers/checkin", completeCheckIn)
	var body map[string]int64
	if err := json.Unmarshal(r.Body.Bytes(), &body); err != nil || r.Code != http.StatusOK {
		t.Fatal(r.Code, r.Body)
	}
	if body["lease_ms"] != 60000 || body["heartbeat_ms"] <= 0 || body["heartbeat_ms"] >= 60000 || len(body) != 2 {
		t.Fatalf("answer %v", body)
	}
}

func missingOf(t *testing.T, r *httptest.ResponseRecorder) []string {
	t.Helper()
	var body struct {
		Missing []string `json:"missing"`
	}
	_ = json.Unmarshal(r.Body.Bytes(), &body)
	return body.Missing
}

// worker-api: Missing run slots.
func TestMissingRunSlots(t *testing.T) {
	c := &clock{t: time.Unix(0, 0)}
	a := newAPI(t, newM(c))
	r := a.post("taichi", "/api/v1/workers/checkin", strings.Replace(completeCheckIn, `"run_slots":2,`, "", 1))
	if r.Code != http.StatusBadRequest || !reflect.DeepEqual(missingOf(t, r), []string{"run_slots"}) || !strings.Contains(r.Body.String(), "run_slots") {
		t.Fatal(r.Code, r.Body)
	}
}

// Every declaration field is required; each missing one is named by
// its dotted path, and a value of the wrong type is refused.
func TestCheckInRefusesIncompleteDeclarations(t *testing.T) {
	c := &clock{t: time.Unix(0, 0)}
	m := newM(c)
	a := newAPI(t, m)
	r := a.post("taichi", "/api/v1/workers/checkin", `{"arch":"x86_64","free":{"cpu":1}}`)
	want := []string{"os", "kvm", "nested", "backends", "run_slots", "running", "free.mem_gib", "free.disk_gib"}
	if r.Code != http.StatusBadRequest || !reflect.DeepEqual(missingOf(t, r), want) {
		t.Fatalf("%d missing %v want %v", r.Code, missingOf(t, r), want)
	}
	for name, body := range map[string]string{
		"not an object":    `[]`,
		"kvm not boolean":  strings.Replace(completeCheckIn, `"kvm":true`, `"kvm":"yes"`, 1),
		"negative running": strings.Replace(completeCheckIn, `"running":1`, `"running":-1`, 1),
		"null free":        strings.Replace(completeCheckIn, `"free":{"cpu":16,"mem_gib":52,"disk_gib":210}`, `"free":null`, 1),
	} {
		if r := a.post("taichi", "/api/v1/workers/checkin", body); r.Code != http.StatusBadRequest {
			t.Fatalf("%s: %d", name, r.Code)
		}
	}
	if len(m.Snapshot().Workers) != 0 {
		t.Fatal("a refused check-in was recorded")
	}
}

// worker-dispatch: A complete check-in.
func TestACompleteCheckIn(t *testing.T) {
	c := &clock{t: time.Unix(100, 0)}
	m := newM(c)
	a := newAPI(t, m)
	if r := a.post("taichi", "/api/v1/workers/checkin", completeCheckIn); r.Code != http.StatusOK {
		t.Fatal(r.Code, r.Body)
	}
	want := Declaration{OS: "linux", Arch: "x86_64", KVM: true, Nested: true, Backends: []string{"smolvm"},
		RunSlots: 2, Running: 1, Free: Free{CPU: 16, MemGiB: 52, DiskGiB: 210}}
	w := m.Snapshot().Workers[0]
	if w.ID != "taichi" || !reflect.DeepEqual(w.Declaration, want) || !w.LastCheckIn.Equal(c.now()) {
		t.Fatalf("%+v", w)
	}
}

// worker-dispatch: A check-in without free disk.
func TestACheckInWithoutFreeDisk(t *testing.T) {
	c := &clock{t: time.Unix(100, 0)}
	m := newM(c)
	a := newAPI(t, m)
	a.post("taichi", "/api/v1/workers/checkin", completeCheckIn)
	before := m.Snapshot().Workers[0]
	c.add(time.Minute)
	r := a.post("taichi", "/api/v1/workers/checkin", `{"os":"linux","arch":"x86_64","kvm":false,"nested":false,"backends":[],"run_slots":1,"running":0,"free":{"cpu":1,"mem_gib":1}}`)
	if r.Code != http.StatusBadRequest || !reflect.DeepEqual(missingOf(t, r), []string{"free.disk_gib"}) {
		t.Fatal(r.Code, r.Body)
	}
	if after := m.Snapshot().Workers[0]; !reflect.DeepEqual(after, before) {
		t.Fatalf("previous declaration replaced: %+v", after)
	}
}

func decodeOffer(t *testing.T, r *httptest.ResponseRecorder) (Offer, map[string]json.RawMessage) {
	t.Helper()
	if r.Code != http.StatusOK {
		t.Fatalf("poll: %d %s", r.Code, r.Body)
	}
	var offer Offer
	if err := json.Unmarshal(r.Body.Bytes(), &offer); err != nil {
		t.Fatal(err)
	}
	var raw map[string]json.RawMessage
	_ = json.Unmarshal(r.Body.Bytes(), &raw)
	return offer, raw
}

func allFlows() map[string]string {
	out := map[string]string{}
	for _, f := range []string{"stdio", "egress", "mcp", "bundles", "usage"} {
		out[f] = "https://edge.example/" + f
	}
	return out
}

// worker-api: An offer.
func TestAnOffer(t *testing.T) {
	c := &clock{t: time.Unix(0, 0)}
	toks := &fakeTokens{}
	m := NewMatcher(NewTTLLeaser(time.Minute), c.now, toks)
	a := newAPI(t, m)
	a.post("taichi", "/api/v1/workers/checkin", completeCheckIn)
	prio := 2
	j := job("ONS-7", 1)
	j.Priority = &prio
	j.Requires = Requires{Capabilities: []string{"smolvm", "nested"}, CPU: 4, MemGiB: 8, DiskGiB: 20}
	j.Flows = allFlows()
	j.Run = Run{Backend: "smolvm", Image: "img:1", Command: "claude", Args: []string{"--verbose"},
		Env: map[string]string{"SORTIE_ISSUE_IDENTIFIER": "ONS-7"}, StallTimeoutMS: 300000, SeedPath: SeedPath("ONS-7", 1)}
	m.Submit(j)
	offer, raw := decodeOffer(t, a.post("taichi", "/api/v1/workers/poll", ""))
	if offer.Job.ID != "ONS-7" || offer.Job.Attempt != 1 || *offer.Job.Priority != 2 ||
		offer.Job.Issue != (Issue{ID: "i-ONS-7", Identifier: "ONS-7", Title: "story ONS-7"}) ||
		!reflect.DeepEqual(offer.Job.Requires, j.Requires) || !offer.Lease.Expires.Equal(c.now().Add(time.Minute)) ||
		!reflect.DeepEqual(offer.Run, j.Run) {
		t.Fatalf("%+v", offer)
	}
	if len(raw) != 4 {
		t.Fatalf("offer keys %v", raw)
	}
	var lease map[string]any
	_ = json.Unmarshal(raw["lease"], &lease)
	if len(lease) != 1 || lease["expires"] != "1970-01-01T00:01:00Z" {
		t.Fatalf("lease %s", raw["lease"])
	}
	if len(offer.Flows) != 5 {
		t.Fatalf("flows %+v", offer.Flows)
	}
	for name, url := range allFlows() {
		if got := offer.Flows[name]; got.URL != url || got.Token != "tok-ONS-7-1-"+name {
			t.Fatalf("flow %s = %+v", name, got)
		}
	}
	if seed := offer.Run.SeedPath; seed != "/jobs/ONS-7/attempts/1/seed" {
		t.Fatalf("seed path %q", seed)
	}
	if strings.Contains(string(raw["run"]), "tok-") || strings.Contains(string(raw["job"]), "tok-") {
		t.Fatalf("a token is outside flows: %s %s", raw["job"], raw["run"])
	}
}

// worker-api: Nothing fits.
func TestNothingFits(t *testing.T) {
	c := &clock{t: time.Unix(0, 0)}
	m := newM(c)
	a := newAPI(t, m)
	a.post("taichi", "/api/v1/workers/checkin", completeCheckIn)
	if r := a.post("taichi", "/api/v1/workers/poll", ""); r.Code != http.StatusNoContent {
		t.Fatalf("empty queue: %d", r.Code)
	}
	j := job("ONS-7", 1)
	j.Requires.MemGiB = 100
	m.Submit(j)
	if r := a.post("taichi", "/api/v1/workers/poll", ""); r.Code != http.StatusNoContent {
		t.Fatalf("no fit: %d", r.Code)
	}
}

// A worker that has not checked in gets 409 from poll.
func TestPollBeforeCheckIn(t *testing.T) {
	c := &clock{t: time.Unix(0, 0)}
	m := newM(c)
	m.Submit(job("ONS-7", 1))
	if r := newAPI(t, m).post("taichi", "/api/v1/workers/poll", ""); r.Code != http.StatusConflict {
		t.Fatalf("poll before check-in: %d", r.Code)
	}
	if len(m.Snapshot().Leases) != 0 {
		t.Fatal("leased")
	}
}

// worker-api: Heartbeat after expiry.
func TestHeartbeatAfterExpiry(t *testing.T) {
	c := &clock{t: time.Unix(0, 0)}
	m := newM(c)
	a := newAPI(t, m)
	a.post("taichi", "/api/v1/workers/checkin", completeCheckIn)
	ended := m.Submit(job("ONS-7", 1))
	a.post("taichi", "/api/v1/workers/poll", "")
	c.add(2 * time.Minute)
	if r := a.post("taichi", "/api/v1/workers/jobs/ONS-7/attempts/1/heartbeat", ""); r.Code != http.StatusConflict {
		t.Fatalf("heartbeat before the sweep: %d", r.Code)
	}
	m.ExpireLeases()
	if o := outcome(t, ended); !o.Expired || o.Succeeded {
		t.Fatalf("outcome %+v", o)
	}
	if r := a.post("taichi", "/api/v1/workers/jobs/ONS-7/attempts/1/heartbeat", ""); r.Code != http.StatusConflict {
		t.Fatalf("heartbeat after the sweep: %d", r.Code)
	}
	if s := m.Snapshot(); len(s.Leases) != 0 || len(s.Queue) != 0 {
		t.Fatalf("attempt revived: %+v", s)
	}
}

// worker-api: A succeeded run.
func TestASucceededRun(t *testing.T) {
	c := &clock{t: time.Unix(0, 0)}
	m := newM(c)
	a := newAPI(t, m)
	a.post("taichi", "/api/v1/workers/checkin", completeCheckIn)
	ended := m.Submit(job("ONS-7", 1))
	a.post("taichi", "/api/v1/workers/poll", "")
	if r := a.post("taichi", "/api/v1/workers/jobs/ONS-7/attempts/1/report", `{"outcome":"succeeded","reason":"","machine":"stopped"}`); r.Code != http.StatusNoContent {
		t.Fatal(r.Code, r.Body)
	}
	if o := outcome(t, ended); !o.Succeeded || o.Machine != MachineStopped {
		t.Fatalf("%+v", o)
	}
	if s := m.Snapshot(); len(s.Leases) != 0 {
		t.Fatalf("lease remains: %+v", s.Leases)
	}
	if r := a.post("taichi", "/api/v1/workers/jobs/ONS-7/attempts/1/report", `{"outcome":"succeeded","reason":"","machine":"stopped"}`); r.Code != http.StatusConflict {
		t.Fatalf("report for an ended attempt: %d", r.Code)
	}
}

// A failed report carries its reason to the attempt's end.
func TestAFailedRunCarriesItsReason(t *testing.T) {
	c := &clock{t: time.Unix(0, 0)}
	m := newM(c)
	a := newAPI(t, m)
	a.post("taichi", "/api/v1/workers/checkin", completeCheckIn)
	ended := m.Submit(job("ONS-7", 1))
	a.post("taichi", "/api/v1/workers/poll", "")
	if r := a.post("taichi", "/api/v1/workers/jobs/ONS-7/attempts/1/report", `{"outcome":"failed","reason":"tests failed","machine":"deleted"}`); r.Code != http.StatusNoContent {
		t.Fatal(r.Code, r.Body)
	}
	if o := outcome(t, ended); o.Succeeded || o.Reason != "tests failed" || o.Machine != MachineDeleted {
		t.Fatalf("%+v", o)
	}
}

// A malformed report is refused and changes nothing.
func TestMalformedReport(t *testing.T) {
	c := &clock{t: time.Unix(0, 0)}
	m := newM(c)
	a := newAPI(t, m)
	a.post("taichi", "/api/v1/workers/checkin", completeCheckIn)
	ended := m.Submit(job("ONS-7", 1))
	a.post("taichi", "/api/v1/workers/poll", "")
	for _, body := range []string{
		`{"outcome":"done","machine":"stopped"}`,
		`{"outcome":"succeeded"}`,
		`{"outcome":"succeeded","machine":"paused"}`,
		`{"outcome":"failed","error":"x","machine":"stopped","reason":7}`,
		`not json`,
	} {
		if r := a.post("taichi", "/api/v1/workers/jobs/ONS-7/attempts/1/report", body); r.Code != http.StatusBadRequest {
			t.Fatalf("%s: %d", body, r.Code)
		}
	}
	notEnded(t, ended)
	if len(m.Snapshot().Leases) != 1 {
		t.Fatal("lease dropped")
	}
}

// worker-api: A report from the wrong worker.
func TestAReportFromTheWrongWorker(t *testing.T) {
	c := &clock{t: time.Unix(0, 0)}
	m := newM(c)
	a := newAPI(t, m)
	a.post("taichi", "/api/v1/workers/checkin", completeCheckIn)
	a.post("p1", "/api/v1/workers/checkin", completeCheckIn)
	ended := m.Submit(job("ONS-7", 1))
	a.post("taichi", "/api/v1/workers/poll", "")
	before := m.Snapshot().Leases
	if r := a.post("p1", "/api/v1/workers/jobs/ONS-7/attempts/1/report", `{"outcome":"failed","reason":"x","machine":"deleted"}`); r.Code != http.StatusConflict {
		t.Fatalf("report from p1: %d", r.Code)
	}
	if r := a.post("p1", "/api/v1/workers/jobs/ONS-7/attempts/1/heartbeat", ""); r.Code != http.StatusConflict {
		t.Fatalf("heartbeat from p1: %d", r.Code)
	}
	if r := a.post("taichi", "/api/v1/workers/jobs/ONS-7/attempts/2/heartbeat", ""); r.Code != http.StatusConflict {
		t.Fatalf("heartbeat for another attempt: %d", r.Code)
	}
	notEnded(t, ended)
	if after := m.Snapshot().Leases; !reflect.DeepEqual(after, before) || len(after) != 1 || after[0].Worker != "taichi" {
		t.Fatalf("lease changed: %+v -> %+v", before, after)
	}
}

// worker-dispatch: Nested work and an ordinary story.
func TestNestedWorkAndAnOrdinaryStory(t *testing.T) {
	c := &clock{t: time.Unix(0, 0)}
	m := newM(c)
	two, one := 2, 1
	a := job("A", 1)
	a.Priority = &two
	a.Requires = Requires{Capabilities: []string{"nested"}, CPU: 8, MemGiB: 20, DiskGiB: 40}
	b := job("B", 1)
	b.Priority = &one
	b.Requires = Requires{CPU: 4, MemGiB: 8, DiskGiB: 10}
	m.Submit(a)
	m.Submit(b)
	d := host()
	d.Nested = false
	m.CheckIn("w", d)
	offer, ok, err := m.Poll("w")
	if !ok || err != nil || offer.Job.ID != "B" {
		t.Fatalf("offer %+v ok=%v err=%v", offer.Job, ok, err)
	}
	if _, ok, _ := m.Poll("w"); ok {
		t.Fatal("story A offered to a worker without nested virtualization")
	}
	q := m.Snapshot().Queue
	if len(q) != 1 || q[0].Job.ID != "A" || len(q[0].Waiting) != 1 || !strings.Contains(q[0].Waiting[0], "nested virtualization") {
		t.Fatalf("queue %+v", q)
	}
}

// worker-dispatch: Not enough free memory.
func TestNotEnoughFreeMemory(t *testing.T) {
	c := &clock{t: time.Unix(0, 0)}
	m := newM(c)
	j := job("ONS-7", 1)
	j.Requires.MemGiB = 20
	m.Submit(j)
	d := host()
	d.Free.MemGiB = 12
	m.CheckIn("w", d)
	if _, ok, _ := m.Poll("w"); ok {
		t.Fatal("offered")
	}
	if w := m.Snapshot().Queue[0].Waiting; len(w) != 1 || !strings.Contains(w[0], "12 of 20 GiB memory free") {
		t.Fatal(w)
	}
}

// Each other capability, the backend and the CPU and disk sizes are
// matched too, and the reasons say what a worker lacks.
func TestCapabilitiesAndResourcesMatched(t *testing.T) {
	for name, tc := range map[string]struct {
		req  Requires
		why  string
		edit func(*Declaration)
	}{
		"kvm":     {Requires{Capabilities: []string{"kvm"}}, "no KVM", func(d *Declaration) { d.KVM = false }},
		"backend": {Requires{Capabilities: []string{"firecracker"}}, "lacks capability firecracker", func(*Declaration) {}},
		"os":      {Requires{Capabilities: []string{"darwin"}}, "lacks capability darwin", func(*Declaration) {}},
		"cpu":     {Requires{CPU: 17}, "16 of 17 CPUs free", func(*Declaration) {}},
		"disk":    {Requires{DiskGiB: 211}, "210 of 211 GiB disk free", func(*Declaration) {}},
	} {
		t.Run(name, func(t *testing.T) {
			c := &clock{t: time.Unix(0, 0)}
			m := newM(c)
			d := host()
			tc.edit(&d)
			m.CheckIn("w", d)
			j := job("j", 1)
			j.Requires = tc.req
			m.Submit(j)
			if _, ok, _ := m.Poll("w"); ok {
				t.Fatal("offered")
			}
			if w := m.Snapshot().Queue[0].Waiting; len(w) != 1 || !strings.Contains(w[0], "worker w: "+tc.why) {
				t.Fatal(w)
			}
		})
	}
	c := &clock{t: time.Unix(0, 0)}
	m := newM(c)
	m.CheckIn("w", host())
	j := job("j", 1)
	j.Requires = Requires{Capabilities: []string{"linux", "x86_64", "kvm", "nested", "smolvm"}, CPU: 16, MemGiB: 52, DiskGiB: 210}
	m.Submit(j)
	if _, ok, _ := m.Poll("w"); !ok {
		t.Fatal("a worker that declares everything got no offer")
	}
}

func TestWaitingWithoutWorkers(t *testing.T) {
	c := &clock{t: time.Unix(0, 0)}
	m := newM(c)
	m.Submit(job("j", 1))
	if w := m.Snapshot().Queue[0].Waiting; !reflect.DeepEqual(w, []string{"no worker has checked in"}) {
		t.Fatal(w)
	}
}

func TestOrderByPriorityThenAge(t *testing.T) {
	c := &clock{t: time.Unix(0, 0)}
	m := newM(c)
	one, two := 1, 2
	sub := func(id string, p *int) {
		j := job(id, 1)
		j.Priority = p
		m.Submit(j)
	}
	sub("none-old", nil)
	c.add(time.Second)
	sub("two", &two)
	c.add(time.Second)
	sub("one-new", &one)
	sub("none-new", nil)
	c.add(time.Second)
	sub("one-newer", &one)
	d := host()
	d.RunSlots = 5
	m.CheckIn("a", d)
	var got []string
	for {
		o, ok, _ := m.Poll("a")
		if !ok {
			break
		}
		got = append(got, o.Job.ID)
	}
	want := []string{"one-new", "one-newer", "two", "none-old", "none-new"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("order %v want %v", got, want)
	}
}

// worker-dispatch: Two workers ask for the same job.
func TestTwoWorkersAskForTheSameJob(t *testing.T) {
	for range 50 {
		c := &clock{t: time.Unix(0, 0)}
		m := newM(c)
		m.CheckIn("p1", host())
		m.CheckIn("taichi", host())
		m.Submit(job("ONS-7", 1))
		var wg sync.WaitGroup
		got := make([]bool, 2)
		for i, id := range []string{"p1", "taichi"} {
			wg.Go(func() {
				_, ok, err := m.Poll(id)
				if err != nil {
					t.Error(err)
				}
				got[i] = ok
			})
		}
		wg.Wait()
		if got[0] == got[1] {
			t.Fatalf("offers %v, want exactly one", got)
		}
		if ls := m.Snapshot().Leases; len(ls) != 1 {
			t.Fatalf("leases %+v", ls)
		}
	}
	l := NewTTLLeaser(time.Minute)
	if _, err := l.Acquire("j", "a", time.Unix(0, 0)); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Acquire("j", "b", time.Unix(0, 0)); !errors.Is(err, ErrLeaseHeld) {
		t.Fatal(err)
	}
	var _ Leaser = l
}

// worker-dispatch: Heartbeat renews the lease.
func TestHeartbeatRenewsTheLease(t *testing.T) {
	c := &clock{t: time.Unix(0, 0)}
	m := newM(c)
	a := newAPI(t, m)
	a.post("taichi", "/api/v1/workers/checkin", completeCheckIn)
	m.Submit(job("ONS-7", 1))
	a.post("taichi", "/api/v1/workers/poll", "")
	c.add(50 * time.Second)
	r := a.post("taichi", "/api/v1/workers/jobs/ONS-7/attempts/1/heartbeat", "")
	var body map[string]any
	_ = json.Unmarshal(r.Body.Bytes(), &body)
	if r.Code != http.StatusOK || len(body) != 1 || body["expires"] != c.now().Add(time.Minute).UTC().Format(time.RFC3339) {
		t.Fatal(r.Code, r.Body)
	}
	if ls := m.Snapshot().Leases; !ls[0].Expires.Equal(c.now().Add(time.Minute)) {
		t.Fatalf("lease %+v", ls)
	}
	c.add(50 * time.Second)
	if got := m.ExpireLeases(); len(got) != 0 {
		t.Fatalf("renewed lease expired: %v", got)
	}
}

// worker-dispatch: The quiet worker comes back.
func TestTheQuietWorkerComesBack(t *testing.T) {
	c := &clock{t: time.Unix(0, 0)}
	m := newM(c)
	a := newAPI(t, m)
	a.post("taichi", "/api/v1/workers/checkin", completeCheckIn)
	ended := m.Submit(job("ONS-7", 1))
	a.post("taichi", "/api/v1/workers/poll", "")
	c.add(2 * time.Minute)
	if got := m.ExpireLeases(); !reflect.DeepEqual(got, []string{"ONS-7"}) {
		t.Fatalf("expired %v", got)
	}
	if o := outcome(t, ended); !o.Expired || o.Succeeded {
		t.Fatalf("outcome %+v", o)
	}
	again := m.Submit(job("ONS-7", 2))
	before := m.Snapshot()
	for _, id := range []string{"taichi", "p1"} {
		if r := a.post(id, "/api/v1/workers/jobs/ONS-7/attempts/1/report", `{"outcome":"succeeded","reason":"","machine":"stopped"}`); r.Code != http.StatusConflict {
			t.Fatalf("late report from %s: %d", id, r.Code)
		}
	}
	notEnded(t, again)
	if after := m.Snapshot(); !reflect.DeepEqual(after, before) {
		t.Fatalf("state changed:\n%+v\n%+v", before, after)
	}
}

// worker-dispatch: Stopped machines on a full-slot host.
func TestStoppedMachinesOnAFullSlotHost(t *testing.T) {
	c := &clock{t: time.Unix(0, 0)}
	m := newM(c)
	m.Submit(job("ONS-7", 1))
	d := host()
	d.RunSlots, d.Running = 2, 2
	m.CheckIn("w", d)
	if _, ok, _ := m.Poll("w"); ok {
		t.Fatal("offered to a worker whose run slots are full")
	}
	if w := m.Snapshot().Queue[0].Waiting; !strings.Contains(w[0], "run slots in use") {
		t.Fatal(w)
	}
	d.Running = 1
	m.CheckIn("w", d)
	if _, ok, _ := m.Poll("w"); !ok {
		t.Fatal("no offer once a running machine stopped")
	}
}

// worker-dispatch: Stopped machines and free disk.
func TestStoppedMachinesAndFreeDisk(t *testing.T) {
	c := &clock{t: time.Unix(0, 0)}
	m := newM(c)
	j := job("ONS-7", 1)
	j.Requires.DiskGiB = 40
	m.Submit(j)
	d := host()
	d.Running = 0
	d.Free.DiskGiB = 50
	m.CheckIn("w", d)
	if _, ok, _ := m.Poll("w"); !ok {
		t.Fatal("no offer")
	}
}

// Between check-ins the matcher counts what it handed out: a taken job
// holds a run slot and its sizes; a report of a stopped machine returns
// all but its disk, a deleted one everything, and a still-running one
// nothing. The next check-in replaces the count with the declaration.
func TestMachinesCountedUntilTheNextCheckIn(t *testing.T) {
	c := &clock{t: time.Unix(0, 0)}
	m := newM(c)
	d := Declaration{OS: "linux", Arch: "x86_64", Backends: []string{"smolvm"}, RunSlots: 1, Free: Free{CPU: 4, MemGiB: 8, DiskGiB: 100}}
	m.CheckIn("w", d)
	needs := Requires{CPU: 2, MemGiB: 4, DiskGiB: 40}
	take := func(id string) {
		t.Helper()
		j := job(id, 1)
		j.Requires = needs
		m.Submit(j)
		if _, ok, _ := m.Poll("w"); !ok {
			t.Fatalf("%s not offered", id)
		}
	}
	avail := func() Capacity { return m.Snapshot().Workers[0].Available }
	take("a")
	if got := avail(); got != (Capacity{RunSlots: 0, CPU: 2, MemGiB: 4, DiskGiB: 60}) {
		t.Fatalf("running machine: %+v", got)
	}
	if err := m.Report("w", "a", 1, Outcome{Succeeded: true, Machine: MachineStopped}); err != nil {
		t.Fatal(err)
	}
	if got := avail(); got != (Capacity{RunSlots: 1, CPU: 4, MemGiB: 8, DiskGiB: 60}) {
		t.Fatalf("stopped machine must hold disk only: %+v", got)
	}
	take("b")
	if err := m.Report("w", "b", 1, Outcome{Machine: MachineDeleted}); err != nil {
		t.Fatal(err)
	}
	if got := avail(); got != (Capacity{RunSlots: 1, CPU: 4, MemGiB: 8, DiskGiB: 60}) {
		t.Fatalf("deleted machine must free everything: %+v", got)
	}
	take("c")
	if err := m.Report("w", "c", 1, Outcome{Machine: MachineRunning}); err != nil {
		t.Fatal(err)
	}
	if got := avail(); got != (Capacity{RunSlots: 0, CPU: 2, MemGiB: 4, DiskGiB: 20}) {
		t.Fatalf("running machine must hold everything: %+v", got)
	}
	m.CheckIn("w", d)
	if got := avail(); got != (Capacity{RunSlots: 1, CPU: 4, MemGiB: 8, DiskGiB: 100}) {
		t.Fatalf("check-in must replace the count: %+v", got)
	}
}

func TestCancelWithdrawsJob(t *testing.T) {
	c := &clock{t: time.Unix(0, 0)}
	m := newM(c)
	m.CheckIn("a", host())
	ended := m.Submit(job("j", 1))
	if _, _, err := m.Poll("a"); err != nil {
		t.Fatal(err)
	}
	m.Cancel("j")
	if o := outcome(t, ended); !o.Cancelled {
		t.Fatalf("%+v", o)
	}
	if _, err := m.Heartbeat("a", "j", 1); !errors.Is(err, ErrLeaseLost) {
		t.Fatal(err)
	}
	if s := m.Snapshot(); len(s.Leases) != 0 {
		t.Fatalf("%+v", s)
	}
}

type fakeTokens struct {
	minted, voided []string
	err            error
}

func (f *fakeTokens) Mint(j Job) (map[string]string, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.minted = append(f.minted, fmt.Sprintf("%s/%d", j.ID, j.Attempt))
	out := map[string]string{}
	for fl := range j.Flows {
		out[fl] = fmt.Sprintf("tok-%s-%d-%s", j.ID, j.Attempt, fl)
	}
	return out, nil
}

func (f *fakeTokens) Void(jobID string) { f.voided = append(f.voided, jobID) }

// An attempt's tokens are minted once at the grant and voided on every
// way the attempt ends: report, lease expiry and cancel.
func TestTokensMintedAtGrantAndVoidedAtEnd(t *testing.T) {
	c := &clock{t: time.Unix(0, 0)}
	toks := &fakeTokens{}
	m := NewMatcher(NewTTLLeaser(time.Minute), c.now, toks)
	m.CheckIn("w", host())
	for _, id := range []string{"r", "e", "c"} {
		j := job(id, 1)
		j.Flows = map[string]string{"stdio": "https://s", "mcp": "https://m"}
		m.Submit(j)
	}
	for range 3 {
		if _, ok, err := m.Poll("w"); !ok || err != nil {
			t.Fatal(ok, err)
		}
	}
	if !reflect.DeepEqual(toks.minted, []string{"r/1", "e/1", "c/1"}) || len(toks.voided) != 0 {
		t.Fatalf("minted %v voided %v", toks.minted, toks.voided)
	}
	if err := m.Report("w", "r", 1, Outcome{Succeeded: true, Machine: MachineStopped}); err != nil {
		t.Fatal(err)
	}
	m.Cancel("c")
	if !reflect.DeepEqual(toks.voided, []string{"r", "c"}) {
		t.Fatalf("voided %v", toks.voided)
	}
	c.add(2 * time.Minute)
	m.ExpireLeases()
	if !reflect.DeepEqual(toks.voided, []string{"r", "c", "e"}) {
		t.Fatalf("voided %v", toks.voided)
	}
}

// A grant whose tokens cannot be minted is undone: the job stays
// queued and the worker keeps its slot.
func TestFailedMintLeavesJobQueued(t *testing.T) {
	c := &clock{t: time.Unix(0, 0)}
	toks := &fakeTokens{err: errors.New("no entropy")}
	m := NewMatcher(NewTTLLeaser(time.Minute), c.now, toks)
	m.CheckIn("w", host())
	m.Submit(job("j", 1))
	if _, ok, err := m.Poll("w"); ok || err == nil {
		t.Fatalf("poll ok=%v err=%v", ok, err)
	}
	s := m.Snapshot()
	if len(s.Queue) != 1 || len(s.Leases) != 0 || s.Workers[0].Available.RunSlots != 4 {
		t.Fatalf("%+v", s)
	}
	toks.err = nil
	if _, ok, err := m.Poll("w"); !ok || err != nil {
		t.Fatalf("retry ok=%v err=%v", ok, err)
	}
}

// Workers dial out and are never dialed: a whole run takes only
// requests the worker makes, and a worker declares no address.
func TestWorkersDialOut(t *testing.T) {
	if _, ok := reflect.TypeFor[Declaration]().FieldByName("Address"); ok {
		t.Fatal("declaration carries an address to dial")
	}
	c := &clock{t: time.Unix(0, 0)}
	m := newM(c)
	srv := httptest.NewServer(Handler(m, ""))
	defer srv.Close()
	ended := m.Submit(job("ONS-7", 1))
	post := func(path, body string) int {
		req, _ := http.NewRequest("POST", srv.URL+path, strings.NewReader(body))
		req.Header.Set(DefaultIdentityHeader, "w")
		req.Header.Set(ProtocolHeader, "1")
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		return resp.StatusCode
	}
	for _, step := range []struct {
		path, body string
		code       int
	}{
		{"/api/v1/workers/checkin", completeCheckIn, 200},
		{"/api/v1/workers/poll", "", 200},
		{"/api/v1/workers/jobs/ONS-7/attempts/1/heartbeat", "", 200},
		{"/api/v1/workers/jobs/ONS-7/attempts/1/report", `{"outcome":"succeeded","reason":"","machine":"stopped"}`, 204},
	} {
		if got := post(step.path, step.body); got != step.code {
			t.Fatalf("%s: %d", step.path, got)
		}
	}
	if o := outcome(t, ended); !o.Succeeded {
		t.Fatal(o)
	}
}

// The worker API lives under /api/v1/workers/; the singular path is not served.
func TestWorkerAPIPathIsPlural(t *testing.T) {
	c := &clock{t: time.Unix(0, 0)}
	a := newAPI(t, newM(c))
	if r := a.post("w", "/api/v1/worker/checkin", completeCheckIn); r.Code != http.StatusNotFound {
		t.Fatalf("singular path: %d, want 404", r.Code)
	}
	if r := a.post("w", "/api/v1/workers/checkin", completeCheckIn); r.Code != http.StatusOK {
		t.Fatalf("plural path: %d, want 200", r.Code)
	}
}

// A job no checked-in worker could take now is not placeable, and says
// why per worker; once a worker that can run it checks in, it is. The
// jobs already queued are counted first, so a slot one of them will
// take is not offered twice.
func TestPlaceable(t *testing.T) {
	c := &clock{t: time.Unix(0, 0)}
	m := newM(c)
	nested := Requires{Capabilities: []string{"nested"}, CPU: 8, MemGiB: 20, DiskGiB: 200}
	if why, ok := m.Placeable(nested); ok || !reflect.DeepEqual(why, []string{"no worker has checked in"}) {
		t.Fatalf("Placeable() before any check-in = %v, %v; want not placeable, no worker has checked in", why, ok)
	}

	p1 := host()
	p1.Nested = false
	m.CheckIn("p1", p1)
	taichi := host()
	taichi.Free.DiskGiB = 92.04
	m.CheckIn("taichi", taichi)
	want := []string{"worker p1: no nested virtualization", "worker taichi: 92.04 of 200 GiB disk free"}
	if why, ok := m.Placeable(nested); ok || !reflect.DeepEqual(why, want) {
		t.Fatalf("Placeable() = %v, %v; want not placeable, %v", why, ok, want)
	}
	if _, ok := m.Placeable(Requires{CPU: 4, MemGiB: 8, DiskGiB: 10}); !ok {
		t.Fatal("Placeable() of an ordinary story = false, want true")
	}

	taichi.Free.DiskGiB = 210
	taichi.RunSlots = 1
	m.CheckIn("taichi", taichi)
	if why, ok := m.Placeable(nested); !ok {
		t.Fatalf("Placeable() once taichi has room = not placeable, %v; want placeable", why)
	}
	j := job("ONS-54", 1)
	j.Requires = nested
	m.Submit(j)
	want = []string{"worker p1: no nested virtualization", "worker taichi: all 1 run slots in use, 10 of 200 GiB disk free"}
	if why, ok := m.Placeable(nested); ok || !reflect.DeepEqual(why, want) {
		t.Fatalf("Placeable() while ONS-54 waits for taichi's one slot = %v, %v; want not placeable, %v", why, ok, want)
	}
}
