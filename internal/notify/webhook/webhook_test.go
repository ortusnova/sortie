package webhook

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/sortie-ai/sortie/internal/domain"
)

func captureServer(t *testing.T) (server *httptest.Server, body func() []byte) {
	t.Helper()
	var captured []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("captureServer: read body: %v", err)
		}
		captured = b
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	return srv, func() []byte { return captured }
}

func makeNotification() domain.Notification {
	return domain.Notification{
		Envelope: domain.NotificationEnvelope{
			NotificationID: "notif-uuid-001",
			Timestamp:      "2026-06-10T14:00:00Z",
			Source:         "test-host",
			IssueID:        "issue-7",
			Identifier:     "PROJ-7",
			DispatchID:     "dispatch-uuid-001",
			SessionID:      "sess-xyz",
			Attempt:        new(2),
			Agent:          "claude-code",
		},
		Message: domain.NotificationMessage{
			Severity: "warning",
			Title:    "Needs review",
			Body:     "Agent paused for human decision.",
			Category: "decision_needed",
		},
	}
}

func TestWebhook_NewNotifier_MissingURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		config map[string]any
	}{
		{name: "absent url key", config: map[string]any{}},
		{name: "empty string url", config: map[string]any{"url": ""}},
		{name: "whitespace only url", config: map[string]any{"url": "   "}},
		{name: "nil url value", config: map[string]any{"url": nil}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := newNotifier(tt.config)
			if err == nil {
				t.Fatalf("newNotifier(%v) = nil error, want constructor error for missing url", tt.config)
			}
		})
	}
}

func TestWebhook_NewNotifier_WrongTypeVsAbsentURL(t *testing.T) {
	t.Parallel()

	t.Run("url wrong type", func(t *testing.T) {
		t.Parallel()

		_, err := newNotifier(map[string]any{"url": 4242})

		if err == nil {
			t.Fatal("newNotifier(url=4242) error = nil, want error")
		}
		if err.Error() != "webhook notifier: url: expected string, got integer" {
			t.Errorf("newNotifier(url=4242) error = %q, want %q", err.Error(), "webhook notifier: url: expected string, got integer")
		}
	})

	t.Run("url absent", func(t *testing.T) {
		t.Parallel()

		_, err := newNotifier(map[string]any{})

		if err == nil {
			t.Fatal("newNotifier({}) error = nil, want error")
		}
		if err.Error() != "webhook notifier: url is required" {
			t.Errorf("newNotifier({}) error = %q, want %q", err.Error(), "webhook notifier: url is required")
		}
	})
}

func TestWebhook_NewNotifier_ValidURL(t *testing.T) {
	t.Parallel()

	srv, _ := captureServer(t)

	n, err := newNotifier(map[string]any{"url": srv.URL})
	if err != nil {
		t.Fatalf("newNotifier(valid url) = %v, want nil", err)
	}
	if n == nil {
		t.Fatal("newNotifier(valid url) returned nil notifier")
	}
}

func TestWebhook_Send_PostsEnvelopeAndMessage(t *testing.T) {
	t.Parallel()

	srv, getBody := captureServer(t)

	n, err := newNotifier(map[string]any{"url": srv.URL})
	if err != nil {
		t.Fatalf("newNotifier: %v", err)
	}

	notif := makeNotification()
	if err := n.Send(context.Background(), notif); err != nil {
		t.Fatalf("Send: %v", err)
	}

	raw := getBody()
	if len(raw) == 0 {
		t.Fatal("Send posted empty body, want non-empty JSON")
	}

	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("Send body unmarshal: %v", err)
	}

	fields := map[string]string{
		"notification_id": "notif-uuid-001",
		"timestamp":       "2026-06-10T14:00:00Z",
		"source":          "test-host",
		"issue_id":        "issue-7",
		"identifier":      "PROJ-7",
		"dispatch_id":     "dispatch-uuid-001",
		"session_id":      "sess-xyz",
		"agent":           "claude-code",
		"severity":        "warning",
		"title":           "Needs review",
		"body":            "Agent paused for human decision.",
		"category":        "decision_needed",
	}
	for key, want := range fields {
		got, ok := m[key].(string)
		if !ok {
			t.Errorf("body[%q] missing or wrong type: %v", key, m[key])
			continue
		}
		if got != want {
			t.Errorf("body[%q] = %q, want %q", key, got, want)
		}
	}

	if attemptRaw, ok := m["attempt"]; !ok {
		t.Error("body[\"attempt\"] missing")
	} else if attempt, ok := attemptRaw.(float64); !ok || int(attempt) != 2 {
		t.Errorf("body[\"attempt\"] = %v, want 2", attemptRaw)
	}
}

func TestWebhook_Send_EmptySessionIDPostsEmptyString(t *testing.T) {
	t.Parallel()

	srv, getBody := captureServer(t)

	n, err := newNotifier(map[string]any{"url": srv.URL})
	if err != nil {
		t.Fatalf("newNotifier: %v", err)
	}

	notif := makeNotification()
	notif.Envelope.SessionID = ""
	if err := n.Send(context.Background(), notif); err != nil {
		t.Fatalf("Send: %v", err)
	}

	var m map[string]any
	if err := json.Unmarshal(getBody(), &m); err != nil {
		t.Fatalf("Send body unmarshal: %v", err)
	}

	got, ok := m["session_id"]
	if !ok {
		t.Fatal("body[\"session_id\"] key absent, want present with an empty value")
	}
	if got != "" {
		t.Errorf("body[\"session_id\"] = %v, want empty string", got)
	}
	if got, _ := m["dispatch_id"].(string); got != notif.Envelope.DispatchID {
		t.Errorf("body[\"dispatch_id\"] = %q, want %q", got, notif.Envelope.DispatchID)
	}
}

func TestWebhook_Send_ContextCancellationReturnsError(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(srv.Close)

	n, err := newNotifier(map[string]any{"url": srv.URL})
	if err != nil {
		t.Fatalf("newNotifier: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = n.Send(ctx, makeNotification())
	if err == nil {
		t.Fatal("Send(cancelled ctx) = nil, want error")
	}
}

func loadFixture(t *testing.T, name string) []byte {
	t.Helper()

	data, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatalf("reading fixture %s: %v", name, err)
	}
	return data
}

func eventPayload(recorded string, eventType domain.EventType, suffix string) string {
	return strings.TrimSuffix(recorded, "}") + `,"event_type":"` + string(eventType) + `"` + suffix + `}`
}

func TestWebhook_Send_EventType(t *testing.T) {
	t.Parallel()

	recorded := string(loadFixture(t, "agent_message_payload.json"))

	tests := []struct {
		name      string
		eventType domain.EventType
		stage     domain.StageTransition
		want      string
	}{
		{
			name:      "agent message keeps the recorded payload",
			eventType: domain.EventAgentMessage,
			want:      recorded,
		},
		{
			name:      "stage.advanced carries the decision without a reason",
			eventType: domain.EventStageAdvanced,
			stage:     domain.StageTransition{SourceRule: "specify", TargetRule: "plan", ChainID: "chain-1", HopCount: 2},
			want:      eventPayload(recorded, domain.EventStageAdvanced, `,"stage":{"source_rule":"specify","target_rule":"plan","chain_id":"chain-1","hop_count":2}`),
		},
		{
			name:      "stage.not_advanced carries the decision with its reason",
			eventType: domain.EventStageNotAdvanced,
			stage:     domain.StageTransition{SourceRule: "specify", TargetRule: "plan", ChainID: "chain-1", HopCount: 3, Reason: "ceiling"},
			want:      eventPayload(recorded, domain.EventStageNotAdvanced, `,"stage":{"source_rule":"specify","target_rule":"plan","chain_id":"chain-1","hop_count":3,"reason":"ceiling"}`),
		},
		{
			name: "a notification without an event type keeps the recorded payload",
			want: recorded,
		},
	}
	for _, eventType := range domain.EventTypes() {
		if eventType.FromOrchestrator() {
			tests = append(tests, struct {
				name      string
				eventType domain.EventType
				stage     domain.StageTransition
				want      string
			}{
				name:      "orchestrator event " + string(eventType) + " appends event_type after category",
				eventType: eventType,
				want:      eventPayload(recorded, eventType, ""),
			})
		}
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv, getBody := captureServer(t)
			n, err := newNotifier(map[string]any{"url": srv.URL})
			if err != nil {
				t.Fatalf("newNotifier: %v", err)
			}
			notification := makeNotification()
			notification.Envelope.EventType = tt.eventType
			notification.Envelope.Stage = tt.stage

			if err := n.Send(context.Background(), notification); err != nil {
				t.Fatalf("Send(%s): %v", tt.eventType, err)
			}

			if got := string(getBody()); got != tt.want {
				t.Errorf("Send(%q) body = %s, want %s", tt.eventType, got, tt.want)
			}
		})
	}
}

func TestWebhook_Send_AgentText(t *testing.T) {
	t.Parallel()

	recorded := string(loadFixture(t, "agent_message_payload.json"))
	const agentText = "Which one should the API expose?\nSoft or hard delete."
	const agentTextJSON = `"Which one should the API expose?\nSoft or hard delete."`

	tests := []struct {
		name      string
		eventType domain.EventType
		agentText string
		want      string
	}{
		{
			name:      "orchestrator event with agent text appends agent_text after event_type",
			eventType: domain.EventSessionStopped,
			agentText: agentText,
			want:      strings.TrimSuffix(recorded, "}") + `,"event_type":"session.stopped","agent_text":` + agentTextJSON + `}`,
		},
		{
			name:      "orchestrator event without agent text omits the key",
			eventType: domain.EventSessionStopped,
			want:      strings.TrimSuffix(recorded, "}") + `,"event_type":"session.stopped"}`,
		},
		{
			name:      "agent message never carries agent_text",
			eventType: domain.EventAgentMessage,
			agentText: agentText,
			want:      recorded,
		},
		{
			name:      "a notification without an event type never carries agent_text",
			agentText: agentText,
			want:      recorded,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv, getBody := captureServer(t)
			n, err := newNotifier(map[string]any{"url": srv.URL})
			if err != nil {
				t.Fatalf("newNotifier: %v", err)
			}
			notification := makeNotification()
			notification.Envelope.EventType = tt.eventType
			notification.Message.AgentText = tt.agentText

			if err := n.Send(context.Background(), notification); err != nil {
				t.Fatalf("Send(%q): %v", tt.eventType, err)
			}

			if got := string(getBody()); got != tt.want {
				t.Errorf("Send(%q, AgentText=%q) body = %s, want %s", tt.eventType, tt.agentText, got, tt.want)
			}
		})
	}
}

func TestWebhook_Send_Escalation(t *testing.T) {
	t.Parallel()

	recorded := string(loadFixture(t, "agent_message_payload.json"))
	srv, getBody := captureServer(t)
	n, err := newNotifier(map[string]any{"url": srv.URL})
	if err != nil {
		t.Fatalf("newNotifier: %v", err)
	}
	notification := makeNotification()
	notification.Envelope.EventType = domain.EventSessionEscalated
	notification.Envelope.Escalation = domain.SessionEscalation{Kind: "question", Rule: "implement", URL: "https://viewer.example/s/1"}
	notification.Message.AgentText = "Which database should I target?"

	if err := n.Send(context.Background(), notification); err != nil {
		t.Fatalf("Send: %v", err)
	}

	want := eventPayload(recorded, domain.EventSessionEscalated,
		`,"agent_text":"Which database should I target?"`)
	want = strings.TrimSuffix(want, "}") + `,"escalation":{"kind":"question","rule":"implement","url":"https://viewer.example/s/1"}}`
	if got := string(getBody()); got != want {
		t.Errorf("Send(session.escalated) body = %s, want %s", got, want)
	}
}

func TestWebhook_Send_Supervisor(t *testing.T) {
	t.Parallel()

	resets := time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name  string
		event domain.EventType
		facts domain.MessageFacts
		want  string
	}{
		{
			name:  "unavailable",
			event: domain.EventSupervisorUnavailable,
			facts: domain.SupervisorUnavailableFacts{Reason: domain.SupervisorUnavailableBudget, ResetsAt: resets},
			want:  `{"reason":"budget_exhausted","resets_at":"2026-11-05T00:00:00Z"}`,
		},
		{
			name:  "available",
			event: domain.EventSupervisorAvailable,
			facts: domain.SupervisorAvailableFacts{Reason: domain.SupervisorUnavailablePaused},
			want:  `{"reason":"paused"}`,
		},
		{
			name:  "velocity",
			event: domain.EventSupervisorLoopSuspected,
			facts: domain.SupervisorLoopSuspectedFacts{HourlyTokens: 300000, HourlyLimit: 250000, Pace: 83333},
			want:  `{"hourly_tokens":300000,"hourly_limit":250000,"pace":83333}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			srv, getBody := captureServer(t)
			n, err := newNotifier(map[string]any{"url": srv.URL})
			if err != nil {
				t.Fatalf("newNotifier: %v", err)
			}
			notification := domain.Notification{
				Envelope: domain.NotificationEnvelope{NotificationID: "n-1", EventType: tt.event},
				Message:  domain.NotificationMessage{Severity: tt.event.Severity(), Facts: tt.facts},
			}
			if err := n.Send(context.Background(), notification); err != nil {
				t.Fatalf("Send: %v", err)
			}
			var got struct {
				EventType  string          `json:"event_type"`
				Supervisor json.RawMessage `json:"supervisor"`
			}
			if err := json.Unmarshal(getBody(), &got); err != nil {
				t.Fatalf("decode %s: %v", getBody(), err)
			}
			if got.EventType != string(tt.event) || string(got.Supervisor) != tt.want {
				t.Errorf("payload = %s, want event_type %s and supervisor %s", getBody(), tt.event, tt.want)
			}
		})
	}
}
