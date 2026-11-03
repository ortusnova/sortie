package sentence

import (
	"strings"
	"testing"
	"time"

	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/orchestrator"
)

const (
	key      = "MT-1"
	fallback = "Status not available"
)

var (
	now  = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	soon = now.Add(40 * time.Second)
	pr   = orchestrator.SnapshotPullRequest{Number: 7}
)

func escalated(e Entry) Entry {
	e.PendingReaction.EscalatedAt = new(now)
	return e
}

func waiting(wait string, e Entry) Entry {
	e.PendingReaction.Wait = wait
	return e
}

func TestFor(t *testing.T) {
	t.Parallel()

	reaction := func(kind string, check *orchestrator.ReactionCheck) Entry {
		return Entry{PendingReaction: &orchestrator.SnapshotPendingReaction{
			Identifier: key, Kind: kind, PullRequest: pr, NextPollAt: soon, LastCheck: check,
		}}
	}
	continuation := func(kind string) Entry {
		return Entry{Running: &orchestrator.SnapshotRunningEntry{
			Identifier: key, ReactionKind: kind,
			Continuation: &orchestrator.SnapshotContinuation{Number: 7, Attempt: 1},
		}}
	}
	retry := func(r orchestrator.SnapshotRetryEntry) Entry {
		r.Identifier, r.Attempt = key, 2
		if r.DueAtMS == 0 {
			r.DueAtMS = soon.UnixMilli()
		}
		return Entry{Retry: &r}
	}
	held := func(h orchestrator.SnapshotBudgetEntry) Entry {
		h.Identifier = key
		return Entry{BudgetHold: &h}
	}
	parked := func(reason string) Entry {
		return Entry{Parked: &orchestrator.SnapshotParkedEntry{Identifier: key, Reason: reason, Label: "sortie-parked"}}
	}
	finished := func(f Finished) Entry {
		f.Identifier = key
		return Entry{Finished: &f}
	}
	holding := func(h orchestrator.SnapshotHold) Entry {
		return Entry{Held: &orchestrator.SnapshotHeldEntry{IssueID: "id-1", Identifier: key, Hold: h}}
	}
	slots := func(slot orchestrator.SnapshotSlotHold) Entry {
		return holding(orchestrator.SnapshotHold{Reason: orchestrator.SkipAgentSlotsFull, Slot: &slot})
	}
	spend := func(reason orchestrator.SkipReason, resetsAt time.Time) Entry {
		return holding(orchestrator.SnapshotHold{Reason: reason, ResetsAt: resetsAt})
	}

	awaitingApproval := &orchestrator.ReactionCheck{Comments: &orchestrator.CommentCheck{AwaitingApproval: true}}

	tests := []struct {
		name  string
		entry Entry
		short string
		long  string
	}{
		{
			name:  "running self-review",
			entry: Entry{Running: &orchestrator.SnapshotRunningEntry{Identifier: key, SelfReviewActive: true}},
			short: "Self-review",
		},
		{
			name:  "running CI fix without a shape",
			entry: continuation("ci"),
			short: "Fixing CI on #7, fix 1",
			long:  "Fixing CI on PR #7, fix 1.",
		},
		{"running merge conflict", continuation("merge-conflict"), "Resolving the merge conflict on #7, rebase 1", ""},
		{"running review fix", continuation("review"), "Answering review comments on #7, fix 1", ""},
		{"running bot review fix", continuation("bot-review"), "Answering bot review comments on #7, fix 1", ""},
		{"running label review", continuation("label-review"), "Reviewing #7", ""},
		{"running label fix", continuation("label-fix"), "Fixing #7", ""},
		{
			name:  "running before the first turn",
			entry: Entry{Running: &orchestrator.SnapshotRunningEntry{Identifier: key}},
			short: "Starting, no turn yet",
		},
		{
			name:  "running turn without a stage position",
			entry: Entry{Running: &orchestrator.SnapshotRunningEntry{Identifier: key, TurnCount: 3}},
			short: "Turn 3",
			long:  "Running, turn 3.",
		},
		{
			name: "running turn on a stage",
			entry: Entry{Running: &orchestrator.SnapshotRunningEntry{
				Identifier: key, TurnCount: 3,
				StagePosition: &orchestrator.SnapshotStagePosition{Chain: []string{"plan", "implement"}, Current: "implement"},
			}},
			short: "Turn 3 on stage implement",
			long:  "Running stage implement (2 of 2), turn 3.",
		},

		{"retry paused", retry(orchestrator.SnapshotRetryEntry{Paused: true, Error: "reaction retry paused by issue state"}), "Retry 2 paused: the issue's tracker state does not allow it", ""},
		{"retry due now", retry(orchestrator.SnapshotRetryEntry{DueAtMS: now.UnixMilli()}), "Retry 2 due now", ""},
		{"retry after an error", retry(orchestrator.SnapshotRetryEntry{Trigger: "error"}), "Retry 2 in 40s after an error", ""},
		{"retry after a clean run", retry(orchestrator.SnapshotRetryEntry{Trigger: "continuation"}), "Next run in 40s: the issue is still active", ""},
		{"retry after a stall", retry(orchestrator.SnapshotRetryEntry{Trigger: "stall"}), "Retry 2 in 40s: the agent stalled", ""},
		{"retry timer", retry(orchestrator.SnapshotRetryEntry{Trigger: "timer", Error: "no available orchestrator slots"}), "Retry 2 in 40s: no agent slot was free", ""},
		{"retry CI fix", retry(orchestrator.SnapshotRetryEntry{Trigger: "ci_fix"}), "CI fix run in 40s", ""},
		{"retry story fault", retry(orchestrator.SnapshotRetryEntry{Trigger: "story_fault"}), "Fix run in 40s after the same failure twice", ""},
		{"retry reaction", retry(orchestrator.SnapshotRetryEntry{ReactionKind: "review"}), "Review fix run in 40s", ""},
		{"retry without a trigger", retry(orchestrator.SnapshotRetryEntry{}), "Retry 2 in 40s", ""},

		{
			name:  "token budget with no measured usage",
			entry: held(orchestrator.SnapshotBudgetEntry{Reason: "token_budget"}),
			short: "Held: token budget reached",
			long:  "Held at the token budget. Tokens used: not measured.",
		},
		{
			name: "token budget with an unmeasured session",
			entry: held(orchestrator.SnapshotBudgetEntry{
				Reason: "token_budget", UsedTokens: new(int64(704000)), UnmeasuredSessions: new(1),
			}),
			short: "Held: token budget reached",
			long:  "Held at the token budget. Tokens used: at least 704K.",
		},
		{
			name: "token budget fully measured",
			entry: held(orchestrator.SnapshotBudgetEntry{
				Reason: "token_budget", UsedTokens: new(int64(704000)), UnmeasuredSessions: new(0),
			}),
			short: "Held: token budget reached",
			long:  "Held at the token budget. Tokens used: 704K.",
		},
		{"session budget", held(orchestrator.SnapshotBudgetEntry{Reason: "session_budget"}), "Held: session budget reached", ""},

		{"parked by the agent", parked("agent_blocked"), "Parked: the agent needs a person", ""},
		{"parked without evidence of work", parked("handoff_absence"), "Parked: no evidence of work", ""},
		{"parked on a repeated failure", parked("repeated_failure"), "Parked: the same failure twice", ""},

		{"escalated CI", escalated(reaction("ci", nil)), "CI escalated on #7", ""},
		{"escalated other reaction", escalated(reaction("review", nil)), "Review comments escalated on #7", ""},
		{"CI passing", reaction("ci", &orchestrator.ReactionCheck{CI: &orchestrator.CICheck{Status: "passing", Passing: 3}}), "CI passing on #7", ""},
		{"CI failing", reaction("ci", &orchestrator.ReactionCheck{CI: &orchestrator.CICheck{Status: "failing", Failing: 1}}), "CI failing on #7", ""},
		{"CI not yet checked", reaction("ci", nil), "Waiting on CI for #7", ""},
		{
			name:  "review comments settling",
			entry: waiting("debounce", reaction("review", &orchestrator.ReactionCheck{Comments: &orchestrator.CommentCheck{Actionable: 3, New: 2}})),
			short: "2 new review comments on #7; next check in 40s",
		},
		{"review wait", reaction("review", nil), "Waiting on review of #7", ""},
		{"review awaiting approval", reaction("review", awaitingApproval), "Waiting on approval of #7", ""},
		{"escalated review awaiting approval", escalated(reaction("review", awaitingApproval)), "Review comments escalated on #7", ""},
		{"bot review wait", reaction("bot-review", nil), "Waiting on bot review of #7", ""},
		{"bot review handled", reaction("bot-review", &orchestrator.ReactionCheck{Comments: &orchestrator.CommentCheck{Handled: true}}), "Bot review of #7 handled", ""},
		{"auto-merge wait", reaction("merge", nil), "Auto-merge waiting on #7", ""},
		{"merge conflict found", reaction("merge-conflict", &orchestrator.ReactionCheck{Mergeability: &orchestrator.MergeabilityCheck{Mergeable: new(false)}}), "Merge conflict on #7", ""},
		{"merge conflict watch", reaction("merge-conflict", nil), "Watching #7 for merge conflicts", ""},
		{"merge completion wait", reaction("merge-completion", nil), "Waiting for #7 to merge", ""},
		{"label command watch", reaction("label-review", nil), "Watching #7 for a label command", ""},
		{"other reaction", reaction("unknown", nil), "Waiting on #7", ""},

		{
			name:  "between stages",
			entry: Entry{BetweenStages: &orchestrator.SnapshotStageHop{Identifier: key, SourceRule: "implement", TargetRule: "review"}},
			short: "Between stages: implement to review",
		},

		{
			name: "held by one blocker",
			entry: holding(orchestrator.SnapshotHold{
				Reason: orchestrator.SkipBlockedBy, Blockers: []orchestrator.SnapshotBlocker{{Identifier: "MT-9", State: "In Progress"}},
			}),
			short: "Blocked by MT-9 (In Progress)",
		},
		{
			name: "held by several blockers",
			entry: holding(orchestrator.SnapshotHold{
				Reason: orchestrator.SkipBlockedBy,
				Blockers: []orchestrator.SnapshotBlocker{
					{Identifier: "MT-8", DisplayID: "acme/web#8", State: "To Do"},
					{Identifier: "MT-9"},
				},
			}),
			short: "Blocked by acme/web#8 (To Do), MT-9 (state unknown)",
		},
		{
			name: "held by a blocker another Sortie runs, not done",
			entry: holding(orchestrator.SnapshotHold{
				Reason: orchestrator.SkipBlockedBy,
				Blockers: []orchestrator.SnapshotBlocker{
					{Identifier: "ONS-16", State: "In Review", Reason: orchestrator.BlockerNotDone},
				},
			}),
			short: "Blocked by ONS-16 (In Review, not done)",
			long:  "Blocked by ONS-16 (In Review, not done). Sortie dispatches the issue once the blocker reaches the done state, with its merge recorded when this Sortie runs it.",
		},
		{
			name: "held by plan blockers, each with its reason",
			entry: holding(orchestrator.SnapshotHold{
				Reason: orchestrator.SkipBlockedBy,
				Blockers: []orchestrator.SnapshotBlocker{
					{Identifier: "ONS-15", State: "Done", Reason: orchestrator.BlockerAwaitingMerge},
					{Identifier: "ONS-16", State: "In Progress", Reason: orchestrator.BlockerNotMerged},
				},
			}),
			short: "Blocked by ONS-15 (Done, no merge recorded), ONS-16 (In Progress, not merged)",
		},
		{
			name:  "held by blockers it lists none of",
			entry: holding(orchestrator.SnapshotHold{Reason: orchestrator.SkipBlockedBy, Blockers: []orchestrator.SnapshotBlocker{}}),
			short: "Held for its blockers",
			long:  "Held for its blockers. Sortie dispatches the issue once the blocker reaches a terminal state.",
		},
		{name: "held, blockers not read after a failure", entry: holding(orchestrator.SnapshotHold{Reason: orchestrator.SkipBlockersUnresolved}), short: "Blockers not read: a tracker request failed"},
		{name: "held, blockers not reached on this poll", entry: holding(orchestrator.SnapshotHold{Reason: orchestrator.SkipBlockersNotRead}), short: "Blockers not read on this poll"},
		{name: "held, blocker list incomplete", entry: holding(orchestrator.SnapshotHold{Reason: orchestrator.SkipBlockersIncomplete}), short: "Blocker list incomplete"},
		{name: "held by the global slot limit", entry: slots(orchestrator.SnapshotSlotHold{Scope: orchestrator.SlotScopeGlobal, Used: 1, Limit: 1}), short: "No free agent slot: 1 of 1 running"},
		{
			name:  "held by a per-state slot limit",
			entry: slots(orchestrator.SnapshotSlotHold{Scope: orchestrator.SlotScopeState, State: "In Progress", Used: 2, Limit: 2}),
			short: "No free agent slot for In Progress: 2 of 2 running",
		},
		{name: "held with no free SSH host", entry: holding(orchestrator.SnapshotHold{Reason: orchestrator.SkipSSHHostsFull}), short: "No free SSH host"},
		{
			name: "held with no worker that can take its job",
			entry: holding(orchestrator.SnapshotHold{Reason: orchestrator.SkipNoWorker, Worker: &orchestrator.WorkerHold{
				Capabilities: []string{"nested"}, Waiting: []string{"worker p1: no nested virtualization", "worker taichi: 92.04 of 200 GiB disk free"}}}),
			short: "No worker can take its job: worker p1: no nested virtualization; worker taichi: 92.04 of 200 GiB disk free",
		},
		{name: "held by the daily spend ceiling", entry: spend(orchestrator.SkipDailySpendCeiling, now.Add(5*time.Minute)), short: "Daily spend ceiling reached, resets in 5m"},
		{name: "held by the weekly spend ceiling", entry: spend(orchestrator.SkipWeeklySpendCeiling, now.Add(5*time.Minute)), short: "Weekly spend ceiling reached, resets in 5m"},
		{name: "held by the monthly spend ceiling", entry: spend(orchestrator.SkipMonthlySpendCeiling, now.Add(5*time.Minute)), short: "Monthly spend ceiling reached, resets in 5m"},
		{name: "spend ceiling whose reset is not ahead", entry: spend(orchestrator.SkipDailySpendCeiling, now), short: "Daily spend ceiling reached"},
		{name: "held waiting for the stage move", entry: holding(orchestrator.SnapshotHold{Reason: orchestrator.SkipStageHopUnconfirmed}), short: "Waiting to confirm the stage move"},

		{"finished without an ending", finished(Finished{}), "Not active; no ending recorded", ""},
		{"finished merged", finished(Finished{Outcome: &Outcome{Kind: "merged", MergePath: "auto_merge", PRNumber: 88}}), "Merged #88", ""},
		{"finished handed off", finished(Finished{Outcome: &Outcome{Kind: "handed_off", State: "In Review"}}), "Handed off to In Review", ""},
		{
			"finished handed off past a refused hop",
			finished(Finished{Outcome: &Outcome{Kind: "handed_off", State: "In Review"}, StageTarget: "review", StageResult: "ceiling"}),
			"Handed off to In Review; stage review not reached", "",
		},
		{"finished in a terminal state", finished(Finished{Outcome: &Outcome{Kind: "terminal", State: "Done"}}), "Reached Done", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := For(tt.entry, Context{Now: now})

			if got.Short != tt.short {
				t.Errorf("For(%s).Short = %q, want %q", tt.name, got.Short, tt.short)
			}
			if tt.long != "" && got.Long != tt.long {
				t.Errorf("For(%s).Long = %q, want %q", tt.name, got.Long, tt.long)
			}
			if got.Short == fallback || got.Long == fallback {
				t.Errorf("For(%s) = %+v, want no fallback text", tt.name, got)
			}
			if !strings.HasSuffix(got.Long, ".") || strings.HasSuffix(got.Short, ".") {
				t.Errorf("For(%s) = %+v, want a long form ending in a period and a short form without", tt.name, got)
			}
			if tt.entry.Held != nil {
				words := Hold(&tt.entry.Held.Hold, Context{Now: now})
				if words.Reason != got.Short || !strings.HasSuffix(words.Release, ".") || strings.Contains(got.Long+words.Release, key) {
					t.Errorf("Hold(%s) = %+v, want Reason %q and a release ending in a period that does not name %q", tt.name, words, got.Short, key)
				}
			}
		})
	}
}

func TestToolLine(t *testing.T) {
	t.Parallel()

	tool := func(name string, kind domain.ToolKind, argument string) orchestrator.RunEvent {
		return orchestrator.RunEvent{Event: domain.EventToolResult, ToolName: name, ToolKind: kind, ToolArgument: argument}
	}

	tests := []struct {
		name  string
		event orchestrator.RunEvent
		want  string
	}{
		{"named shell call reads as its command", tool("Bash", domain.ToolKindShell, "git status"), "git status"},
		{"named read call", tool("Read", domain.ToolKindRead, "internal/foo.go"), "Read internal/foo.go"},
		{"nameless edit call", tool("", domain.ToolKindEdit, "internal/foo.go"), "Edit internal/foo.go"},
		{"nameless shell call without an argument", tool("", domain.ToolKindShell, ""), "Command"},
		{"name without a kind", tool("github.get_issue", "", ""), "github.get_issue"},
		{"neither name nor kind", tool("", "", ""), "Tool"},
		{"another event type", orchestrator.RunEvent{Event: domain.EventNotification, ToolName: "Bash"}, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := ToolLine(tt.event)

			if got != tt.want {
				t.Errorf("ToolLine(%+v) = %q, want %q", tt.event, got, tt.want)
			}
		})
	}
}
