-- Migration 1014 (fork range): criteria_verdicts, the latest review verdict of each story
--
-- One row per story: the per-criterion verdict its latest criteria review
-- recorded. Auto-merge merges a story's pull request only when this row
-- says every criterion is met, so the gate outlives a restart.

CREATE TABLE criteria_verdicts (
    issue_id    TEXT PRIMARY KEY,
    dispatch_id TEXT NOT NULL DEFAULT '',
    outcome     TEXT NOT NULL,
    criteria    TEXT NOT NULL DEFAULT '',
    problem     TEXT NOT NULL DEFAULT '',
    recorded_at TEXT NOT NULL
);
