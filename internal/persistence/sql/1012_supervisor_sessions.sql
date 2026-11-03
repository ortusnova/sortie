-- Migration 1010 (fork range): the supervisor's sessions
--
-- One row per supervisor session Sortie started, with what started it,
-- how it ended, and the tokens it spent, so the supervisor's daily budget
-- and its last session outlive a restart. Rows are never deleted.

CREATE TABLE supervisor_sessions (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    trigger_kind   TEXT    NOT NULL,
    trigger_issue  TEXT    NOT NULL DEFAULT '',
    trigger_detail TEXT    NOT NULL DEFAULT '',
    triggers       INTEGER NOT NULL DEFAULT 1,
    started_at     TEXT    NOT NULL,
    ended_at       TEXT    NOT NULL DEFAULT '',
    outcome        TEXT    NOT NULL DEFAULT '',
    error          TEXT    NOT NULL DEFAULT '',
    tokens         INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX idx_supervisor_sessions_started_at ON supervisor_sessions (started_at);
