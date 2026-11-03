-- Migration 1007 (fork range): sessions an operator granted an issue
--
-- One row per grant. An issue's session budget is agent.max_sessions plus
-- the sum of its rows. Rows are never deleted, so a grant outlives a
-- restart and keeps who gave it and why.

CREATE TABLE session_grants (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    issue_id   TEXT    NOT NULL,
    identifier TEXT    NOT NULL,
    sessions   INTEGER NOT NULL,
    granted_by TEXT    NOT NULL,
    reason     TEXT    NOT NULL DEFAULT '',
    granted_at TEXT    NOT NULL
);

CREATE INDEX idx_session_grants_issue_id ON session_grants (issue_id);
