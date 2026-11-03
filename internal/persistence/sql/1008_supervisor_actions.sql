-- Migration 1008 (fork range): what the supervisor token did or tried
--
-- One row per request the supervisor token made to an operator route,
-- refused ones included, with the evidence the supervisor gave and the
-- result Sortie answered. Rows are never deleted.

CREATE TABLE supervisor_actions (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    action           TEXT    NOT NULL,
    route            TEXT    NOT NULL,
    issue_identifier TEXT    NOT NULL DEFAULT '',
    evidence         TEXT    NOT NULL DEFAULT '',
    result           TEXT    NOT NULL,
    status           INTEGER NOT NULL,
    detail           TEXT    NOT NULL DEFAULT '',
    pull_request     TEXT    NOT NULL DEFAULT '',
    merge_commit     TEXT    NOT NULL DEFAULT '',
    acted_at         TEXT    NOT NULL
);

CREATE INDEX idx_supervisor_actions_acted_at ON supervisor_actions (acted_at);
