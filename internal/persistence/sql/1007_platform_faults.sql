-- Migration 1007 (fork range): platform faults and the stories they hold
--
-- A platform fault is a stopped failure no session of a story can fix,
-- such as a push refused for a token scope. It is recorded once, by its
-- failure signature, with its cause and the person action that clears it
-- (empty when Sortie's own check clears it). Every story that stops on it
-- has a row in platform_fault_holds and is not dispatched while the fault
-- is open. resolved_at is set when a check passes, and the holds of that
-- fault are then deleted.

CREATE TABLE platform_faults (
    id               TEXT PRIMARY KEY,
    signature        TEXT NOT NULL,
    class            TEXT NOT NULL,
    step             TEXT NOT NULL DEFAULT '',
    cause            TEXT NOT NULL DEFAULT '',
    detail           TEXT NOT NULL DEFAULT '',
    action           TEXT NOT NULL DEFAULT '',
    action_where     TEXT NOT NULL DEFAULT '',
    recorded_at      TEXT NOT NULL,
    action_done_at   TEXT NOT NULL DEFAULT '',
    action_done_by   TEXT NOT NULL DEFAULT '',
    last_check_at    TEXT NOT NULL DEFAULT '',
    last_check_error TEXT NOT NULL DEFAULT '',
    resolved_at      TEXT NOT NULL DEFAULT ''
);

CREATE TABLE platform_fault_holds (
    fault_id   TEXT NOT NULL,
    issue_id   TEXT NOT NULL,
    identifier TEXT NOT NULL,
    display_id TEXT NOT NULL DEFAULT '',
    held_at    TEXT NOT NULL,
    PRIMARY KEY (fault_id, issue_id)
);
