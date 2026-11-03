-- Migration 1009 (fork range): the runs workers hold under the remote kind
--
-- One row per running remote attempt, from its lease grant to its end. A
-- process that restarts re-adopts every row: it restores the attempt's
-- lease and job tokens and follows the run to its end, so a restart
-- interrupts no run. record is the attempt's job, written at the grant.
-- entry is the run as the orchestrator tracks it (issue, dispatch, stage,
-- budgets, broker), and the session columns follow the agent's session.

CREATE TABLE remote_runs (
    issue_id        TEXT    PRIMARY KEY,
    job_id          TEXT    NOT NULL,
    attempt         INTEGER NOT NULL,
    worker_id       TEXT    NOT NULL,
    record          TEXT    NOT NULL,
    entry           TEXT    NOT NULL DEFAULT '',
    session_id      TEXT    NOT NULL DEFAULT '',
    turns_completed INTEGER NOT NULL DEFAULT 0,
    session_ended   INTEGER NOT NULL DEFAULT 0,
    updated_at      TEXT    NOT NULL
);
