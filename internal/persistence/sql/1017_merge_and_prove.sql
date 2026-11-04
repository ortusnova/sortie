-- Migration 1017 (fork range): stories merged, proven after deploy, or reverted
--
-- One row per story the supervisor sent through merge-and-prove, until its
-- run through the queue ends (proven or reverted, or given up). seq orders
-- the queue. phase is queued, merging, deploying, proving or reverting.
-- pull_request is owner/repo#number and head the commit whose CI passed
-- when the story was accepted. merge_commit is set once the pull request
-- merged, deployed lists the host=commit pairs each host deployed it at,
-- comma-separated, once every host did. phase_at is when the phase began
-- and deadline when it ends without its record, both RFC 3339 in UTC, the
-- deadline empty in the queue and while merging. revert_pull_request is
-- the number of the revert Sortie opened, 0 before. reason says why the
-- story is being reverted. The rule fields keep what the live-proof job
-- runs with.

CREATE TABLE merge_and_prove (
    issue_id            TEXT PRIMARY KEY,
    identifier          TEXT NOT NULL,
    seq                 INTEGER NOT NULL,
    phase               TEXT NOT NULL,
    pull_request        TEXT NOT NULL,
    branch              TEXT NOT NULL DEFAULT '',
    head                TEXT NOT NULL,
    criteria            TEXT NOT NULL DEFAULT '',
    merge_commit        TEXT NOT NULL DEFAULT '',
    deployed            TEXT NOT NULL DEFAULT '',
    queued_at           TEXT NOT NULL,
    phase_at            TEXT NOT NULL,
    deadline            TEXT NOT NULL DEFAULT '',
    revert_pull_request INTEGER NOT NULL DEFAULT 0,
    reason              TEXT NOT NULL DEFAULT '',
    agent_kind          TEXT NOT NULL DEFAULT '',
    configured_model    TEXT NOT NULL DEFAULT '',
    rule_name           TEXT NOT NULL DEFAULT '',
    template_id         TEXT NOT NULL DEFAULT ''
);
