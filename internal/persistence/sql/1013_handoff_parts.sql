-- Migration 1013 (fork range): the parts of a story a session could not do
--
-- One row per part a session recorded in .sortie/handoff.json, keyed by
-- the story and the part's ID, with what it needs, how to check it, and
-- the work Sortie turned it into: a check job on a worker, an issue whose
-- run opens a pull request in another repository, or a platform fault.
-- A part closes with status 'done', and rows are never deleted.

CREATE TABLE handoff_parts (
    issue_id        TEXT    NOT NULL,
    part_id         TEXT    NOT NULL,
    identifier      TEXT    NOT NULL,
    kind            TEXT    NOT NULL,
    need            TEXT    NOT NULL,
    how_to_check    TEXT    NOT NULL,
    capabilities    TEXT    NOT NULL DEFAULT '[]',
    repository      TEXT    NOT NULL DEFAULT '',
    patch           TEXT    NOT NULL DEFAULT '',
    action          TEXT    NOT NULL DEFAULT '',
    action_where    TEXT    NOT NULL DEFAULT '',
    status          TEXT    NOT NULL,
    job_attempt     INTEGER NOT NULL DEFAULT 0,
    work_issue_id   TEXT    NOT NULL DEFAULT '',
    work_identifier TEXT    NOT NULL DEFAULT '',
    fault_id        TEXT    NOT NULL DEFAULT '',
    result          TEXT    NOT NULL DEFAULT '',
    recorded_at     TEXT    NOT NULL,
    updated_at      TEXT    NOT NULL,
    done_at         TEXT    NOT NULL DEFAULT '',
    PRIMARY KEY (issue_id, part_id)
);
