-- Migration 1016 (fork range): a park a pull request record released
--
-- A parked story whose pull request holds the record its criteria verdict
-- leaves open (sortie:click-proofs passed <sha>, sortie:live-proof proven
-- <sha>) at its head is released by that record, with no new session. The
-- park row keeps the release, gesture 'record', with the pull request
-- (owner/repo#number), the head the record names and the comment ID of each
-- record, comma-separated. park_record_releases keeps the latest such
-- release of each story after the park row is gone, so the story's outcome
-- names it for as long as the outcome is of that pull request.

ALTER TABLE parked_issues ADD COLUMN release_pull_request TEXT NOT NULL DEFAULT '';
ALTER TABLE parked_issues ADD COLUMN release_head TEXT NOT NULL DEFAULT '';
ALTER TABLE parked_issues ADD COLUMN release_comment TEXT NOT NULL DEFAULT '';

CREATE TABLE park_record_releases (
    issue_id     TEXT PRIMARY KEY,
    pull_request TEXT NOT NULL,
    head         TEXT NOT NULL,
    comments     TEXT NOT NULL DEFAULT '',
    released_at  TEXT NOT NULL
);
