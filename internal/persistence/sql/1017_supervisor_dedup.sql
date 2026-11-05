-- Migration 1017 (fork range): the supervisor's questions by kind, their
-- withdrawal, and the issues it filed
--
-- A question the supervisor raises carries its action kind (credential,
-- key, approval, decision, host_action, live_proof) in question_kind and
-- who raised it, supervisor or operator, in raised_by. A later question of
-- the same story and kind joins the open one. The supervisor may withdraw
-- a question it raised: withdrawn holds the evidence it gave, and the row
-- is resolved. A question recorded before this migration counts as the
-- supervisor's, since the supervisor is who asks through that route.
--
-- supervisor_issues keeps each issue the supervisor filed with its kind
-- (platform_defect, follow_up), the stories it concerns (sorted,
-- comma-separated) and the fault signature it names, so a later filing of
-- the same stories and kind, or the same signature, joins the open issue
-- as a comment.

ALTER TABLE platform_faults ADD COLUMN question_kind TEXT NOT NULL DEFAULT '';
ALTER TABLE platform_faults ADD COLUMN raised_by TEXT NOT NULL DEFAULT '';
ALTER TABLE platform_faults ADD COLUMN withdrawn TEXT NOT NULL DEFAULT '';

UPDATE platform_faults SET raised_by = 'supervisor' WHERE class = 'supervisor_question';

CREATE TABLE supervisor_issues (
    issue_id   TEXT PRIMARY KEY,
    identifier TEXT NOT NULL,
    kind       TEXT NOT NULL,
    stories    TEXT NOT NULL DEFAULT '',
    signature  TEXT NOT NULL DEFAULT '',
    filed_at   TEXT NOT NULL
);
