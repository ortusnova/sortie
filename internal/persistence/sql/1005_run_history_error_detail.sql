-- Migration 1005 (fork range): the whole cause of a failed run
--
-- error_detail is the full output of the step that failed the run, such
-- as a git command's porcelain output and standard error or an agent
-- runtime's error object, capped and masked when it was recorded. Empty
-- when the failing step left no output, for a run that did not fail, and
-- for rows written before this migration: nothing is backfilled.

ALTER TABLE run_history ADD COLUMN error_detail TEXT NOT NULL DEFAULT '';
