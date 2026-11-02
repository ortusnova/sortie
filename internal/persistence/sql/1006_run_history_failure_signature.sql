-- Migration 1006 (fork range): the signature of a failed run
--
-- failure_signature identifies how a run failed, the same for two runs
-- that failed the same way: the failed step, the failure's class, and the
-- first line of its cause with run-specific values removed. A run whose
-- signature equals that of its issue's previous run is not retried on a
-- timer unless its class is transient. Empty for a run that did not fail
-- on an error, and for rows written before this migration: nothing is
-- backfilled.

ALTER TABLE run_history ADD COLUMN failure_signature TEXT NOT NULL DEFAULT '';
