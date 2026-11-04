-- Migration 1013 (fork range): the worker a remote run ran on
--
-- worker is the ID of the worker that held the run's lease under
-- environment.kind remote. Empty for a run no worker took, for a run
-- under any other kind, and for rows written before this migration:
-- nothing is backfilled.

ALTER TABLE run_history ADD COLUMN worker TEXT NOT NULL DEFAULT '';
