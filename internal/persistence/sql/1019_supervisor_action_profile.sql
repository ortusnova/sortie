-- Migration 1019 (fork range): the job profile a supervisor action named
--
-- A supervisor's fix run or live-proof request may name the
-- environment.remote.profiles entry its job runs with. profile keeps that
-- name. It is empty for an action that named none and for rows written
-- before this migration: nothing is backfilled.

ALTER TABLE supervisor_actions ADD COLUMN profile TEXT NOT NULL DEFAULT '';
