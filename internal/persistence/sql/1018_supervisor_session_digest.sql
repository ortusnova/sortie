-- Migration 1018 (fork range): the state digest of each supervisor session
--
-- digest is the state digest Sortie computed when it started the session.
-- A session starts only when the digest differs from the last session's,
-- so the column outlives a restart. actions counts the supervisor actions
-- Sortie took in the session. It is -1 while the session runs, and when
-- the count could not be read. Rows written before this migration keep ''
-- and -1.

ALTER TABLE supervisor_sessions ADD COLUMN digest TEXT NOT NULL DEFAULT '';
ALTER TABLE supervisor_sessions ADD COLUMN actions INTEGER NOT NULL DEFAULT (-1);
