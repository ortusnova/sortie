-- Migration 1021 (fork range): the tracker state a question's story was
-- held in
--
-- A question the supervisor raised closes for a story once that story
-- leaves the tracker state it was in when the question took it: state
-- holds that state, empty until Sortie observed it and for a hold that is
-- not a question's.

ALTER TABLE platform_fault_holds ADD COLUMN state TEXT NOT NULL DEFAULT '';
