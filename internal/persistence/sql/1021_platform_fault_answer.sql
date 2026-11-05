-- Migration 1021 (fork range): the answer a person gave a question
--
-- A question that asks a person to choose, such as whether a story whose
-- merge someone else reverted lands again (requeue) or is cancelled
-- (cancel), keeps the choice in answer beside who gave it in
-- action_done_by, so the choice survives a restart before Sortie carried
-- it out. Empty for every other fault and until a person answers.

ALTER TABLE platform_faults ADD COLUMN answer TEXT NOT NULL DEFAULT '';
