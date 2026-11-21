-- Migration 1022 (fork range): the GitHub token sets Sortie minted
--
-- One row per set: the installation tokens of one remote attempt, one per
-- repository owner of its grant, or of one sandbox the operator asked
-- for. sealed holds the tokens, current and retired, encrypted with a key
-- derived from the GitHub App's private key, so the row alone gives no
-- token. A row lives until every token it holds is revoked or expired, so
-- a restarted process revokes the tokens of an attempt that is no longer
-- leased. grant_json is the grant the tokens are narrowed to.

CREATE TABLE forge_token_sets (
    id              TEXT    PRIMARY KEY,
    kind            TEXT    NOT NULL,
    job_id          TEXT    NOT NULL DEFAULT '',
    attempt         INTEGER NOT NULL DEFAULT 0,
    grant_json      TEXT    NOT NULL,
    sealed          TEXT    NOT NULL,
    ended           INTEGER NOT NULL DEFAULT 0,
    revoke_error    TEXT    NOT NULL DEFAULT '',
    revoke_attempts INTEGER NOT NULL DEFAULT 0,
    created_at      TEXT    NOT NULL,
    updated_at      TEXT    NOT NULL
);
