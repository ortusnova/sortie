-- Migration 1009 (fork range): standing holds, leases on issues that are not running
--
-- One row per held issue. A row keeps the issue out of dispatch until a
-- release deletes it, so the hold outlives a restart. A hold is a lease:
-- held_until is when it ends unless a renewal with new evidence moves it,
-- and expired_at is set once Sortie found it ended without one. An expired
-- row still keeps the issue out of dispatch, for a person to act on.

CREATE TABLE standing_holds (
    issue_id   TEXT PRIMARY KEY,
    identifier TEXT NOT NULL,
    holder     TEXT NOT NULL DEFAULT '',
    reason     TEXT NOT NULL DEFAULT '',
    evidence   TEXT NOT NULL DEFAULT '',
    held_at    TEXT NOT NULL,
    renewed_at TEXT NOT NULL,
    held_until TEXT NOT NULL,
    expired_at TEXT NOT NULL DEFAULT ''
);
