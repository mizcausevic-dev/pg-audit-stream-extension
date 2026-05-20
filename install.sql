-- Standalone install path (no PGXS / no superuser-required CREATE EXTENSION needed).
-- Use this if you don't have access to the Postgres extension directory but can
-- still create schemas and objects in the target database.
--
--   psql -d your_db -f install.sql
--
-- For the proper extension install (managed via `CREATE EXTENSION audit_stream`),
-- run `make install` from the repo root (requires PGXS) then `CREATE EXTENSION
-- audit_stream;` in your database.

CREATE SCHEMA IF NOT EXISTS audit_stream;

SET search_path TO audit_stream, public;

\i audit_stream--0.1.0.sql

RESET search_path;
