-- t2c_data prune script
-- Default mode: DRY-RUN (no DROP).
-- Execute mode:
--   psql "$DATABASE_URL" -v EXECUTE=1 -f backend/scripts/sql/t2c_data_prune.sql
-- Dry-run mode:
--   psql "$DATABASE_URL" -f backend/scripts/sql/t2c_data_prune.sql

\echo === t2c_data prune: inventory start ===

CREATE SCHEMA IF NOT EXISTS t2c_data;

CREATE TEMP TABLE _used_tables(name text primary key);
INSERT INTO _used_tables(name) VALUES
  ('alembic_version'),
  ('audit_logs'),
  ('columns'),
  ('data_sources'),
  ('databases'),
  ('glossary_assignments'),
  ('glossary_terms'),
  ('roles'),
  ('scan_diffs'),
  ('scan_runs'),
  ('scan_snapshots'),
  ('schemas'),
  ('tables'),
  ('tag_assignments'),
  ('tags'),
  ('user_role'),
  ('users');

CREATE TEMP TABLE _unused_tables AS
SELECT t.tablename AS name
FROM pg_tables t
WHERE t.schemaname = 't2c_data'
  AND NOT EXISTS (SELECT 1 FROM _used_tables u WHERE u.name = t.tablename)
ORDER BY t.tablename;

\echo --- Existing tables in t2c_data ---
SELECT tablename FROM pg_tables WHERE schemaname = 't2c_data' ORDER BY tablename;

\echo --- Used tables (protected) ---
SELECT name FROM _used_tables ORDER BY name;

\echo --- Unused tables candidate for DROP ---
SELECT name FROM _unused_tables ORDER BY name;

\if :{?EXECUTE}
\echo EXECUTE mode ON: dropping only unused tables
DO $$
DECLARE
  r record;
BEGIN
  FOR r IN SELECT name FROM _unused_tables ORDER BY name LOOP
    EXECUTE format('DROP TABLE IF EXISTS t2c_data.%I CASCADE', r.name);
    RAISE NOTICE 'Dropped table: t2c_data.%', r.name;
  END LOOP;
END $$;
\else
\echo DRY-RUN mode ON: no table dropped
\endif

\echo === t2c_data prune: done ===
