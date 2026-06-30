CREATE SCHEMA IF NOT EXISTS t2c_data;

DO $$
DECLARE
  t text;
  tables text[] := ARRAY[
    'users','roles','user_role',
    'data_sources','databases','schemas','tables','columns',
    'scan_runs','scan_snapshots','scan_diffs',
    'tags','tag_assignments',
    'glossary_terms','glossary_assignments',
    'audit_logs'
  ];
BEGIN
  FOREACH t IN ARRAY tables
  LOOP
    IF EXISTS (
      SELECT 1 FROM pg_tables
      WHERE schemaname = 'public' AND tablename = t
    ) THEN
      EXECUTE format('ALTER TABLE public.%I SET SCHEMA t2c_data', t);
    END IF;
  END LOOP;
END $$;

DO $$
DECLARE
  seq record;
BEGIN
  FOR seq IN
    SELECT n.nspname AS sequence_schema, c.relname AS sequence_name
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_depend d ON d.objid = c.oid AND d.deptype = 'a'
    JOIN pg_class t ON t.oid = d.refobjid
    JOIN pg_namespace tn ON tn.oid = t.relnamespace
    WHERE c.relkind = 'S'
      AND tn.nspname = 't2c_data'
      AND n.nspname = 'public'
  LOOP
    EXECUTE format('ALTER SEQUENCE %I.%I SET SCHEMA t2c_data', seq.sequence_schema, seq.sequence_name);
  END LOOP;
END $$;
