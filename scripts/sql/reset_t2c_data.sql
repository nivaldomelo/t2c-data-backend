DO $$
DECLARE
  stmt text;
BEGIN
  SELECT
    'TRUNCATE TABLE ' || string_agg(format('%I.%I', schemaname, tablename), ', ') || ' RESTART IDENTITY CASCADE'
  INTO stmt
  FROM pg_tables
  WHERE schemaname = 't2c_data'
    AND tablename NOT IN ('alembic_version');

  IF stmt IS NOT NULL THEN
    EXECUTE stmt;
  END IF;
END $$;
