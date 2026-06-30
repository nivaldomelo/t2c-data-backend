BEGIN;

CREATE SCHEMA IF NOT EXISTS "t2c_data";

CREATE TABLE t2c_data.alembic_version (
    version_num VARCHAR(32) NOT NULL, 
    CONSTRAINT alembic_version_pkc PRIMARY KEY (version_num)
);

-- Running upgrade  -> 55da547ae3ae

CREATE TABLE t2c_data.data_sources (
    id SERIAL NOT NULL, 
    name VARCHAR(100) NOT NULL, 
    db_type VARCHAR(20) NOT NULL, 
    host VARCHAR(255) NOT NULL, 
    port INTEGER NOT NULL, 
    database VARCHAR(255) NOT NULL, 
    username VARCHAR(255) NOT NULL, 
    password TEXT NOT NULL, 
    include_schemas JSON, 
    exclude_schemas JSON, 
    is_active BOOLEAN NOT NULL, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT uq_data_sources_name UNIQUE (name)
);

CREATE TABLE t2c_data.glossary_terms (
    id SERIAL NOT NULL, 
    name VARCHAR(200) NOT NULL, 
    definition TEXT NOT NULL, 
    description TEXT, 
    steward VARCHAR(120), 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT uq_glossary_terms_name UNIQUE (name)
);

CREATE TABLE t2c_data.lineage_processes (
    id SERIAL NOT NULL, 
    name VARCHAR(200) NOT NULL, 
    description TEXT, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id)
);

CREATE TABLE t2c_data.roles (
    id SERIAL NOT NULL, 
    name VARCHAR(50) NOT NULL, 
    description VARCHAR(255), 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT uq_roles_name UNIQUE (name)
);

CREATE TABLE t2c_data.tags (
    id SERIAL NOT NULL, 
    name VARCHAR(120) NOT NULL, 
    color VARCHAR(20), 
    description VARCHAR(255), 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT uq_tags_name UNIQUE (name)
);

CREATE TABLE t2c_data.users (
    id SERIAL NOT NULL, 
    email VARCHAR(255) NOT NULL, 
    full_name VARCHAR(255), 
    password_hash VARCHAR(255) NOT NULL, 
    is_active BOOLEAN NOT NULL, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT uq_users_email UNIQUE (email)
);

CREATE TABLE t2c_data.audit_logs (
    id SERIAL NOT NULL, 
    actor_user_id INTEGER, 
    action VARCHAR(60) NOT NULL, 
    entity_type VARCHAR(40) NOT NULL, 
    entity_id INTEGER, 
    message TEXT, 
    changes JSON NOT NULL, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(actor_user_id) REFERENCES t2c_data.users (id) ON DELETE SET NULL
);

CREATE TABLE t2c_data.databases (
    id SERIAL NOT NULL, 
    datasource_id INTEGER NOT NULL, 
    name VARCHAR(100) NOT NULL, 
    description_source TEXT, 
    description_manual TEXT, 
    owner VARCHAR(120), 
    lifecycle_status VARCHAR(50), 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(datasource_id) REFERENCES t2c_data.data_sources (id) ON DELETE CASCADE, 
    CONSTRAINT uq_databases_datasource_name UNIQUE (datasource_id, name)
);

CREATE TABLE t2c_data.glossary_assignments (
    id SERIAL NOT NULL, 
    term_id INTEGER NOT NULL, 
    datasource_id INTEGER, 
    entity_type VARCHAR(40) NOT NULL, 
    entity_id INTEGER NOT NULL, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(datasource_id) REFERENCES t2c_data.data_sources (id) ON DELETE CASCADE, 
    FOREIGN KEY(term_id) REFERENCES t2c_data.glossary_terms (id) ON DELETE CASCADE, 
    CONSTRAINT uq_glossary_assignment_entity UNIQUE (term_id, entity_type, entity_id)
);

CREATE INDEX ix_t2c_data_glossary_assignments_datasource_id ON t2c_data.glossary_assignments (datasource_id);

CREATE TABLE t2c_data.lineage_edges (
    id SERIAL NOT NULL, 
    process_id INTEGER NOT NULL, 
    datasource_id INTEGER, 
    from_entity_type VARCHAR(40) NOT NULL, 
    from_entity_id INTEGER NOT NULL, 
    to_entity_type VARCHAR(40) NOT NULL, 
    to_entity_id INTEGER NOT NULL, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(datasource_id) REFERENCES t2c_data.data_sources (id) ON DELETE CASCADE, 
    FOREIGN KEY(process_id) REFERENCES t2c_data.lineage_processes (id) ON DELETE CASCADE
);

CREATE INDEX ix_t2c_data_lineage_edges_datasource_id ON t2c_data.lineage_edges (datasource_id);

CREATE TABLE t2c_data.scan_runs (
    id SERIAL NOT NULL, 
    datasource_id INTEGER NOT NULL, 
    status VARCHAR(30) NOT NULL, 
    started_by INTEGER, 
    summary JSON NOT NULL, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(datasource_id) REFERENCES t2c_data.data_sources (id) ON DELETE CASCADE, 
    FOREIGN KEY(started_by) REFERENCES t2c_data.users (id) ON DELETE SET NULL
);

CREATE TABLE t2c_data.datasource_scan_schedules (
    id SERIAL NOT NULL, 
    datasource_id INTEGER NOT NULL, 
    schedule_mode VARCHAR(20) NOT NULL DEFAULT 'manual', 
    schedule_enabled BOOLEAN NOT NULL DEFAULT true, 
    schedule_every_minutes INTEGER, 
    schedule_time VARCHAR(5), 
    schedule_day_of_week INTEGER, 
    schedule_day_of_month INTEGER, 
    schedule_anchor_date TIMESTAMP WITH TIME ZONE, 
    schedule_last_run_at TIMESTAMP WITH TIME ZONE, 
    schedule_last_started_at TIMESTAMP WITH TIME ZONE, 
    schedule_last_finished_at TIMESTAMP WITH TIME ZONE, 
    schedule_last_status VARCHAR(20), 
    schedule_last_error TEXT, 
    schedule_next_run_at TIMESTAMP WITH TIME ZONE, 
    schedule_summary TEXT, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT uq_datasource_scan_schedules_datasource UNIQUE (datasource_id), 
    FOREIGN KEY(datasource_id) REFERENCES t2c_data.data_sources (id) ON DELETE CASCADE
);

CREATE TABLE t2c_data.datasource_scan_schedule_recipients (
    schedule_id INTEGER NOT NULL, 
    user_id INTEGER NOT NULL, 
    PRIMARY KEY (schedule_id, user_id), 
    FOREIGN KEY(schedule_id) REFERENCES t2c_data.datasource_scan_schedules (id) ON DELETE CASCADE, 
    FOREIGN KEY(user_id) REFERENCES t2c_data.users (id) ON DELETE CASCADE
);

CREATE TABLE t2c_data.datasource_scan_scheduler_status (
    id SERIAL NOT NULL, 
    scheduler_name VARCHAR(80) NOT NULL DEFAULT 'datasource_scan', 
    mode VARCHAR(20) NOT NULL DEFAULT 'embedded', 
    is_enabled BOOLEAN NOT NULL DEFAULT true, 
    last_started_at VARCHAR(64), 
    last_heartbeat_at VARCHAR(64), 
    last_success_at VARCHAR(64), 
    last_failure_at VARCHAR(64), 
    last_error TEXT, 
    last_run_summary_json JSON, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id)
);

CREATE TABLE t2c_data.tag_assignments (
    id SERIAL NOT NULL, 
    tag_id INTEGER NOT NULL, 
    datasource_id INTEGER, 
    entity_type VARCHAR(40) NOT NULL, 
    entity_id INTEGER NOT NULL, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(datasource_id) REFERENCES t2c_data.data_sources (id) ON DELETE CASCADE, 
    FOREIGN KEY(tag_id) REFERENCES t2c_data.tags (id) ON DELETE CASCADE, 
    CONSTRAINT uq_tag_assignment_entity UNIQUE (tag_id, entity_type, entity_id)
);

CREATE INDEX ix_t2c_data_tag_assignments_datasource_id ON t2c_data.tag_assignments (datasource_id);

CREATE TABLE t2c_data.user_role (
    user_id INTEGER NOT NULL, 
    role_id INTEGER NOT NULL, 
    PRIMARY KEY (user_id, role_id), 
    FOREIGN KEY(role_id) REFERENCES t2c_data.roles (id) ON DELETE CASCADE, 
    FOREIGN KEY(user_id) REFERENCES t2c_data.users (id) ON DELETE CASCADE
);

CREATE TABLE t2c_data.scan_diffs (
    id SERIAL NOT NULL, 
    scan_run_id INTEGER NOT NULL, 
    entity_type VARCHAR(40) NOT NULL, 
    entity_key VARCHAR(400) NOT NULL, 
    diff_type VARCHAR(20) NOT NULL, 
    old_hash VARCHAR(64), 
    new_hash VARCHAR(64), 
    details TEXT, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(scan_run_id) REFERENCES t2c_data.scan_runs (id) ON DELETE CASCADE
);

CREATE INDEX ix_t2c_data_scan_diffs_entity_key ON t2c_data.scan_diffs (entity_key);

CREATE TABLE t2c_data.scan_snapshots (
    id SERIAL NOT NULL, 
    scan_run_id INTEGER NOT NULL, 
    entity_type VARCHAR(40) NOT NULL, 
    entity_key VARCHAR(400) NOT NULL, 
    entity_hash VARCHAR(64) NOT NULL, 
    payload JSON NOT NULL, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(scan_run_id) REFERENCES t2c_data.scan_runs (id) ON DELETE CASCADE
);

CREATE INDEX ix_t2c_data_scan_snapshots_entity_hash ON t2c_data.scan_snapshots (entity_hash);

CREATE INDEX ix_t2c_data_scan_snapshots_entity_key ON t2c_data.scan_snapshots (entity_key);

CREATE INDEX ix_t2c_data_datasource_scan_schedules_datasource_id ON t2c_data.datasource_scan_schedules (datasource_id);

CREATE INDEX ix_t2c_data_datasource_scan_schedules_schedule_enabled ON t2c_data.datasource_scan_schedules (schedule_enabled);

CREATE INDEX ix_t2c_data_datasource_scan_schedules_schedule_mode ON t2c_data.datasource_scan_schedules (schedule_mode);

CREATE INDEX ix_t2c_data_datasource_scan_schedules_schedule_next_run_at ON t2c_data.datasource_scan_schedules (schedule_next_run_at);

CREATE INDEX ix_t2c_data_datasource_scan_scheduler_status_scheduler_name ON t2c_data.datasource_scan_scheduler_status (scheduler_name);

CREATE TABLE t2c_data.schemas (
    id SERIAL NOT NULL, 
    database_id INTEGER NOT NULL, 
    name VARCHAR(100) NOT NULL, 
    description_source TEXT, 
    description_manual TEXT, 
    owner VARCHAR(120), 
    lifecycle_status VARCHAR(50), 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(database_id) REFERENCES t2c_data.databases (id) ON DELETE CASCADE, 
    CONSTRAINT uq_schemas_database_name UNIQUE (database_id, name)
);

CREATE TABLE t2c_data.tables (
    id SERIAL NOT NULL, 
    schema_id INTEGER NOT NULL, 
    name VARCHAR(200) NOT NULL, 
    table_type VARCHAR(20) NOT NULL, 
    description_source TEXT, 
    description_manual TEXT, 
    owner VARCHAR(120), 
    owner_email VARCHAR(255), 
    lifecycle_status VARCHAR(50), 
    schema_hash VARCHAR(64), 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(schema_id) REFERENCES t2c_data.schemas (id) ON DELETE CASCADE, 
    CONSTRAINT uq_tables_schema_name UNIQUE (schema_id, name)
);

CREATE INDEX ix_t2c_data_tables_schema_hash ON t2c_data.tables (schema_hash);

CREATE TABLE t2c_data.columns (
    id SERIAL NOT NULL, 
    table_id INTEGER NOT NULL, 
    name VARCHAR(200) NOT NULL, 
    data_type VARCHAR(200) NOT NULL, 
    is_primary_key BOOLEAN NOT NULL, 
    is_nullable BOOLEAN NOT NULL, 
    ordinal_position INTEGER NOT NULL, 
    description_source TEXT, 
    description_manual TEXT, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(table_id) REFERENCES t2c_data.tables (id) ON DELETE CASCADE, 
    CONSTRAINT uq_columns_table_name UNIQUE (table_id, name)
);

CREATE TABLE t2c_data.lineage_nodes (
    id SERIAL NOT NULL, 
    lineage_table_id INTEGER NOT NULL, 
    kind VARCHAR(30) NOT NULL, 
    label VARCHAR(255) NOT NULL, 
    datasource_id INTEGER, 
    table_id INTEGER, 
    meta JSON, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(datasource_id) REFERENCES t2c_data.data_sources (id) ON DELETE CASCADE, 
    FOREIGN KEY(lineage_table_id) REFERENCES t2c_data.tables (id) ON DELETE CASCADE, 
    FOREIGN KEY(table_id) REFERENCES t2c_data.tables (id) ON DELETE SET NULL
);

CREATE TABLE t2c_data.lineage_graph_edges (
    id SERIAL NOT NULL, 
    lineage_table_id INTEGER NOT NULL, 
    from_node_id INTEGER NOT NULL, 
    to_node_id INTEGER NOT NULL, 
    edge_type VARCHAR(30) NOT NULL, 
    transform TEXT, 
    notes TEXT, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(from_node_id) REFERENCES t2c_data.lineage_nodes (id) ON DELETE CASCADE, 
    FOREIGN KEY(lineage_table_id) REFERENCES t2c_data.tables (id) ON DELETE CASCADE, 
    FOREIGN KEY(to_node_id) REFERENCES t2c_data.lineage_nodes (id) ON DELETE CASCADE
);

INSERT INTO t2c_data.alembic_version (version_num) VALUES ('55da547ae3ae') RETURNING t2c_data.alembic_version.version_num;

-- Running upgrade 55da547ae3ae -> 7c5bf86829b1

CREATE TABLE t2c_data.permissions (
    id SERIAL NOT NULL, 
    name VARCHAR(120) NOT NULL, 
    description VARCHAR(255), 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT uq_permissions_name UNIQUE (name)
);

CREATE TABLE t2c_data.role_permissions (
    role_id INTEGER NOT NULL, 
    permission_id INTEGER NOT NULL, 
    PRIMARY KEY (role_id, permission_id), 
    FOREIGN KEY(permission_id) REFERENCES t2c_data.permissions (id) ON DELETE CASCADE, 
    FOREIGN KEY(role_id) REFERENCES t2c_data.roles (id) ON DELETE CASCADE
);

UPDATE t2c_data.alembic_version SET version_num='7c5bf86829b1' WHERE t2c_data.alembic_version.version_num = '55da547ae3ae';

-- Running upgrade 7c5bf86829b1 -> c9f8b65af01d

ALTER TABLE t2c_data.users ADD COLUMN name VARCHAR(255);

UPDATE t2c_data.users SET name = full_name WHERE name IS NULL AND full_name IS NOT NULL;

UPDATE t2c_data.alembic_version SET version_num='c9f8b65af01d' WHERE t2c_data.alembic_version.version_num = '7c5bf86829b1';

-- Running upgrade c9f8b65af01d -> e0a4d2c3b901

CREATE TABLE t2c_data.dq_runs (
    id SERIAL NOT NULL, 
    datasource_id INTEGER NOT NULL, 
    table_id INTEGER NOT NULL, 
    status VARCHAR(20) NOT NULL, 
    error_message TEXT, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(datasource_id) REFERENCES t2c_data.data_sources (id) ON DELETE CASCADE, 
    FOREIGN KEY(table_id) REFERENCES t2c_data.tables (id) ON DELETE CASCADE
);

CREATE INDEX ix_t2c_data_dq_runs_datasource_id ON t2c_data.dq_runs (datasource_id);

CREATE INDEX ix_t2c_data_dq_runs_table_id ON t2c_data.dq_runs (table_id);

CREATE TABLE t2c_data.dq_table_metrics (
    id SERIAL NOT NULL, 
    run_id INTEGER NOT NULL, 
    table_id INTEGER NOT NULL, 
    row_count BIGINT NOT NULL, 
    completeness_pct_avg FLOAT NOT NULL, 
    dq_score FLOAT NOT NULL, 
    duplicates_count BIGINT NOT NULL, 
    failed_rules INTEGER NOT NULL, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(run_id) REFERENCES t2c_data.dq_runs (id) ON DELETE CASCADE, 
    FOREIGN KEY(table_id) REFERENCES t2c_data.tables (id) ON DELETE CASCADE, 
    CONSTRAINT uq_dq_table_metrics_run_table UNIQUE (run_id, table_id)
);

CREATE INDEX ix_t2c_data_dq_table_metrics_run_id ON t2c_data.dq_table_metrics (run_id);

CREATE INDEX ix_t2c_data_dq_table_metrics_table_id ON t2c_data.dq_table_metrics (table_id);

CREATE TABLE t2c_data.dq_column_metrics (
    id SERIAL NOT NULL, 
    run_id INTEGER NOT NULL, 
    table_metric_id INTEGER NOT NULL, 
    column_id INTEGER, 
    column_name VARCHAR(255) NOT NULL, 
    data_type VARCHAR(255) NOT NULL, 
    null_count BIGINT NOT NULL, 
    distinct_count BIGINT NOT NULL, 
    null_pct FLOAT NOT NULL, 
    min_value TEXT, 
    max_value TEXT, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(column_id) REFERENCES t2c_data.columns (id) ON DELETE SET NULL, 
    FOREIGN KEY(run_id) REFERENCES t2c_data.dq_runs (id) ON DELETE CASCADE, 
    FOREIGN KEY(table_metric_id) REFERENCES t2c_data.dq_table_metrics (id) ON DELETE CASCADE, 
    CONSTRAINT uq_dq_column_metrics_unique UNIQUE (run_id, table_metric_id, column_name)
);

CREATE INDEX ix_t2c_data_dq_column_metrics_column_id ON t2c_data.dq_column_metrics (column_id);

CREATE INDEX ix_t2c_data_dq_column_metrics_run_id ON t2c_data.dq_column_metrics (run_id);

CREATE INDEX ix_t2c_data_dq_column_metrics_table_metric_id ON t2c_data.dq_column_metrics (table_metric_id);

UPDATE t2c_data.alembic_version SET version_num='e0a4d2c3b901' WHERE t2c_data.alembic_version.version_num = 'c9f8b65af01d';

-- Running upgrade e0a4d2c3b901 -> 5e0cb6f8c1a2

CREATE SCHEMA IF NOT EXISTS "t2c_ops";

CREATE TABLE t2c_ops.incidents (
    id SERIAL NOT NULL, 
    title VARCHAR(255) NOT NULL, 
    description TEXT, 
    entity_type VARCHAR(11) NOT NULL, 
    table_fqn VARCHAR(500), 
    airflow_dag_id VARCHAR(255), 
    detected_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    last_seen_at TIMESTAMP WITH TIME ZONE, 
    status VARCHAR(13) DEFAULT 'open' NOT NULL, 
    severity VARCHAR(4) DEFAULT 'sev3' NOT NULL, 
    owner_user_id INTEGER, 
    reporter_user_id INTEGER, 
    tags JSON, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(owner_user_id) REFERENCES t2c_data.users (id) ON DELETE SET NULL, 
    FOREIGN KEY(reporter_user_id) REFERENCES t2c_data.users (id) ON DELETE SET NULL
);

CREATE INDEX ix_incidents_status ON t2c_ops.incidents (status);

CREATE INDEX ix_incidents_severity ON t2c_ops.incidents (severity);

CREATE INDEX ix_incidents_entity_type ON t2c_ops.incidents (entity_type);

CREATE INDEX ix_incidents_detected_at ON t2c_ops.incidents (detected_at);

CREATE INDEX ix_incidents_owner_user_id ON t2c_ops.incidents (owner_user_id);

CREATE INDEX ix_incidents_table_fqn ON t2c_ops.incidents (table_fqn);

CREATE INDEX ix_incidents_airflow_dag_id ON t2c_ops.incidents (airflow_dag_id);

UPDATE t2c_data.alembic_version SET version_num='5e0cb6f8c1a2' WHERE t2c_data.alembic_version.version_num = 'e0a4d2c3b901';

-- Running upgrade 5e0cb6f8c1a2 -> 8b7a6cc9f2d1

CREATE OR REPLACE VIEW t2c_data.vw_catalog_metadata AS
        WITH table_comments AS (
            SELECT
                n.nspname AS table_schema,
                c.relname AS table_name,
                obj_description(c.oid, 'pg_class') AS table_description
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind IN ('r', 'v', 'm', 'f', 'p')
        ),
        column_comments AS (
            SELECT
                n.nspname AS table_schema,
                c.relname AS table_name,
                a.attname AS column_name,
                col_description(a.attrelid, a.attnum) AS column_description
            FROM pg_attribute a
            JOIN pg_class c ON c.oid = a.attrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE a.attnum > 0
              AND NOT a.attisdropped
              AND c.relkind IN ('r', 'v', 'm', 'f', 'p')
        ),
        pk_columns AS (
            SELECT
                kcu.table_schema,
                kcu.table_name,
                kcu.column_name,
                TRUE AS is_primary_key
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
             AND tc.table_name = kcu.table_name
            WHERE tc.constraint_type = 'PRIMARY KEY'
        ),
        fk_columns AS (
            SELECT
                kcu.table_schema,
                kcu.table_name,
                kcu.column_name,
                concat(ccu.table_schema, '.', ccu.table_name, '.', ccu.column_name) AS foreign_key_ref
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
             AND tc.table_name = kcu.table_name
            JOIN information_schema.constraint_column_usage ccu
              ON ccu.constraint_name = tc.constraint_name
             AND ccu.constraint_schema = tc.constraint_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
        )
        SELECT
            c.table_schema,
            c.table_name,
            concat(c.table_schema, '.', c.table_name) AS table_fqn,
            tc.table_description,
            c.column_name,
            c.data_type,
            c.is_nullable,
            cc.column_description,
            COALESCE(pk.is_primary_key, FALSE) AS is_primary_key,
            fk.foreign_key_ref
        FROM information_schema.columns c
        JOIN information_schema.tables t
          ON c.table_schema = t.table_schema
         AND c.table_name = t.table_name
        LEFT JOIN table_comments tc
          ON tc.table_schema = c.table_schema
         AND tc.table_name = c.table_name
        LEFT JOIN column_comments cc
          ON cc.table_schema = c.table_schema
         AND cc.table_name = c.table_name
         AND cc.column_name = c.column_name
        LEFT JOIN pk_columns pk
          ON pk.table_schema = c.table_schema
         AND pk.table_name = c.table_name
         AND pk.column_name = c.column_name
        LEFT JOIN fk_columns fk
          ON fk.table_schema = c.table_schema
         AND fk.table_name = c.table_name
         AND fk.column_name = c.column_name
        WHERE c.table_schema NOT IN ('pg_catalog', 'information_schema')
          AND c.table_schema NOT LIKE 'pg_toast%%'
          AND t.table_type IN ('BASE TABLE', 'VIEW', 'FOREIGN TABLE')
        ORDER BY c.table_schema, c.table_name, c.ordinal_position;

UPDATE t2c_data.alembic_version SET version_num='8b7a6cc9f2d1' WHERE t2c_data.alembic_version.version_num = '5e0cb6f8c1a2';

-- Running upgrade 8b7a6cc9f2d1 -> 9f2a6d1d77aa

CREATE TABLE t2c_data.dq_rules (
    id SERIAL NOT NULL, 
    table_id INTEGER, 
    execution_engine VARCHAR(20) DEFAULT 'python' NOT NULL, 
    notification_recipient_user_id INTEGER, 
    schedule_mode VARCHAR(20) DEFAULT 'manual' NOT NULL, 
    schedule_enabled BOOLEAN DEFAULT true NOT NULL, 
    schedule_every_minutes INTEGER, 
    schedule_time VARCHAR(5), 
    schedule_day_of_week INTEGER, 
    schedule_day_of_month INTEGER, 
    schedule_anchor_date TIMESTAMP WITH TIME ZONE, 
    schedule_last_run_at TIMESTAMP WITH TIME ZONE, 
    table_fqn VARCHAR(500) NOT NULL, 
    name VARCHAR(255) NOT NULL, 
    description TEXT, 
    rule_type VARCHAR(50) NOT NULL, 
    severity VARCHAR(20) NOT NULL, 
    sql_text TEXT NOT NULL, 
    is_active BOOLEAN DEFAULT true NOT NULL, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(table_id) REFERENCES t2c_data.tables (id) ON DELETE SET NULL,
    FOREIGN KEY(notification_recipient_user_id) REFERENCES t2c_data.users (id) ON DELETE SET NULL
);

CREATE INDEX ix_t2c_data_dq_rules_table_id ON t2c_data.dq_rules (table_id);

CREATE INDEX ix_t2c_data_dq_rules_execution_engine ON t2c_data.dq_rules (execution_engine);

CREATE INDEX ix_t2c_data_dq_rules_notification_recipient_user_id ON t2c_data.dq_rules (notification_recipient_user_id);

CREATE INDEX ix_t2c_data_dq_rules_schedule_mode ON t2c_data.dq_rules (schedule_mode);

CREATE INDEX ix_t2c_data_dq_rules_schedule_enabled ON t2c_data.dq_rules (schedule_enabled);

CREATE INDEX ix_t2c_data_dq_rules_schedule_every_minutes ON t2c_data.dq_rules (schedule_every_minutes);

CREATE INDEX ix_t2c_data_dq_rules_schedule_last_run_at ON t2c_data.dq_rules (schedule_last_run_at);

CREATE INDEX ix_t2c_data_dq_rules_table_fqn ON t2c_data.dq_rules (table_fqn);

CREATE INDEX ix_t2c_data_dq_rules_is_active ON t2c_data.dq_rules (is_active);

CREATE TABLE t2c_data.dq_rule_runs (
    id SERIAL NOT NULL, 
    rule_id INTEGER NOT NULL, 
    status VARCHAR(20) NOT NULL, 
    violations_count BIGINT NOT NULL, 
    sample_rows_json JSON, 
    error_message TEXT, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(rule_id) REFERENCES t2c_data.dq_rules (id) ON DELETE CASCADE
);

CREATE INDEX ix_t2c_data_dq_rule_runs_rule_id ON t2c_data.dq_rule_runs (rule_id);

UPDATE t2c_data.alembic_version SET version_num='9f2a6d1d77aa' WHERE t2c_data.alembic_version.version_num = '8b7a6cc9f2d1';

-- Running upgrade 9f2a6d1d77aa -> b742be9a32c1

CREATE SCHEMA IF NOT EXISTS "t2c_data";

CREATE TABLE IF NOT EXISTS t2c_data.dq_rules (
              id SERIAL PRIMARY KEY,
              table_id INTEGER NULL REFERENCES t2c_data.tables(id) ON DELETE SET NULL,
              execution_engine VARCHAR(20) NOT NULL DEFAULT 'python',
              notification_recipient_user_id INTEGER NULL REFERENCES t2c_data.users(id) ON DELETE SET NULL,
              schedule_mode VARCHAR(20) NOT NULL DEFAULT 'manual',
              schedule_enabled BOOLEAN NOT NULL DEFAULT true,
              schedule_every_minutes INTEGER NULL,
              schedule_time VARCHAR(5) NULL,
              schedule_day_of_week INTEGER NULL,
              schedule_day_of_month INTEGER NULL,
              schedule_anchor_date TIMESTAMP WITH TIME ZONE NULL,
              schedule_last_run_at TIMESTAMP WITH TIME ZONE NULL,
              table_fqn VARCHAR(500) NOT NULL,
              name VARCHAR(255) NOT NULL,
              description TEXT NULL,
              rule_type VARCHAR(50) NOT NULL DEFAULT 'row_violation',
              severity VARCHAR(20) NOT NULL DEFAULT 'medium',
              sql_text TEXT NOT NULL,
              is_active BOOLEAN NOT NULL DEFAULT TRUE,
              created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
              updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );

CREATE TABLE IF NOT EXISTS t2c_data.dq_rule_runs (
              id SERIAL PRIMARY KEY,
              rule_id INTEGER NOT NULL,
              status VARCHAR(20) NOT NULL DEFAULT 'pass',
              violations_count BIGINT NOT NULL DEFAULT 0,
              sample_rows_json JSON NULL,
              error_message TEXT NULL,
              created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
              updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );

CREATE INDEX IF NOT EXISTS ix_t2c_data_dq_rules_table_id ON t2c_data.dq_rules (table_id);

CREATE INDEX IF NOT EXISTS ix_t2c_data_dq_rules_execution_engine ON t2c_data.dq_rules (execution_engine);

CREATE INDEX IF NOT EXISTS ix_t2c_data_dq_rules_notification_recipient_user_id ON t2c_data.dq_rules (notification_recipient_user_id);

CREATE INDEX IF NOT EXISTS ix_t2c_data_dq_rules_schedule_mode ON t2c_data.dq_rules (schedule_mode);

CREATE INDEX IF NOT EXISTS ix_t2c_data_dq_rules_schedule_enabled ON t2c_data.dq_rules (schedule_enabled);

CREATE INDEX IF NOT EXISTS ix_t2c_data_dq_rules_schedule_every_minutes ON t2c_data.dq_rules (schedule_every_minutes);

CREATE INDEX IF NOT EXISTS ix_t2c_data_dq_rules_schedule_last_run_at ON t2c_data.dq_rules (schedule_last_run_at);

CREATE INDEX IF NOT EXISTS ix_t2c_data_dq_rules_table_fqn ON t2c_data.dq_rules (table_fqn);

CREATE INDEX IF NOT EXISTS ix_t2c_data_dq_rules_is_active ON t2c_data.dq_rules (is_active);

CREATE INDEX IF NOT EXISTS ix_t2c_data_dq_rule_runs_rule_id ON t2c_data.dq_rule_runs (rule_id);

DO $$
            BEGIN
              IF NOT EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conname = 'fk_dq_rule_runs_rule_id_dq_rules'
              ) THEN
                ALTER TABLE t2c_data.dq_rule_runs
                  ADD CONSTRAINT fk_dq_rule_runs_rule_id_dq_rules
                  FOREIGN KEY (rule_id)
                  REFERENCES t2c_data.dq_rules(id)
                  ON DELETE CASCADE;
              END IF;
            END
            $$;;

UPDATE t2c_data.alembic_version SET version_num='b742be9a32c1' WHERE t2c_data.alembic_version.version_num = '9f2a6d1d77aa';

-- Running upgrade b742be9a32c1 -> d1a8f9c77b21

CREATE SCHEMA IF NOT EXISTS "t2c_ops";

ALTER TABLE t2c_ops.incidents ADD COLUMN IF NOT EXISTS source_type VARCHAR(30);

ALTER TABLE t2c_ops.incidents ADD COLUMN IF NOT EXISTS source_ref_id INTEGER;

ALTER TABLE t2c_ops.incidents ADD COLUMN IF NOT EXISTS evidence_json JSONB;

ALTER TABLE t2c_ops.incidents ADD COLUMN IF NOT EXISTS occurrences INTEGER;

UPDATE t2c_ops.incidents SET occurrences = 1 WHERE occurrences IS NULL;

ALTER TABLE t2c_ops.incidents ALTER COLUMN occurrences SET DEFAULT 1;

ALTER TABLE t2c_ops.incidents ALTER COLUMN occurrences SET NOT NULL;

CREATE INDEX IF NOT EXISTS ix_incidents_source_ref_status ON t2c_ops.incidents (source_type, source_ref_id, status);

UPDATE t2c_data.alembic_version SET version_num='d1a8f9c77b21' WHERE t2c_data.alembic_version.version_num = 'b742be9a32c1';

-- Running upgrade d1a8f9c77b21 -> f2c7b0de91aa

CREATE SCHEMA IF NOT EXISTS "t2c_ops";

ALTER TABLE t2c_ops.incidents ADD COLUMN IF NOT EXISTS source_type VARCHAR(30);

ALTER TABLE t2c_ops.incidents ADD COLUMN IF NOT EXISTS source_ref_id INTEGER;

ALTER TABLE t2c_ops.incidents ADD COLUMN IF NOT EXISTS evidence_json JSONB;

ALTER TABLE t2c_ops.incidents ADD COLUMN IF NOT EXISTS occurrences INTEGER;

ALTER TABLE t2c_ops.incidents ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ;

UPDATE t2c_ops.incidents SET status = 'investigating' WHERE status = 'in_progress';

UPDATE t2c_ops.incidents SET occurrences = 1 WHERE occurrences IS NULL;

ALTER TABLE t2c_ops.incidents ALTER COLUMN occurrences SET DEFAULT 1;

ALTER TABLE t2c_ops.incidents ALTER COLUMN occurrences SET NOT NULL;

ALTER TABLE t2c_ops.incidents ALTER COLUMN status SET DEFAULT 'open';

ALTER TABLE t2c_ops.incidents DROP CONSTRAINT IF EXISTS incident_status;

ALTER TABLE t2c_ops.incidents ADD CONSTRAINT incident_status CHECK (status IN ('open','investigating','mitigated','resolved','closed'));

CREATE INDEX IF NOT EXISTS ix_incidents_source_ref ON t2c_ops.incidents (source_type, source_ref_id);

CREATE INDEX IF NOT EXISTS ix_incidents_source_ref_status ON t2c_ops.incidents (source_type, source_ref_id, status);

CREATE INDEX IF NOT EXISTS ix_incidents_status_detected_at ON t2c_ops.incidents (status, detected_at);

UPDATE t2c_data.alembic_version SET version_num='f2c7b0de91aa' WHERE t2c_data.alembic_version.version_num = 'd1a8f9c77b21';

-- Running upgrade f2c7b0de91aa -> a7b5fe120b3f

CREATE SCHEMA IF NOT EXISTS "t2c_ops";

ALTER TABLE t2c_ops.incidents ADD COLUMN IF NOT EXISTS source_type VARCHAR(50);

ALTER TABLE t2c_ops.incidents ADD COLUMN IF NOT EXISTS source_ref_id INTEGER;

ALTER TABLE t2c_ops.incidents ADD COLUMN IF NOT EXISTS evidence_json JSONB;

ALTER TABLE t2c_ops.incidents ADD COLUMN IF NOT EXISTS occurrences INTEGER;

ALTER TABLE t2c_ops.incidents ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ;

ALTER TABLE t2c_ops.incidents ALTER COLUMN source_type TYPE VARCHAR(50);

UPDATE t2c_ops.incidents SET occurrences = 1 WHERE occurrences IS NULL;

ALTER TABLE t2c_ops.incidents ALTER COLUMN occurrences SET DEFAULT 1;

ALTER TABLE t2c_ops.incidents ALTER COLUMN occurrences SET NOT NULL;

CREATE INDEX IF NOT EXISTS ix_incidents_source_ref ON t2c_ops.incidents (source_type, source_ref_id);

UPDATE t2c_data.alembic_version SET version_num='a7b5fe120b3f' WHERE t2c_data.alembic_version.version_num = 'f2c7b0de91aa';

-- Running upgrade a7b5fe120b3f -> b1d3a9c2f401

CREATE SCHEMA IF NOT EXISTS "t2c_data";

CREATE TABLE t2c_data.dq_job_runs (
    id SERIAL NOT NULL, 
    job_type VARCHAR(30) NOT NULL, 
    status VARCHAR(20) DEFAULT 'queued' NOT NULL, 
    table_id INTEGER, 
    table_fqn VARCHAR(500), 
    datasource_id INTEGER, 
    requested_by_user_id INTEGER, 
    spark_app_id VARCHAR(120), 
    command TEXT, 
    stdout_log TEXT, 
    stderr_log TEXT, 
    result_json JSON, 
    error_message TEXT, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(datasource_id) REFERENCES t2c_data.data_sources (id) ON DELETE SET NULL, 
    FOREIGN KEY(requested_by_user_id) REFERENCES t2c_data.users (id) ON DELETE SET NULL, 
    FOREIGN KEY(table_id) REFERENCES t2c_data.tables (id) ON DELETE SET NULL
);

CREATE INDEX ix_dq_job_runs_job_type ON t2c_data.dq_job_runs (job_type);

CREATE INDEX ix_dq_job_runs_status ON t2c_data.dq_job_runs (status);

CREATE INDEX ix_dq_job_runs_table_id ON t2c_data.dq_job_runs (table_id);

CREATE INDEX ix_dq_job_runs_table_fqn ON t2c_data.dq_job_runs (table_fqn);

CREATE INDEX ix_dq_job_runs_datasource_id ON t2c_data.dq_job_runs (datasource_id);

CREATE INDEX ix_dq_job_runs_requested_by_user_id ON t2c_data.dq_job_runs (requested_by_user_id);

UPDATE t2c_data.alembic_version SET version_num='b1d3a9c2f401' WHERE t2c_data.alembic_version.version_num = 'a7b5fe120b3f';

-- Running upgrade b1d3a9c2f401 -> 85e30b8d49cd

ALTER TABLE t2c_data.dq_runs ADD COLUMN execution_engine VARCHAR(20) DEFAULT 'python' NOT NULL;

ALTER TABLE t2c_data.dq_rule_runs ADD COLUMN execution_engine VARCHAR(20) DEFAULT 'python' NOT NULL;

ALTER TABLE t2c_data.dq_job_runs ADD COLUMN execution_engine VARCHAR(20) DEFAULT 'spark' NOT NULL;

ALTER TABLE t2c_data.dq_job_runs ADD COLUMN spark_master_url VARCHAR(255);

ALTER TABLE t2c_data.dq_job_runs ADD COLUMN logs_path VARCHAR(1000);

CREATE INDEX ix_t2c_data_dq_job_runs_execution_engine ON t2c_data.dq_job_runs (execution_engine);

ALTER TABLE t2c_data.dq_runs ALTER COLUMN execution_engine DROP DEFAULT;

ALTER TABLE t2c_data.dq_rule_runs ALTER COLUMN execution_engine DROP DEFAULT;

ALTER TABLE t2c_data.dq_job_runs ALTER COLUMN execution_engine DROP DEFAULT;

UPDATE t2c_data.alembic_version SET version_num='85e30b8d49cd' WHERE t2c_data.alembic_version.version_num = 'b1d3a9c2f401';

-- Running upgrade 85e30b8d49cd -> 1d6c80ac7e58

ALTER TABLE t2c_data.dq_runs ADD COLUMN spark_app_id TEXT;

ALTER TABLE t2c_data.dq_runs ADD COLUMN queued_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL;

ALTER TABLE t2c_data.dq_runs ADD COLUMN started_at TIMESTAMP WITH TIME ZONE;

ALTER TABLE t2c_data.dq_runs ADD COLUMN finished_at TIMESTAMP WITH TIME ZONE;

ALTER TABLE t2c_data.dq_runs ADD COLUMN duration_ms BIGINT;

ALTER TABLE t2c_data.dq_runs ADD COLUMN log_tail TEXT;

ALTER TABLE t2c_data.dq_runs ALTER COLUMN queued_at DROP DEFAULT;

UPDATE t2c_data.alembic_version SET version_num='1d6c80ac7e58' WHERE t2c_data.alembic_version.version_num = '85e30b8d49cd';

-- Running upgrade 1d6c80ac7e58 -> 6c0464c9c46a

ALTER TABLE t2c_data.dq_job_runs ADD COLUMN dq_run_id INTEGER;

CREATE INDEX ix_t2c_data_dq_job_runs_dq_run_id ON t2c_data.dq_job_runs (dq_run_id);

ALTER TABLE t2c_data.dq_job_runs ADD CONSTRAINT fk_t2c_data_dq_job_runs_dq_run_id_dq_runs FOREIGN KEY(dq_run_id) REFERENCES t2c_data.dq_runs (id) ON DELETE SET NULL;

UPDATE t2c_data.alembic_version SET version_num='6c0464c9c46a' WHERE t2c_data.alembic_version.version_num = '1d6c80ac7e58';

-- Running upgrade 6c0464c9c46a -> 2f1c4be8aa10

CREATE TABLE t2c_data.audit_log (
    id BIGSERIAL NOT NULL, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    user_id BIGINT, 
    user_email TEXT, 
    ip INET, 
    user_agent TEXT, 
    action TEXT NOT NULL, 
    entity_type TEXT, 
    entity_id TEXT, 
    route TEXT, 
    method TEXT, 
    status_code INTEGER, 
    request_id TEXT, 
    before_json JSONB, 
    after_json JSONB, 
    metadata_json JSONB, 
    PRIMARY KEY (id), 
    FOREIGN KEY(user_id) REFERENCES t2c_data.users (id) ON DELETE SET NULL
);

CREATE INDEX ix_t2c_data_audit_log_created_at ON t2c_data.audit_log (created_at);

CREATE INDEX ix_t2c_data_audit_log_user_id ON t2c_data.audit_log (user_id);

CREATE INDEX ix_t2c_data_audit_log_entity_ref ON t2c_data.audit_log (entity_type, entity_id);

UPDATE t2c_data.alembic_version SET version_num='2f1c4be8aa10' WHERE t2c_data.alembic_version.version_num = '6c0464c9c46a';

-- Running upgrade 2f1c4be8aa10 -> 7f3c9d1a4b22

ALTER TABLE t2c_data.dq_runs ALTER COLUMN datasource_id DROP NOT NULL;

ALTER TABLE t2c_data.dq_runs ALTER COLUMN table_id DROP NOT NULL;

ALTER TABLE t2c_data.dq_runs ADD COLUMN scope VARCHAR(20);

ALTER TABLE t2c_data.dq_runs ADD COLUMN schema_name VARCHAR(255);

ALTER TABLE t2c_data.dq_runs ADD COLUMN parent_run_id BIGINT;

UPDATE t2c_data.dq_runs SET scope = 'table' WHERE scope IS NULL;

ALTER TABLE t2c_data.dq_runs ALTER COLUMN scope SET NOT NULL;

ALTER TABLE t2c_data.dq_runs ALTER COLUMN scope SET DEFAULT 'table';

CREATE INDEX ix_t2c_data_dq_runs_scope ON t2c_data.dq_runs (scope);

CREATE INDEX ix_t2c_data_dq_runs_schema_name ON t2c_data.dq_runs (schema_name);

CREATE INDEX ix_t2c_data_dq_runs_parent_run_id ON t2c_data.dq_runs (parent_run_id);

ALTER TABLE t2c_data.dq_runs ADD CONSTRAINT fk_t2c_data_dq_runs_parent_run_id FOREIGN KEY(parent_run_id) REFERENCES t2c_data.dq_runs (id) ON DELETE CASCADE;

UPDATE t2c_data.alembic_version SET version_num='7f3c9d1a4b22' WHERE t2c_data.alembic_version.version_num = '2f1c4be8aa10';

-- Running upgrade 7f3c9d1a4b22 -> 3c1f0b7d2e4a

CREATE TABLE t2c_data.data_owners (
    id SERIAL NOT NULL, 
    name VARCHAR(160) NOT NULL, 
    email VARCHAR(255) NOT NULL, 
    area VARCHAR(160), 
    description TEXT, 
    is_active BOOLEAN DEFAULT true NOT NULL, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT uq_data_owners_email UNIQUE (email)
);

ALTER TABLE t2c_data.tables ADD COLUMN data_owner_id INTEGER;

ALTER TABLE t2c_data.tables ADD CONSTRAINT fk_tables_data_owner_id_data_owners FOREIGN KEY(data_owner_id) REFERENCES t2c_data.data_owners (id) ON DELETE SET NULL;

CREATE INDEX ix_tables_data_owner_id ON t2c_data.tables (data_owner_id);

UPDATE t2c_data.alembic_version SET version_num='3c1f0b7d2e4a' WHERE t2c_data.alembic_version.version_num = '7f3c9d1a4b22';

-- Running upgrade 3c1f0b7d2e4a -> 6f4d2b8c1a90

ALTER TABLE t2c_data.tags ADD COLUMN external_id VARCHAR(40);

ALTER TABLE t2c_data.tags ADD COLUMN slug VARCHAR(160);

ALTER TABLE t2c_data.tags ADD COLUMN group_name VARCHAR(120);

ALTER TABLE t2c_data.tags ADD COLUMN subgroup_name VARCHAR(120);

ALTER TABLE t2c_data.tags ADD COLUMN example_of_use TEXT;

ALTER TABLE t2c_data.tags ADD COLUMN tag_type VARCHAR(120);

ALTER TABLE t2c_data.tags ADD COLUMN suggested_scope VARCHAR(160);

ALTER TABLE t2c_data.tags ADD COLUMN status VARCHAR(30) DEFAULT 'active' NOT NULL;

ALTER TABLE t2c_data.tags ADD COLUMN synonyms TEXT;

ALTER TABLE t2c_data.tags ADD COLUMN notes TEXT;

ALTER TABLE t2c_data.tags ALTER COLUMN description TYPE TEXT;

UPDATE t2c_data.tags
        SET slug = lower(
            trim(
                regexp_replace(
                    regexp_replace(name, '[^a-zA-Z0-9]+', '-', 'g'),
                    '-{2,}',
                    '-',
                    'g'
                )
            )
        );

UPDATE t2c_data.tags SET slug = trim(both '-' from slug);

UPDATE t2c_data.tags
        SET slug = concat('tag-', id)
        WHERE slug IS NULL OR slug = '';

ALTER TABLE t2c_data.tags ALTER COLUMN slug SET NOT NULL;

ALTER TABLE t2c_data.tags ADD CONSTRAINT uq_tags_slug UNIQUE (slug);

ALTER TABLE t2c_data.tags ADD CONSTRAINT uq_tags_external_id UNIQUE (external_id);

UPDATE t2c_data.alembic_version SET version_num='6f4d2b8c1a90' WHERE t2c_data.alembic_version.version_num = '3c1f0b7d2e4a';

-- Running upgrade 6f4d2b8c1a90 -> 8a2d4f1c6b77

ALTER TABLE t2c_data.glossary_terms ADD COLUMN external_id VARCHAR(40);

ALTER TABLE t2c_data.glossary_terms ADD COLUMN slug VARCHAR(160);

ALTER TABLE t2c_data.glossary_terms ADD COLUMN category VARCHAR(120);

ALTER TABLE t2c_data.glossary_terms ADD COLUMN subcategory VARCHAR(120);

ALTER TABLE t2c_data.glossary_terms ADD COLUMN example_of_use TEXT;

ALTER TABLE t2c_data.glossary_terms ADD COLUMN synonyms TEXT;

ALTER TABLE t2c_data.glossary_terms ADD COLUMN suggested_priority VARCHAR(40);

ALTER TABLE t2c_data.glossary_terms ADD COLUMN status VARCHAR(30) DEFAULT 'active' NOT NULL;

ALTER TABLE t2c_data.glossary_terms ADD COLUMN tag_labels TEXT;

ALTER TABLE t2c_data.glossary_terms ADD COLUMN notes TEXT;

UPDATE t2c_data.glossary_terms
        SET slug = lower(
            trim(
                regexp_replace(
                    regexp_replace(name, '[^a-zA-Z0-9]+', '-', 'g'),
                    '-{2,}',
                    '-',
                    'g'
                )
            )
        );

UPDATE t2c_data.glossary_terms SET slug = trim(both '-' from slug);

UPDATE t2c_data.glossary_terms
        SET slug = concat('term-', id)
        WHERE slug IS NULL OR slug = '';

ALTER TABLE t2c_data.glossary_terms ALTER COLUMN slug SET NOT NULL;

ALTER TABLE t2c_data.glossary_terms ADD CONSTRAINT uq_glossary_terms_slug UNIQUE (slug);

ALTER TABLE t2c_data.glossary_terms ADD CONSTRAINT uq_glossary_terms_external_id UNIQUE (external_id);

UPDATE t2c_data.alembic_version SET version_num='8a2d4f1c6b77' WHERE t2c_data.alembic_version.version_num = '6f4d2b8c1a90';

-- Running upgrade 8a2d4f1c6b77 -> 9b7e3c2d1f44

ALTER TABLE t2c_data.columns ADD COLUMN external_id VARCHAR(64);

ALTER TABLE t2c_data.columns ADD COLUMN slug VARCHAR(255);

ALTER TABLE t2c_data.columns ADD COLUMN udt_name VARCHAR(255);

ALTER TABLE t2c_data.columns ADD COLUMN character_maximum_length INTEGER;

ALTER TABLE t2c_data.columns ADD COLUMN numeric_precision INTEGER;

ALTER TABLE t2c_data.columns ADD COLUMN numeric_scale INTEGER;

ALTER TABLE t2c_data.columns ADD COLUMN column_default TEXT;

ALTER TABLE t2c_data.columns ADD COLUMN existing_comment TEXT;

ALTER TABLE t2c_data.columns ADD COLUMN dictionary_description TEXT;

ALTER TABLE t2c_data.columns ADD COLUMN dictionary_comment TEXT;

CREATE INDEX ix_columns_slug ON t2c_data.columns (slug);

UPDATE t2c_data.alembic_version SET version_num='9b7e3c2d1f44' WHERE t2c_data.alembic_version.version_num = '8a2d4f1c6b77';

-- Running upgrade 9b7e3c2d1f44 -> a31c5d9f4b22

ALTER TABLE t2c_data.tables ADD COLUMN certification_status VARCHAR(40) DEFAULT 'not_assessed' NOT NULL;

ALTER TABLE t2c_data.tables ADD COLUMN certification_criticality VARCHAR(20);

ALTER TABLE t2c_data.tables ADD COLUMN certification_badges JSON;

ALTER TABLE t2c_data.tables ADD COLUMN certification_notes TEXT;

ALTER TABLE t2c_data.tables ADD COLUMN certification_decided_by_user_id INTEGER;

ALTER TABLE t2c_data.tables ADD COLUMN certification_decided_at TIMESTAMP WITH TIME ZONE;

ALTER TABLE t2c_data.tables ADD COLUMN certification_review_at TIMESTAMP WITH TIME ZONE;

ALTER TABLE t2c_data.tables ADD CONSTRAINT fk_tables_certification_decided_by_user_id_users FOREIGN KEY(certification_decided_by_user_id) REFERENCES t2c_data.users (id) ON DELETE SET NULL;

CREATE INDEX ix_tables_certification_status ON t2c_data.tables (certification_status);

CREATE INDEX ix_tables_certification_criticality ON t2c_data.tables (certification_criticality);

CREATE INDEX ix_tables_certification_decided_by_user_id ON t2c_data.tables (certification_decided_by_user_id);

UPDATE t2c_data.alembic_version SET version_num='a31c5d9f4b22' WHERE t2c_data.alembic_version.version_num = '9b7e3c2d1f44';

-- Running upgrade a31c5d9f4b22 -> c4e7a2b1d9f0

ALTER TABLE "t2c_data"."tables" ADD COLUMN IF NOT EXISTS certification_badges JSON;

UPDATE t2c_data.alembic_version SET version_num='c4e7a2b1d9f0' WHERE t2c_data.alembic_version.version_num = 'a31c5d9f4b22';

-- Running upgrade c4e7a2b1d9f0 -> e6d4a1b9c2f3

ALTER TABLE t2c_data.data_sources ADD COLUMN connection_config JSON;

UPDATE t2c_data.alembic_version SET version_num='e6d4a1b9c2f3' WHERE t2c_data.alembic_version.version_num = 'c4e7a2b1d9f0';

-- Running upgrade e6d4a1b9c2f3 -> f7c1d2e3a4b5

ALTER TABLE t2c_data.tables ADD COLUMN sensitivity_level VARCHAR(30);

ALTER TABLE t2c_data.tables ADD COLUMN has_personal_data BOOLEAN DEFAULT false NOT NULL;

ALTER TABLE t2c_data.tables ADD COLUMN has_sensitive_personal_data BOOLEAN DEFAULT false NOT NULL;

ALTER TABLE t2c_data.tables ADD COLUMN legal_basis VARCHAR(50);

ALTER TABLE t2c_data.tables ADD COLUMN retention_policy VARCHAR(255);

ALTER TABLE t2c_data.tables ADD COLUMN is_masked BOOLEAN DEFAULT false NOT NULL;

ALTER TABLE t2c_data.tables ADD COLUMN external_sharing BOOLEAN DEFAULT false NOT NULL;

ALTER TABLE t2c_data.tables ADD COLUMN access_scope VARCHAR(30);

ALTER TABLE t2c_data.tables ADD COLUMN access_roles JSON;

ALTER TABLE t2c_data.tables ADD COLUMN privacy_notes TEXT;

ALTER TABLE t2c_data.tables ADD COLUMN privacy_reviewed_by_user_id INTEGER;

ALTER TABLE t2c_data.tables ADD COLUMN privacy_reviewed_at TIMESTAMP WITH TIME ZONE;

ALTER TABLE t2c_data.tables ADD CONSTRAINT fk_tables_privacy_reviewed_by_user_id_users FOREIGN KEY(privacy_reviewed_by_user_id) REFERENCES t2c_data.users (id) ON DELETE SET NULL;

ALTER TABLE t2c_data.tables ALTER COLUMN has_personal_data DROP DEFAULT;

ALTER TABLE t2c_data.tables ALTER COLUMN has_sensitive_personal_data DROP DEFAULT;

ALTER TABLE t2c_data.tables ALTER COLUMN is_masked DROP DEFAULT;

ALTER TABLE t2c_data.tables ALTER COLUMN external_sharing DROP DEFAULT;

UPDATE t2c_data.alembic_version SET version_num='f7c1d2e3a4b5' WHERE t2c_data.alembic_version.version_num = 'e6d4a1b9c2f3';

-- Running upgrade f7c1d2e3a4b5 -> ab39c8d4e2f1

CREATE TABLE t2c_data.lineage_assets (
    id SERIAL NOT NULL, 
    catalog_table_id INTEGER, 
    datasource_id INTEGER, 
    asset_key VARCHAR(255) NOT NULL, 
    asset_name VARCHAR(255) NOT NULL, 
    asset_type VARCHAR(30) NOT NULL, 
    layer VARCHAR(30) NOT NULL, 
    schema_name VARCHAR(100), 
    object_name VARCHAR(200), 
    system_name VARCHAR(120), 
    description TEXT, 
    is_active BOOLEAN DEFAULT true NOT NULL, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(catalog_table_id) REFERENCES t2c_data.tables (id) ON DELETE SET NULL, 
    FOREIGN KEY(datasource_id) REFERENCES t2c_data.data_sources (id) ON DELETE SET NULL, 
    CONSTRAINT uq_lineage_assets_asset_key UNIQUE (asset_key), 
    CONSTRAINT uq_lineage_assets_catalog_table_id UNIQUE (catalog_table_id)
);

CREATE INDEX ix_lineage_assets_asset_key ON t2c_data.lineage_assets (asset_key);

CREATE INDEX ix_lineage_assets_catalog_table_id ON t2c_data.lineage_assets (catalog_table_id);

CREATE INDEX ix_lineage_assets_datasource_id ON t2c_data.lineage_assets (datasource_id);

CREATE INDEX ix_lineage_assets_asset_type ON t2c_data.lineage_assets (asset_type);

CREATE INDEX ix_lineage_assets_layer ON t2c_data.lineage_assets (layer);

CREATE INDEX ix_lineage_assets_is_active ON t2c_data.lineage_assets (is_active);

CREATE TABLE t2c_data.lineage_relations (
    id SERIAL NOT NULL, 
    source_asset_id INTEGER NOT NULL, 
    target_asset_id INTEGER NOT NULL, 
    relation_type VARCHAR(30) NOT NULL, 
    process_name VARCHAR(255), 
    process_type VARCHAR(50), 
    dashboard_name VARCHAR(255), 
    notes TEXT, 
    discovery_method VARCHAR(30) DEFAULT 'manual' NOT NULL, 
    confidence_score INTEGER DEFAULT '100' NOT NULL, 
    is_active BOOLEAN DEFAULT true NOT NULL, 
    created_by_user_id INTEGER, 
    updated_by_user_id INTEGER, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(source_asset_id) REFERENCES t2c_data.lineage_assets (id) ON DELETE CASCADE, 
    FOREIGN KEY(target_asset_id) REFERENCES t2c_data.lineage_assets (id) ON DELETE CASCADE, 
    FOREIGN KEY(created_by_user_id) REFERENCES t2c_data.users (id) ON DELETE SET NULL, 
    FOREIGN KEY(updated_by_user_id) REFERENCES t2c_data.users (id) ON DELETE SET NULL
);

CREATE INDEX ix_lineage_relations_source_asset_id ON t2c_data.lineage_relations (source_asset_id);

CREATE INDEX ix_lineage_relations_target_asset_id ON t2c_data.lineage_relations (target_asset_id);

CREATE INDEX ix_lineage_relations_relation_type ON t2c_data.lineage_relations (relation_type);

CREATE INDEX ix_lineage_relations_is_active ON t2c_data.lineage_relations (is_active);

ALTER TABLE t2c_data.lineage_assets ALTER COLUMN is_active DROP DEFAULT;

ALTER TABLE t2c_data.lineage_relations ALTER COLUMN discovery_method DROP DEFAULT;

ALTER TABLE t2c_data.lineage_relations ALTER COLUMN confidence_score DROP DEFAULT;

ALTER TABLE t2c_data.lineage_relations ALTER COLUMN is_active DROP DEFAULT;

UPDATE t2c_data.alembic_version SET version_num='ab39c8d4e2f1' WHERE t2c_data.alembic_version.version_num = 'f7c1d2e3a4b5';

-- Running upgrade ab39c8d4e2f1 -> c1d2e3f4a5b6

CREATE TABLE t2c_data.lineage_source_configs (
    id SERIAL NOT NULL, 
    name VARCHAR(120) NOT NULL, 
    source_type VARCHAR(30) NOT NULL, 
    base_url VARCHAR(500) NOT NULL, 
    default_namespace VARCHAR(255), 
    auth_type VARCHAR(30), 
    auth_username VARCHAR(255), 
    auth_secret TEXT, 
    enabled BOOLEAN DEFAULT true NOT NULL, 
    last_sync_at VARCHAR(40), 
    last_sync_status VARCHAR(30), 
    last_sync_message TEXT, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT uq_lineage_source_configs_name UNIQUE (name)
);

CREATE INDEX ix_lineage_source_configs_enabled ON t2c_data.lineage_source_configs (enabled);

CREATE INDEX ix_lineage_source_configs_source_type ON t2c_data.lineage_source_configs (source_type);

CREATE TABLE t2c_data.lineage_jobs (
    id SERIAL NOT NULL, 
    lineage_source_id INTEGER NOT NULL, 
    namespace VARCHAR(255) NOT NULL, 
    job_name VARCHAR(500) NOT NULL, 
    display_name VARCHAR(500) NOT NULL, 
    job_type VARCHAR(80), 
    location VARCHAR(500), 
    latest_run_id VARCHAR(255), 
    latest_run_status VARCHAR(80), 
    latest_run_at VARCHAR(40), 
    raw_json TEXT, 
    is_active BOOLEAN DEFAULT true NOT NULL, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(lineage_source_id) REFERENCES t2c_data.lineage_source_configs (id) ON DELETE CASCADE, 
    CONSTRAINT uq_lineage_jobs_source_namespace_job UNIQUE (lineage_source_id, namespace, job_name)
);

CREATE INDEX ix_lineage_jobs_is_active ON t2c_data.lineage_jobs (is_active);

CREATE INDEX ix_lineage_jobs_job_name ON t2c_data.lineage_jobs (job_name);

CREATE INDEX ix_lineage_jobs_lineage_source_id ON t2c_data.lineage_jobs (lineage_source_id);

CREATE INDEX ix_lineage_jobs_namespace ON t2c_data.lineage_jobs (namespace);

CREATE TABLE t2c_data.lineage_runs (
    id SERIAL NOT NULL, 
    lineage_job_id INTEGER NOT NULL, 
    external_run_id VARCHAR(255) NOT NULL, 
    status VARCHAR(80), 
    started_at VARCHAR(40), 
    ended_at VARCHAR(40), 
    nominal_start_time VARCHAR(40), 
    raw_json TEXT, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(lineage_job_id) REFERENCES t2c_data.lineage_jobs (id) ON DELETE CASCADE, 
    CONSTRAINT uq_lineage_runs_job_external_run UNIQUE (lineage_job_id, external_run_id)
);

CREATE INDEX ix_lineage_runs_external_run_id ON t2c_data.lineage_runs (external_run_id);

CREATE INDEX ix_lineage_runs_lineage_job_id ON t2c_data.lineage_runs (lineage_job_id);

ALTER TABLE t2c_data.lineage_assets ADD COLUMN lineage_source_id INTEGER;

ALTER TABLE t2c_data.lineage_assets ADD COLUMN asset_origin VARCHAR(30) DEFAULT 'manual' NOT NULL;

ALTER TABLE t2c_data.lineage_assets ADD COLUMN external_node_id VARCHAR(255);

ALTER TABLE t2c_data.lineage_assets ADD COLUMN external_namespace VARCHAR(255);

ALTER TABLE t2c_data.lineage_assets ADD COLUMN external_name VARCHAR(500);

ALTER TABLE t2c_data.lineage_assets ADD COLUMN external_type VARCHAR(30);

ALTER TABLE t2c_data.lineage_assets ADD COLUMN aliases_text TEXT;

ALTER TABLE t2c_data.lineage_assets ADD CONSTRAINT fk_lineage_assets_lineage_source_id FOREIGN KEY(lineage_source_id) REFERENCES t2c_data.lineage_source_configs (id) ON DELETE SET NULL;

CREATE INDEX ix_lineage_assets_asset_origin ON t2c_data.lineage_assets (asset_origin);

CREATE INDEX ix_lineage_assets_external_node_id ON t2c_data.lineage_assets (external_node_id);

CREATE INDEX ix_lineage_assets_external_namespace ON t2c_data.lineage_assets (external_namespace);

CREATE INDEX ix_lineage_assets_lineage_source_id ON t2c_data.lineage_assets (lineage_source_id);

ALTER TABLE t2c_data.lineage_relations ADD COLUMN lineage_source_id INTEGER;

ALTER TABLE t2c_data.lineage_relations ADD COLUMN lineage_job_id INTEGER;

ALTER TABLE t2c_data.lineage_relations ADD COLUMN external_edge_key VARCHAR(500);

ALTER TABLE t2c_data.lineage_relations ADD CONSTRAINT fk_lineage_relations_lineage_source_id FOREIGN KEY(lineage_source_id) REFERENCES t2c_data.lineage_source_configs (id) ON DELETE SET NULL;

ALTER TABLE t2c_data.lineage_relations ADD CONSTRAINT fk_lineage_relations_lineage_job_id FOREIGN KEY(lineage_job_id) REFERENCES t2c_data.lineage_jobs (id) ON DELETE SET NULL;

CREATE INDEX ix_lineage_relations_external_edge_key ON t2c_data.lineage_relations (external_edge_key);

CREATE INDEX ix_lineage_relations_lineage_job_id ON t2c_data.lineage_relations (lineage_job_id);

CREATE INDEX ix_lineage_relations_lineage_source_id ON t2c_data.lineage_relations (lineage_source_id);

ALTER TABLE t2c_data.lineage_source_configs ALTER COLUMN source_type SET DEFAULT 'openlineage';

CREATE TABLE t2c_data.lineage_column_edges (
    id SERIAL NOT NULL,
    lineage_source_id INTEGER,
    lineage_job_id INTEGER,
    source_asset_id INTEGER NOT NULL,
    target_asset_id INTEGER NOT NULL,
    source_column_name VARCHAR(255) NOT NULL,
    target_column_name VARCHAR(255) NOT NULL,
    relation_type VARCHAR(30) DEFAULT 'transformation' NOT NULL,
    discovery_method VARCHAR(30) DEFAULT 'automatic' NOT NULL,
    confidence_score INTEGER DEFAULT 100 NOT NULL,
    external_edge_key VARCHAR(500),
    is_active BOOLEAN DEFAULT true NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    PRIMARY KEY (id),
    FOREIGN KEY(lineage_source_id) REFERENCES t2c_data.lineage_source_configs (id) ON DELETE SET NULL,
    FOREIGN KEY(lineage_job_id) REFERENCES t2c_data.lineage_jobs (id) ON DELETE SET NULL,
    FOREIGN KEY(source_asset_id) REFERENCES t2c_data.lineage_assets (id) ON DELETE CASCADE,
    FOREIGN KEY(target_asset_id) REFERENCES t2c_data.lineage_assets (id) ON DELETE CASCADE,
    CONSTRAINT uq_lineage_column_edges_unique UNIQUE (source_asset_id, target_asset_id, source_column_name, target_column_name, relation_type)
);

CREATE INDEX ix_lineage_column_edges_lineage_source_id ON t2c_data.lineage_column_edges (lineage_source_id);
CREATE INDEX ix_lineage_column_edges_lineage_job_id ON t2c_data.lineage_column_edges (lineage_job_id);
CREATE INDEX ix_lineage_column_edges_source_asset_id ON t2c_data.lineage_column_edges (source_asset_id);
CREATE INDEX ix_lineage_column_edges_target_asset_id ON t2c_data.lineage_column_edges (target_asset_id);
CREATE INDEX ix_lineage_column_edges_relation_type ON t2c_data.lineage_column_edges (relation_type);
CREATE INDEX ix_lineage_column_edges_is_active ON t2c_data.lineage_column_edges (is_active);

CREATE TABLE t2c_data.lineage_event_raw (
    id SERIAL NOT NULL,
    lineage_source_id INTEGER,
    event_key VARCHAR(500) NOT NULL,
    event_type VARCHAR(80),
    producer VARCHAR(500),
    namespace VARCHAR(255),
    job_name VARCHAR(500),
    run_id VARCHAR(255),
    datasource_id INTEGER,
    schema_name VARCHAR(100),
    object_name VARCHAR(200),
    object_type VARCHAR(30),
    event_time VARCHAR(40),
    status VARCHAR(80),
    payload_json TEXT NOT NULL,
    processed_at TIMESTAMP WITH TIME ZONE,
    error_message TEXT,
    is_processed BOOLEAN DEFAULT false NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    PRIMARY KEY (id),
    FOREIGN KEY(lineage_source_id) REFERENCES t2c_data.lineage_source_configs (id) ON DELETE SET NULL,
    FOREIGN KEY(datasource_id) REFERENCES t2c_data.data_sources (id) ON DELETE SET NULL,
    CONSTRAINT uq_lineage_event_raw_source_event_key UNIQUE (lineage_source_id, event_key)
);

CREATE INDEX ix_lineage_event_raw_lineage_source_id ON t2c_data.lineage_event_raw (lineage_source_id);
CREATE INDEX ix_lineage_event_raw_event_key ON t2c_data.lineage_event_raw (event_key);
CREATE INDEX ix_lineage_event_raw_event_type ON t2c_data.lineage_event_raw (event_type);
CREATE INDEX ix_lineage_event_raw_namespace ON t2c_data.lineage_event_raw (namespace);
CREATE INDEX ix_lineage_event_raw_job_name ON t2c_data.lineage_event_raw (job_name);
CREATE INDEX ix_lineage_event_raw_run_id ON t2c_data.lineage_event_raw (run_id);
CREATE INDEX ix_lineage_event_raw_datasource_id ON t2c_data.lineage_event_raw (datasource_id);
CREATE INDEX ix_lineage_event_raw_schema_name ON t2c_data.lineage_event_raw (schema_name);
CREATE INDEX ix_lineage_event_raw_object_name ON t2c_data.lineage_event_raw (object_name);
CREATE INDEX ix_lineage_event_raw_object_type ON t2c_data.lineage_event_raw (object_type);
CREATE INDEX ix_lineage_event_raw_event_time ON t2c_data.lineage_event_raw (event_time);
CREATE INDEX ix_lineage_event_raw_status ON t2c_data.lineage_event_raw (status);
CREATE INDEX ix_lineage_event_raw_is_processed ON t2c_data.lineage_event_raw (is_processed);

CREATE TABLE t2c_data.lineage_sync_checkpoints (
    id SERIAL NOT NULL,
    lineage_source_id INTEGER NOT NULL,
    checkpoint_type VARCHAR(40) DEFAULT 'openlineage' NOT NULL,
    last_event_raw_id INTEGER,
    last_processed_at TIMESTAMP WITH TIME ZONE,
    last_status VARCHAR(40),
    message TEXT,
    cursor_value VARCHAR(255),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    PRIMARY KEY (id),
    FOREIGN KEY(lineage_source_id) REFERENCES t2c_data.lineage_source_configs (id) ON DELETE CASCADE,
    FOREIGN KEY(last_event_raw_id) REFERENCES t2c_data.lineage_event_raw (id) ON DELETE SET NULL,
    CONSTRAINT uq_lineage_sync_checkpoints_source_type UNIQUE (lineage_source_id, checkpoint_type)
);

CREATE INDEX ix_lineage_sync_checkpoints_lineage_source_id ON t2c_data.lineage_sync_checkpoints (lineage_source_id);
CREATE INDEX ix_lineage_sync_checkpoints_checkpoint_type ON t2c_data.lineage_sync_checkpoints (checkpoint_type);
CREATE INDEX ix_lineage_sync_checkpoints_last_event_raw_id ON t2c_data.lineage_sync_checkpoints (last_event_raw_id);
CREATE INDEX ix_lineage_sync_checkpoints_last_status ON t2c_data.lineage_sync_checkpoints (last_status);

UPDATE t2c_data.alembic_version SET version_num='c1d2e3f4a5b6' WHERE t2c_data.alembic_version.version_num = 'ab39c8d4e2f1';

-- Running upgrade c1d2e3f4a5b6 -> d4b9e6a1c2f0

ALTER TABLE t2c_data.dq_runs ADD COLUMN profile_payload_json JSONB;

ALTER TABLE t2c_data.dq_table_metrics ADD COLUMN column_count INTEGER;

ALTER TABLE t2c_data.dq_table_metrics ADD COLUMN metrics_json JSONB;

UPDATE t2c_data.dq_table_metrics tm
        SET column_count = sub.cnt
        FROM (
            SELECT table_metric_id, COUNT(*)::integer AS cnt
            FROM t2c_data.dq_column_metrics
            GROUP BY table_metric_id
        ) sub
        WHERE sub.table_metric_id = tm.id;

UPDATE t2c_data.dq_table_metrics SET column_count = 0 WHERE column_count IS NULL;

ALTER TABLE t2c_data.dq_table_metrics ALTER COLUMN column_count SET NOT NULL;

UPDATE t2c_data.alembic_version SET version_num='d4b9e6a1c2f0' WHERE t2c_data.alembic_version.version_num = 'c1d2e3f4a5b6';

-- Running upgrade d4b9e6a1c2f0 -> 1f9a2b3c4d5e

CREATE TABLE t2c_data.table_search_aliases (
    id SERIAL NOT NULL, 
    table_id INTEGER NOT NULL, 
    label_kind VARCHAR(30) NOT NULL, 
    label VARCHAR(255) NOT NULL, 
    normalized_label VARCHAR(255) NOT NULL, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(table_id) REFERENCES t2c_data.tables (id) ON DELETE CASCADE, 
    CONSTRAINT uq_table_search_alias_label UNIQUE (table_id, label_kind, label)
);

CREATE INDEX ix_table_search_aliases_table_id ON t2c_data.table_search_aliases (table_id);

CREATE INDEX ix_table_search_aliases_table_kind ON t2c_data.table_search_aliases (table_id, label_kind);

CREATE INDEX ix_table_search_aliases_normalized ON t2c_data.table_search_aliases (normalized_label);

CREATE TABLE t2c_data.column_search_aliases (
    id SERIAL NOT NULL, 
    column_id INTEGER NOT NULL, 
    label_kind VARCHAR(30) NOT NULL, 
    label VARCHAR(255) NOT NULL, 
    normalized_label VARCHAR(255) NOT NULL, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(column_id) REFERENCES t2c_data.columns (id) ON DELETE CASCADE, 
    CONSTRAINT uq_column_search_alias_label UNIQUE (column_id, label_kind, label)
);

CREATE INDEX ix_column_search_aliases_column_id ON t2c_data.column_search_aliases (column_id);

CREATE INDEX ix_column_search_aliases_column_kind ON t2c_data.column_search_aliases (column_id, label_kind);

CREATE INDEX ix_column_search_aliases_normalized ON t2c_data.column_search_aliases (normalized_label);

CREATE TABLE t2c_data.search_query_history (
    id SERIAL NOT NULL, 
    user_id INTEGER NOT NULL, 
    raw_query VARCHAR(255) NOT NULL, 
    normalized_query VARCHAR(255) NOT NULL, 
    search_count INTEGER DEFAULT '1' NOT NULL, 
    last_searched_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(user_id) REFERENCES t2c_data.users (id) ON DELETE CASCADE, 
    CONSTRAINT uq_search_query_history_user_query UNIQUE (user_id, normalized_query)
);

CREATE INDEX ix_search_query_history_user_id ON t2c_data.search_query_history (user_id);

CREATE INDEX ix_search_query_history_user_recent ON t2c_data.search_query_history (user_id, last_searched_at);

CREATE INDEX ix_search_query_history_normalized ON t2c_data.search_query_history (normalized_query);

CREATE TABLE t2c_data.search_result_clicks (
    id SERIAL NOT NULL, 
    user_id INTEGER, 
    entity_type VARCHAR(40) NOT NULL, 
    entity_id INTEGER NOT NULL, 
    query_text VARCHAR(255), 
    normalized_query VARCHAR(255), 
    target_url VARCHAR(500), 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(user_id) REFERENCES t2c_data.users (id) ON DELETE SET NULL
);

CREATE INDEX ix_search_result_clicks_user_id ON t2c_data.search_result_clicks (user_id);

CREATE INDEX ix_search_result_clicks_entity ON t2c_data.search_result_clicks (entity_type, entity_id);

CREATE INDEX ix_search_result_clicks_user_created ON t2c_data.search_result_clicks (user_id, created_at);

CREATE INDEX ix_search_result_clicks_query ON t2c_data.search_result_clicks (normalized_query);

UPDATE t2c_data.alembic_version SET version_num='1f9a2b3c4d5e' WHERE t2c_data.alembic_version.version_num = 'd4b9e6a1c2f0';

-- Running upgrade 1f9a2b3c4d5e -> 2d6b7c8a9e10

ALTER TABLE t2c_data.audit_log ADD COLUMN actor_name TEXT;

ALTER TABLE t2c_data.audit_log ADD COLUMN parent_entity_type TEXT;

ALTER TABLE t2c_data.audit_log ADD COLUMN parent_entity_id TEXT;

ALTER TABLE t2c_data.audit_log ADD COLUMN change_set_id TEXT;

ALTER TABLE t2c_data.audit_log ADD COLUMN change_type TEXT;

ALTER TABLE t2c_data.audit_log ADD COLUMN field_name TEXT;

ALTER TABLE t2c_data.audit_log ADD COLUMN source_module TEXT;

CREATE INDEX ix_audit_log_change_set_id ON t2c_data.audit_log (change_set_id);

CREATE INDEX ix_audit_log_change_type ON t2c_data.audit_log (change_type);

CREATE INDEX ix_audit_log_field_name ON t2c_data.audit_log (field_name);

CREATE INDEX ix_audit_log_source_module ON t2c_data.audit_log (source_module);

UPDATE t2c_data.alembic_version SET version_num='2d6b7c8a9e10' WHERE t2c_data.alembic_version.version_num = '1f9a2b3c4d5e';

-- Running upgrade 2d6b7c8a9e10 -> 3c4d5e6f7a8b

ALTER TABLE t2c_data.audit_log ADD COLUMN is_sensitive_change BOOLEAN DEFAULT false NOT NULL;

ALTER TABLE t2c_data.audit_log ADD COLUMN sensitive_category TEXT;

CREATE INDEX ix_audit_log_is_sensitive_change ON t2c_data.audit_log (is_sensitive_change);

CREATE INDEX ix_audit_log_sensitive_category ON t2c_data.audit_log (sensitive_category);

UPDATE t2c_data.alembic_version SET version_num='3c4d5e6f7a8b' WHERE t2c_data.alembic_version.version_num = '2d6b7c8a9e10';

-- Running upgrade 3c4d5e6f7a8b -> 4d7e8f9a1b2c

ALTER TABLE t2c_data.tables ADD COLUMN certification_submitted_by_user_id INTEGER;

ALTER TABLE t2c_data.tables ADD COLUMN certification_submitted_at TIMESTAMP WITH TIME ZONE;

ALTER TABLE t2c_data.tables ADD COLUMN certification_expires_at TIMESTAMP WITH TIME ZONE;

ALTER TABLE t2c_data.tables ADD COLUMN owner_reviewed_by_user_id INTEGER;

ALTER TABLE t2c_data.tables ADD COLUMN owner_reviewed_at TIMESTAMP WITH TIME ZONE;

UPDATE t2c_data.tables SET certification_status = 'not_eligible' WHERE certification_status = 'not_assessed';

ALTER TABLE t2c_data.tables ADD CONSTRAINT fk_tables_certification_submitted_by_user_id FOREIGN KEY(certification_submitted_by_user_id) REFERENCES t2c_data.users (id) ON DELETE SET NULL;

ALTER TABLE t2c_data.tables ADD CONSTRAINT fk_tables_owner_reviewed_by_user_id FOREIGN KEY(owner_reviewed_by_user_id) REFERENCES t2c_data.users (id) ON DELETE SET NULL;

CREATE INDEX ix_tables_owner_reviewed_at ON t2c_data.tables (owner_reviewed_at);

CREATE INDEX ix_tables_certification_review_at ON t2c_data.tables (certification_review_at);

CREATE INDEX ix_tables_certification_expires_at ON t2c_data.tables (certification_expires_at);

UPDATE t2c_data.alembic_version SET version_num='4d7e8f9a1b2c' WHERE t2c_data.alembic_version.version_num = '3c4d5e6f7a8b';

-- Running upgrade 4d7e8f9a1b2c -> 5e8f9a1b2c3d

CREATE TABLE t2c_data.governance_settings (
    id SERIAL NOT NULL, 
    owner_review_interval_days INTEGER DEFAULT '90' NOT NULL, 
    privacy_review_interval_days INTEGER DEFAULT '180' NOT NULL, 
    sensitive_privacy_review_interval_days INTEGER DEFAULT '90' NOT NULL, 
    certification_review_interval_days INTEGER DEFAULT '180' NOT NULL, 
    certification_review_sla_days INTEGER DEFAULT '7' NOT NULL, 
    certification_revalidation_window_days INTEGER DEFAULT '30' NOT NULL, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id)
);

INSERT INTO t2c_data.governance_settings (
                id,
                owner_review_interval_days,
                privacy_review_interval_days,
                sensitive_privacy_review_interval_days,
                certification_review_interval_days,
                certification_review_sla_days,
                certification_revalidation_window_days
            )
            VALUES (1, 90, 180, 90, 180, 7, 30)
            ON CONFLICT (id) DO NOTHING;

SELECT setval(
                pg_get_serial_sequence('t2c_data.governance_settings', 'id'),
                GREATEST((SELECT COALESCE(MAX(id), 0) FROM t2c_data.governance_settings), 1),
                (SELECT COALESCE(MAX(id), 0) > 0 FROM t2c_data.governance_settings)
            );

UPDATE t2c_data.alembic_version SET version_num='5e8f9a1b2c3d' WHERE t2c_data.alembic_version.version_num = '4d7e8f9a1b2c';

-- Running upgrade 5e8f9a1b2c3d -> 6f0a1b2c3d4e

CREATE TABLE t2c_data.search_read_model (
    id SERIAL NOT NULL, 
    entity_type VARCHAR(40) NOT NULL, 
    entity_id INTEGER NOT NULL, 
    parent_table_id INTEGER, 
    category VARCHAR(80) NOT NULL, 
    title VARCHAR(255) NOT NULL, 
    subtitle VARCHAR(255), 
    description TEXT, 
    context_path TEXT, 
    target_url VARCHAR(1000) NOT NULL, 
    searchable_name JSON NOT NULL, 
    searchable_aliases JSON NOT NULL, 
    searchable_synonyms JSON NOT NULL, 
    searchable_descriptions JSON NOT NULL, 
    searchable_context JSON NOT NULL, 
    source_name VARCHAR(255), 
    database_name VARCHAR(255), 
    schema_name VARCHAR(255), 
    owner_name VARCHAR(255), 
    domain_name VARCHAR(255), 
    classification VARCHAR(120), 
    certified BOOLEAN DEFAULT false NOT NULL, 
    open_incidents INTEGER DEFAULT '0' NOT NULL, 
    popularity_count INTEGER DEFAULT '0' NOT NULL, 
    metadata_json JSON, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(parent_table_id) REFERENCES t2c_data.tables (id) ON DELETE SET NULL, 
    CONSTRAINT uq_search_read_model_entity UNIQUE (entity_type, entity_id)
);

CREATE INDEX ix_search_read_model_entity_type ON t2c_data.search_read_model (entity_type);

CREATE INDEX ix_search_read_model_entity_id ON t2c_data.search_read_model (entity_id);

CREATE INDEX ix_search_read_model_parent_table_id ON t2c_data.search_read_model (parent_table_id);

CREATE TABLE t2c_data.dashboard_asset_read_model (
    table_id INTEGER NOT NULL, 
    datasource_id INTEGER NOT NULL, 
    database_id INTEGER, 
    schema_id INTEGER NOT NULL, 
    table_name VARCHAR(255) NOT NULL, 
    table_type VARCHAR(40) NOT NULL, 
    schema_name VARCHAR(255) NOT NULL, 
    database_name VARCHAR(255) NOT NULL, 
    datasource_name VARCHAR(255) NOT NULL, 
    engine VARCHAR(40) NOT NULL, 
    owner_defined BOOLEAN DEFAULT false NOT NULL, 
    description_complete BOOLEAN DEFAULT false NOT NULL, 
    dictionary_complete BOOLEAN DEFAULT false NOT NULL, 
    classification_defined BOOLEAN DEFAULT false NOT NULL, 
    tags_count INTEGER DEFAULT '0' NOT NULL, 
    terms_count INTEGER DEFAULT '0' NOT NULL, 
    certification_status VARCHAR(40) NOT NULL, 
    certification_criticality VARCHAR(40), 
    certification_badges JSON NOT NULL, 
    certification_decided_at VARCHAR(64), 
    certification_review_at VARCHAR(64), 
    certification_expires_at VARCHAR(64), 
    review_recent BOOLEAN DEFAULT false NOT NULL, 
    dq_score FLOAT, 
    completeness_pct_avg FLOAT, 
    freshness_seconds INTEGER, 
    open_incidents INTEGER DEFAULT '0' NOT NULL, 
    critical_open_incidents INTEGER DEFAULT '0' NOT NULL, 
    owner_name VARCHAR(255), 
    data_owner_id INTEGER, 
    domain_name VARCHAR(255), 
    sensitivity_level VARCHAR(40), 
    has_personal_data BOOLEAN DEFAULT false NOT NULL, 
    has_sensitive_personal_data BOOLEAN DEFAULT false NOT NULL, 
    owner_reviewed_at VARCHAR(64), 
    privacy_reviewed_at VARCHAR(64), 
    last_review_at VARCHAR(64), 
    last_sync_at VARCHAR(64), 
    last_updated_at VARCHAR(64), 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (table_id), 
    FOREIGN KEY(table_id) REFERENCES t2c_data.tables (id) ON DELETE CASCADE
);

CREATE INDEX ix_dashboard_asset_read_model_datasource_id ON t2c_data.dashboard_asset_read_model (datasource_id);

CREATE INDEX ix_dashboard_asset_read_model_database_id ON t2c_data.dashboard_asset_read_model (database_id);

CREATE INDEX ix_dashboard_asset_read_model_schema_id ON t2c_data.dashboard_asset_read_model (schema_id);

CREATE INDEX ix_dashboard_asset_read_model_data_owner_id ON t2c_data.dashboard_asset_read_model (data_owner_id);

CREATE TABLE t2c_data.asset_visibility_rules (
    id SERIAL NOT NULL, 
    entity_type VARCHAR(40) NOT NULL, 
    entity_id INTEGER NOT NULL, 
    allowed_role VARCHAR(80), 
    allowed_user_id INTEGER, 
    visibility_scope VARCHAR(20) DEFAULT 'full' NOT NULL, 
    reason TEXT, 
    is_active BOOLEAN DEFAULT true NOT NULL, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(allowed_user_id) REFERENCES t2c_data.users (id) ON DELETE CASCADE
);

CREATE INDEX ix_asset_visibility_rules_entity_type ON t2c_data.asset_visibility_rules (entity_type);

CREATE INDEX ix_asset_visibility_rules_entity_id ON t2c_data.asset_visibility_rules (entity_id);

CREATE INDEX ix_asset_visibility_rules_allowed_role ON t2c_data.asset_visibility_rules (allowed_role);

CREATE INDEX ix_asset_visibility_rules_allowed_user_id ON t2c_data.asset_visibility_rules (allowed_user_id);

CREATE TABLE t2c_data.platform_usage_events (
    id SERIAL NOT NULL, 
    user_id INTEGER, 
    event_name VARCHAR(80) NOT NULL, 
    module_name VARCHAR(80) NOT NULL, 
    page_path VARCHAR(255), 
    entity_type VARCHAR(40), 
    entity_id INTEGER, 
    target_url VARCHAR(1000), 
    metadata_json JSON, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(user_id) REFERENCES t2c_data.users (id) ON DELETE SET NULL
);

CREATE INDEX ix_platform_usage_events_user_id ON t2c_data.platform_usage_events (user_id);

CREATE INDEX ix_platform_usage_events_event_name ON t2c_data.platform_usage_events (event_name);

CREATE INDEX ix_platform_usage_events_module_name ON t2c_data.platform_usage_events (module_name);

CREATE INDEX ix_platform_usage_events_page_path ON t2c_data.platform_usage_events (page_path);

CREATE INDEX ix_platform_usage_events_entity_type ON t2c_data.platform_usage_events (entity_type);

CREATE INDEX ix_platform_usage_events_entity_id ON t2c_data.platform_usage_events (entity_id);

UPDATE t2c_data.alembic_version SET version_num='6f0a1b2c3d4e' WHERE t2c_data.alembic_version.version_num = '5e8f9a1b2c3d';

-- Running upgrade 6f0a1b2c3d4e -> 7a1b2c3d4e5f

ALTER TABLE t2c_data.asset_visibility_rules ADD COLUMN rule_scope VARCHAR(30) DEFAULT 'asset' NOT NULL;

ALTER TABLE t2c_data.asset_visibility_rules ADD COLUMN match_value VARCHAR(255);

ALTER TABLE t2c_data.asset_visibility_rules ADD COLUMN mask_sensitive_fields BOOLEAN DEFAULT false NOT NULL;

ALTER TABLE t2c_data.asset_visibility_rules ALTER COLUMN entity_id DROP NOT NULL;

CREATE INDEX ix_asset_visibility_rules_rule_scope ON t2c_data.asset_visibility_rules (rule_scope);

CREATE INDEX ix_asset_visibility_rules_match_value ON t2c_data.asset_visibility_rules (match_value);

UPDATE t2c_data.alembic_version SET version_num='7a1b2c3d4e5f' WHERE t2c_data.alembic_version.version_num = '6f0a1b2c3d4e';

-- Running upgrade 7a1b2c3d4e5f -> 8b2c3d4e5f6a

CREATE INDEX ix_audit_log_created_at ON t2c_data.audit_log (created_at);

CREATE INDEX ix_audit_log_action_created_at ON t2c_data.audit_log (action, created_at);

CREATE INDEX ix_audit_log_entity_created_at ON t2c_data.audit_log (entity_type, entity_id, created_at);

CREATE INDEX ix_audit_log_source_created_at ON t2c_data.audit_log (source_module, created_at);

CREATE INDEX ix_platform_usage_events_created_at ON t2c_data.platform_usage_events (created_at);

CREATE INDEX ix_platform_usage_events_module_created ON t2c_data.platform_usage_events (module_name, created_at);

CREATE INDEX ix_platform_usage_events_event_created ON t2c_data.platform_usage_events (event_name, created_at);

CREATE INDEX ix_search_result_clicks_created_at ON t2c_data.search_result_clicks (created_at);

UPDATE t2c_data.alembic_version SET version_num='8b2c3d4e5f6a' WHERE t2c_data.alembic_version.version_num = '7a1b2c3d4e5f';

-- Running upgrade 8b2c3d4e5f6a -> 9c3d4e5f6a7b

CREATE TABLE t2c_data.access_log (
    id SERIAL NOT NULL, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    user_id INTEGER, 
    actor_name TEXT, 
    user_email TEXT, 
    ip INET, 
    user_agent TEXT, 
    route TEXT NOT NULL, 
    method TEXT, 
    status_code INTEGER, 
    request_id TEXT, 
    api_version TEXT DEFAULT 'v1' NOT NULL, 
    module_name TEXT, 
    duration_ms INTEGER, 
    metadata_json JSONB, 
    PRIMARY KEY (id), 
    FOREIGN KEY(user_id) REFERENCES t2c_data.users (id) ON DELETE SET NULL
);

CREATE TABLE t2c_data.access_log_archive (
    id SERIAL NOT NULL, 
    created_at TIMESTAMP WITH TIME ZONE NOT NULL, 
    user_id INTEGER, 
    actor_name TEXT, 
    user_email TEXT, 
    ip INET, 
    user_agent TEXT, 
    route TEXT NOT NULL, 
    method TEXT, 
    status_code INTEGER, 
    request_id TEXT, 
    api_version TEXT DEFAULT 'v1' NOT NULL, 
    module_name TEXT, 
    duration_ms INTEGER, 
    metadata_json JSONB, 
    PRIMARY KEY (id)
);

CREATE INDEX ix_access_log_created_at ON t2c_data.access_log (created_at);

CREATE INDEX ix_access_log_api_version_created_at ON t2c_data.access_log (api_version, created_at);

CREATE INDEX ix_access_log_module_created_at ON t2c_data.access_log (module_name, created_at);

CREATE INDEX ix_access_log_route_created_at ON t2c_data.access_log (route, created_at);

CREATE INDEX ix_access_log_archive_created_at ON t2c_data.access_log_archive (created_at);

CREATE INDEX ix_access_log_archive_api_version_created_at ON t2c_data.access_log_archive (api_version, created_at);

CREATE INDEX ix_access_log_archive_module_created_at ON t2c_data.access_log_archive (module_name, created_at);

ALTER TABLE t2c_data.governance_settings ADD COLUMN audit_log_retention_days INTEGER DEFAULT '730' NOT NULL;

ALTER TABLE t2c_data.governance_settings ADD COLUMN access_log_retention_days INTEGER DEFAULT '30' NOT NULL;

ALTER TABLE t2c_data.governance_settings ADD COLUMN access_log_archive_retention_days INTEGER DEFAULT '365' NOT NULL;

ALTER TABLE t2c_data.governance_settings ADD COLUMN platform_usage_event_retention_days INTEGER DEFAULT '180' NOT NULL;

ALTER TABLE t2c_data.governance_settings ADD COLUMN search_result_click_retention_days INTEGER DEFAULT '180' NOT NULL;

INSERT INTO t2c_data.access_log (
                id, created_at, user_id, actor_name, user_email, ip, user_agent, route, method,
                status_code, request_id, api_version, module_name, duration_ms, metadata_json
            )
            SELECT
                id,
                created_at,
                user_id,
                actor_name,
                user_email,
                ip,
                user_agent,
                route,
                method,
                status_code,
                request_id,
                CASE WHEN route LIKE '/api/v1/%%' OR route = '/api/v1' THEN 'v1' ELSE 'legacy' END,
                CASE
                    WHEN route LIKE '/api/v1/%%' THEN split_part(substr(route, 9), '/', 1)
                    WHEN route LIKE '/api/%%' THEN split_part(substr(route, 6), '/', 1)
                    ELSE 'api'
                END,
                CAST(COALESCE((metadata_json->>'duration_ms')::numeric, 0) AS integer),
                metadata_json
            FROM t2c_data.audit_log
            WHERE action = 'http_request';

DELETE FROM t2c_data.audit_log WHERE action = 'http_request';

SELECT setval(
                pg_get_serial_sequence('t2c_data.access_log', 'id'),
                GREATEST((SELECT COALESCE(MAX(id), 0) FROM t2c_data.access_log), 1),
                (SELECT COALESCE(MAX(id), 0) > 0 FROM t2c_data.access_log)
            );

UPDATE t2c_data.alembic_version SET version_num='9c3d4e5f6a7b' WHERE t2c_data.alembic_version.version_num = '8b2c3d4e5f6a';

-- Running upgrade 9c3d4e5f6a7b -> a1b2c3d4e5f6

ALTER TABLE t2c_data.governance_settings ADD COLUMN legacy_api_disabled_modules TEXT;

UPDATE t2c_data.alembic_version SET version_num='a1b2c3d4e5f6' WHERE t2c_data.alembic_version.version_num = '9c3d4e5f6a7b';

-- Running upgrade a1b2c3d4e5f6 -> b2c3d4e5f6a

ALTER TABLE t2c_data.governance_settings ADD COLUMN legacy_api_cutoff_window_days INTEGER DEFAULT '30' NOT NULL;

ALTER TABLE t2c_data.governance_settings ADD COLUMN legacy_api_force_enabled_modules TEXT;

UPDATE t2c_data.alembic_version SET version_num='b2c3d4e5f6a' WHERE t2c_data.alembic_version.version_num = 'a1b2c3d4e5f6';

-- Running upgrade b2c3d4e5f6a -> e1f2a3b4c5d6

ALTER TABLE t2c_data.governance_settings ADD COLUMN governance_score_weights TEXT;

UPDATE t2c_data.governance_settings
            SET governance_score_weights = '{"certification": 10, "certification_review": 5, "column_description_complete": 12, "dq_score": 15, "glossary_terms": 8, "incident_health": 10, "owner_defined": 10, "owner_review": 7, "privacy_review": 5, "table_description_complete": 10, "tags_applied": 8}'
            WHERE governance_score_weights IS NULL;

UPDATE t2c_data.alembic_version SET version_num='e1f2a3b4c5d6' WHERE t2c_data.alembic_version.version_num = 'b2c3d4e5f6a';

-- Running upgrade e1f2a3b4c5d6 -> f3a4b5c6d7e8

CREATE TABLE t2c_data.stewardship_requests (
    id SERIAL NOT NULL, 
    table_id INTEGER, 
    request_type VARCHAR(40) NOT NULL, 
    status VARCHAR(20) DEFAULT 'pending' NOT NULL, 
    request_origin VARCHAR(40) DEFAULT 'manual' NOT NULL, 
    requested_by_user_id INTEGER, 
    approver_user_id INTEGER, 
    decided_by_user_id INTEGER, 
    requester_comment TEXT, 
    decision_comment TEXT, 
    current_value_json JSON, 
    proposed_value_json JSON, 
    context_json JSON, 
    decided_at TIMESTAMP WITH TIME ZONE, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(approver_user_id) REFERENCES t2c_data.users (id) ON DELETE SET NULL, 
    FOREIGN KEY(decided_by_user_id) REFERENCES t2c_data.users (id) ON DELETE SET NULL, 
    FOREIGN KEY(requested_by_user_id) REFERENCES t2c_data.users (id) ON DELETE SET NULL, 
    FOREIGN KEY(table_id) REFERENCES t2c_data.tables (id) ON DELETE SET NULL
);

CREATE INDEX ix_stewardship_requests_status ON t2c_data.stewardship_requests (status);

CREATE INDEX ix_stewardship_requests_request_type ON t2c_data.stewardship_requests (request_type);

CREATE INDEX ix_stewardship_requests_table_id ON t2c_data.stewardship_requests (table_id);

CREATE TABLE t2c_data.stewardship_request_events (
    id SERIAL NOT NULL, 
    stewardship_request_id INTEGER NOT NULL, 
    event_type VARCHAR(30) NOT NULL, 
    actor_user_id INTEGER, 
    comment TEXT, 
    payload_json JSON, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(actor_user_id) REFERENCES t2c_data.users (id) ON DELETE SET NULL, 
    FOREIGN KEY(stewardship_request_id) REFERENCES t2c_data.stewardship_requests (id) ON DELETE CASCADE
);

CREATE INDEX ix_stewardship_request_events_request_id ON t2c_data.stewardship_request_events (stewardship_request_id);

CREATE INDEX ix_stewardship_request_events_event_type ON t2c_data.stewardship_request_events (event_type);

UPDATE t2c_data.alembic_version SET version_num='f3a4b5c6d7e8' WHERE t2c_data.alembic_version.version_num = 'e1f2a3b4c5d6';

-- Running upgrade b2c3d4e5f6a -> c3d4e5f6a7b

CREATE TABLE t2c_data.audit_log_archive (
    id SERIAL NOT NULL, 
    created_at TIMESTAMP WITH TIME ZONE NOT NULL, 
    user_id INTEGER, 
    actor_name TEXT, 
    user_email TEXT, 
    ip INET, 
    user_agent TEXT, 
    action TEXT NOT NULL, 
    entity_type TEXT, 
    entity_id TEXT, 
    parent_entity_type TEXT, 
    parent_entity_id TEXT, 
    change_set_id TEXT, 
    change_type TEXT, 
    field_name TEXT, 
    source_module TEXT, 
    is_sensitive_change BOOLEAN DEFAULT 'false' NOT NULL, 
    sensitive_category TEXT, 
    route TEXT, 
    method TEXT, 
    status_code INTEGER, 
    request_id TEXT, 
    before_json JSONB, 
    after_json JSONB, 
    metadata_json JSONB, 
    PRIMARY KEY (id)
);

CREATE INDEX ix_audit_log_archive_created_at ON t2c_data.audit_log_archive (created_at);

CREATE INDEX ix_audit_log_archive_action_created_at ON t2c_data.audit_log_archive (action, created_at);

CREATE INDEX ix_audit_log_archive_entity_created_at ON t2c_data.audit_log_archive (entity_type, entity_id, created_at);

CREATE INDEX ix_audit_log_archive_source_created_at ON t2c_data.audit_log_archive (source_module, created_at);

CREATE TABLE t2c_data.platform_scheduler_status (
    id SERIAL NOT NULL, 
    scheduler_name VARCHAR(80) DEFAULT 'platform_maintenance' NOT NULL, 
    mode VARCHAR(20) DEFAULT 'embedded' NOT NULL, 
    is_enabled BOOLEAN DEFAULT 'true' NOT NULL, 
    last_started_at VARCHAR(64), 
    last_heartbeat_at VARCHAR(64), 
    last_success_at VARCHAR(64), 
    last_failure_at VARCHAR(64), 
    last_error TEXT, 
    last_run_summary_json JSON, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id)
);

ALTER TABLE t2c_data.governance_settings ADD COLUMN audit_log_archive_retention_days INTEGER DEFAULT '2555' NOT NULL;

INSERT INTO t2c_data.platform_scheduler_status (id, scheduler_name, mode, is_enabled)
            VALUES (1, 'platform_maintenance', 'embedded', true)
            ON CONFLICT (id) DO NOTHING;

SELECT setval(
                pg_get_serial_sequence('t2c_data.platform_scheduler_status', 'id'),
                GREATEST((SELECT COALESCE(MAX(id), 0) FROM t2c_data.platform_scheduler_status), 1),
                (SELECT COALESCE(MAX(id), 0) > 0 FROM t2c_data.platform_scheduler_status)
            );

INSERT INTO t2c_data.alembic_version (version_num) VALUES ('c3d4e5f6a7b') RETURNING t2c_data.alembic_version.version_num;

-- Running upgrade c3d4e5f6a7b -> d5e6f7a8b9c0

UPDATE t2c_data.alembic_version SET version_num='d5e6f7a8b9c0' WHERE t2c_data.alembic_version.version_num = 'c3d4e5f6a7b';

-- Running upgrade d5e6f7a8b9c0, f3a4b5c6d7e8 -> f8a9b0c1d2e3

ALTER TABLE t2c_data.governance_settings ADD COLUMN governance_notifications_enabled BOOLEAN DEFAULT true NOT NULL;

ALTER TABLE t2c_data.governance_settings ADD COLUMN governance_notification_repeat_days INTEGER DEFAULT '7' NOT NULL;

ALTER TABLE t2c_data.governance_settings ADD COLUMN governance_notification_critical_repeat_hours INTEGER DEFAULT '24' NOT NULL;

CREATE TABLE t2c_data.governance_notifications (
    id SERIAL NOT NULL, 
    dedupe_key VARCHAR(255) NOT NULL, 
    rule_key VARCHAR(80) NOT NULL, 
    channel VARCHAR(20) DEFAULT 'in_app' NOT NULL, 
    status VARCHAR(20) DEFAULT 'active' NOT NULL, 
    severity VARCHAR(20) DEFAULT 'medium' NOT NULL, 
    origin VARCHAR(40) DEFAULT 'governance' NOT NULL, 
    title VARCHAR(200) NOT NULL, 
    message TEXT NOT NULL, 
    entity_type VARCHAR(40) DEFAULT 'table' NOT NULL, 
    table_id INTEGER, 
    data_owner_id INTEGER, 
    target_href TEXT, 
    context_json JSON, 
    first_detected_at TIMESTAMP WITH TIME ZONE NOT NULL, 
    last_detected_at TIMESTAMP WITH TIME ZONE NOT NULL, 
    last_sent_at TIMESTAMP WITH TIME ZONE, 
    next_send_at TIMESTAMP WITH TIME ZONE, 
    resolved_at TIMESTAMP WITH TIME ZONE, 
    resolved_reason TEXT, 
    send_count INTEGER DEFAULT '0' NOT NULL, 
    last_delivery_status VARCHAR(20), 
    last_delivery_error TEXT, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(data_owner_id) REFERENCES t2c_data.data_owners (id) ON DELETE SET NULL, 
    FOREIGN KEY(table_id) REFERENCES t2c_data.tables (id) ON DELETE SET NULL
);

CREATE INDEX ix_governance_notifications_status ON t2c_data.governance_notifications (status);

CREATE INDEX ix_governance_notifications_severity ON t2c_data.governance_notifications (severity);

CREATE INDEX ix_governance_notifications_rule_key ON t2c_data.governance_notifications (rule_key);

CREATE INDEX ix_governance_notifications_table_id ON t2c_data.governance_notifications (table_id);

CREATE INDEX ix_governance_notifications_next_send_at ON t2c_data.governance_notifications (next_send_at);

CREATE INDEX ix_governance_notifications_active_status ON t2c_data.governance_notifications (status, next_send_at);

CREATE UNIQUE INDEX ix_governance_notifications_dedupe_key ON t2c_data.governance_notifications (dedupe_key);

DELETE FROM t2c_data.alembic_version WHERE t2c_data.alembic_version.version_num = 'd5e6f7a8b9c0';

UPDATE t2c_data.alembic_version SET version_num='f8a9b0c1d2e3' WHERE t2c_data.alembic_version.version_num = 'f3a4b5c6d7e8';

-- Running upgrade f8a9b0c1d2e3 -> a9b8c7d6e5f4

ALTER TABLE t2c_data.governance_settings ADD COLUMN pipeline_failure_owner_sla_hours INTEGER DEFAULT '24' NOT NULL;

ALTER TABLE t2c_data.governance_settings ADD COLUMN operational_high_volume_threshold_rows INTEGER DEFAULT '100000' NOT NULL;

ALTER TABLE t2c_data.governance_settings ADD COLUMN airflow_ui_base_url TEXT;

UPDATE t2c_data.alembic_version SET version_num='a9b8c7d6e5f4' WHERE t2c_data.alembic_version.version_num = 'f8a9b0c1d2e3';

-- Running upgrade a9b8c7d6e5f4 -> b1c2d3e4f5a6

ALTER TABLE t2c_data.governance_settings ADD COLUMN dq_operational_failure_penalty_points INTEGER DEFAULT '15' NOT NULL;

ALTER TABLE t2c_data.governance_settings ADD COLUMN dq_operational_stale_penalty_points INTEGER DEFAULT '8' NOT NULL;

ALTER TABLE t2c_data.governance_settings ADD COLUMN dq_operational_recurrent_penalty_points INTEGER DEFAULT '5' NOT NULL;

CREATE TABLE t2c_data.operational_stability_snapshots (
    id SERIAL NOT NULL, 
    table_id INTEGER NOT NULL, 
    datasource_id INTEGER, 
    schema_name VARCHAR(100) NOT NULL, 
    table_name VARCHAR(200) NOT NULL, 
    pipeline_name VARCHAR(255), 
    dag_id VARCHAR(255), 
    task_name VARCHAR(255), 
    latest_status_label VARCHAR(60), 
    last_success_at TIMESTAMP WITH TIME ZONE, 
    last_execution_finished_at TIMESTAMP WITH TIME ZONE, 
    rows_processed INTEGER, 
    window_runs INTEGER DEFAULT '0' NOT NULL, 
    success_rate_pct FLOAT DEFAULT '0' NOT NULL, 
    failed_runs INTEGER DEFAULT '0' NOT NULL, 
    recurrent_degradation BOOLEAN DEFAULT false NOT NULL, 
    currently_stale BOOLEAN DEFAULT false NOT NULL, 
    bucket_start_at TIMESTAMP WITH TIME ZONE NOT NULL, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT uq_operational_stability_table_bucket UNIQUE (table_id, bucket_start_at), 
    FOREIGN KEY(table_id) REFERENCES t2c_data.tables (id) ON DELETE CASCADE, 
    FOREIGN KEY(datasource_id) REFERENCES t2c_data.data_sources (id) ON DELETE SET NULL
);

CREATE INDEX ix_operational_stability_bucket ON t2c_data.operational_stability_snapshots (bucket_start_at);

CREATE INDEX ix_operational_stability_table_bucket ON t2c_data.operational_stability_snapshots (table_id, bucket_start_at);

CREATE INDEX ix_operational_stability_dag ON t2c_data.operational_stability_snapshots (dag_id);

UPDATE t2c_data.alembic_version SET version_num='b1c2d3e4f5a6' WHERE t2c_data.alembic_version.version_num = 'a9b8c7d6e5f4';

-- Running upgrade b1c2d3e4f5a6 -> c2d3e4f5a6b7

CREATE TABLE t2c_data.user_notification_preferences (
    id SERIAL NOT NULL, 
    user_id INTEGER NOT NULL, 
    in_app_enabled BOOLEAN DEFAULT true NOT NULL, 
    email_enabled BOOLEAN DEFAULT false NOT NULL, 
    slack_enabled BOOLEAN DEFAULT false NOT NULL, 
    teams_enabled BOOLEAN DEFAULT false NOT NULL, 
    governance_enabled BOOLEAN DEFAULT true NOT NULL, 
    stewardship_enabled BOOLEAN DEFAULT true NOT NULL, 
    operational_enabled BOOLEAN DEFAULT true NOT NULL, 
    only_assigned_items BOOLEAN DEFAULT false NOT NULL, 
    daily_digest_enabled BOOLEAN DEFAULT false NOT NULL, 
    slack_webhook_url TEXT, 
    teams_webhook_url TEXT, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(user_id) REFERENCES t2c_data.users (id) ON DELETE CASCADE, 
    CONSTRAINT uq_user_notification_preferences_user_id UNIQUE (user_id)
);

CREATE TABLE t2c_data.user_inbox_notifications (
    id SERIAL NOT NULL, 
    user_id INTEGER NOT NULL, 
    dedupe_key VARCHAR(255) NOT NULL, 
    category VARCHAR(40) NOT NULL, 
    severity VARCHAR(20) DEFAULT 'medium' NOT NULL, 
    source_module VARCHAR(40) NOT NULL, 
    source_entity_type VARCHAR(40) NOT NULL, 
    source_entity_id VARCHAR(255) NOT NULL, 
    title VARCHAR(200) NOT NULL, 
    message TEXT NOT NULL, 
    href TEXT, 
    state VARCHAR(20) DEFAULT 'unread' NOT NULL, 
    delivery_state VARCHAR(20) DEFAULT 'pending' NOT NULL, 
    context_json JSON, 
    first_seen_at TIMESTAMP WITH TIME ZONE NOT NULL, 
    last_seen_at TIMESTAMP WITH TIME ZONE NOT NULL, 
    last_notified_at TIMESTAMP WITH TIME ZONE, 
    next_delivery_at TIMESTAMP WITH TIME ZONE, 
    read_at TIMESTAMP WITH TIME ZONE, 
    archived_at TIMESTAMP WITH TIME ZONE, 
    delivery_channels_json JSON, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(user_id) REFERENCES t2c_data.users (id) ON DELETE CASCADE, 
    CONSTRAINT uq_user_inbox_notifications_user_dedupe UNIQUE (user_id, dedupe_key)
);

CREATE INDEX ix_user_inbox_notifications_user_state ON t2c_data.user_inbox_notifications (user_id, state);

CREATE INDEX ix_user_inbox_notifications_category ON t2c_data.user_inbox_notifications (category);

CREATE INDEX ix_user_inbox_notifications_due_delivery ON t2c_data.user_inbox_notifications (delivery_state, next_delivery_at);

CREATE INDEX ix_user_inbox_notifications_created ON t2c_data.user_inbox_notifications (created_at);

CREATE TABLE t2c_data.notification_delivery_attempts (
    id SERIAL NOT NULL, 
    inbox_notification_id INTEGER NOT NULL, 
    channel VARCHAR(20) NOT NULL, 
    status VARCHAR(20) DEFAULT 'pending' NOT NULL, 
    provider VARCHAR(40), 
    external_message_id VARCHAR(255), 
    attempt_count INTEGER DEFAULT '0' NOT NULL, 
    next_attempt_at TIMESTAMP WITH TIME ZONE, 
    last_attempt_at TIMESTAMP WITH TIME ZONE, 
    last_error TEXT, 
    response_payload_json JSON, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(inbox_notification_id) REFERENCES t2c_data.user_inbox_notifications (id) ON DELETE CASCADE
);

CREATE INDEX ix_notification_delivery_attempts_status ON t2c_data.notification_delivery_attempts (status);

CREATE INDEX ix_notification_delivery_attempts_channel ON t2c_data.notification_delivery_attempts (channel);

CREATE INDEX ix_notification_delivery_attempts_due ON t2c_data.notification_delivery_attempts (status, next_attempt_at);

CREATE TABLE t2c_data.governance_score_snapshots (
    id SERIAL NOT NULL, 
    table_id INTEGER NOT NULL, 
    datasource_id INTEGER, 
    owner_name VARCHAR(255), 
    domain_label VARCHAR(255), 
    score INTEGER NOT NULL, 
    label VARCHAR(40) NOT NULL, 
    tone VARCHAR(20) NOT NULL, 
    dq_score FLOAT, 
    open_incidents INTEGER DEFAULT '0' NOT NULL, 
    bucket_date TIMESTAMP WITH TIME ZONE NOT NULL, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(table_id) REFERENCES t2c_data.tables (id) ON DELETE CASCADE, 
    FOREIGN KEY(datasource_id) REFERENCES t2c_data.data_sources (id) ON DELETE SET NULL, 
    CONSTRAINT uq_governance_score_snapshot_table_bucket UNIQUE (table_id, bucket_date)
);

CREATE INDEX ix_governance_score_snapshots_bucket_date ON t2c_data.governance_score_snapshots (bucket_date);

CREATE INDEX ix_governance_score_snapshots_table_bucket ON t2c_data.governance_score_snapshots (table_id, bucket_date);

CREATE INDEX ix_governance_score_snapshots_score ON t2c_data.governance_score_snapshots (score);

DROP TABLE IF EXISTS t2c_data.lineage_graph_edges CASCADE;

DROP TABLE IF EXISTS t2c_data.lineage_nodes CASCADE;

DROP TABLE IF EXISTS t2c_data.lineage_edges CASCADE;

DROP TABLE IF EXISTS t2c_data.lineage_processes CASCADE;

UPDATE t2c_data.alembic_version SET version_num='c2d3e4f5a6b7' WHERE t2c_data.alembic_version.version_num = 'b1c2d3e4f5a6';

-- Running upgrade c2d3e4f5a6b7 -> e7f8a9b0c1d2

ALTER TABLE t2c_data.governance_settings ADD COLUMN stewardship_assignment_rules TEXT;

ALTER TABLE t2c_data.user_notification_preferences ADD COLUMN last_daily_digest_at TIMESTAMP WITH TIME ZONE;

ALTER TABLE t2c_data.user_notification_preferences ADD COLUMN next_daily_digest_at TIMESTAMP WITH TIME ZONE;

ALTER TABLE t2c_data.user_notification_preferences ADD COLUMN last_daily_digest_status VARCHAR(20);

UPDATE t2c_data.alembic_version SET version_num='e7f8a9b0c1d2' WHERE t2c_data.alembic_version.version_num = 'c2d3e4f5a6b7';

-- Profiling schedules for Data Quality

CREATE TABLE IF NOT EXISTS t2c_data.dq_profiling_schedules (
    id SERIAL PRIMARY KEY,
    scope VARCHAR(20) NOT NULL DEFAULT 'table',
    table_id INTEGER NULL REFERENCES t2c_data.tables(id) ON DELETE CASCADE,
    datasource_id INTEGER NULL REFERENCES t2c_data.data_sources(id) ON DELETE CASCADE,
    schema_name VARCHAR(255) NULL,
    execution_engine VARCHAR(20) NOT NULL DEFAULT 'spark',
    schedule_mode VARCHAR(20) NOT NULL DEFAULT 'manual',
    schedule_enabled BOOLEAN NOT NULL DEFAULT true,
    schedule_every_minutes INTEGER NULL,
    schedule_time VARCHAR(5) NULL,
    schedule_day_of_week INTEGER NULL,
    schedule_day_of_month INTEGER NULL,
    schedule_anchor_date TIMESTAMP WITH TIME ZONE NULL,
    schedule_last_run_at TIMESTAMP WITH TIME ZONE NULL,
    schedule_last_started_at TIMESTAMP WITH TIME ZONE NULL,
    schedule_last_finished_at TIMESTAMP WITH TIME ZONE NULL,
    schedule_last_status VARCHAR(20) NULL,
    schedule_last_error TEXT NULL,
    schedule_next_run_at TIMESTAMP WITH TIME ZONE NULL,
    schema_limit INTEGER NULL,
    schema_concurrency INTEGER NULL,
    schema_sample_fraction FLOAT NULL,
    schema_include_tables_json JSON NULL,
    schema_exclude_tables_json JSON NULL,
    schema_columns_json JSON NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL
);

CREATE TABLE IF NOT EXISTS t2c_data.dq_profiling_schedule_recipients (
    schedule_id INTEGER NOT NULL REFERENCES t2c_data.dq_profiling_schedules(id) ON DELETE CASCADE,
    user_id INTEGER NOT NULL REFERENCES t2c_data.users(id) ON DELETE CASCADE,
    PRIMARY KEY (schedule_id, user_id)
);

CREATE TABLE IF NOT EXISTS t2c_data.dq_profiling_scheduler_status (
    id SERIAL PRIMARY KEY,
    scheduler_name VARCHAR(80) NOT NULL DEFAULT 'dq_profiling',
    mode VARCHAR(20) NOT NULL DEFAULT 'embedded',
    is_enabled BOOLEAN NOT NULL DEFAULT true,
    last_started_at VARCHAR(64) NULL,
    last_heartbeat_at VARCHAR(64) NULL,
    last_success_at VARCHAR(64) NULL,
    last_failure_at VARCHAR(64) NULL,
    last_error TEXT NULL,
    last_run_summary_json JSON NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL
);

ALTER TABLE t2c_data.dq_runs
    ADD COLUMN IF NOT EXISTS profiling_schedule_id INTEGER NULL REFERENCES t2c_data.dq_profiling_schedules(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS ix_dq_runs_profiling_schedule_id ON t2c_data.dq_runs (profiling_schedule_id);
CREATE INDEX IF NOT EXISTS ix_t2c_data_dq_profiling_schedules_scope ON t2c_data.dq_profiling_schedules (scope);
CREATE INDEX IF NOT EXISTS ix_t2c_data_dq_profiling_schedules_table_id ON t2c_data.dq_profiling_schedules (table_id);
CREATE INDEX IF NOT EXISTS ix_t2c_data_dq_profiling_schedules_datasource_id ON t2c_data.dq_profiling_schedules (datasource_id);
CREATE INDEX IF NOT EXISTS ix_t2c_data_dq_profiling_schedules_schema_name ON t2c_data.dq_profiling_schedules (schema_name);
CREATE INDEX IF NOT EXISTS ix_t2c_data_dq_profiling_schedules_execution_engine ON t2c_data.dq_profiling_schedules (execution_engine);
CREATE INDEX IF NOT EXISTS ix_t2c_data_dq_profiling_schedules_schedule_enabled ON t2c_data.dq_profiling_schedules (schedule_enabled);
CREATE INDEX IF NOT EXISTS ix_t2c_data_dq_profiling_schedules_schedule_mode ON t2c_data.dq_profiling_schedules (schedule_mode);
CREATE INDEX IF NOT EXISTS ix_t2c_data_dq_profiling_schedules_schedule_next_run_at ON t2c_data.dq_profiling_schedules (schedule_next_run_at);
CREATE INDEX IF NOT EXISTS ix_t2c_data_dq_profiling_scheduler_status_scheduler_name ON t2c_data.dq_profiling_scheduler_status (scheduler_name);

UPDATE t2c_data.alembic_version SET version_num='d1e2f3a4b5c6' WHERE t2c_data.alembic_version.version_num = 'e7f8a9b0c1d2';

COMMIT;
