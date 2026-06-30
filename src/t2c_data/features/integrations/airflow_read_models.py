from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass

from t2c_data.core.config import settings
from sqlalchemy import text

from t2c_data.core.sql_utils import safe_relation

logger = logging.getLogger(__name__)

AIRFLOW_CATALOG_SCHEMA = "t2c_data"
AIRFLOW_SOURCE_SCHEMA = (settings.airflow_source_schema or "airflow_meta").strip() or "airflow_meta"
AIRFLOW_DAGS_VIEW = "vw_airflow_dags_resumo"
AIRFLOW_DAG_RUNS_VIEW = "vw_airflow_dag_runs_recentes"
AIRFLOW_FAILURES_VIEW = "vw_airflow_tasks_falhas"
AIRFLOW_OPERATIONAL_VIEW = "vw_airflow_operacional"


@dataclass(slots=True)
class AirflowReadModelContractSnapshot:
    source_schema: str
    schema_exists: bool
    dag_runs_table_exists: bool
    dag_table_exists: bool
    task_instance_table_exists: bool
    dag_tag_table_exists: bool
    task_fail_table_exists: bool
    log_table_exists: bool
    dag_runs_view_exists: bool
    dags_view_exists: bool
    failures_view_exists: bool
    operational_view_exists: bool
    ready: bool
    missing_tables: list[str]
    missing_views: list[str]
    contract_version: str


def _relation_exists(executor, schema_name: str, relation_name: str) -> bool:
    relation = safe_relation(schema_name, relation_name, label="relation")
    return bool(executor.execute(text("SELECT to_regclass(:relation_name)"), {"relation_name": relation}).scalar_one())


def _schema_exists(executor, schema_name: str) -> bool:
    return bool(
        executor.execute(
            text(
                """
                SELECT 1
                FROM information_schema.schemata
                WHERE schema_name = :schema_name
                LIMIT 1
                """
            ),
            {"schema_name": schema_name},
        ).scalar_one_or_none()
    )


def _table_exists(executor, table_name: str) -> bool:
    return bool(
        executor.execute(
            text(
                """
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = :schema_name
                  AND table_name = :table_name
                LIMIT 1
                """
            ),
            {"schema_name": AIRFLOW_SOURCE_SCHEMA, "table_name": table_name},
        ).scalar_one_or_none()
    )


def _tables_ready(executor, tables: Iterable[str]) -> bool:
    return all(_table_exists(executor, table_name) for table_name in tables)


def _create_schema(executor) -> None:
    executor.execute(text(f"CREATE SCHEMA IF NOT EXISTS {AIRFLOW_CATALOG_SCHEMA}"))


def inspect_airflow_operational_contract(executor) -> AirflowReadModelContractSnapshot:
    schema_exists = _schema_exists(executor, AIRFLOW_SOURCE_SCHEMA)
    dag_runs_table_exists = _table_exists(executor, "dag_run")
    dag_table_exists = _table_exists(executor, "dag")
    task_instance_table_exists = _table_exists(executor, "task_instance")
    dag_tag_table_exists = _table_exists(executor, "dag_tag")
    task_fail_table_exists = _table_exists(executor, "task_fail")
    log_table_exists = _table_exists(executor, "log")
    dag_runs_view_exists = _relation_exists(executor, AIRFLOW_CATALOG_SCHEMA, AIRFLOW_DAG_RUNS_VIEW)
    dags_view_exists = _relation_exists(executor, AIRFLOW_CATALOG_SCHEMA, AIRFLOW_DAGS_VIEW)
    failures_view_exists = _relation_exists(executor, AIRFLOW_CATALOG_SCHEMA, AIRFLOW_FAILURES_VIEW)
    operational_view_exists = _relation_exists(executor, AIRFLOW_CATALOG_SCHEMA, AIRFLOW_OPERATIONAL_VIEW)
    missing_tables = [
        name
        for name, exists in (
            ("dag_run", dag_runs_table_exists),
            ("dag", dag_table_exists),
            ("task_instance", task_instance_table_exists),
            ("dag_tag", dag_tag_table_exists),
            ("task_fail", task_fail_table_exists),
            ("log", log_table_exists),
        )
        if not exists
    ]
    missing_views = [
        name
        for name, exists in (
            (AIRFLOW_DAG_RUNS_VIEW, dag_runs_view_exists),
            (AIRFLOW_DAGS_VIEW, dags_view_exists),
            (AIRFLOW_FAILURES_VIEW, failures_view_exists),
            (AIRFLOW_OPERATIONAL_VIEW, operational_view_exists),
        )
        if not exists
    ]
    ready = schema_exists and not missing_tables and not missing_views
    return AirflowReadModelContractSnapshot(
        source_schema=AIRFLOW_SOURCE_SCHEMA,
        schema_exists=schema_exists,
        dag_runs_table_exists=dag_runs_table_exists,
        dag_table_exists=dag_table_exists,
        task_instance_table_exists=task_instance_table_exists,
        dag_tag_table_exists=dag_tag_table_exists,
        task_fail_table_exists=task_fail_table_exists,
        log_table_exists=log_table_exists,
        dag_runs_view_exists=dag_runs_view_exists,
        dags_view_exists=dags_view_exists,
        failures_view_exists=failures_view_exists,
        operational_view_exists=operational_view_exists,
        ready=ready,
        missing_tables=missing_tables,
        missing_views=missing_views,
        contract_version=settings.airflow_contract_version,
    )


def validate_airflow_operational_contract(executor) -> AirflowReadModelContractSnapshot:
    return inspect_airflow_operational_contract(executor)


def _dag_runs_sql() -> str:
    return f"""
    CREATE OR REPLACE VIEW t2c_data.vw_airflow_dag_runs_recentes AS
    SELECT
        d.dag_display_name,
        d.is_active,
        d.is_paused,
        dr.dag_id,
        dr.id AS dag_run_pk,
        dr.run_id,
        dr.state,
        dr.start_date,
        dr.end_date,
        CASE
            WHEN dr.start_date IS NULL THEN NULL
            ELSE EXTRACT(EPOCH FROM (COALESCE(dr.end_date, now()) - dr.start_date))::bigint
        END AS duration_seconds,
        dr.run_type,
        dr.execution_date,
        dr.execution_date AS logical_date,
        dr.queued_at,
        dr.external_trigger,
        dr.data_interval_start,
        dr.data_interval_end,
        dr.last_scheduling_decision,
        dr.updated_at
    FROM {AIRFLOW_SOURCE_SCHEMA}.dag_run dr
    JOIN {AIRFLOW_SOURCE_SCHEMA}.dag d
      ON d.dag_id = dr.dag_id
    """


def _dags_resumo_sql(*, include_tags: bool) -> str:
    if include_tags:
        return f"""
        CREATE OR REPLACE VIEW t2c_data.vw_airflow_dags_resumo AS
        WITH dag_tags AS (
            SELECT
                dt.dag_id,
                COALESCE(
                    array_agg(DISTINCT dt.name ORDER BY dt.name) FILTER (WHERE dt.name IS NOT NULL),
                    ARRAY[]::text[]
                ) AS tags
            FROM {AIRFLOW_SOURCE_SCHEMA}.dag_tag dt
            GROUP BY dt.dag_id
        ),
        latest_run AS (
            SELECT DISTINCT ON (dr.dag_id)
                dr.dag_id,
                dr.id AS latest_run_pk,
                dr.run_id AS latest_run_id,
                dr.state AS latest_state,
                dr.start_date AS latest_start_date,
                dr.end_date AS latest_end_date,
                CASE
                    WHEN dr.start_date IS NULL THEN NULL
                    ELSE EXTRACT(EPOCH FROM (COALESCE(dr.end_date, now()) - dr.start_date))::bigint
                END AS latest_duration_seconds,
                COALESCE(dr.end_date, dr.start_date, dr.queued_at, dr.updated_at) AS last_execution_at
            FROM {AIRFLOW_SOURCE_SCHEMA}.dag_run dr
            ORDER BY dr.dag_id, COALESCE(dr.end_date, dr.start_date, dr.queued_at, dr.updated_at) DESC NULLS LAST, dr.id DESC
        ),
        recent_activity AS (
            SELECT
                dr.dag_id,
                COUNT(*) FILTER (WHERE COALESCE(dr.end_date, dr.start_date, dr.queued_at, dr.updated_at) >= now() - interval '24 hours') AS recent_runs_count_24h,
                COUNT(*) FILTER (
                    WHERE dr.state IN ('failed', 'upstream_failed')
                      AND COALESCE(dr.end_date, dr.start_date, dr.queued_at, dr.updated_at) >= now() - interval '24 hours'
                ) AS recent_failures_count_24h
            FROM {AIRFLOW_SOURCE_SCHEMA}.dag_run dr
            GROUP BY dr.dag_id
        )
        SELECT
            d.dag_id,
            d.dag_display_name,
            d.description,
            d.is_active,
            d.is_paused,
            d.schedule_interval,
            d.timetable_description,
            d.next_dagrun AS next_dagrun_at,
            d.has_import_errors,
            d.fileloc,
            d.owners AS owner,
            dt.tags,
            lr.latest_run_pk,
            lr.latest_run_id,
            lr.last_execution_at,
            lr.latest_state,
            lr.latest_duration_seconds,
            COALESCE(ra.recent_runs_count_24h, 0)::int AS recent_runs_count_24h,
            COALESCE(ra.recent_failures_count_24h, 0)::int AS recent_failures_count_24h,
            now() AS updated_at
        FROM {AIRFLOW_SOURCE_SCHEMA}.dag d
        LEFT JOIN dag_tags dt
          ON dt.dag_id = d.dag_id
        LEFT JOIN latest_run lr
          ON lr.dag_id = d.dag_id
        LEFT JOIN recent_activity ra
          ON ra.dag_id = d.dag_id
        """
    return f"""
    CREATE OR REPLACE VIEW t2c_data.vw_airflow_dags_resumo AS
    WITH latest_run AS (
        SELECT DISTINCT ON (dr.dag_id)
            dr.dag_id,
            dr.id AS latest_run_pk,
            dr.run_id AS latest_run_id,
            dr.state AS latest_state,
            dr.start_date AS latest_start_date,
            dr.end_date AS latest_end_date,
                CASE
                    WHEN dr.start_date IS NULL THEN NULL
                    ELSE EXTRACT(EPOCH FROM (COALESCE(dr.end_date, now()) - dr.start_date))::bigint
                END AS latest_duration_seconds,
                COALESCE(dr.end_date, dr.start_date, dr.queued_at, dr.updated_at) AS last_execution_at
        FROM {AIRFLOW_SOURCE_SCHEMA}.dag_run dr
        ORDER BY dr.dag_id, COALESCE(dr.end_date, dr.start_date, dr.queued_at, dr.updated_at) DESC NULLS LAST, dr.id DESC
    ),
    recent_activity AS (
        SELECT
            dr.dag_id,
            COUNT(*) FILTER (WHERE COALESCE(dr.end_date, dr.start_date, dr.queued_at, dr.updated_at) >= now() - interval '24 hours') AS recent_runs_count_24h,
                COUNT(*) FILTER (
                    WHERE dr.state IN ('failed', 'upstream_failed')
                      AND COALESCE(dr.end_date, dr.start_date, dr.queued_at, dr.updated_at) >= now() - interval '24 hours'
                ) AS recent_failures_count_24h
        FROM {AIRFLOW_SOURCE_SCHEMA}.dag_run dr
        GROUP BY dr.dag_id
    )
    SELECT
        d.dag_id,
        d.dag_display_name,
        d.description,
        d.is_active,
        d.is_paused,
        d.schedule_interval,
        d.timetable_description,
        d.next_dagrun AS next_dagrun_at,
        d.has_import_errors,
        d.fileloc,
        d.owners AS owner,
        ARRAY[]::text[] AS tags,
        lr.latest_run_pk,
        lr.latest_run_id,
        lr.last_execution_at,
        lr.latest_state,
        lr.latest_duration_seconds,
        COALESCE(ra.recent_runs_count_24h, 0)::int AS recent_runs_count_24h,
        COALESCE(ra.recent_failures_count_24h, 0)::int AS recent_failures_count_24h,
        now() AS updated_at
    FROM {AIRFLOW_SOURCE_SCHEMA}.dag d
    LEFT JOIN latest_run lr
      ON lr.dag_id = d.dag_id
    LEFT JOIN recent_activity ra
      ON ra.dag_id = d.dag_id
    """


def _tasks_falhas_sql(*, include_task_fail: bool, include_log: bool) -> str:
    task_fail_counts_cte = ""
    task_fail_counts_join = ""
    if include_task_fail:
        task_fail_counts_cte = f"""
        task_fail_counts AS (
            SELECT
                tf.dag_id,
                tf.task_id,
                tf.run_id,
                COALESCE(tf.map_index, -1) AS map_index,
                COUNT(*) AS task_fail_count,
                MAX(tf.end_date) AS last_task_fail_at
            FROM {AIRFLOW_SOURCE_SCHEMA}.task_fail tf
            GROUP BY tf.dag_id, tf.task_id, tf.run_id, COALESCE(tf.map_index, -1)
        ),
        """
        task_fail_counts_join = """
        LEFT JOIN task_fail_counts tfc
          ON tfc.dag_id = ft.dag_id
         AND tfc.task_id = ft.task_id
         AND tfc.run_id = ft.run_id
         AND tfc.map_index = COALESCE(ft.map_index, -1)
        """

    latest_log_cte = ""
    latest_log_join = ""
    log_select = """
        NULL::character varying AS log_event,
        NULL::timestamp with time zone AS log_dttm,
        NULL::text AS log_extra,
        NULL::integer AS log_try_number,
    """
    if include_log:
        latest_log_cte = f"""
        latest_log AS (
            SELECT DISTINCT ON (l.dag_id, l.task_id, l.run_id, COALESCE(l.map_index, -1))
                l.dag_id,
                l.task_id,
                l.run_id,
                COALESCE(l.map_index, -1) AS map_index,
                l.event AS log_event,
                l.dttm AS log_dttm,
                l.extra AS log_extra,
                l.try_number AS log_try_number
            FROM {AIRFLOW_SOURCE_SCHEMA}.log l
            WHERE l.dag_id IS NOT NULL
              AND l.task_id IS NOT NULL
              AND l.run_id IS NOT NULL
            ORDER BY l.dag_id, l.task_id, l.run_id, COALESCE(l.map_index, -1), l.dttm DESC NULLS LAST, l.id DESC
        ),
        """
        latest_log_join = """
        LEFT JOIN latest_log ll
          ON ll.dag_id = ft.dag_id
         AND ll.task_id = ft.task_id
         AND ll.run_id = ft.run_id
         AND ll.map_index = COALESCE(ft.map_index, -1)
        """
        log_select = """
        ll.log_event,
        ll.log_dttm,
        ll.log_extra,
        ll.log_try_number,
        """

    troubleshooting_context = """
        concat_ws(
            ' | ',
            ft.operator,
            ft.queue,
            ft.hostname,
            ft.unixname,
            COALESCE(ll.log_event, ''),
            left(COALESCE(ll.log_extra, ''), 240)
        ) AS troubleshooting_context
    """
    if not include_log:
        troubleshooting_context = """
        concat_ws(
            ' | ',
            ft.operator,
            ft.queue,
            ft.hostname,
            ft.unixname
        ) AS troubleshooting_context
    """

    return f"""
    CREATE OR REPLACE VIEW t2c_data.vw_airflow_tasks_falhas AS
    WITH {task_fail_counts_cte}{latest_log_cte}
    failed_tasks AS (
        SELECT
            ti.dag_id,
            d.dag_display_name,
            ti.task_id,
            ti.run_id,
            ti.map_index,
            ti.state,
            ti.try_number,
            ti.start_date,
            ti.end_date,
            ti.duration,
            ti.operator,
            ti.queue,
            ti.hostname,
            ti.unixname,
            ti.job_id,
            ti.queued_dttm,
            ti.updated_at,
            ti.task_display_name,
            ti.next_method,
            ti.next_kwargs,
            ti.external_executor_id,
            COALESCE(ti.end_date, ti.start_date, ti.queued_dttm) AS failure_at,
            ROW_NUMBER() OVER (
                PARTITION BY ti.dag_id, ti.task_id, ti.run_id, COALESCE(ti.map_index, -1)
                ORDER BY COALESCE(ti.end_date, ti.start_date, ti.queued_dttm) DESC NULLS LAST, ti.try_number DESC NULLS LAST
            ) AS rn
        FROM {AIRFLOW_SOURCE_SCHEMA}.task_instance ti
        JOIN {AIRFLOW_SOURCE_SCHEMA}.dag d
          ON d.dag_id = ti.dag_id
        WHERE ti.state IN ('failed', 'upstream_failed')
    )
    SELECT
        ft.dag_id,
        ft.dag_display_name,
        ft.task_id,
        ft.run_id,
        ft.map_index,
        ft.state,
        ft.try_number,
        ft.start_date,
        ft.end_date,
        CASE
            WHEN ft.start_date IS NULL THEN NULL
            ELSE EXTRACT(EPOCH FROM (COALESCE(ft.end_date, now()) - ft.start_date))::bigint
        END AS duration_seconds,
        ft.operator,
        ft.queue,
        ft.hostname,
        ft.unixname,
        ft.job_id,
        ft.queued_dttm,
        ft.updated_at,
        ft.task_display_name,
        ft.next_method,
        ft.next_kwargs,
        ft.external_executor_id,
        ft.failure_at,
        {"COALESCE(tfc.task_fail_count, 0)::bigint AS task_fail_count,\n        tfc.last_task_fail_at," if include_task_fail else "0::bigint AS task_fail_count,\n        NULL::timestamp with time zone AS last_task_fail_at,"}
        {log_select}
        {troubleshooting_context}
    FROM failed_tasks ft
    {task_fail_counts_join}
    {latest_log_join}
    WHERE ft.rn = 1
    """


def _operational_sql(*, include_log: bool) -> str:
    latest_log_select = f"(SELECT MAX(l.dttm) FROM {AIRFLOW_SOURCE_SCHEMA}.log l)" if include_log else "NULL::timestamp with time zone"
    return f"""
    CREATE OR REPLACE VIEW t2c_data.vw_airflow_operacional AS
    SELECT
        (SELECT COUNT(*) FROM {AIRFLOW_SOURCE_SCHEMA}.dag)::int AS total_dags,
        (SELECT COUNT(*) FROM {AIRFLOW_SOURCE_SCHEMA}.dag WHERE is_active IS TRUE AND is_paused IS FALSE)::int AS active_dags,
        (SELECT COUNT(*) FROM {AIRFLOW_SOURCE_SCHEMA}.dag WHERE is_paused IS TRUE)::int AS paused_dags,
        COALESCE(
            (SELECT COUNT(*) FROM {AIRFLOW_SOURCE_SCHEMA}.dag_run dr
             WHERE dr.state = 'success'
               AND COALESCE(dr.end_date, dr.start_date, dr.queued_at, dr.updated_at) >= now() - interval '24 hours'),
            0
        )::int AS success_runs_24h,
        COALESCE(
            (SELECT COUNT(*) FROM {AIRFLOW_SOURCE_SCHEMA}.dag_run dr
             WHERE dr.state = 'failed'
               AND COALESCE(dr.end_date, dr.start_date, dr.queued_at, dr.updated_at) >= now() - interval '24 hours'),
            0
        )::int AS failed_runs_24h,
        COALESCE(
            (SELECT COUNT(*) FROM {AIRFLOW_SOURCE_SCHEMA}.task_instance ti
             WHERE ti.state IN ('failed', 'upstream_failed')
               AND COALESCE(ti.end_date, ti.start_date, ti.queued_dttm) >= now() - interval '24 hours'),
            0
        )::int AS task_failures_24h,
        (SELECT MAX(COALESCE(dr.end_date, dr.start_date, dr.queued_at, dr.updated_at)) FROM {AIRFLOW_SOURCE_SCHEMA}.dag_run dr) AS last_execution_at,
        (SELECT MAX(COALESCE(ti.end_date, ti.start_date, ti.queued_dttm)) FROM {AIRFLOW_SOURCE_SCHEMA}.task_instance ti WHERE ti.state IN ('failed', 'upstream_failed')) AS latest_failure_at,
        {latest_log_select} AS latest_log_at,
        now() AS updated_at
    """


def ensure_airflow_operational_read_models(executor) -> bool:
    bind = getattr(executor, "bind", None)
    dialect = getattr(bind, "dialect", None) or getattr(executor, "dialect", None)
    if getattr(dialect, "name", None) != "postgresql":
        logger.debug("Skipping airflow read-model creation outside PostgreSQL")
        return False

    _create_schema(executor)

    dags_ready = _tables_ready(executor, ("dag", "dag_run"))
    tasks_ready = _tables_ready(executor, ("dag", "task_instance"))
    operational_ready = _tables_ready(executor, ("dag", "dag_run", "task_instance"))

    if not dags_ready or not tasks_ready or not operational_ready:
        logger.info(
            "Skipping airflow read-model creation because source tables are not ready",
            extra={
                "dags_ready": dags_ready,
                "tasks_ready": tasks_ready,
                "operational_ready": operational_ready,
            },
        )
        return False

    if not _relation_exists(executor, AIRFLOW_CATALOG_SCHEMA, AIRFLOW_DAG_RUNS_VIEW):
        executor.execute(text(_dag_runs_sql()))
    if not _relation_exists(executor, AIRFLOW_CATALOG_SCHEMA, AIRFLOW_DAGS_VIEW):
        executor.execute(text(_dags_resumo_sql(include_tags=_table_exists(executor, "dag_tag"))))
    if not _relation_exists(executor, AIRFLOW_CATALOG_SCHEMA, AIRFLOW_FAILURES_VIEW):
        executor.execute(
            text(
                _tasks_falhas_sql(
                    include_task_fail=_table_exists(executor, "task_fail"),
                    include_log=_table_exists(executor, "log"),
                )
            )
        )
    if not _relation_exists(executor, AIRFLOW_CATALOG_SCHEMA, AIRFLOW_OPERATIONAL_VIEW):
        executor.execute(text(_operational_sql(include_log=_table_exists(executor, "log"))))
    return True


__all__ = [
    "AIRFLOW_CATALOG_SCHEMA",
    "AIRFLOW_SOURCE_SCHEMA",
    "AIRFLOW_DAGS_VIEW",
    "AIRFLOW_DAG_RUNS_VIEW",
    "AIRFLOW_FAILURES_VIEW",
    "AIRFLOW_OPERATIONAL_VIEW",
    "AirflowReadModelContractSnapshot",
    "ensure_airflow_operational_read_models",
    "inspect_airflow_operational_contract",
    "validate_airflow_operational_contract",
]
