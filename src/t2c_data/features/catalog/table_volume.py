from __future__ import annotations

import logging
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from time import perf_counter
from typing import Iterator

from sqlalchemy import create_engine, select, text
from sqlalchemy.engine import Connection, URL
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, selectinload

from t2c_data.features.datasource.api_support import resolved_connection
from t2c_data.models.catalog import DataSource, Database, Schema, TableEntity
from t2c_data.schemas.catalog import TableVolumeRunOut, TableVolumeSnapshotOut

logger = logging.getLogger(__name__)

TRUSTED_MEASUREMENT_SOURCES = {
    "postgres_count",
    "mysql_count",
    "sql_count",
    "catalog_profile",
    "datalake_footer",
    "manual",
}

DEFAULT_ROW_COUNT_TIMEOUT_SECONDS = 30
DEFAULT_ROW_COUNT_STRATEGY = "exact"
ROW_COUNT_STRATEGY_DISABLED = "disabled"
ROW_COUNT_STRATEGY_EXACT = "exact"
ROW_COUNT_STRATEGY_ESTIMATED = "estimated"


@dataclass(slots=True)
class TableVolumeTarget:
    table: TableEntity
    schema_name: str | None
    database_name: str | None
    connection_name: str | None
    datasource: DataSource
    fqn: str | None


@dataclass(slots=True)
class TableVolumeRuntimeConfig:
    enabled: bool = True
    strategy: str = DEFAULT_ROW_COUNT_STRATEGY
    timeout_seconds: int = DEFAULT_ROW_COUNT_TIMEOUT_SECONDS
    max_tables_per_run: int | None = None
    exact_max_rows_before_estimate: int | None = None


def _normalize_db_type(value: str | None) -> str:
    normalized = (value or "").strip().lower()
    if normalized == "postgresql":
        return "postgres"
    if normalized in {"mysql", "mariadb", "postgres", "sqlserver", "oracle"}:
        return normalized
    return normalized


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_text(value: str | None) -> str | None:
    if value is None:
        return None
    clean = value.strip()
    return clean or None


def _normalize_bool(value: object | None, default: bool = True) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "y", "on", "enabled"}:
            return True
        if normalized in {"0", "false", "no", "n", "off", "disabled"}:
            return False
    return default


def _normalize_int(value: object | None, default: int | None) -> int | None:
    if value is None:
        return default
    try:
        return max(int(value), 1)
    except (TypeError, ValueError):
        return default


def _row_count_runtime_config(datasource: DataSource) -> TableVolumeRuntimeConfig:
    connection_config = datasource.connection_config or {}
    enabled = _normalize_bool(connection_config.get("row_count_enabled"), default=True)
    strategy = str(connection_config.get("row_count_strategy") or DEFAULT_ROW_COUNT_STRATEGY).strip().lower()
    if strategy not in {ROW_COUNT_STRATEGY_DISABLED, ROW_COUNT_STRATEGY_EXACT, ROW_COUNT_STRATEGY_ESTIMATED}:
        strategy = DEFAULT_ROW_COUNT_STRATEGY
    timeout_seconds = _normalize_int(connection_config.get("row_count_timeout_seconds"), DEFAULT_ROW_COUNT_TIMEOUT_SECONDS)
    max_tables_per_run = _normalize_int(connection_config.get("row_count_max_tables_per_run"), None)
    exact_max_rows_before_estimate = _normalize_int(
        connection_config.get("row_count_exact_max_rows_before_estimate"),
        None,
    )
    return TableVolumeRuntimeConfig(
        enabled=enabled,
        strategy=strategy,
        timeout_seconds=timeout_seconds or DEFAULT_ROW_COUNT_TIMEOUT_SECONDS,
        max_tables_per_run=max_tables_per_run,
        exact_max_rows_before_estimate=exact_max_rows_before_estimate,
    )


def load_table_volume_runtime_config(datasource: DataSource) -> TableVolumeRuntimeConfig:
    return _row_count_runtime_config(datasource)


def _table_volume_table_ready(session: Session) -> bool:
    bind = session.get_bind()
    if bind is None:
        return False
    from sqlalchemy import inspect

    inspector = inspect(bind)
    return inspector.has_table("table_row_count_snapshots", schema="controle")


def _trusted_measurement_source(datasource: DataSource) -> str:
    db_type = _normalize_db_type(datasource.db_type)
    if db_type == "postgres":
        return "postgres_count"
    if db_type in {"mysql", "mariadb"}:
        return "mysql_count"
    return f"{db_type}_count" if db_type else "unavailable"


def _measurement_source_for_snapshot(datasource: DataSource) -> str:
    measurement_source = _trusted_measurement_source(datasource)
    return measurement_source if measurement_source != "unavailable" else "unavailable"


def _datasource_url(datasource: DataSource) -> URL | None:
    connection = resolved_connection(datasource)
    host = _normalize_text(str(connection.get("host") or datasource.host or ""))
    database = _normalize_text(str(connection.get("database") or datasource.database or ""))
    username = _normalize_text(str(connection.get("username") or datasource.username or ""))
    password = datasource.get_secret("password") or ""
    port = int(connection.get("port") or datasource.port or 5432)

    if not host or not database or not username:
        return None

    db_type = _normalize_db_type(datasource.db_type)
    if db_type == "postgres":
        return URL.create(
            "postgresql+psycopg",
            username=username,
            password=password,
            host=host,
            port=port,
            database=database,
        )
    if db_type in {"mysql", "mariadb"}:
        return URL.create(
            "mysql+pymysql",
            username=username,
            password=password,
            host=host,
            port=port or 3306,
            database=database,
        )
    return None


@contextmanager
def _datasource_connection(datasource: DataSource) -> Iterator[Connection]:
    url = _datasource_url(datasource)
    if url is None:
        raise ValueError("O datasource não possui credenciais suficientes para medir volume.")
    engine = create_engine(url, future=True, pool_pre_ping=True)
    connection = engine.connect()
    try:
        yield connection
    finally:
        connection.close()
        engine.dispose()


def _quoting_quote(connection: Connection, identifier: str) -> str:
    return connection.dialect.identifier_preparer.quote(identifier)


def _build_relation_sql(connection: Connection, schema_name: str, table_name: str) -> str:
    quoted_schema = _quoting_quote(connection, schema_name)
    quoted_table = _quoting_quote(connection, table_name)
    return f"SELECT COUNT(*) AS row_count FROM {quoted_schema}.{quoted_table}"


def _build_estimate_sql(connection: Connection, datasource: DataSource, schema_name: str, table_name: str) -> str:
    db_type = _normalize_db_type(datasource.db_type)
    if db_type == "postgres":
        return (
            "SELECT COALESCE(c.reltuples::bigint, 0) AS row_count "
            "FROM pg_class c "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = :schema_name AND c.relname = :table_name "
            "LIMIT 1"
        )
    if db_type in {"mysql", "mariadb"}:
        return (
            "SELECT COALESCE(table_rows, 0) AS row_count "
            "FROM information_schema.tables "
            "WHERE table_schema = :schema_name AND table_name = :table_name "
            "LIMIT 1"
        )
    raise ValueError(f"Fonte {datasource.db_type} ainda não suportada para estimativa de volume.")


def _load_table_target(session: Session, table_id: int) -> TableVolumeTarget | None:
    table = session.scalar(
        select(TableEntity)
        .options(
            selectinload(TableEntity.schema).selectinload(Schema.database).selectinload(Database.datasource),
            selectinload(TableEntity.data_owner),
        )
        .where(TableEntity.id == table_id)
    )
    if table is None:
        return None

    schema = table.schema
    database = schema.database if schema else None
    datasource = database.datasource if database else None
    if schema is None or database is None or datasource is None:
        return None

    fqn = f"{datasource.name}.{database.name}.{schema.name}.{table.name}"
    return TableVolumeTarget(
        table=table,
        schema_name=schema.name,
        database_name=database.name,
        connection_name=datasource.name,
        datasource=datasource,
        fqn=fqn,
    )


def _measure_relation_row_count(
    *,
    connection: Connection,
    datasource: DataSource,
    schema_name: str,
    table_name: str,
    strategy: str,
) -> int:
    normalized_strategy = (strategy or DEFAULT_ROW_COUNT_STRATEGY).strip().lower()
    if normalized_strategy == ROW_COUNT_STRATEGY_ESTIMATED:
        estimate_sql = _build_estimate_sql(connection, datasource, schema_name, table_name)
        row_count = connection.execute(text(estimate_sql), {"schema_name": schema_name, "table_name": table_name}).scalar_one()
        return int(row_count or 0)

    relation_sql = _build_relation_sql(connection, schema_name, table_name)
    return int(connection.exec_driver_sql(relation_sql).scalar_one())


def _resolve_measurement_strategy(
    *,
    connection: Connection,
    target: TableVolumeTarget,
    runtime_config: TableVolumeRuntimeConfig,
) -> tuple[str, int | None]:
    strategy = runtime_config.strategy
    if strategy != ROW_COUNT_STRATEGY_EXACT or runtime_config.exact_max_rows_before_estimate is None:
        return strategy, None

    estimated_row_count = _measure_relation_row_count(
        connection=connection,
        datasource=target.datasource,
        schema_name=target.schema_name or target.database_name or "",
        table_name=target.table.name,
        strategy=ROW_COUNT_STRATEGY_ESTIMATED,
    )
    if estimated_row_count > runtime_config.exact_max_rows_before_estimate:
        logger.info(
            "table volume exact count skipped for large table table_id=%s estimated_row_count=%s threshold=%s",
            target.table.id,
            estimated_row_count,
            runtime_config.exact_max_rows_before_estimate,
        )
        return ROW_COUNT_STRATEGY_ESTIMATED, estimated_row_count
    return strategy, estimated_row_count


def _snapshot_row_to_payload(row: dict[str, object | None]) -> TableVolumeSnapshotOut:
    measured_at = row.get("measured_at") or row.get("snapshot_at")
    return TableVolumeSnapshotOut(
        table_id=int(row["table_id"]),
        datasource_id=int(row["datasource_id"]) if row.get("datasource_id") is not None else None,
        schema_id=int(row["schema_id"]) if row.get("schema_id") is not None else None,
        connection_name=row.get("connection_name"),
        database_name=row.get("database_name"),
        schema_name=row.get("schema_name"),
        table_name=row.get("table_name"),
        fqn=row.get("fqn"),
        row_count=int(row["row_count"]) if row.get("row_count") is not None else None,
        measurement_type=row.get("measurement_type"),
        measurement_source=row.get("measurement_source"),
        status=row.get("status"),
        measured_at=measured_at if isinstance(measured_at, datetime) else None,
        duration_ms=int(row["duration_ms"]) if row.get("duration_ms") is not None else None,
        error_message=row.get("error_message"),
    )


def list_table_volume_history(*, db: Session, table_id: int, limit: int = 30) -> list[TableVolumeSnapshotOut]:
    if not _table_volume_table_ready(db):
        return []
    rows = db.execute(
        text(
            """
            select
                table_id,
                datasource_id,
                schema_id,
                connection_name,
                database_name,
                schema_name,
                table_name,
                fqn,
                row_count,
                coalesce(measurement_type, collection_method) as measurement_type,
                measurement_source,
                coalesce(status, collection_status) as status,
                coalesce(measured_at, snapshot_at) as measured_at,
                duration_ms,
                error_message
            from controle.table_row_count_snapshots
            where table_id = :table_id
            order by coalesce(measured_at, snapshot_at) desc nulls last, id desc
            limit :limit
            """
        ),
        {"table_id": table_id, "limit": max(int(limit or 30), 1)},
    ).mappings().all()
    return [_snapshot_row_to_payload(row) for row in rows]


def _is_trusted_success(snapshot: TableVolumeSnapshotOut) -> bool:
    status = (snapshot.status or "").strip().lower()
    source = (snapshot.measurement_source or "").strip().lower()
    if status != "success" or snapshot.row_count is None:
        return False
    if source in TRUSTED_MEASUREMENT_SOURCES:
        return True
    return False


def get_latest_table_volume(*, db: Session, table_id: int) -> TableVolumeSnapshotOut | None:
    history = list_table_volume_history(db=db, table_id=table_id, limit=5)
    if not history:
        return None

    latest = history[0]
    latest_status = (latest.status or "").strip().lower()
    if latest_status == "error":
        return latest
    if latest_status == "skipped" and latest.row_count is None:
        return latest

    for snapshot in history:
        if _is_trusted_success(snapshot):
            return snapshot

    return None


def _persist_volume_snapshot(
    *,
    db: Session,
    target: TableVolumeTarget,
    row_count: int | None,
    measurement_type: str,
    measurement_source: str,
    status: str,
    error_message: str | None,
    duration_ms: int | None,
) -> TableVolumeSnapshotOut | None:
    if not _table_volume_table_ready(db):
        logger.warning("table volume snapshot table unavailable schema=controle table=table_row_count_snapshots")
        return None

    measured_at = _now()
    payload = {
        "table_id": target.table.id,
        "datasource_id": target.datasource.id,
        "schema_id": target.table.schema_id,
        "connection_name": target.connection_name,
        "database_name": target.database_name,
        "schema_name": target.schema_name,
        "table_name": target.table.name,
        "fqn": target.fqn,
        "row_count": row_count,
        "measurement_type": measurement_type,
        "measurement_source": measurement_source,
        "status": status,
        "measured_at": measured_at,
        "snapshot_at": measured_at,
        "duration_ms": duration_ms,
        "error_message": error_message,
        "collection_method": measurement_type,
        "collection_status": status,
        "snapshot_date": measured_at.date(),
    }
    db.execute(
        text(
            """
            insert into controle.table_row_count_snapshots (
                table_id,
                datasource_id,
                schema_id,
                connection_name,
                database_name,
                schema_name,
                table_name,
                fqn,
                row_count,
                measurement_type,
                measurement_source,
                status,
                measured_at,
                snapshot_at,
                duration_ms,
                error_message,
                collection_method,
                collection_status,
                snapshot_date
            ) values (
                :table_id,
                :datasource_id,
                :schema_id,
                :connection_name,
                :database_name,
                :schema_name,
                :table_name,
                :fqn,
                :row_count,
                :measurement_type,
                :measurement_source,
                :status,
                :measured_at,
                :snapshot_at,
                :duration_ms,
                :error_message,
                :collection_method,
                :collection_status,
                :snapshot_date
            )
            """
        ),
        payload,
    )
    db.commit()
    return TableVolumeSnapshotOut(**payload)


def measure_table_volume(
    *,
    db: Session,
    table_id: int,
    measurement_context: str = "manual",
) -> TableVolumeSnapshotOut | None:
    target = _load_table_target(db, table_id)
    if target is None:
        return None

    runtime_config = _row_count_runtime_config(target.datasource)
    db_type = _normalize_db_type(target.datasource.db_type)
    measurement_source = _measurement_source_for_snapshot(target.datasource)
    if measurement_context != "manual" and (not runtime_config.enabled or runtime_config.strategy == ROW_COUNT_STRATEGY_DISABLED):
        return None

    if db_type not in {"postgres", "mysql", "mariadb"}:
        return _persist_volume_snapshot(
            db=db,
            target=target,
            row_count=None,
            measurement_type="unavailable",
            measurement_source=measurement_source,
            status="skipped",
            error_message=f"Fonte {target.datasource.db_type} ainda não suportada para medição de volume.",
            duration_ms=None,
        )

    started_at = perf_counter()
    try:
        with _datasource_connection(target.datasource) as connection:
            timeout_ms = max(int(runtime_config.timeout_seconds or DEFAULT_ROW_COUNT_TIMEOUT_SECONDS), 1) * 1000
            if connection.dialect.name == "postgresql":
                connection.exec_driver_sql(f"SET statement_timeout = {timeout_ms}")
            elif connection.dialect.name in {"mysql", "mariadb"}:
                try:
                    connection.exec_driver_sql(f"SET SESSION max_execution_time = {timeout_ms}")
                except SQLAlchemyError:
                    logger.debug("mysql max_execution_time not supported for table_id=%s", table_id)

            effective_strategy, prefetched_estimate = _resolve_measurement_strategy(
                connection=connection,
                target=target,
                runtime_config=runtime_config,
            )
            if effective_strategy == ROW_COUNT_STRATEGY_ESTIMATED and prefetched_estimate is not None:
                row_count = prefetched_estimate
            else:
                row_count = _measure_relation_row_count(
                    connection=connection,
                    datasource=target.datasource,
                    schema_name=target.schema_name or target.database_name or "",
                    table_name=target.table.name,
                    strategy=effective_strategy,
                )

        duration_ms = int((perf_counter() - started_at) * 1000)
        return _persist_volume_snapshot(
            db=db,
            target=target,
            row_count=row_count,
            measurement_type=effective_strategy if effective_strategy == ROW_COUNT_STRATEGY_ESTIMATED else "exact",
            measurement_source=measurement_source,
            status="success",
            error_message=None,
            duration_ms=duration_ms,
        )
    except Exception as exc:  # noqa: BLE001
        duration_ms = int((perf_counter() - started_at) * 1000)
        logger.exception("table volume measurement failed table_id=%s", table_id)
        return _persist_volume_snapshot(
            db=db,
            target=target,
            row_count=None,
            measurement_type="unavailable",
            measurement_source=measurement_source,
            status="error",
            error_message=str(exc)[:4000],
            duration_ms=duration_ms,
        )


def measure_all_active_tables_volume(
    *,
    db: Session,
    datasource_id: int | None = None,
    schema_id: int | None = None,
    limit: int | None = None,
) -> TableVolumeRunOut:
    query = select(TableEntity)
    if schema_id is not None:
        query = query.where(TableEntity.schema_id == int(schema_id))
    if datasource_id is not None:
        query = (
            query.join(TableEntity.schema)
            .join(Schema.database)
            .join(Database.datasource)
            .where(DataSource.id == int(datasource_id))
        )
    query = query.order_by(TableEntity.id.asc())
    if limit is not None:
        query = query.limit(max(int(limit), 1))

    tables = db.scalars(query).all()
    if datasource_id is not None:
        datasource = db.get(DataSource, int(datasource_id))
        runtime_config = _row_count_runtime_config(datasource) if datasource is not None else TableVolumeRuntimeConfig()
        if runtime_config.max_tables_per_run is not None and len(tables) > runtime_config.max_tables_per_run:
            tables = tables[: runtime_config.max_tables_per_run]
    else:
        runtime_config = TableVolumeRuntimeConfig()

    items: list[TableVolumeSnapshotOut] = []
    succeeded = 0
    failed = 0
    skipped = 0
    for table in tables:
        snapshot = measure_table_volume(db=db, table_id=table.id, measurement_context="scheduled_job")
        if snapshot is None:
            skipped += 1
            continue
        items.append(snapshot)
        status = (snapshot.status or "").strip().lower()
        if status == "success":
            succeeded += 1
        elif status == "error":
            failed += 1
        else:
            skipped += 1

    return TableVolumeRunOut(total_tables=len(tables), succeeded=succeeded, failed=failed, skipped=skipped, items=items)
