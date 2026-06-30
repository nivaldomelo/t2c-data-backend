from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from t2c_data.models.catalog import DataSource, Database, Schema, TableEntity
from t2c_data.models.dq import DQColumnMetric, DQRun, DQTableMetric
from t2c_data.features.data_quality.observability import build_dq_observability_payload


def _split_table_fqn(table_fqn: str | None) -> list[str]:
    return [part.strip() for part in str(table_fqn or "").split(".") if part and part.strip()]


def table_fqn_candidates(table_fqn: str | None) -> list[str]:
    parts = _split_table_fqn(table_fqn)
    if not parts:
        return []

    candidates: list[str] = []
    raw = str(table_fqn or "").strip()
    if raw:
        candidates.append(raw)

    if len(parts) >= 4:
        # Normalize datasource.database.schema.table -> datasource.schema.table
        candidates.append(f"{parts[0]}.{parts[-2]}.{parts[-1]}")
    if len(parts) >= 3:
        # Normalize datasource.schema.table or datasource.database.schema.table -> datasource.schema.table
        candidates.append(f"{parts[0]}.{parts[-2]}.{parts[-1]}")
    if len(parts) >= 2:
        candidates.append(f"{parts[-2]}.{parts[-1]}")
    candidates.append(parts[-1])
    return list(dict.fromkeys(candidate for candidate in candidates if candidate))


def table_fqn_candidates_for_table(session: Session, table_id: int) -> list[str]:
    row = session.execute(
        select(DataSource.name, Database.name, Schema.name, TableEntity.name)
        .join(Schema, TableEntity.schema_id == Schema.id)
        .join(Database, Schema.database_id == Database.id)
        .join(DataSource, Database.datasource_id == DataSource.id)
        .where(TableEntity.id == table_id)
    ).first()
    if not row:
        return []

    datasource_name = str(row[0] or "").strip()
    database_name = str(row[1] or "").strip()
    schema_name = str(row[2] or "").strip()
    table_name = str(row[3] or "").strip()

    candidates = []
    if datasource_name and database_name and schema_name and table_name:
        candidates.append(f"{datasource_name}.{database_name}.{schema_name}.{table_name}")
    if datasource_name and schema_name and table_name:
        candidates.append(f"{datasource_name}.{schema_name}.{table_name}")
    if schema_name and table_name:
        candidates.append(f"{schema_name}.{table_name}")
    if table_name:
        candidates.append(table_name)
    return list(dict.fromkeys(candidate for candidate in candidates if candidate))


def resolve_table_context_by_fqn(session: Session, table_fqn: str) -> tuple[TableEntity, Schema, Database, DataSource]:
    parts = _split_table_fqn(table_fqn)
    if len(parts) < 2:
        raise ValueError("table_fqn inválido. Use schema.table, datasource.schema.table ou datasource.database.schema.table")

    if len(parts) == 2:
        schema_name = parts[0]
        table_name = parts[1]
        datasource_name = None
        database_name = None
    elif len(parts) == 3:
        datasource_name = parts[0]
        schema_name = parts[1]
        table_name = parts[2]
        database_name = None
    else:
        datasource_name = parts[0]
        database_name = parts[1]
        schema_name = parts[-2]
        table_name = parts[-1]

    query = (
        select(TableEntity, Schema, Database, DataSource)
        .join(Schema, TableEntity.schema_id == Schema.id)
        .join(Database, Schema.database_id == Database.id)
        .join(DataSource, Database.datasource_id == DataSource.id)
        .where(Schema.name == schema_name, TableEntity.name == table_name)
    )
    if datasource_name:
        query = query.where(DataSource.name == datasource_name)
    if len(parts) >= 4 and database_name:
        query = query.where(Database.name == database_name)

    rows = session.execute(query.order_by(TableEntity.id.desc()).limit(2)).all()
    if not rows:
        raise ValueError("Tabela não encontrada no catálogo para a regra")
    if len(rows) > 1:
        raise ValueError(
            "table_fqn ambíguo no catálogo. Informe um identificador mais específico; a heurística por FQN não escolhe automaticamente entre múltiplos ativos."
        )
    row = rows[0]
    return row[0], row[1], row[2], row[3]


def _table_fqn(session: Session, table_id: int) -> str:
    row = session.execute(
        select(DataSource.name, Schema.name, TableEntity.name)
        .join(Schema, TableEntity.schema_id == Schema.id)
        .join(Database, Schema.database_id == Database.id)
        .join(DataSource, Database.datasource_id == DataSource.id)
        .where(TableEntity.id == table_id)
    ).first()
    if not row:
        return f"table:{table_id}"
    return f"{row[0]}.{row[1]}.{row[2]}"


def _metric_snapshot(
    table_metric: DQTableMetric,
    run: DQRun,
    columns: list[DQColumnMetric],
    now: datetime,
) -> dict:
    freshness = int((now - run.created_at).total_seconds())
    return {
        "run_id": run.id,
        "run_at": run.created_at,
        "execution_engine": getattr(run, "execution_engine", None),
        "spark_app_id": getattr(run, "spark_app_id", None),
        "queued_at": getattr(run, "queued_at", None),
        "started_at": getattr(run, "started_at", None),
        "finished_at": getattr(run, "finished_at", None),
        "duration_ms": getattr(run, "duration_ms", None),
        "log_tail": getattr(run, "log_tail", None),
        "row_count": int(table_metric.row_count),
        "completeness_pct_avg": float(table_metric.completeness_pct_avg),
        "dq_score": float(table_metric.dq_score),
        "duplicates_count": int(table_metric.duplicates_count),
        "failed_rules": int(table_metric.failed_rules),
        "freshness_seconds": freshness,
        "columns": [
            {
                "column_name": c.column_name,
                "data_type": c.data_type,
                "null_count": int(c.null_count),
                "null_pct": float(c.null_pct),
                "distinct_count": int(c.distinct_count),
                "min_value": c.min_value,
                "max_value": c.max_value,
            }
            for c in columns
        ],
    }


def latest_table_metrics(session: Session, table: TableEntity, *, current_user=None) -> dict | None:
    return table_metrics_with_history(session, table, history_runs=14, current_user=current_user)


def table_metrics_with_history(
    session: Session,
    table: TableEntity,
    history_runs: int = 14,
    *,
    current_user=None,
) -> dict | None:
    total_runs = max(2, min(history_runs, 30))
    rows = session.execute(
        select(DQTableMetric, DQRun)
        .join(DQRun, DQTableMetric.run_id == DQRun.id)
        .where(DQTableMetric.table_id == table.id, DQRun.status == "success")
        .order_by(DQRun.created_at.desc())
        .limit(total_runs)
    ).all()
    if not rows:
        return None

    metric_ids = [table_metric.id for table_metric, _ in rows]
    columns = session.scalars(select(DQColumnMetric).where(DQColumnMetric.table_metric_id.in_(metric_ids))).all()
    columns_by_metric: dict[int, list[DQColumnMetric]] = {metric_id: [] for metric_id in metric_ids}
    for column in columns:
        columns_by_metric.setdefault(column.table_metric_id, []).append(column)
    for metric_id in columns_by_metric:
        columns_by_metric[metric_id].sort(key=lambda c: c.column_name)

    now = datetime.now(timezone.utc)
    snapshots = [
        _metric_snapshot(table_metric, run, columns_by_metric.get(table_metric.id, []), now) for table_metric, run in rows
    ]
    current = snapshots[0]
    previous = snapshots[1] if len(snapshots) > 1 else None

    chronological = list(reversed(snapshots))
    history = [
        {
            "run_id": snapshot["run_id"],
            "run_at": snapshot["run_at"],
            "execution_engine": snapshot.get("execution_engine"),
            "dq_score": snapshot["dq_score"],
            "completeness_pct_avg": snapshot["completeness_pct_avg"],
            "row_count": snapshot["row_count"],
            "freshness_seconds": snapshot["freshness_seconds"],
        }
        for snapshot in chronological
    ]

    column_history: dict[str, list[dict]] = {}
    for snapshot in chronological:
        for column in snapshot["columns"]:
            column_history.setdefault(column["column_name"], []).append(
                {
                    "run_id": snapshot["run_id"],
                    "run_at": snapshot["run_at"],
                    "null_count": column["null_count"],
                    "null_pct": column["null_pct"],
                    "distinct_count": column["distinct_count"],
                    "min_value": column["min_value"],
                    "max_value": column["max_value"],
                }
            )

    return {
        "table_id": table.id,
        "table_fqn": _table_fqn(session, table.id),
        "run_id": current["run_id"],
        "run_at": current["run_at"],
        "execution_engine": current.get("execution_engine"),
        "spark_app_id": current.get("spark_app_id"),
        "queued_at": current.get("queued_at"),
        "started_at": current.get("started_at"),
        "finished_at": current.get("finished_at"),
        "duration_ms": current.get("duration_ms"),
        "log_tail": current.get("log_tail"),
        "row_count": current["row_count"],
        "completeness_pct_avg": current["completeness_pct_avg"],
        "dq_score": current["dq_score"],
        "duplicates_count": current["duplicates_count"],
        "failed_rules": current["failed_rules"],
        "freshness_seconds": current["freshness_seconds"],
        "columns": current["columns"],
        "current": current,
        "previous": previous,
        "history": history,
        "column_history": column_history,
        "observability": build_dq_observability_payload(
            session=session,
            table=table,
            current_snapshot=current,
            previous_snapshot=previous,
            history=history,
            column_history=column_history,
            current_user=current_user,
        ),
    }
