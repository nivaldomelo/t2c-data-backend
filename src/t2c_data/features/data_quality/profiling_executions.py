from __future__ import annotations

from datetime import UTC, date, datetime, time

from sqlalchemy import String, and_, exists, func, or_, select
from sqlalchemy.orm import Session

from t2c_data.models.catalog import DataSource, Schema, TableEntity
from t2c_data.models.dq import DQProfilingWatermark, DQRun, DQTableMetric
from t2c_data.schemas.dq import (
    DQProfilingExecutionDetailOut,
    DQProfilingExecutionItemOut,
    DQProfilingExecutionPageOut,
    DQProfilingExecutionSummaryOut,
)

TERMINAL_SUCCESS_STATUSES = {"success", "no_data"}
TERMINAL_FAILED_STATUSES = {"failed", "timeout"}


def list_profiling_executions(
    db: Session,
    *,
    datasource_id: int | None = None,
    schema_name: str | None = None,
    table_id: int | None = None,
    status: str | None = None,
    scope: str | None = None,
    search: str | None = None,
    started_from: date | None = None,
    started_to: date | None = None,
    limit: int = 10,
    offset: int = 0,
) -> DQProfilingExecutionPageOut:
    effective_started_at = func.coalesce(DQRun.started_at, DQRun.queued_at, DQRun.created_at)
    query = (
        select(DQRun.id)
        .select_from(DQRun)
        .outerjoin(DataSource, DataSource.id == DQRun.datasource_id)
        .outerjoin(TableEntity, TableEntity.id == DQRun.table_id)
        .where(DQRun.parent_run_id.is_(None))
    )
    if datasource_id is not None:
        query = query.where(DQRun.datasource_id == datasource_id)
    if schema_name:
        query = query.where(DQRun.schema_name == schema_name)
    if status:
        query = query.where(DQRun.status == status)
    if scope:
        query = query.where(DQRun.scope == scope)
    if started_from is not None:
        query = query.where(effective_started_at >= _start_of_day(started_from))
    if started_to is not None:
        query = query.where(effective_started_at <= _end_of_day(started_to))
    if table_id is not None:
        child_alias = DQRun.__table__.alias("child_runs")
        query = query.where(
            or_(
                DQRun.table_id == table_id,
                and_(
                    DQRun.scope == "schema",
                    exists(
                        select(child_alias.c.id).where(
                            child_alias.c.parent_run_id == DQRun.id,
                            child_alias.c.table_id == table_id,
                        )
                    ),
                ),
            )
        )
    search_term = (search or "").strip().lower()
    if search_term:
        like_term = f"%{search_term}%"
        query = query.where(
            or_(
                func.lower(func.cast(DQRun.id, String)).like(like_term),
                func.lower(func.coalesce(DataSource.name, "")).like(like_term),
                func.lower(func.coalesce(DQRun.schema_name, "")).like(like_term),
                func.lower(func.coalesce(TableEntity.name, "")).like(like_term),
                func.lower(func.coalesce(DQRun.status, "")).like(like_term),
                func.lower(func.coalesce(DQRun.execution_engine, "")).like(like_term),
            )
        )

    total = db.scalar(select(func.count()).select_from(query.subquery())) or 0
    ordered_ids = db.scalars(
        query.order_by(
            effective_started_at.desc(),
            DQRun.id.desc(),
        ).offset(offset).limit(limit)
    ).all()
    if not ordered_ids:
        return DQProfilingExecutionPageOut(items=[], total=total, limit=limit, offset=offset)

    runs_by_id = {
        run.id: run
        for run in db.scalars(select(DQRun).where(DQRun.id.in_(ordered_ids))).all()
    }
    summaries = [_build_execution_summary(db, runs_by_id[run_id]) for run_id in ordered_ids if run_id in runs_by_id]
    return DQProfilingExecutionPageOut(items=summaries, total=total, limit=limit, offset=offset)


def get_profiling_execution_detail(db: Session, run_id: int) -> DQProfilingExecutionDetailOut | None:
    run = db.get(DQRun, run_id)
    if not run:
        return None
    summary = _build_execution_summary(db, run)
    child_runs = _child_runs(db, run.id)
    items = [_build_execution_item(db, item) for item in child_runs] if child_runs else [_build_execution_item(db, run)]
    return DQProfilingExecutionDetailOut(
        **summary.model_dump(),
        items=items,
    )


def _child_runs(db: Session, run_id: int) -> list[DQRun]:
    return db.scalars(
        select(DQRun)
        .where(DQRun.parent_run_id == run_id)
        .order_by(DQRun.id.asc())
    ).all()


def _build_execution_summary(db: Session, run: DQRun) -> DQProfilingExecutionSummaryOut:
    child_runs = _child_runs(db, run.id)
    totals = _totals_for_run(run, child_runs)
    datasource = db.get(DataSource, run.datasource_id) if run.datasource_id is not None else None
    metrics = _metric_payload(db, run)
    table_fqn = _table_fqn(db, run)
    window = _profiling_window(db, run)
    return DQProfilingExecutionSummaryOut(
        id=run.id,
        parent_run_id=run.parent_run_id,
        scope=run.scope,
        datasource_id=run.datasource_id,
        datasource_name=datasource.name if datasource else None,
        schema_name=run.schema_name,
        table_id=run.table_id,
        table_fqn=table_fqn,
        status=run.status,
        execution_engine=run.execution_engine,
        queued_at=run.queued_at,
        started_at=run.started_at,
        finished_at=run.finished_at,
        duration_ms=run.duration_ms,
        error_message=run.error_message,
        spark_app_id=run.spark_app_id,
        log_tail=run.log_tail,
        trigger_source=_trigger_source(run),
        total_items=totals["total_items"],
        queued_items=totals["queued_items"],
        running_items=totals["running_items"],
        success_items=totals["success_items"],
        failed_items=totals["failed_items"],
        row_count=metrics["row_count"],
        completeness_pct_avg=metrics["completeness_pct_avg"],
        dq_score=metrics["dq_score"],
        duplicates_count=metrics["duplicates_count"],
        failed_rules_count=metrics["failed_rules_count"],
        observation=metrics["observation"],
        profile_summary=metrics["profile_summary"],
        profiling_intelligence=metrics["profiling_intelligence"],
        profiling_mode=window["profiling_mode"],
        watermark_column=window["watermark_column"],
        window_start=window["window_start"],
        window_end=window["window_end"],
    )


def _build_execution_item(db: Session, run: DQRun) -> DQProfilingExecutionItemOut:
    datasource = db.get(DataSource, run.datasource_id) if run.datasource_id is not None else None
    metrics = _metric_payload(db, run)
    window = _profiling_window(db, run)
    return DQProfilingExecutionItemOut(
        id=run.id,
        parent_run_id=run.parent_run_id,
        scope=run.scope,
        datasource_id=run.datasource_id,
        datasource_name=datasource.name if datasource else None,
        schema_name=run.schema_name,
        table_id=run.table_id,
        table_fqn=_table_fqn(db, run),
        status=run.status,
        execution_engine=run.execution_engine,
        queued_at=run.queued_at,
        started_at=run.started_at,
        finished_at=run.finished_at,
        duration_ms=run.duration_ms,
        error_message=run.error_message,
        spark_app_id=run.spark_app_id,
        log_tail=run.log_tail,
        trigger_source=_trigger_source(run),
        row_count=metrics["row_count"],
        completeness_pct_avg=metrics["completeness_pct_avg"],
        dq_score=metrics["dq_score"],
        duplicates_count=metrics["duplicates_count"],
        failed_rules_count=metrics["failed_rules_count"],
        observation=metrics["observation"],
        profile_summary=metrics["profile_summary"],
        profiling_intelligence=metrics["profiling_intelligence"],
        profiling_mode=window["profiling_mode"],
        watermark_column=window["watermark_column"],
        window_start=window["window_start"],
        window_end=window["window_end"],
    )


def _totals_for_run(run: DQRun, child_runs: list[DQRun]) -> dict[str, int]:
    if not child_runs:
        return {
            "total_items": 1,
            "queued_items": 1 if run.status == "queued" else 0,
            "running_items": 1 if run.status == "running" else 0,
            "success_items": 1 if run.status in TERMINAL_SUCCESS_STATUSES else 0,
            "failed_items": 1 if run.status in TERMINAL_FAILED_STATUSES else 0,
        }
    return {
        "total_items": len(child_runs),
        "queued_items": sum(1 for item in child_runs if item.status == "queued"),
        "running_items": sum(1 for item in child_runs if item.status == "running"),
        "success_items": sum(1 for item in child_runs if item.status in TERMINAL_SUCCESS_STATUSES),
        "failed_items": sum(1 for item in child_runs if item.status in TERMINAL_FAILED_STATUSES),
    }


def _metric_payload(db: Session, run: DQRun) -> dict[str, int | float | str | None]:
    metric = db.scalar(
        select(DQTableMetric)
        .where(DQTableMetric.run_id == run.id)
        .order_by(DQTableMetric.id.desc())
        .limit(1)
    )
    profile_payload = run.profile_payload_json if isinstance(run.profile_payload_json, dict) else {}
    metrics_json = profile_payload.get("metrics_json") if isinstance(profile_payload.get("metrics_json"), dict) else {}
    return {
        "row_count": metric.row_count if metric is not None else None,
        "completeness_pct_avg": metric.completeness_pct_avg if metric is not None else None,
        "dq_score": metric.dq_score if metric is not None else None,
        "duplicates_count": metric.duplicates_count if metric is not None else None,
        "failed_rules_count": metric.failed_rules if metric is not None else None,
        "observation": (
            None
            if profile_payload.get("observation") is None
            else str(profile_payload.get("observation"))
        ),
        "profile_summary": metrics_json.get("profile_summary"),
        "profiling_intelligence": metrics_json.get("profiling_intelligence"),
    }


def _profiling_window(db: Session, run: DQRun) -> dict[str, object | None]:
    """Incremental profiling window (full/delta) recorded for this run, if any."""
    watermark = db.scalar(
        select(DQProfilingWatermark)
        .where(DQProfilingWatermark.dq_run_id == run.id)
        .order_by(DQProfilingWatermark.id.desc())
        .limit(1)
    )
    if watermark is None:
        return {"profiling_mode": None, "watermark_column": None, "window_start": None, "window_end": None}
    return {
        "profiling_mode": watermark.mode,
        "watermark_column": watermark.watermark_column,
        "window_start": watermark.window_start,
        "window_end": watermark.window_end,
    }


def _trigger_source(run: DQRun) -> str | None:
    payload = run.profile_payload_json if isinstance(run.profile_payload_json, dict) else {}
    source = payload.get("trigger_source")
    if isinstance(source, str) and source.strip():
        return source.strip()
    if run.profiling_schedule_id is not None:
        return "scheduled"
    if run.parent_run_id is None:
        return "manual"
    return None


def _table_fqn(db: Session, run: DQRun) -> str | None:
    if run.table_id is None:
        return None
    table = db.get(TableEntity, run.table_id)
    if not table:
        return None
    schema = db.get(Schema, table.schema_id)
    if not schema:
        return table.name
    return f"{schema.name}.{table.name}"
def _start_of_day(value: date) -> datetime:
    return datetime.combine(value, time.min, tzinfo=UTC)


def _end_of_day(value: date) -> datetime:
    return datetime.combine(value, time.max, tzinfo=UTC)


__all__ = [
    "get_profiling_execution_detail",
    "list_profiling_executions",
]
