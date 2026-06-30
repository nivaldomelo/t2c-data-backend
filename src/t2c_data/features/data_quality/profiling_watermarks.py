"""Incremental (full-then-delta) profiling watermark control.

The first successful profiling of a table runs FULL; subsequent runs read only the
rows whose date/time (watermark) column falls in ``(window_start, window_end]`` and
run DELTA. The watermark column is auto-detected from the catalog by name convention,
restricted to date/timestamp typed columns. When none is found the run stays FULL.

The watermark only advances on SUCCESS, so a failed run never skips its window
(no data loss). Bookkeeping uses its own DB session so it is independent from the
profiling worker's transaction (which may roll back on failure).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from t2c_data.core.db import SessionLocal
from t2c_data.models.catalog import ColumnEntity
from t2c_data.models.dq import DQProfilingTableSetting, DQProfilingWatermark

# Name conventions, highest priority first. A column is only eligible if it is also a
# date/time typed column (see _is_temporal_type).
_WATERMARK_NAME_PRIORITY: tuple[str, ...] = (
    "updated_at",
    "atualizado_em",
    "atualizada_em",
    "data_atualizacao",
    "dt_atualizacao",
    "data_modificacao",
    "modified_at",
    "last_modified",
    "ingested_at",
    "dt_ingestao",
    "data_ingestao",
    "dt_carga",
    "data_carga",
    "loaded_at",
    "load_date",
    "dt_load",
    "created_at",
    "criado_em",
    "criada_em",
    "data_criacao",
    "dt_criacao",
    "event_time",
    "event_timestamp",
    "data_referencia",
    "dt_referencia",
    "reference_date",
    "data_evento",
)


@dataclass(frozen=True)
class ProfilingWindow:
    mode: str  # "full" | "delta"
    watermark_column: str | None
    window_start: datetime | None
    window_end: datetime
    note: str | None = None


def _is_temporal_type(data_type: str | None, udt_name: str | None) -> bool:
    haystack = f"{(data_type or '').lower()} {(udt_name or '').lower()}"
    return "timestamp" in haystack or "date" in haystack or "datetime" in haystack


def detect_watermark_column(db: Session, table_id: int) -> str | None:
    """Auto-detect the date/time column that defines the delta window for a table.

    Matches catalog column names against the convention list (exact first, then
    substring), and only accepts date/timestamp typed columns.
    """
    columns = db.scalars(
        select(ColumnEntity).where(ColumnEntity.table_id == table_id).order_by(ColumnEntity.ordinal_position)
    ).all()
    if not columns:
        return None

    temporal = [col for col in columns if _is_temporal_type(col.data_type, col.udt_name)]
    if not temporal:
        return None

    by_lower = {col.name.lower(): col.name for col in temporal}

    # Pass 1: exact name match by priority.
    for candidate in _WATERMARK_NAME_PRIORITY:
        if candidate in by_lower:
            return by_lower[candidate]

    # Pass 2: substring match by priority (handles prefixed/suffixed names).
    for candidate in _WATERMARK_NAME_PRIORITY:
        for col in temporal:
            if candidate in col.name.lower():
                return col.name

    return None


def resolve_effective_watermark_column(db: Session, table_id: int) -> str | None:
    """Manual override (per-table setting) wins over auto-detection."""
    setting = db.scalar(
        select(DQProfilingTableSetting).where(DQProfilingTableSetting.table_id == table_id)
    )
    if setting is not None and (setting.watermark_column or "").strip():
        return setting.watermark_column.strip()
    return detect_watermark_column(db, table_id)


def resolve_profiling_window(
    db: Session,
    *,
    table_id: int,
    now: datetime | None = None,
) -> ProfilingWindow:
    """Decide whether the next profiling of this table is FULL or DELTA.

    Order of precedence:
    1. If there is a previous successful run, continue as DELTA from its window_end.
    2. Else, if a per-table start_date (floor) is configured and a date/time column is
       available, the FIRST run is a bounded DELTA from the floor (avoids full read).
    3. Else, FULL (first run with no floor, or no usable date/time column).
    """
    window_end = now or datetime.now(timezone.utc)

    column = resolve_effective_watermark_column(db, table_id)
    if column is None:
        return ProfilingWindow(
            mode="full",
            watermark_column=None,
            window_start=None,
            window_end=window_end,
            note="Sem coluna de data/hora — profiling full. Defina a coluna nas configurações para usar início por data.",
        )

    last_success = db.scalar(
        select(DQProfilingWatermark)
        .where(
            DQProfilingWatermark.table_id == table_id,
            DQProfilingWatermark.status == "success",
        )
        .order_by(DQProfilingWatermark.window_end.desc())
        .limit(1)
    )
    if last_success is not None and last_success.window_end is not None:
        window_start = last_success.window_end
        if window_start.tzinfo is None:
            window_start = window_start.replace(tzinfo=timezone.utc)
        if window_start >= window_end:
            return ProfilingWindow(
                mode="full",
                watermark_column=column,
                window_start=None,
                window_end=window_end,
                note="Janela anterior à frente do relógio — profiling full por segurança.",
            )
        return ProfilingWindow(
            mode="delta",
            watermark_column=column,
            window_start=window_start,
            window_end=window_end,
            note=f"Delta por '{column}' de {window_start.isoformat()} até {window_end.isoformat()}.",
        )

    # No previous success: honor a configured start date (floor) for the first run.
    setting = db.scalar(
        select(DQProfilingTableSetting).where(DQProfilingTableSetting.table_id == table_id)
    )
    floor = setting.start_date if setting is not None else None
    if floor is not None:
        if floor.tzinfo is None:
            floor = floor.replace(tzinfo=timezone.utc)
        return ProfilingWindow(
            mode="delta",
            watermark_column=column,
            window_start=floor,
            window_end=window_end,
            note=f"Primeira execução a partir da data configurada ({floor.isoformat()}) por '{column}'.",
        )

    return ProfilingWindow(
        mode="full",
        watermark_column=column,
        window_start=None,
        window_end=window_end,
        note="Primeira execução — profiling full.",
    )


def open_watermark_record(
    *,
    table_id: int,
    datasource_id: int | None,
    dq_run_id: int | None,
    job_id: int | None,
    window: ProfilingWindow,
) -> int:
    """Record a profiling execution as 'running' (own session). Returns its id."""
    with SessionLocal() as session:
        record = DQProfilingWatermark(
            table_id=table_id,
            datasource_id=datasource_id,
            dq_run_id=dq_run_id,
            job_id=job_id,
            mode=window.mode,
            watermark_column=window.watermark_column,
            window_start=window.window_start,
            window_end=window.window_end,
            status="running",
            note=window.note,
        )
        session.add(record)
        session.commit()
        return record.id


def finalize_watermark_record(
    *,
    record_id: int | None,
    status: str,
    rows_processed: int | None = None,
    note: str | None = None,
) -> None:
    """Advance a watermark record to its terminal status (own session)."""
    if not record_id:
        return
    with SessionLocal() as session:
        record = session.get(DQProfilingWatermark, record_id)
        if record is None:
            return
        record.status = status
        if rows_processed is not None:
            record.rows_processed = int(rows_processed)
        if note:
            record.note = note
        session.add(record)
        session.commit()


__all__ = [
    "ProfilingWindow",
    "detect_watermark_column",
    "resolve_effective_watermark_column",
    "resolve_profiling_window",
    "open_watermark_record",
    "finalize_watermark_record",
]
