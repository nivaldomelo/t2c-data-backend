"""Per-table profiling settings (start date floor + watermark column override)."""

from __future__ import annotations

from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from t2c_data.features.data_quality.profiling_watermarks import (
    detect_watermark_column,
    resolve_effective_watermark_column,
)
from t2c_data.models.auth import User
from t2c_data.models.catalog import Database, Schema, TableEntity
from t2c_data.models.dq import DQProfilingTableSetting, DQProfilingWatermark
from t2c_data.schemas.dq import DQProfilingTableSettingIn, DQProfilingTableSettingOut


def _table_fqn(db: Session, table: TableEntity) -> str | None:
    schema = db.get(Schema, table.schema_id) if table.schema_id is not None else None
    database = db.get(Database, schema.database_id) if schema is not None else None
    parts = [p for p in [database.name if database else None, schema.name if schema else None, table.name] if p]
    return ".".join(parts) if parts else None


def _has_previous_success(db: Session, table_id: int) -> bool:
    return db.scalar(
        select(DQProfilingWatermark.id)
        .where(
            DQProfilingWatermark.table_id == table_id,
            DQProfilingWatermark.status == "success",
        )
        .limit(1)
    ) is not None


def _build_out(db: Session, table: TableEntity, setting: DQProfilingTableSetting | None) -> DQProfilingTableSettingOut:
    return DQProfilingTableSettingOut(
        table_id=table.id,
        table_fqn=_table_fqn(db, table),
        start_date=setting.start_date if setting else None,
        watermark_column=setting.watermark_column if setting else None,
        detected_watermark_column=detect_watermark_column(db, table.id),
        effective_watermark_column=resolve_effective_watermark_column(db, table.id),
        has_previous_success=_has_previous_success(db, table.id),
        updated_at=setting.updated_at if setting else None,
    )


def get_profiling_table_setting(db: Session, *, table_id: int) -> DQProfilingTableSettingOut:
    table = db.get(TableEntity, table_id)
    if table is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Tabela não encontrada.")
    setting = db.scalar(select(DQProfilingTableSetting).where(DQProfilingTableSetting.table_id == table_id))
    return _build_out(db, table, setting)


def upsert_profiling_table_setting(
    db: Session,
    *,
    payload: DQProfilingTableSettingIn,
    current_user: User | None = None,
) -> DQProfilingTableSettingOut:
    table = db.get(TableEntity, payload.table_id)
    if table is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Tabela não encontrada.")

    column = (payload.watermark_column or "").strip() or None
    if column is not None:
        valid = {c.name for c in table.columns}
        if column not in valid:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"A coluna '{column}' não existe na tabela.",
            )

    setting = db.scalar(select(DQProfilingTableSetting).where(DQProfilingTableSetting.table_id == payload.table_id))
    if setting is None:
        setting = DQProfilingTableSetting(table_id=payload.table_id)
        db.add(setting)
    setting.start_date = payload.start_date
    setting.watermark_column = column
    setting.updated_by_user_id = getattr(current_user, "id", None)
    db.commit()
    db.refresh(setting)
    return _build_out(db, table, setting)


__all__ = ["get_profiling_table_setting", "upsert_profiling_table_setting"]
