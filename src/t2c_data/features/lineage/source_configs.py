from __future__ import annotations

from fastapi import HTTPException, status
from sqlalchemy.exc import OperationalError
from sqlalchemy import Select, func, select
from sqlalchemy.orm import Session

from t2c_data.core.rbac import is_admin_role, user_role_names
from t2c_data.models.lineage import (
    LineageAsset,
    LineageColumnEdge,
    LineageEventRaw,
    LineageJob,
    LineageRelation,
    LineageSourceConfig,
)
from t2c_data.models.auth import User
from t2c_data.schemas.lineage import (
    LineageSourceConfigCreate,
    LineageSourceConfigOut,
    LineageSourceConfigUpdate,
    LineageSourceStatusOut,
)


def _source_secret(source: LineageSourceConfig) -> str | None:
    return source.auth_secret or None


def _set_source_secret(source: LineageSourceConfig, value: str | None) -> None:
    source.auth_secret = value


def _mask_sensitive_text(_value: str | None) -> str:
    return "Oculto para seu perfil"


def _display_source_name(source: LineageSourceConfig) -> str:
    return "Linhagem automática interna"


def _display_source_kind(source: LineageSourceConfig) -> str:
    return "internal_openlineage"


def _safe_count(db: Session, stmt) -> int:
    try:
        value = db.scalar(stmt)
    except OperationalError:
        return 0
    return int(value or 0)


def serialize_source_config(source: LineageSourceConfig, *, current_user: User | None = None) -> LineageSourceConfigOut:
    is_admin = bool(current_user and is_admin_role(user_role_names(current_user)))
    auth_secret = _source_secret(source)
    return LineageSourceConfigOut(
        id=source.id,
        name=_display_source_name(source),
        source_type=_display_source_kind(source),
        base_url=source.base_url if is_admin else _mask_sensitive_text(source.base_url),
        default_namespace=source.default_namespace if is_admin else (_mask_sensitive_text(source.default_namespace) if source.default_namespace else None),
        auth_type=source.auth_type,
        auth_username=source.auth_username if is_admin else (_mask_sensitive_text(source.auth_username) if source.auth_username else None),
        auth_secret=auth_secret if is_admin else None,
        configured_auth=bool(_source_secret(source)),
        enabled=source.enabled,
        last_sync_at=source.last_sync_at,
        last_sync_status=source.last_sync_status,
        last_sync_message=source.last_sync_message,
        created_at=source.created_at,
        updated_at=source.updated_at,
    )


def serialize_source_status(db: Session, source: LineageSourceConfig) -> LineageSourceStatusOut:
    events_processed = _safe_count(
        db,
        select(func.count(LineageEventRaw.id)).where(
            LineageEventRaw.lineage_source_id == source.id,
            LineageEventRaw.is_processed.is_(True),
        ),
    )
    jobs_synced = _safe_count(db, select(func.count(LineageJob.id)).where(LineageJob.lineage_source_id == source.id))
    datasets_synced = _safe_count(db, select(func.count(LineageAsset.id)).where(LineageAsset.lineage_source_id == source.id))
    relations_synced = _safe_count(db, select(func.count(LineageRelation.id)).where(LineageRelation.lineage_source_id == source.id))
    column_edges_synced = _safe_count(db, select(func.count(LineageColumnEdge.id)).where(LineageColumnEdge.lineage_source_id == source.id))
    return LineageSourceStatusOut(
        id=source.id,
        name=_display_source_name(source),
        source_type=_display_source_kind(source),
        enabled=source.enabled,
        last_sync_at=source.last_sync_at,
        last_sync_status=source.last_sync_status,
        last_sync_message=source.last_sync_message,
        events_processed=int(events_processed),
        jobs_synced=int(jobs_synced),
        datasets_synced=int(datasets_synced),
        relations_synced=int(relations_synced),
        column_edges_synced=int(column_edges_synced),
        created_at=source.created_at,
        updated_at=source.updated_at,
    )


def list_source_statuses(db: Session) -> list[LineageSourceStatusOut]:
    stmt: Select[tuple[LineageSourceConfig]] = select(LineageSourceConfig).order_by(LineageSourceConfig.updated_at.desc())
    return [serialize_source_status(db, item) for item in db.scalars(stmt).all()]


def list_source_configs(db: Session) -> list[LineageSourceConfig]:
    stmt: Select[tuple[LineageSourceConfig]] = select(LineageSourceConfig).order_by(LineageSourceConfig.updated_at.desc())
    return db.scalars(stmt).all()


def get_source_config(db: Session, source_id: int) -> LineageSourceConfig:
    source = db.get(LineageSourceConfig, source_id)
    if not source:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Lineage source not found")
    return source


def create_source_config(db: Session, payload: LineageSourceConfigCreate) -> LineageSourceConfig:
    source = LineageSourceConfig(
        name=payload.name.strip(),
        source_type=payload.source_type,
        base_url=payload.base_url.strip().rstrip("/"),
        default_namespace=(payload.default_namespace or "").strip() or None,
        auth_type=payload.auth_type,
        auth_username=(payload.auth_username or "").strip() or None,
        enabled=payload.enabled,
    )
    _set_source_secret(source, payload.auth_secret)
    db.add(source)
    db.flush()
    return source


def update_source_config(db: Session, source: LineageSourceConfig, payload: LineageSourceConfigUpdate) -> LineageSourceConfig:
    updates = payload.model_dump(exclude_unset=True)
    for field in ("name", "base_url", "default_namespace", "auth_type", "auth_username", "enabled"):
        if field in updates:
            value = updates[field]
            if isinstance(value, str):
                value = value.strip() or None
            if field == "base_url" and value:
                value = str(value).rstrip("/")
            setattr(source, field, value)
    if "auth_secret" in updates:
        _set_source_secret(source, updates.get("auth_secret"))
    db.flush()
    return source


__all__ = [
    "create_source_config",
    "get_source_config",
    "list_source_configs",
    "list_source_statuses",
    "serialize_source_config",
    "serialize_source_status",
    "update_source_config",
]
