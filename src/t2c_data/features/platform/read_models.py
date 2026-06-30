from __future__ import annotations

from datetime import datetime, timezone
import logging
from threading import Lock
from time import perf_counter
from typing import Iterable

from sqlalchemy import and_, delete, func, or_, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import OperationalError, ProgrammingError
from sqlalchemy.orm import Session

from t2c_data.core.config import settings
from t2c_data.core.rbac import user_role_names
from t2c_data.features.dashboard.profile_loader import filter_table_profiles_for_user, load_table_profiles
from t2c_data.features.dashboard.support import TableProfile, normalize_dt
from t2c_data.features.shared_cache import get_cached_value, session_cache_key, set_cached_value
from t2c_data.features.search.global_search import SearchRecord, load_search_records_live
from t2c_data.models.catalog import ColumnEntity, DataOwner, DataSource, Database, Schema, TableEntity
from t2c_data.models.dq import DQTableMetric
from t2c_data.models.glossary import GlossaryAssignment, GlossaryTerm
from t2c_data.models.incident import Incident
from t2c_data.models.platform import DashboardAssetReadModel, SearchReadModel
from t2c_data.models.search import ColumnSearchAlias, SearchResultClick, TableSearchAlias
from t2c_data.models.tag import Tag, TagAssignment

logger = logging.getLogger(__name__)

_SEARCH_REFRESH_LOCK_KEY = 684931731
_SEARCH_REFRESH_PROCESS_LOCK = Lock()
_DASHBOARD_PROFILES_CACHE_TTL_SECONDS = 60


def _search_row_to_payload(record: SearchRecord, *, now: datetime) -> dict[str, object]:
    metadata = dict(record.metadata or {})
    parent_table_id = metadata.get("table_id")
    return {
        "entity_type": record.entity_type,
        "entity_id": record.entity_id,
        "parent_table_id": parent_table_id if isinstance(parent_table_id, int) else None,
        "category": record.entity_type,
        "title": record.title,
        "subtitle": record.subtitle,
        "description": record.description,
        "context_path": record.context_path,
        "target_url": record.target_url,
        "searchable_name": record.searchable_name,
        "searchable_aliases": record.searchable_aliases,
        "searchable_synonyms": record.searchable_synonyms,
        "searchable_descriptions": record.searchable_descriptions,
        "searchable_context": record.searchable_context,
        "source_name": record.source_name,
        "database_name": record.database_name,
        "schema_name": record.schema_name,
        "owner_name": record.owner_name,
        "domain_name": record.domain_name,
        "classification": record.classification,
        "certified": record.certified,
        "open_incidents": record.open_incidents,
        "popularity_count": record.popularity_count,
        "metadata_json": metadata,
        "created_at": now,
        "updated_at": now,
    }


def _iso(dt: datetime | None) -> str | None:
    value = normalize_dt(dt)
    return value.isoformat() if value else None


def _from_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _latest_refresh_timestamp(session: Session, model) -> datetime | None:
    return normalize_dt(session.scalar(select(func.max(model.updated_at))))


def _read_model_status_payload(*, name: str, total: int, last_refresh: datetime | None) -> dict[str, object]:
    has_materialized_rows = total > 0
    mode = "materialized" if has_materialized_rows else ("not_configured" if not settings.platform_read_model_auto_refresh_enabled else "live")
    materialized_records_count = total if has_materialized_rows else None
    last_refresh_at = _iso(last_refresh) if has_materialized_rows else None
    source_kind = "materialized_read_model" if has_materialized_rows else "live_query"
    if has_materialized_rows:
        status_message = f"Origem materializada · {total:,} registros".replace(",", ".")
    elif mode == "not_configured":
        status_message = f"Nenhum read model materializado configurado. {name} está operando em modo live neste ambiente."
    else:
        status_message = f"Origem live · sem índice materializado"
    return {
        "name": name,
        "mode": mode,
        "materialized_records_count": materialized_records_count,
        "last_refresh_at": last_refresh_at,
        "last_refreshed_at": last_refresh_at,
        "status_message": status_message,
        "source_kind": source_kind,
        "total": materialized_records_count,
        "source": "materialized" if has_materialized_rows else "live",
    }


def _search_row_from_record(record: SearchRecord, *, now: datetime) -> SearchReadModel:
    return SearchReadModel(**_search_row_to_payload(record, now=now))


def _search_refresh_lock_is_available(session: Session) -> bool:
    bind = session.get_bind()
    return bool(bind is not None and getattr(bind.dialect, "name", "") == "postgresql")


def _acquire_search_refresh_lock(session: Session) -> bool:
    if _search_refresh_lock_is_available(session):
        try:
            return bool(
                session.execute(
                    text("SELECT pg_try_advisory_lock(:lock_key)"),
                    {"lock_key": _SEARCH_REFRESH_LOCK_KEY},
                ).scalar_one()
            )
        except Exception:  # noqa: BLE001
            logger.exception("platform search read model advisory lock acquisition failed")
            return False
    return _SEARCH_REFRESH_PROCESS_LOCK.acquire(blocking=False)


def _release_search_refresh_lock(session: Session) -> None:
    if _search_refresh_lock_is_available(session):
        try:
            session.execute(text("SELECT pg_advisory_unlock(:lock_key)"), {"lock_key": _SEARCH_REFRESH_LOCK_KEY})
        except Exception:  # noqa: BLE001
            logger.exception("platform search read model advisory lock release failed")
        return
    if _SEARCH_REFRESH_PROCESS_LOCK.locked():
        try:
            _SEARCH_REFRESH_PROCESS_LOCK.release()
        except RuntimeError:
            pass


def _dedupe_search_records(records: Iterable[SearchRecord]) -> tuple[list[SearchRecord], int]:
    deduped: dict[tuple[str, int], SearchRecord] = {}
    duplicate_count = 0
    for record in records:
        key = (str(record.entity_type), int(record.entity_id))
        if key in deduped:
            duplicate_count += 1
        deduped[key] = record
    return list(deduped.values()), duplicate_count


def _search_read_model_insert(session: Session):
    bind = session.get_bind()
    dialect_name = getattr(getattr(bind, "dialect", None), "name", "") if bind is not None else ""
    if dialect_name == "postgresql":
        return pg_insert(SearchReadModel)
    if dialect_name == "sqlite":
        return sqlite_insert(SearchReadModel)
    return pg_insert(SearchReadModel)


def _search_incremental_filter(changed_table_ids: list[int]):
    return or_(
        SearchReadModel.parent_table_id.in_(changed_table_ids),
        and_(
            SearchReadModel.entity_type.in_(["table", "classification"]),
            SearchReadModel.entity_id.in_(changed_table_ids),
        ),
    )


def _chunked(values: list[dict[str, object]], size: int = 20) -> Iterable[list[dict[str, object]]]:
    for start in range(0, len(values), size):
        yield values[start : start + size]


def _bulk_upsert_search_read_model(session: Session, rows: list[dict[str, object]]) -> None:
    if not rows:
        return
    insert_stmt = _search_read_model_insert(session)
    update_columns = {
        "parent_table_id": insert_stmt.excluded.parent_table_id,
        "category": insert_stmt.excluded.category,
        "title": insert_stmt.excluded.title,
        "subtitle": insert_stmt.excluded.subtitle,
        "description": insert_stmt.excluded.description,
        "context_path": insert_stmt.excluded.context_path,
        "target_url": insert_stmt.excluded.target_url,
        "searchable_name": insert_stmt.excluded.searchable_name,
        "searchable_aliases": insert_stmt.excluded.searchable_aliases,
        "searchable_synonyms": insert_stmt.excluded.searchable_synonyms,
        "searchable_descriptions": insert_stmt.excluded.searchable_descriptions,
        "searchable_context": insert_stmt.excluded.searchable_context,
        "source_name": insert_stmt.excluded.source_name,
        "database_name": insert_stmt.excluded.database_name,
        "schema_name": insert_stmt.excluded.schema_name,
        "owner_name": insert_stmt.excluded.owner_name,
        "domain_name": insert_stmt.excluded.domain_name,
        "classification": insert_stmt.excluded.classification,
        "certified": insert_stmt.excluded.certified,
        "open_incidents": insert_stmt.excluded.open_incidents,
        "popularity_count": insert_stmt.excluded.popularity_count,
        "metadata_json": insert_stmt.excluded.metadata_json,
        "updated_at": insert_stmt.excluded.updated_at,
    }
    for chunk in _chunked(rows):
        stmt = insert_stmt.values(chunk).on_conflict_do_update(
            index_elements=["entity_type", "entity_id"],
            set_=update_columns,
        )
        session.execute(stmt)


def _dashboard_row_from_profile(table: TableProfile, *, now: datetime) -> DashboardAssetReadModel:
    return DashboardAssetReadModel(
        table_id=table.table_id,
        datasource_id=table.datasource_id,
        database_id=table.database_id,
        schema_id=table.schema_id,
        table_name=table.table_name,
        table_type=table.table_type,
        schema_name=table.schema_name,
        database_name=table.database_name,
        datasource_name=table.datasource_name,
        engine=table.engine,
        owner_defined=table.owner_defined,
        description_complete=table.description_complete,
        dictionary_complete=table.dictionary_complete,
        classification_defined=table.classification_defined,
        tags_count=table.tags_count,
        terms_count=table.terms_count,
        search_clicks_30d=table.search_clicks_30d,
        active_dq_rules_count=table.active_dq_rules_count,
        recent_dq_failure_runs_30d=table.recent_dq_failure_runs_30d,
        certification_status=table.certification_status,
        certification_criticality=table.certification_criticality,
        certification_badges=table.certification_badges,
        certification_decided_at=_iso(table.certification_decided_at),
        certification_review_at=_iso(table.certification_review_at),
        certification_expires_at=_iso(table.certification_expires_at),
        review_recent=table.review_recent,
        dq_score=table.dq_score,
        completeness_pct_avg=table.completeness_pct_avg,
        freshness_seconds=table.freshness_seconds,
        open_incidents=table.open_incidents,
        critical_open_incidents=table.critical_open_incidents,
        owner_name=table.owner_name,
        data_owner_id=table.data_owner_id,
        domain_name=table.domain_name,
        sensitivity_level=table.sensitivity_level,
        has_personal_data=table.has_personal_data,
        has_sensitive_personal_data=table.has_sensitive_personal_data,
        owner_reviewed_at=_iso(table.owner_reviewed_at),
        privacy_reviewed_at=_iso(table.privacy_reviewed_at),
        last_review_at=_iso(table.last_review_at),
        last_sync_at=_iso(table.last_sync_at),
        last_updated_at=_iso(table.last_updated_at),
        created_at=now,
        updated_at=now,
    )


def _changed_table_ids_from_stmt(session: Session, stmt) -> set[int]:
    return {int(value) for value in session.scalars(stmt).all() if value is not None}


def _table_related_changes_since(session: Session, since: datetime) -> set[int]:
    changed: set[int] = set()
    changed |= _changed_table_ids_from_stmt(session, select(TableEntity.id).where(TableEntity.updated_at >= since))
    changed |= _changed_table_ids_from_stmt(session, select(ColumnEntity.table_id).where(ColumnEntity.updated_at >= since))
    changed |= _changed_table_ids_from_stmt(session, select(DQTableMetric.table_id).where(DQTableMetric.updated_at >= since))
    incident_fqns = [
        value
        for value in session.scalars(
            select(Incident.table_fqn).where(
                Incident.updated_at >= since,
                Incident.entity_type == "table",
                Incident.table_fqn.is_not(None),
            )
        ).all()
        if value
    ]
    if incident_fqns:
        changed |= _changed_table_ids_from_stmt(
            session,
            select(TableEntity.id)
            .join(Schema, TableEntity.schema_id == Schema.id)
            .where((Schema.name + "." + TableEntity.name).in_(incident_fqns)),
        )
    changed |= _changed_table_ids_from_stmt(session, select(TableSearchAlias.table_id).where(TableSearchAlias.updated_at >= since))
    changed |= _changed_table_ids_from_stmt(
        session,
        select(ColumnEntity.table_id)
        .join(ColumnSearchAlias, ColumnSearchAlias.column_id == ColumnEntity.id)
        .where(ColumnSearchAlias.updated_at >= since),
    )
    changed |= _changed_table_ids_from_stmt(
        session,
        select(TagAssignment.entity_id)
        .where(TagAssignment.entity_type == "table", TagAssignment.updated_at >= since),
    )
    changed |= _changed_table_ids_from_stmt(
        session,
        select(GlossaryAssignment.entity_id)
        .where(GlossaryAssignment.entity_type == "table", GlossaryAssignment.updated_at >= since),
    )
    changed |= _changed_table_ids_from_stmt(
        session,
        select(TableEntity.id)
        .join(DataOwner, TableEntity.data_owner_id == DataOwner.id)
        .where(DataOwner.updated_at >= since),
    )
    return changed


def _search_requires_full_refresh(session: Session, since: datetime) -> bool:
    global_change_checks = [
        session.scalar(select(func.count(GlossaryTerm.id)).where(GlossaryTerm.updated_at >= since)),
        session.scalar(select(func.count(Tag.id)).where(Tag.updated_at >= since)),
        session.scalar(select(func.count(DataOwner.id)).where(DataOwner.updated_at >= since)),
        session.scalar(select(func.count(DataSource.id)).where(DataSource.updated_at >= since)),
        session.scalar(select(func.count(Database.id)).where(Database.updated_at >= since)),
        session.scalar(select(func.count(Schema.id)).where(Schema.updated_at >= since)),
        session.scalar(select(func.count(SearchResultClick.id)).where(SearchResultClick.updated_at >= since)),
    ]
    return any(int(value or 0) > 0 for value in global_change_checks)


def refresh_search_read_model(session: Session, *, mode: str = "full") -> dict[str, object]:
    now = datetime.now(timezone.utc)
    refresh_mode = mode
    changed_table_ids: list[int] | None = None
    start = perf_counter()

    if mode in {"auto", "incremental"}:
        since = _latest_refresh_timestamp(session, SearchReadModel)
        if since is None:
            refresh_mode = "full"
        elif _search_requires_full_refresh(session, since):
            refresh_mode = "full"
        else:
            changed_table_ids = sorted(_table_related_changes_since(session, since))
            refresh_mode = "incremental"

    if not _acquire_search_refresh_lock(session):
        logger.info("platform search read model refresh skipped mode=%s reason=lock_unavailable", refresh_mode)
        return {
            "entries": 0,
            "inserted": 0,
            "updated": 0,
            "removed": 0,
            "duplicates_detected": 0,
            "refreshed_at": now.isoformat(),
            "mode": refresh_mode,
            "updated_tables": len(changed_table_ids or []),
            "skipped": "lock_unavailable",
        }

    try:
        if refresh_mode == "incremental":
            if not changed_table_ids:
                logger.info("platform search read model refresh skipped mode=incremental reason=no_changed_tables")
                return {
                    "entries": 0,
                    "inserted": 0,
                    "updated": 0,
                    "removed": 0,
                    "duplicates_detected": 0,
                    "refreshed_at": now.isoformat(),
                    "mode": "incremental",
                    "updated_tables": 0,
                }
            records = load_search_records_live(session, table_ids=changed_table_ids, include_global_entities=False)
            existing_count = int(session.scalar(select(func.count(SearchReadModel.id)).where(_search_incremental_filter(changed_table_ids))) or 0)
            existing_keys = {
                (str(entity_type), int(entity_id))
                for entity_type, entity_id in session.execute(
                    select(SearchReadModel.entity_type, SearchReadModel.entity_id).where(
                        _search_incremental_filter(changed_table_ids)
                    )
                ).all()
                if entity_type is not None and entity_id is not None
            }
            session.execute(delete(SearchReadModel).where(_search_incremental_filter(changed_table_ids)))
        else:
            records = load_search_records_live(session)
            existing_count = int(session.scalar(select(func.count(SearchReadModel.id))) or 0)
            existing_keys = {
                (str(entity_type), int(entity_id))
                for entity_type, entity_id in session.execute(select(SearchReadModel.entity_type, SearchReadModel.entity_id)).all()
                if entity_type is not None and entity_id is not None
            }
            session.execute(delete(SearchReadModel))

        deduped_records, duplicate_count = _dedupe_search_records(records)
        rows = [_search_row_to_payload(record, now=now) for record in deduped_records]
        incoming_keys = {(str(row["entity_type"]), int(row["entity_id"])) for row in rows}
        updated_count = len(existing_keys & incoming_keys)
        inserted_count = len(rows) - updated_count
        _bulk_upsert_search_read_model(session, rows)
        session.flush()
        elapsed_ms = round((perf_counter() - start) * 1000, 2)
        logger.info(
            "platform search read model refresh completed mode=%s entries=%s inserted=%s updated=%s removed=%s duplicates=%s elapsed_ms=%s",
            refresh_mode,
            len(rows),
            inserted_count,
            updated_count,
            existing_count,
            duplicate_count,
            elapsed_ms,
        )
        return {
            "entries": len(rows),
            "inserted": inserted_count,
            "updated": updated_count,
            "removed": existing_count,
            "duplicates_detected": duplicate_count,
            "refreshed_at": now.isoformat(),
            "mode": refresh_mode,
            "updated_tables": len(changed_table_ids or rows),
        }
    finally:
        _release_search_refresh_lock(session)


def search_read_model_status(session: Session) -> dict[str, object]:
    total = int(session.scalar(select(func.count(SearchReadModel.id))) or 0)
    last_refresh = session.scalar(select(func.max(SearchReadModel.updated_at)))
    return _read_model_status_payload(name="Search", total=total, last_refresh=last_refresh)


def load_search_records_from_read_model(session: Session) -> list[SearchRecord]:
    rows = session.scalars(select(SearchReadModel)).all()
    return [
        SearchRecord(
            entity_type=row.entity_type,
            entity_id=row.entity_id,
            title=row.title,
            subtitle=row.subtitle,
            description=row.description,
            context_path=row.context_path,
            target_url=row.target_url,
            searchable_name=list(row.searchable_name or []),
            searchable_aliases=list(row.searchable_aliases or []),
            searchable_synonyms=list(row.searchable_synonyms or []),
            searchable_descriptions=list(row.searchable_descriptions or []),
            searchable_context=list(row.searchable_context or []),
            source_name=row.source_name,
            database_name=row.database_name,
            schema_name=row.schema_name,
            owner_name=row.owner_name,
            domain_name=row.domain_name,
            classification=row.classification,
            certified=bool(row.certified),
            open_incidents=int(row.open_incidents or 0),
            popularity_count=int(row.popularity_count or 0),
            metadata=dict(row.metadata_json or {}),
        )
        for row in rows
    ]


def refresh_dashboard_asset_read_model(session: Session, *, mode: str = "full") -> dict[str, object]:
    now = datetime.now(timezone.utc)
    refresh_mode = mode
    changed_table_ids: list[int] | None = None
    start = perf_counter()

    if mode in {"auto", "incremental"}:
        since = _latest_refresh_timestamp(session, DashboardAssetReadModel)
        if since is None:
            refresh_mode = "full"
        else:
            changed_table_ids = sorted(_table_related_changes_since(session, since))
            refresh_mode = "incremental"

    if refresh_mode == "incremental":
        if not changed_table_ids:
            return {
                "entries": 0,
                "refreshed_at": now.isoformat(),
                "mode": "incremental",
                "updated_tables": 0,
                "elapsed_ms": round((perf_counter() - start) * 1000, 2),
            }
        tables = load_table_profiles(session, now, table_ids=changed_table_ids)
        session.execute(delete(DashboardAssetReadModel).where(DashboardAssetReadModel.table_id.in_(changed_table_ids)))
        entries = [_dashboard_row_from_profile(table, now=now) for table in tables]
        session.add_all(entries)
        session.flush()
        elapsed_ms = round((perf_counter() - start) * 1000, 2)
        logger.info(
            "platform dashboard read model refresh completed mode=%s entries=%s updated_tables=%s elapsed_ms=%s",
            "incremental",
            len(entries),
            len(changed_table_ids),
            elapsed_ms,
        )
        return {
            "entries": len(entries),
            "refreshed_at": now.isoformat(),
            "mode": "incremental",
            "updated_tables": len(changed_table_ids),
            "elapsed_ms": elapsed_ms,
        }

    tables = load_table_profiles(session, now)
    session.execute(delete(DashboardAssetReadModel))
    entries = [_dashboard_row_from_profile(table, now=now) for table in tables]
    session.add_all(entries)
    session.flush()
    elapsed_ms = round((perf_counter() - start) * 1000, 2)
    logger.info(
        "platform dashboard read model refresh completed mode=%s entries=%s updated_tables=%s elapsed_ms=%s",
        "full",
        len(entries),
        len(entries),
        elapsed_ms,
    )
    return {
        "entries": len(entries),
        "refreshed_at": now.isoformat(),
        "mode": "full",
        "updated_tables": len(entries),
        "elapsed_ms": elapsed_ms,
    }


def refresh_platform_read_models(session: Session, *, mode: str = "auto") -> dict[str, object]:
    start = perf_counter()
    search_payload = refresh_search_read_model(session, mode=mode)
    dashboard_payload = refresh_dashboard_asset_read_model(session, mode=mode)
    return {
        "refreshed_at": dashboard_payload["refreshed_at"],
        "mode": mode,
        "search": search_payload,
        "dashboard": dashboard_payload,
        "elapsed_ms": round((perf_counter() - start) * 1000, 2),
    }


def dashboard_read_model_status(session: Session) -> dict[str, object]:
    total = int(session.scalar(select(func.count(DashboardAssetReadModel.table_id))) or 0)
    last_refresh = session.scalar(select(func.max(DashboardAssetReadModel.updated_at)))
    return _read_model_status_payload(name="Dashboard", total=total, last_refresh=last_refresh)


def load_dashboard_profiles_from_read_model(
    session: Session,
    *,
    table_ids: list[int] | None = None,
    current_user=None,
) -> list[TableProfile]:
    stmt = select(DashboardAssetReadModel)
    if table_ids:
        stmt = stmt.where(DashboardAssetReadModel.table_id.in_(table_ids))
    rows = session.scalars(stmt).all()
    profiles = [
        TableProfile(
            table_id=row.table_id,
            datasource_id=row.datasource_id,
            database_id=row.database_id,
            schema_id=row.schema_id,
            table_name=row.table_name,
            table_type=row.table_type,
            schema_name=row.schema_name,
            database_name=row.database_name,
            datasource_name=row.datasource_name,
            engine=row.engine,
            owner_defined=row.owner_defined,
            description_complete=row.description_complete,
            dictionary_complete=row.dictionary_complete,
            classification_defined=row.classification_defined,
            tags_count=row.tags_count,
            terms_count=row.terms_count,
            search_clicks_30d=row.search_clicks_30d,
            active_dq_rules_count=row.active_dq_rules_count,
            recent_dq_failure_runs_30d=row.recent_dq_failure_runs_30d,
            total_columns=1 if row.dictionary_complete else 0,
            documented_columns=1 if row.dictionary_complete else 0,
            certification_status=row.certification_status,
            certification_criticality=row.certification_criticality,
            certification_badges=list(row.certification_badges or []),
            certification_decided_at=_from_iso(row.certification_decided_at),
            certification_review_at=_from_iso(row.certification_review_at),
            certification_expires_at=_from_iso(row.certification_expires_at),
            review_recent=row.review_recent,
            dq_score=row.dq_score,
            completeness_pct_avg=row.completeness_pct_avg,
            freshness_seconds=row.freshness_seconds,
            open_incidents=row.open_incidents,
            critical_open_incidents=row.critical_open_incidents,
            active_dq_violation=False,
            active_dq_violation_count=0,
            active_dq_rule_names=[],
            owner_name=row.owner_name,
            data_owner_id=row.data_owner_id,
            domain_name=row.domain_name,
            sensitivity_level=row.sensitivity_level,
            has_personal_data=row.has_personal_data,
            has_sensitive_personal_data=row.has_sensitive_personal_data,
            owner_reviewed_at=_from_iso(row.owner_reviewed_at),
            privacy_reviewed_at=_from_iso(row.privacy_reviewed_at),
            last_review_at=_from_iso(row.last_review_at),
            last_sync_at=_from_iso(row.last_sync_at),
            last_updated_at=_from_iso(row.last_updated_at),
        )
        for row in rows
    ]
    return filter_table_profiles_for_user(session, profiles, current_user=current_user)


def load_dashboard_profiles_with_fallback(
    session: Session,
    now: datetime,
    *,
    table_ids: list[int] | None = None,
    current_user=None,
) -> tuple[list[TableProfile], str]:
    cache_key = (
        session_cache_key(session),
        getattr(current_user, "id", None),
        tuple(sorted(user_role_names(current_user))) if current_user is not None else (),
        tuple(sorted(int(table_id) for table_id in table_ids)) if table_ids else (),
    )
    cached = get_cached_value("dashboard_profiles_with_fallback", cache_key, now=now)
    if isinstance(cached, tuple) and len(cached) == 2:
        cached_profiles, cached_source = cached
        if isinstance(cached_profiles, list) and isinstance(cached_source, str):
            return cached_profiles, cached_source
    try:
        materialized = load_dashboard_profiles_from_read_model(session, table_ids=table_ids, current_user=current_user)
        if materialized:
            result = (materialized, "materialized")
            set_cached_value(
                "dashboard_profiles_with_fallback",
                cache_key,
                result,
                ttl_seconds=_DASHBOARD_PROFILES_CACHE_TTL_SECONDS,
                now=now,
            )
            return result
    except (OperationalError, ProgrammingError):
        session.rollback()
    result = (load_table_profiles(session, now, table_ids=table_ids, current_user=current_user), "live")
    set_cached_value(
        "dashboard_profiles_with_fallback",
        cache_key,
        result,
        ttl_seconds=_DASHBOARD_PROFILES_CACHE_TTL_SECONDS,
        now=now,
    )
    return result
