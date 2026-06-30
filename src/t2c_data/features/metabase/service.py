from __future__ import annotations

import logging
from collections import Counter, defaultdict, deque
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable

from fastapi import HTTPException, status
from sqlalchemy import case, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from t2c_data.core.json_utils import make_json_safe
from t2c_data.core.ssrf import SsrfValidationError, validate_public_http_url
from t2c_data.features.integrations.health import (
    DEFAULT_BREAKER_OPEN_SECONDS,
    DEFAULT_BREAKER_THRESHOLD,
    IntegrationHealthSnapshot,
    build_retryable_predicate,
    classify_integration_issue,
    close_breaker,
    get_integration_health,
    get_integration_health_details,
    is_breaker_open,
    open_breaker,
    retry_with_backoff,
    upsert_integration_health,
)
from t2c_data.features.lineage.persistence import get_or_create_asset_for_table
from t2c_data.features.lineage.openlineage_persistence import match_catalog_table
from t2c_data.features.lineage.sql_lineage import extract_sql_table_lineage
from t2c_data.features.governance.settings import get_governance_settings_snapshot
from t2c_data.features.platform.jobs import enqueue_integration_job, finish_integration_job, finish_integration_job_record, maybe_start_integration_job
from t2c_data.models.lineage import LineageAsset, LineageRelation
from t2c_data.models.catalog import ColumnEntity, TableEntity
from t2c_data.models.metabase import MetabaseInstance, MetabaseObject, MetabaseObjectLink, MetabaseSyncRun
from t2c_data.models.platform import IntegrationSyncJob
from t2c_data.schemas.metabase import (
    MetabaseConsumptionItemOut,
    MetabaseConsumptionSummaryOut,
    MetabaseInstanceCreate,
    MetabaseInstanceOut,
    MetabaseInstanceUpdate,
    MetabaseSyncRunOut,
)
from t2c_data.schemas.platform import IntegrationSyncJobOut
from t2c_data.services.audit import write_audit_log_sync

from .bootstrap import ensure_metabase_instance_from_settings
from .client import MetabaseClient, MetabaseClientConfig, MetabaseClientError

logger = logging.getLogger(__name__)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_text(value: str | None) -> str:
    return (value or "").strip()


def _normalize_external_id(value: Any) -> str:
    text = _normalize_text(str(value) if value is not None else None)
    return text or "0"


def _object_url(base_url: str, kind: str, external_id: str) -> str:
    return f"{base_url.rstrip('/')}/#{kind}/{external_id}"


def _parse_remote_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _instance_secret(instance: MetabaseInstance) -> str | None:
    raw = instance._secret_payload
    if not raw:
        return None
    return instance.auth_secret


def _metabase_sync_health_snapshot(
    *,
    instance: MetabaseInstance,
    health_row: IntegrationHealth | None,
    status: str,
    status_message: str | None,
    category: str | None,
    checked_at: datetime,
    last_success_at: datetime | None = None,
    last_failure_at: datetime | None = None,
    consecutive_failures: int | None = None,
    failure_count: int | None = None,
    latency_ms: int | None = None,
    error_type: str | None = None,
    error_summary: str | None = None,
    details_json: dict[str, object] | None = None,
    breaker_state: str = "closed",
    breaker_open_until_at: datetime | None = None,
) -> IntegrationHealthSnapshot:
    return IntegrationHealthSnapshot(
        integration_name="metabase",
        status=status,
        status_message=status_message,
        category=category,
        base_url=instance.base_url,
        checked_at=checked_at,
        last_success_at=last_success_at if last_success_at is not None else (health_row.last_success_at if health_row is not None else None),
        last_failure_at=last_failure_at if last_failure_at is not None else (health_row.last_failure_at if health_row is not None else None),
        consecutive_failures=consecutive_failures if consecutive_failures is not None else (health_row.consecutive_failures if health_row is not None else 0),
        failure_count=failure_count if failure_count is not None else (health_row.failure_count if health_row is not None else 0),
        latency_ms=latency_ms,
        error_type=error_type,
        error_summary=error_summary,
        details_json=make_json_safe(details_json),
        breaker_state=breaker_state,
        breaker_open_until_at=breaker_open_until_at,
    )


def serialize_metabase_instance(instance: MetabaseInstance) -> MetabaseInstanceOut:
    return MetabaseInstanceOut(
        id=instance.id,
        name=instance.name,
        base_url=instance.base_url,
        auth_type=instance.auth_type,
        auth_username=instance.auth_username,
        auth_secret_configured=bool(_instance_secret(instance)),
        timeout_seconds=instance.timeout_seconds,
        sync_dashboards=instance.sync_dashboards,
        sync_questions=instance.sync_questions,
        sync_collections=instance.sync_collections,
        enabled=instance.enabled,
        last_sync_at=instance.last_sync_at,
        last_sync_status=instance.last_sync_status,
        last_sync_message=instance.last_sync_message,
        last_sync_dashboards=instance.last_sync_dashboards,
        last_sync_questions=instance.last_sync_questions,
        last_sync_collections=instance.last_sync_collections,
        last_sync_links=instance.last_sync_links,
        last_sync_unresolved=instance.last_sync_unresolved,
        last_sync_warnings=instance.last_sync_warnings,
        created_at=instance.created_at,
        updated_at=instance.updated_at,
    )


def list_metabase_instances(session: Session, *, offset: int = 0, limit: int = 50) -> list[MetabaseInstanceOut]:
    ensure_metabase_instance_from_settings(session)
    items = session.scalars(
        select(MetabaseInstance)
        .order_by(MetabaseInstance.updated_at.desc(), MetabaseInstance.id.desc())
        .offset(max(int(offset or 0), 0))
        .limit(max(1, min(int(limit or 50), 200)))
    ).all()
    return [serialize_metabase_instance(item) for item in items]


def get_metabase_instance(session: Session, instance_id: int) -> MetabaseInstance:
    instance = session.get(MetabaseInstance, instance_id)
    if instance is None:
        raise KeyError(instance_id)
    return instance


def _validate_metabase_base_url(value: str) -> str:
    try:
        return validate_public_http_url(value, label="base_url do Metabase").rstrip("/")
    except SsrfValidationError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc


def _apply_instance_updates(instance: MetabaseInstance, payload: MetabaseInstanceUpdate) -> MetabaseInstance:
    updates = payload.model_dump(exclude_unset=True)
    for field in ("name", "base_url", "auth_type", "auth_username", "timeout_seconds", "sync_dashboards", "sync_questions", "sync_collections", "enabled"):
        if field in updates:
            value = updates[field]
            if isinstance(value, str):
                value = value.strip() or None
            if field == "base_url" and value:
                value = _validate_metabase_base_url(str(value))
            setattr(instance, field, value)
    if "auth_secret" in updates:
        instance.auth_secret = updates.get("auth_secret")
    return instance


def create_metabase_instance(session: Session, payload: MetabaseInstanceCreate) -> MetabaseInstance:
    instance = MetabaseInstance(
        name=payload.name.strip(),
        base_url=_validate_metabase_base_url(payload.base_url),
        auth_type=(payload.auth_type or "").strip() or None,
        auth_username=(payload.auth_username or "").strip() or None,
        timeout_seconds=max(int(payload.timeout_seconds or 10), 1),
        sync_dashboards=bool(payload.sync_dashboards),
        sync_questions=bool(payload.sync_questions),
        sync_collections=bool(payload.sync_collections),
        enabled=bool(payload.enabled),
    )
    instance.auth_secret = payload.auth_secret
    session.add(instance)
    session.flush()
    return instance


def update_metabase_instance(session: Session, instance: MetabaseInstance, payload: MetabaseInstanceUpdate) -> MetabaseInstance:
    _apply_instance_updates(instance, payload)
    if "timeout_seconds" in payload.model_dump(exclude_unset=True):
        instance.timeout_seconds = max(int(instance.timeout_seconds or 10), 1)
    session.flush()
    return instance


def _get_or_create_link(
    session: Session,
    *,
    instance_id: int,
    object_id: int,
    table_id: int,
    column_id: int | None,
    match_method: str,
) -> MetabaseObjectLink | None:
    return session.scalar(
        select(MetabaseObjectLink).where(
            MetabaseObjectLink.metabase_object_id == object_id,
            MetabaseObjectLink.table_id == table_id,
            MetabaseObjectLink.column_id == column_id,
            MetabaseObjectLink.match_method == match_method,
        )
    )


def _upsert_object(
    session: Session,
    *,
    instance_id: int,
    external_id: str,
    object_type: str,
    title: str,
    description: str | None,
    collection_external_id: str | None,
    collection_name: str | None,
    url: str | None,
    database_id: int | None,
    archived: bool,
    raw_json: dict[str, Any] | list | None,
    dataset_query_json: dict[str, Any] | list | None,
    remote_updated_at: datetime | None,
    last_seen_at: datetime,
) -> MetabaseObject:
    raw_json = make_json_safe(raw_json)
    dataset_query_json = make_json_safe(dataset_query_json)
    obj = session.scalar(
        select(MetabaseObject).where(
            MetabaseObject.instance_id == instance_id,
            MetabaseObject.object_type == object_type,
            MetabaseObject.external_id == external_id,
        )
    )
    if obj is None:
        obj = MetabaseObject(
            instance_id=instance_id,
            external_id=external_id,
            object_type=object_type,
            title=title,
            description=description,
            collection_external_id=collection_external_id,
            collection_name=collection_name,
            url=url,
            database_id=database_id,
            archived=archived,
            last_seen_at=last_seen_at,
            remote_updated_at=remote_updated_at,
            raw_json=raw_json,
            dataset_query_json=dataset_query_json,
            metadata_json=raw_json,
        )
        session.add(obj)
    else:
        obj.title = title
        obj.description = description
        obj.collection_external_id = collection_external_id
        obj.collection_name = collection_name
        obj.url = url
        obj.database_id = database_id
        obj.archived = archived
        obj.last_seen_at = last_seen_at
        obj.remote_updated_at = remote_updated_at
        obj.raw_json = raw_json
        obj.dataset_query_json = dataset_query_json
        obj.metadata_json = raw_json
    session.flush()
    return obj


def _link_confidence_label(match_method: str) -> str:
    normalized = (match_method or "").strip().lower()
    if normalized in {"confirmed", "direct"}:
        return "confirmed"
    if normalized in {"sql", "inferred", "indirect_view", "indirect_lineage", "lineage_indirect"}:
        return "inferred"
    if normalized in {"dashboard_card", "partial"}:
        return "partial"
    return "partial"


def _match_state_from_method(match_method: str) -> str:
    normalized = (match_method or "").strip().lower()
    if normalized in {"confirmed", "direct"}:
        return "direct"
    if normalized in {"sql", "inferred"}:
        return "direct"
    if normalized in {"indirect_view", "indirect_lineage", "lineage_indirect"}:
        return "indirect"
    if normalized in {"dashboard_card", "collection_membership"}:
        return "partial"
    return "partial"


@dataclass(frozen=True)
class LineageTraversalTarget:
    table_id: int
    source_asset: LineageAsset
    hop_count: int


@dataclass(frozen=True)
class ConsumptionMatchCandidate:
    table_id: int
    table_type: str
    source_table_name: str
    source_schema_name: str
    source_database_name: str
    hop_count: int
    is_direct: bool


def _table_fqn_from_table(table: TableEntity) -> str:
    return f"{table.schema.database.datasource.name}.{table.schema.database.name}.{table.schema.name}.{table.name}"


def _catalog_lineage_asset_for_table(session: Session, table_id: int) -> LineageAsset | None:
    asset = session.scalar(
        select(LineageAsset).where(LineageAsset.catalog_table_id == table_id, LineageAsset.is_active.is_(True))
    )
    if asset is not None:
        return asset
    with suppress(Exception):
        fallback_asset = get_or_create_asset_for_table(session, table_id)
        logger.debug(
            "metabase lineage fallback asset created table_id=%s asset_id=%s asset_key=%s",
            table_id,
            fallback_asset.id,
            fallback_asset.asset_key,
        )
        return fallback_asset
    logger.debug("metabase lineage fallback asset unavailable table_id=%s", table_id)
    return None


def _collect_upstream_catalog_tables(
    session: Session,
    *,
    table_id: int,
) -> list[LineageTraversalTarget]:
    root_asset = _catalog_lineage_asset_for_table(session, table_id)
    if root_asset is None:
        logger.debug("metabase lineage traversal skipped table_id=%s reason=no_lineage_asset", table_id)
        return []

    queue: deque[tuple[LineageAsset, int]] = deque([(root_asset, 0)])
    visited_assets: set[int] = {root_asset.id}
    visited_tables: set[int] = {table_id}
    results: list[LineageTraversalTarget] = []

    while queue:
        current_asset, hop_count = queue.popleft()
        logger.debug(
            "metabase lineage traversal step table_id=%s asset_id=%s catalog_table_id=%s hop=%s",
            table_id,
            current_asset.id,
            current_asset.catalog_table_id,
            hop_count,
        )
        relations = session.execute(
            select(LineageRelation, LineageAsset)
            .join(LineageAsset, LineageRelation.source_asset_id == LineageAsset.id)
            .where(
                LineageRelation.is_active.is_(True),
                LineageRelation.target_asset_id == current_asset.id,
                LineageAsset.is_active.is_(True),
            )
            .order_by(LineageRelation.updated_at.desc(), LineageRelation.id.desc())
        ).all()
        for relation, source_asset in relations:
            if source_asset.id in visited_assets:
                continue
            visited_assets.add(source_asset.id)
            next_hop = hop_count + 1
            if source_asset.catalog_table_id is not None and source_asset.catalog_table_id not in visited_tables:
                visited_tables.add(source_asset.catalog_table_id)
                results.append(
                    LineageTraversalTarget(
                        table_id=source_asset.catalog_table_id,
                        source_asset=source_asset,
                        hop_count=next_hop,
                    )
                )
            queue.append((source_asset, next_hop))
    logger.debug(
        "metabase lineage traversal finished table_id=%s upstream_tables=%s",
        table_id,
        [target.table_id for target in results],
    )
    return results


def _collect_downstream_catalog_tables(
    session: Session,
    *,
    table_id: int,
) -> list[LineageTraversalTarget]:
    root_asset = _catalog_lineage_asset_for_table(session, table_id)
    if root_asset is None:
        logger.debug("metabase lineage downstream traversal skipped table_id=%s reason=no_lineage_asset", table_id)
        return []

    queue: deque[tuple[LineageAsset, int]] = deque([(root_asset, 0)])
    visited_assets: set[int] = {root_asset.id}
    visited_tables: set[int] = {table_id}
    results: list[LineageTraversalTarget] = []

    while queue:
        current_asset, hop_count = queue.popleft()
        relations = session.execute(
            select(LineageRelation, LineageAsset)
            .join(LineageAsset, LineageRelation.target_asset_id == LineageAsset.id)
            .where(
                LineageRelation.is_active.is_(True),
                LineageRelation.source_asset_id == current_asset.id,
                LineageAsset.is_active.is_(True),
            )
            .order_by(LineageRelation.updated_at.desc(), LineageRelation.id.desc())
        ).all()
        for relation, target_asset in relations:
            if target_asset.id in visited_assets:
                continue
            visited_assets.add(target_asset.id)
            next_hop = hop_count + 1
            if target_asset.catalog_table_id is not None and target_asset.catalog_table_id not in visited_tables:
                visited_tables.add(target_asset.catalog_table_id)
                results.append(
                    LineageTraversalTarget(
                        table_id=target_asset.catalog_table_id,
                        source_asset=target_asset,
                        hop_count=next_hop,
                    )
                )
            queue.append((target_asset, next_hop))
    logger.debug(
        "metabase lineage downstream traversal finished table_id=%s downstream_tables=%s",
        table_id,
        [target.table_id for target in results],
    )
    return results


def _resolve_consumption_match_candidates(
    session: Session,
    *,
    table_id: int,
) -> dict[int, ConsumptionMatchCandidate]:
    table = session.get(TableEntity, table_id)
    if table is None:
        return {}
    candidates: dict[int, ConsumptionMatchCandidate] = {
        table.id: ConsumptionMatchCandidate(
            table_id=table.id,
            table_type=table.table_type,
            source_table_name=table.name,
            source_schema_name=table.schema.name,
            source_database_name=table.schema.database.name,
            hop_count=0,
            is_direct=True,
        )
    }
    for target in _collect_downstream_catalog_tables(session, table_id=table.id):
        target_table = session.get(TableEntity, target.table_id)
        if target_table is None:
            continue
        candidate = ConsumptionMatchCandidate(
            table_id=target_table.id,
            table_type=target_table.table_type,
            source_table_name=target_table.name,
            source_schema_name=target_table.schema.name,
            source_database_name=target_table.schema.database.name,
            hop_count=target.hop_count,
            is_direct=False,
        )
        existing = candidates.get(candidate.table_id)
        if existing is None or candidate.hop_count < existing.hop_count:
            candidates[candidate.table_id] = candidate
    logger.debug(
        "metabase consumption candidate tables resolved table_id=%s candidate_table_ids=%s",
        table_id,
        sorted(candidates.keys()),
    )
    return candidates


def _upsert_link(
    session: Session,
    *,
    instance_id: int,
    object_id: int,
    table_id: int,
    column_id: int | None,
    match_method: str,
    confidence_reason: str | None,
    source_table_name: str | None = None,
    source_schema_name: str | None = None,
    source_database_name: str | None = None,
    source_column_name: str | None = None,
) -> MetabaseObjectLink:
    link = _get_or_create_link(
        session,
        instance_id=instance_id,
        object_id=object_id,
        table_id=table_id,
        column_id=column_id,
        match_method=match_method,
    )
    confidence_level = _link_confidence_label(match_method)
    if link is None:
        link = MetabaseObjectLink(
            instance_id=instance_id,
            metabase_object_id=object_id,
            table_id=table_id,
            column_id=column_id,
            match_method=match_method,
            confidence_level=confidence_level,
            confidence_reason=confidence_reason,
            source_table_name=source_table_name,
            source_schema_name=source_schema_name,
            source_database_name=source_database_name,
            source_column_name=source_column_name,
            is_active=True,
        )
        session.add(link)
    else:
        link.confidence_level = confidence_level
        link.confidence_reason = confidence_reason
        link.source_table_name = source_table_name
        link.source_schema_name = source_schema_name
        link.source_database_name = source_database_name
        link.source_column_name = source_column_name
        link.is_active = True
    session.flush()
    return link


def _collection_label(item: dict[str, Any]) -> str | None:
    for key in ("name", "label", "title"):
        value = item.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _collection_metadata_items(collections: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [item for item in collections if _collection_label(item)]


def _dashcard_card_ids(dashboard: dict[str, Any]) -> list[str]:
    ordered = dashboard.get("ordered_cards")
    if isinstance(ordered, list):
        ids: list[str] = []
        for item in ordered:
            data = item if isinstance(item, dict) else {}
            card_id = data.get("card_id") or data.get("cardId") or data.get("id")
            if card_id is not None:
                ids.append(str(card_id))
        if ids:
            return ids
    dashcards = dashboard.get("dashcards")
    if isinstance(dashcards, list):
        ids: list[str] = []
        for item in dashcards:
            data = item if isinstance(item, dict) else {}
            card = data.get("card") if isinstance(data.get("card"), dict) else {}
            card_id = data.get("card_id") or data.get("cardId") or card.get("id")
            if card_id is not None:
                ids.append(str(card_id))
        return ids
    return []


def _extract_cards_from_dashboard(dashboard: dict[str, Any]) -> list[str]:
    ids = _dashcard_card_ids(dashboard)
    return list(dict.fromkeys(ids))


def _database_table_lookup(metadata: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Map Metabase numeric table id -> {name, schema_name} from database metadata.

    Only the top-level ``tables`` collection is consulted. Walking every nested dict
    is wrong: each table embeds ``fields`` that also carry ``id``/``name`` pairs, so a
    recursive walk pollutes the map with column ids (and loses the schema), which made
    structured (MBQL) ``source-table`` ids resolve to garbage and produced zero links.
    """
    lookup: dict[str, dict[str, Any]] = {}
    if not isinstance(metadata, dict):
        return lookup

    tables = metadata.get("tables")
    if not isinstance(tables, list):
        return lookup

    for table in tables:
        if not isinstance(table, dict):
            continue
        table_id = table.get("id") or table.get("table_id") or table.get("tableId")
        name = table.get("name") or table.get("table_name")
        if table_id is None or not isinstance(name, str) or not name.strip():
            continue
        schema_name = table.get("schema") or table.get("schema_name") or table.get("schemaName")
        lookup[str(table_id)] = {
            "name": name.strip(),
            "schema_name": schema_name.strip() if isinstance(schema_name, str) and schema_name.strip() else None,
            "display_name": table.get("display_name") or table.get("displayName") or name.strip(),
        }
    return lookup


def _parse_dataset_query(dataset_query: dict[str, Any] | None) -> tuple[list[str], str | None]:
    if not isinstance(dataset_query, dict):
        return [], None

    tables: list[str] = []
    sql: str | None = None

    def walk(value: Any) -> None:
        nonlocal sql
        if isinstance(value, dict):
            native_value = value.get("native")
            if isinstance(native_value, str) and native_value.strip():
                if sql is None:
                    sql = native_value.strip()
                tables.extend(extract_sql_table_lineage(native_value))
            elif isinstance(native_value, dict):
                native_sql = native_value.get("query")
                if isinstance(native_sql, str) and native_sql.strip():
                    if sql is None:
                        sql = native_sql.strip()
                    tables.extend(extract_sql_table_lineage(native_sql))

            source_table = value.get("source-table")
            if source_table is not None:
                tables.append(str(source_table))

            for nested in value.values():
                walk(nested)
        elif isinstance(value, list):
            for item in value:
                walk(item)

    walk(dataset_query)
    normalized_tables = [table for table in tables if str(table).strip()]
    return list(dict.fromkeys(normalized_tables)), sql


def _resolve_referenced_tables(
    table_refs: list[str],
    table_lookup: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    """Turn raw dataset-query refs into displayable table entries.

    Native-SQL queries already yield ``schema.table`` names; structured (MBQL)
    queries yield numeric Metabase ``source-table`` ids, resolved to real
    ``schema.table`` names through ``table_lookup``. Entries that stay numeric
    (metadata unavailable) are kept and flagged ``resolved=False`` so the UI can
    distinguish them.
    """
    resolved: list[dict[str, Any]] = []
    seen: set[str] = set()
    for ref in table_refs:
        normalized = str(ref).strip()
        if not normalized:
            continue
        metabase_table_id: str | None = None
        schema_name: str | None = None
        if normalized in table_lookup:
            meta = table_lookup[normalized]
            metabase_table_id = normalized
            schema_name = meta.get("schema_name")
            table_name = str(meta.get("name") or normalized)
            is_resolved = True
        elif normalized.isdigit():
            metabase_table_id = normalized
            table_name = normalized
            is_resolved = False
        else:
            parts = [part.strip('`" ') for part in normalized.split(".") if part.strip('`" ')]
            if len(parts) >= 2:
                schema_name = parts[-2]
                table_name = parts[-1]
            else:
                table_name = parts[0] if parts else normalized
            is_resolved = True
        full_name = f"{schema_name}.{table_name}" if schema_name else table_name
        key = full_name.lower()
        if key in seen:
            continue
        seen.add(key)
        resolved.append(
            {
                "metabase_table_id": metabase_table_id,
                "schema": schema_name,
                "name": table_name,
                "full_name": full_name,
                "source": "mbql" if metabase_table_id is not None else "sql",
                "resolved": is_resolved,
            }
        )
    return resolved


def _candidate_matches_for_card(
    *,
    session: Session,
    card: dict[str, Any],
    card_detail: dict[str, Any],
    base_database_metadata: dict[str, dict[str, Any]],
    base_url: str,
) -> list[tuple[TableEntity, str, str | None, str | None, str | None]]:
    matches: list[tuple[TableEntity, str, str | None, str | None, str | None]] = []
    dataset_query = card_detail.get("dataset_query") if isinstance(card_detail.get("dataset_query"), dict) else card.get("dataset_query")
    database_id = card_detail.get("database_id") if card_detail.get("database_id") is not None else card.get("database_id")
    table_refs, sql = _parse_dataset_query(dataset_query if isinstance(dataset_query, dict) else None)
    table_lookup = dict(base_database_metadata)
    if database_id is not None and not table_lookup:
        # The caller may attach database metadata as a per-card hint. Keep the branch explicit so
        # future Metabase payloads can inject richer metadata without changing the matching flow.
        metadata = card_detail.get("database_metadata")
        if isinstance(metadata, dict):
            table_lookup.update(_database_table_lookup(metadata))
    if not table_lookup and isinstance(card_detail.get("database"), dict):
        db_payload = card_detail.get("database")
        if isinstance(db_payload, dict):
            table_lookup.update(_database_table_lookup(db_payload))
    if not table_lookup and isinstance(card.get("database_metadata"), dict):
        table_lookup.update(_database_table_lookup(card.get("database_metadata")))

    for table_ref in table_refs:
        normalized = str(table_ref).strip()
        if not normalized:
            continue
        table_name = normalized
        schema_name = None
        if normalized in table_lookup:
            meta = table_lookup[normalized]
            table_name = str(meta.get("name") or normalized)
            schema_name = meta.get("schema_name")
        else:
            parts = [part.strip('`" ') for part in normalized.split(".") if part.strip('`" ')]
            if len(parts) >= 2:
                table_name = parts[-1]
                schema_name = parts[-2]
            elif len(parts) == 1:
                table_name = parts[0]
        match = match_catalog_table(
            session,
            dataset_name=f"{schema_name}.{table_name}" if schema_name else table_name,
            physical_name=table_name,
            namespace=None,
            aliases=[normalized],
        )
        if match is None:
            continue
        table, schema, database, datasource = match
        confidence = "confirmed" if normalized in table_lookup else "inferred"
        reason = "Metabase source-table" if normalized in table_lookup else "SQL parse"
        matches.append((table, confidence, reason, schema.name if schema else None, datasource.name if datasource else None))
    if matches:
        return matches

    if sql:
        # Keep the SQL handy for future heuristics while returning no hard links.
        logger.debug("metabase card SQL parsed without table match card_id=%s", card.get("id"))
    return []


def _ensure_object_links_for_card(
    session: Session,
    *,
    instance: MetabaseInstance,
    object_row: MetabaseObject,
    card: dict[str, Any],
    card_detail: dict[str, Any],
    dashboard_title: str | None = None,
    card_title: str | None = None,
) -> tuple[int, int]:
    matched = 0
    unresolved = 0
    base_url = instance.base_url
    base_database_metadata: dict[str, dict[str, Any]] = {}
    database_id = card_detail.get("database_id") or card.get("database_id")
    if database_id is not None:
        metadata = card_detail.get("database_metadata")
        if not isinstance(metadata, dict):
            metadata = {}
        if not metadata:
            metadata = card.get("database_metadata") if isinstance(card.get("database_metadata"), dict) else {}
        if metadata:
            base_database_metadata.update(_database_table_lookup(metadata))
    matches = _candidate_matches_for_card(
        session=session,
        card=card,
        card_detail=card_detail,
        base_database_metadata=base_database_metadata,
        base_url=base_url,
    )
    if not matches:
        unresolved += 1
    for table, confidence, reason, schema_name, datasource_name in matches:
        direct_match_method = "direct" if confidence == "confirmed" else "sql"
        logger.info(
            "metabase card direct match object_id=%s table_id=%s match_method=%s confidence=%s reason=%s",
            object_row.id,
            table.id,
            direct_match_method,
            confidence,
            reason,
        )
        _upsert_link(
            session,
            instance_id=instance.id,
            object_id=object_row.id,
            table_id=table.id,
            column_id=None,
            match_method=direct_match_method,
            confidence_reason=reason,
            source_table_name=table.name,
            source_schema_name=schema_name or table.schema.name,
            source_database_name=datasource_name or table.schema.database.name,
        )
        matched += 1
        lineage_targets = _collect_upstream_catalog_tables(session, table_id=table.id)
        if lineage_targets:
            match_kind = "indirect_view" if table.table_type == "view" else "indirect_lineage"
            source_fqn = f"{schema_name or table.schema.name}.{table.name}"
            if datasource_name:
                source_fqn = f"{datasource_name}.{source_fqn}"
            logger.info(
                "metabase card lineage match object_id=%s source_table=%s upstream_targets=%s match_method=%s",
                object_row.id,
                source_fqn,
                [target.table_id for target in lineage_targets],
                match_kind,
            )
            for target in lineage_targets:
                _upsert_link(
                    session,
                    instance_id=instance.id,
                    object_id=object_row.id,
                    table_id=target.table_id,
                    column_id=None,
                    match_method=match_kind,
                    confidence_reason=f"Encontrado via {'view' if table.table_type == 'view' else 'linhagem'} {source_fqn}",
                    source_table_name=table.name,
                    source_schema_name=schema_name or table.schema.name,
                    source_database_name=datasource_name or table.schema.database.name,
                )
                matched += 1
        elif table.table_type == "view":
            logger.warning(
                "metabase card matched a view but no upstream lineage was found object_id=%s table_id=%s source_table=%s",
                object_row.id,
                table.id,
                f"{schema_name or table.schema.name}.{table.name}",
            )
    return matched, unresolved


def _upsert_collection_object(session: Session, instance: MetabaseInstance, collection: dict[str, Any]) -> MetabaseObject:
    external_id = _normalize_external_id(collection.get("id") or collection.get("collection_id") or collection.get("collectionId"))
    title = _collection_label(collection) or f"Collection {external_id}"
    return _upsert_object(
        session,
        instance_id=instance.id,
        external_id=external_id,
        object_type="collection",
        title=title,
        description=collection.get("description") if isinstance(collection.get("description"), str) else None,
        collection_external_id=str(collection.get("parent_id") or collection.get("parentId")) if collection.get("parent_id") or collection.get("parentId") else None,
        collection_name=None,
        url=f"{instance.base_url}/collection/{external_id}",
        database_id=None,
        archived=bool(collection.get("archived") or collection.get("is_archived")),
        raw_json=collection,
        dataset_query_json=None,
        remote_updated_at=_parse_remote_datetime(collection.get("updated_at") or collection.get("updatedAt")),
        last_seen_at=_now(),
    )


def _upsert_question_object(session: Session, instance: MetabaseInstance, card: dict[str, Any]) -> MetabaseObject:
    external_id = _normalize_external_id(card.get("id"))
    collection_id = card.get("collection_id") or card.get("collectionId")
    collection_name = card.get("collection_name") or card.get("collectionName")
    title = str(card.get("name") or card.get("display_name") or card.get("displayName") or f"Question {external_id}")
    return _upsert_object(
        session,
        instance_id=instance.id,
        external_id=external_id,
        object_type="question",
        title=title,
        description=card.get("description") if isinstance(card.get("description"), str) else None,
        collection_external_id=str(collection_id) if collection_id is not None else None,
        collection_name=str(collection_name) if collection_name else None,
        url=card.get("url") or _object_url(instance.base_url, "question", external_id),
        database_id=card.get("database_id") if isinstance(card.get("database_id"), int) else None,
        archived=bool(card.get("archived") or card.get("is_archived")),
        raw_json=card,
        dataset_query_json=card.get("dataset_query") if isinstance(card.get("dataset_query"), (dict, list)) else None,
        remote_updated_at=_parse_remote_datetime(card.get("updated_at") or card.get("updatedAt")),
        last_seen_at=_now(),
    )


def _upsert_dashboard_object(session: Session, instance: MetabaseInstance, dashboard: dict[str, Any]) -> MetabaseObject:
    external_id = _normalize_external_id(dashboard.get("id"))
    collection_id = dashboard.get("collection_id") or dashboard.get("collectionId")
    collection_name = dashboard.get("collection_name") or dashboard.get("collectionName")
    title = str(dashboard.get("name") or dashboard.get("display_name") or dashboard.get("displayName") or f"Dashboard {external_id}")
    return _upsert_object(
        session,
        instance_id=instance.id,
        external_id=external_id,
        object_type="dashboard",
        title=title,
        description=dashboard.get("description") if isinstance(dashboard.get("description"), str) else None,
        collection_external_id=str(collection_id) if collection_id is not None else None,
        collection_name=str(collection_name) if collection_name else None,
        url=dashboard.get("url") or _object_url(instance.base_url, "dashboard", external_id),
        database_id=None,
        archived=bool(dashboard.get("archived") or dashboard.get("is_archived")),
        raw_json=dashboard,
        dataset_query_json=None,
        remote_updated_at=_parse_remote_datetime(dashboard.get("updated_at") or dashboard.get("updatedAt")),
        last_seen_at=_now(),
    )


def _dashboard_consumes_question_links(
    session: Session,
    *,
    instance: MetabaseInstance,
    dashboard_object: MetabaseObject,
    dashboard: dict[str, Any],
    card_links: dict[str, list[MetabaseObjectLink]],
    ) -> tuple[int, int]:
    matched = 0
    unresolved = 0
    card_ids = _extract_cards_from_dashboard(dashboard)
    for card_id in card_ids:
        for link in card_links.get(str(card_id), []):
            _upsert_link(
                session,
                instance_id=instance.id,
                object_id=dashboard_object.id,
                table_id=link.table_id,
                column_id=link.column_id,
                match_method="dashboard_card",
                confidence_reason=f"Dashboard card linked to question {link.object.title}",
                source_table_name=link.source_table_name,
                source_schema_name=link.source_schema_name,
                source_database_name=link.source_database_name,
                source_column_name=link.source_column_name,
            )
            matched += 1
        if not card_links.get(str(card_id)):
            unresolved += 1
    return matched, unresolved


def _collection_consumes_table_links(
    session: Session,
    *,
    instance: MetabaseInstance,
    collection_object: MetabaseObject,
    table_ids: Iterable[int],
    reason: str,
) -> int:
    created = 0
    for table_id in dict.fromkeys(int(table_id) for table_id in table_ids if table_id is not None):
        _upsert_link(
            session,
            instance_id=instance.id,
            object_id=collection_object.id,
            table_id=table_id,
            column_id=None,
            match_method="collection_membership",
            confidence_reason=reason,
            source_table_name=collection_object.collection_name or collection_object.title,
        )
        created += 1
    return created


def _consume_object_link_items(
    session: Session,
    *,
    table_id: int,
    object_type: str,
) -> list[MetabaseConsumptionItemOut]:
    match_candidates = _resolve_consumption_match_candidates(session, table_id=table_id)
    candidate_table_ids = sorted(match_candidates.keys())
    rows = session.execute(
        select(MetabaseObjectLink, MetabaseObject)
        .join(MetabaseObject, MetabaseObjectLink.metabase_object_id == MetabaseObject.id)
        .where(
            MetabaseObjectLink.table_id.in_(candidate_table_ids),
            MetabaseObjectLink.is_active.is_(True),
            MetabaseObject.object_type == object_type,
        )
        .order_by(
            MetabaseObject.updated_at.desc(),
            MetabaseObject.id.desc(),
            case(
                (
                    MetabaseObjectLink.match_method.in_(["confirmed", "direct"]),
                    0,
                ),
                (
                    MetabaseObjectLink.match_method.in_(["sql", "inferred"]),
                    1,
                ),
                (
                    MetabaseObjectLink.match_method.in_(["indirect_view", "indirect_lineage", "lineage_indirect"]),
                    2,
                ),
                (
                    MetabaseObjectLink.match_method == "dashboard_card",
                    3,
                ),
                (
                    MetabaseObjectLink.match_method == "collection_membership",
                    4,
                ),
                else_=5,
            ),
            MetabaseObjectLink.id.asc(),
        )
    ).all()
    grouped: dict[int, list[tuple[MetabaseObjectLink, MetabaseObject, ConsumptionMatchCandidate | None]]] = defaultdict(list)
    for link, obj in rows:
        grouped[obj.id].append((link, obj, match_candidates.get(link.table_id)))

    def _candidate_priority(link: MetabaseObjectLink, candidate: ConsumptionMatchCandidate | None) -> tuple[int, int, int]:
        if candidate is None:
            return (9, 9, link.id)
        if candidate.is_direct:
            direct_priority = 0 if link.table_id == table_id else 1
        else:
            direct_priority = 2 if candidate.table_type == "view" else 3
        return (direct_priority, candidate.hop_count, link.id)

    items: list[MetabaseConsumptionItemOut] = []
    for object_links in grouped.values():
        link, obj, candidate = min(object_links, key=lambda item: _candidate_priority(item[0], item[2]))
        if candidate is None:
            continue
        if candidate.is_direct and link.table_id == table_id:
            effective_match_method = link.match_method
            confidence_reason = link.confidence_reason
            confidence_level = link.confidence_level
            match_state = _match_state_from_method(effective_match_method)
            source_table_name = link.source_table_name
            source_schema_name = link.source_schema_name
            source_database_name = link.source_database_name
        else:
            effective_match_method = "indirect_view" if candidate.table_type == "view" else "indirect_lineage"
            via_label = "view" if candidate.table_type == "view" else "linhagem"
            source_fqn = f"{candidate.source_schema_name}.{candidate.source_table_name}"
            confidence_reason = f"Encontrado via {via_label} {source_fqn}"
            confidence_level = "inferred"
            match_state = "indirect"
            source_table_name = candidate.source_table_name
            source_schema_name = candidate.source_schema_name
            source_database_name = candidate.source_database_name
        items.append(
            MetabaseConsumptionItemOut(
                object_id=obj.id,
                external_id=obj.external_id,
                object_type=obj.object_type,  # type: ignore[arg-type]
                title=obj.title,
                description=obj.description,
                url=obj.url,
                collection_name=obj.collection_name,
                collection_external_id=obj.collection_external_id,
                confidence_level=confidence_level,
                confidence_reason=confidence_reason,
                match_method=effective_match_method,
                match_state=match_state,
                link_count=1,
                source_table_name=source_table_name,
                source_schema_name=source_schema_name,
                source_database_name=source_database_name,
                source_column_name=link.source_column_name,
            )
        )
    return items


def _consume_collection_items(session: Session, *, table_id: int) -> list[MetabaseConsumptionItemOut]:
    match_candidates = _resolve_consumption_match_candidates(session, table_id=table_id)
    candidate_table_ids = sorted(match_candidates.keys())
    rows = session.execute(
        select(MetabaseObjectLink, MetabaseObject)
        .join(MetabaseObject, MetabaseObjectLink.metabase_object_id == MetabaseObject.id)
        .where(
            MetabaseObjectLink.table_id.in_(candidate_table_ids),
            MetabaseObjectLink.is_active.is_(True),
            MetabaseObject.object_type == "collection",
        )
        .order_by(
            MetabaseObject.updated_at.desc(),
            MetabaseObject.id.desc(),
            case(
                (
                    MetabaseObjectLink.match_method.in_(["confirmed", "direct"]),
                    0,
                ),
                (
                    MetabaseObjectLink.match_method.in_(["sql", "inferred"]),
                    1,
                ),
                (
                    MetabaseObjectLink.match_method.in_(["indirect_view", "indirect_lineage", "lineage_indirect"]),
                    2,
                ),
                (
                    MetabaseObjectLink.match_method == "dashboard_card",
                    3,
                ),
                (
                    MetabaseObjectLink.match_method == "collection_membership",
                    4,
                ),
                else_=5,
            ),
            MetabaseObjectLink.id.asc(),
        )
    ).all()
    grouped: dict[int, list[tuple[MetabaseObjectLink, MetabaseObject, ConsumptionMatchCandidate | None]]] = defaultdict(list)
    for link, obj in rows:
        grouped[obj.id].append((link, obj, match_candidates.get(link.table_id)))

    def _candidate_priority(link: MetabaseObjectLink, candidate: ConsumptionMatchCandidate | None) -> tuple[int, int, int]:
        if candidate is None:
            return (9, 9, link.id)
        if candidate.is_direct:
            direct_priority = 0 if link.table_id == table_id else 1
        else:
            direct_priority = 2 if candidate.table_type == "view" else 3
        return (direct_priority, candidate.hop_count, link.id)

    items: list[MetabaseConsumptionItemOut] = []
    for object_links in grouped.values():
        link, obj, candidate = min(object_links, key=lambda item: _candidate_priority(item[0], item[2]))
        if candidate is None:
            continue
        if candidate.is_direct and link.table_id == table_id:
            effective_match_method = link.match_method
            confidence_reason = link.confidence_reason
            confidence_level = link.confidence_level
            match_state = _match_state_from_method(effective_match_method)
            source_table_name = link.source_table_name
            source_schema_name = link.source_schema_name
            source_database_name = link.source_database_name
        else:
            effective_match_method = "indirect_view" if candidate.table_type == "view" else "indirect_lineage"
            via_label = "view" if candidate.table_type == "view" else "linhagem"
            source_fqn = f"{candidate.source_schema_name}.{candidate.source_table_name}"
            confidence_reason = f"Encontrado via {via_label} {source_fqn}"
            confidence_level = "inferred"
            match_state = "indirect"
            source_table_name = candidate.source_table_name
            source_schema_name = candidate.source_schema_name
            source_database_name = candidate.source_database_name
        items.append(
            MetabaseConsumptionItemOut(
                object_id=obj.id,
                external_id=obj.external_id,
                object_type=obj.object_type,  # type: ignore[arg-type]
                title=obj.title,
                description=obj.description,
                url=obj.url,
                collection_name=obj.collection_name,
                collection_external_id=obj.collection_external_id,
                confidence_level=confidence_level,
                confidence_reason=confidence_reason,
                match_method=effective_match_method,
                match_state=match_state,
                link_count=1,
                source_table_name=source_table_name,
                source_schema_name=source_schema_name,
                source_database_name=source_database_name,
                source_column_name=link.source_column_name,
            )
        )
    return items


def list_metabase_sync_runs(
    session: Session,
    instance_id: int,
    *,
    offset: int = 0,
    limit: int = 20,
) -> list[MetabaseSyncRunOut]:
    rows = session.scalars(
        select(MetabaseSyncRun)
        .where(MetabaseSyncRun.instance_id == instance_id)
        .order_by(MetabaseSyncRun.started_at.desc(), MetabaseSyncRun.id.desc())
        .offset(max(int(offset or 0), 0))
        .limit(max(1, min(int(limit or 20), 200)))
    ).all()
    return [MetabaseSyncRunOut.model_validate(row, from_attributes=True) for row in rows]


def _metabase_running_sync_job(session: Session, instance_id: int) -> IntegrationSyncJob | None:
    return session.scalar(
        select(IntegrationSyncJob)
        .where(
            IntegrationSyncJob.source == "metabase",
            IntegrationSyncJob.job_type == "sync",
            IntegrationSyncJob.target_type == "metabase_instance",
            IntegrationSyncJob.target_id == instance_id,
            IntegrationSyncJob.status == "running",
        )
        .order_by(IntegrationSyncJob.started_at.desc(), IntegrationSyncJob.id.desc())
        .limit(1)
    )


def _metabase_active_sync_job(session: Session, instance_id: int) -> IntegrationSyncJob | None:
    return session.scalar(
        select(IntegrationSyncJob)
        .where(
            IntegrationSyncJob.source == "metabase",
            IntegrationSyncJob.job_type == "sync",
            IntegrationSyncJob.target_type == "metabase_instance",
            IntegrationSyncJob.target_id == instance_id,
            IntegrationSyncJob.status.in_(["queued", "running"]),
        )
        .order_by(IntegrationSyncJob.queued_at.desc().nulls_last(), IntegrationSyncJob.started_at.desc().nulls_last(), IntegrationSyncJob.id.desc())
        .limit(1)
    )


def _metabase_sync_now_conflict_detail(session: Session, running_job: IntegrationSyncJob) -> dict[str, Any]:
    try:
        settings_snapshot = get_governance_settings_snapshot(session)
    except SQLAlchemyError:
        settings_snapshot = None
    diagnostic = {
        "diagnostic_status": "unknown",
        "diagnostic_severity": "info",
        "diagnostic_label": "Sem diagnóstico",
        "diagnostic_description": "Não foi possível derivar um diagnóstico para a execução atual.",
        "running_duration_seconds": None,
        "is_stalled": False,
    }
    with suppress(Exception):
        from t2c_data.features.platform.job_diagnostics import diagnose_integration_job

        diagnostic = diagnose_integration_job(
            running_job,
            now=_now(),
            attention_minutes=getattr(settings_snapshot, "platform_job_running_attention_minutes", 120) if settings_snapshot is not None else 120,
            critical_hours=getattr(settings_snapshot, "platform_job_running_critical_hours", 24) if settings_snapshot is not None else 24,
            next_expected_delay_minutes=getattr(settings_snapshot, "platform_job_next_expected_delay_minutes", 60) if settings_snapshot is not None else 60,
        )

    force_eligible = bool(diagnostic.get("is_stalled"))
    detail: dict[str, Any] = {
        "message": "Já existe uma sincronização em andamento. Aguarde finalizar ou revise possível execução travada.",
        "force_eligible": force_eligible,
        "running_duration_seconds": diagnostic.get("running_duration_seconds"),
        "diagnostic_status": diagnostic.get("diagnostic_status"),
        "diagnostic_severity": diagnostic.get("diagnostic_severity"),
        "diagnostic_label": diagnostic.get("diagnostic_label"),
        "diagnostic_description": diagnostic.get("diagnostic_description"),
        "running_job": IntegrationSyncJobOut.model_validate(running_job, from_attributes=True).model_dump(mode="json"),
    }
    return detail


def enqueue_metabase_instance_sync(
    session: Session,
    instance_id: int,
    *,
    current_user,
    force: bool = False,
    reason: str = "manual",
) -> MetabaseSyncRunOut:
    instance = get_metabase_instance(session, instance_id)
    instance_id_value = int(instance.id)
    instance_name = instance.name
    instance_base_url = instance.base_url
    normalized_reason = (reason or "manual").strip().lower() or "manual"
    active_job = _metabase_active_sync_job(session, int(instance.id))
    if active_job is not None:
        if active_job.status != "running":
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "message": "Já existe uma sincronização enfileirada ou em andamento para esta instância.",
                    "force_eligible": False,
                    "running_job": IntegrationSyncJobOut.model_validate(active_job, from_attributes=True).model_dump(mode="json"),
                },
            )
        conflict_detail = _metabase_sync_now_conflict_detail(session, active_job)
        if not force or not bool(conflict_detail.get("force_eligible")):
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=conflict_detail)

    sync_run = MetabaseSyncRun(
        instance_id=instance_id_value,
        status="queued",
        started_at=_now(),
        summary_json={"phase": "queued"},
    )
    session.add(sync_run)
    session.flush()
    sync_run_id = int(sync_run.id)
    queued_started_at = sync_run.started_at
    queued_created_at = sync_run.created_at
    queued_updated_at = sync_run.updated_at
    enqueue_integration_job(
        session,
        source="metabase",
        job_type="sync",
        target_type="metabase_instance",
        target_id=instance_id_value,
        target_name=instance_name,
        trigger_mode=normalized_reason,
        requested_by_user_id=getattr(current_user, "id", None),
        payload_json={
            "instance_id": instance_id_value,
            "sync_run_id": sync_run_id,
            "force": bool(force),
            "reason": normalized_reason,
        },
        context_json={
            "instance_id": instance_id_value,
            "instance_name": instance_name,
            "base_url": instance_base_url,
            "reason": normalized_reason,
        },
        replace_stalled_running_job=bool(force),
    )
    session.commit()
    return MetabaseSyncRunOut.model_validate(
        {
            "id": sync_run_id,
            "instance_id": instance_id_value,
            "instance_name": instance_name,
            "status": "queued",
            "started_at": queued_started_at,
            "finished_at": None,
            "dashboards_count": 0,
            "questions_count": 0,
            "collections_count": 0,
            "links_count": 0,
            "artifacts_processed": 0,
            "links_created": 0,
            "unresolved_count": 0,
            "warnings_count": 0,
            "error_message": None,
            "error_type": None,
            "summary_json": {"phase": "queued"},
            "created_at": queued_created_at,
            "updated_at": queued_updated_at,
        }
    )


def run_metabase_instance_sync(
    session: Session,
    instance_id: int,
    *,
    commit: bool = True,
    force: bool = False,
    audit_kwargs: dict[str, Any] | None = None,
    integration_job: IntegrationSyncJob | None = None,
    sync_run_id: int | None = None,
) -> MetabaseSyncRunOut:
    instance = get_metabase_instance(session, instance_id)
    instance_id_value = int(instance.id)
    instance_name = instance.name
    instance_base_url = instance.base_url
    instance_enabled = bool(instance.enabled)
    instance_sync_dashboards = bool(instance.sync_dashboards)
    instance_sync_questions = bool(instance.sync_questions)
    instance_sync_collections = bool(instance.sync_collections)
    running_job = _metabase_running_sync_job(session, instance_id_value)
    if running_job is not None and (integration_job is None or running_job.id != integration_job.id):
        conflict_detail = _metabase_sync_now_conflict_detail(session, running_job)
        if not force or not bool(conflict_detail.get("force_eligible")):
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=conflict_detail)
    job_handle = None
    if integration_job is None:
        job_handle = maybe_start_integration_job(
            session,
            source="metabase",
            job_type="sync",
            target_type="metabase_instance",
            target_id=instance_id_value,
            target_name=instance_name,
            trigger_mode="manual",
            force_stale_running_job=force,
        )
    job_status = "success"
    job_error: str | None = None
    job_records: int | None = None
    job_context: dict[str, Any] | None = {
        "instance_id": instance_id_value,
        "instance_name": instance_name,
        "base_url": instance_base_url,
    }
    started_at = _now()
    if sync_run_id is not None:
        sync_run = session.get(MetabaseSyncRun, int(sync_run_id))
        if sync_run is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Metabase sync run not found")
        sync_run.status = "running"
        sync_run.started_at = started_at
        sync_run.finished_at = None
        sync_run.error_message = None
        sync_run.summary_json = {"phase": "starting"}
    else:
        sync_run = MetabaseSyncRun(
            instance_id=instance_id_value,
            status="running",
            started_at=started_at,
            summary_json={"phase": "starting"},
        )
        session.add(sync_run)
        session.flush()
    if commit:
        session.commit()
        instance = get_metabase_instance(session, instance_id_value)
        sync_run = session.get(MetabaseSyncRun, int(sync_run.id))
    logger.info(
        "metabase sync started instance_id=%s base_url=%s enabled=%s dashboards=%s questions=%s collections=%s",
        instance_id_value,
        instance_base_url,
        instance_enabled,
        instance_sync_dashboards,
        instance_sync_questions,
        instance_sync_collections,
    )

    dashboards_count = 0
    questions_count = 0
    collections_count = 0
    links_count = 0
    unresolved_count = 0
    warnings_count = 0
    summary: dict[str, Any] = {}
    health_row = get_integration_health(session, "metabase")
    if is_breaker_open(health_row):
        breaker_message = health_row.status_message or "O circuito de proteção do Metabase está aberto."
        logger.warning("metabase sync skipped because breaker is open instance_id=%s base_url=%s", instance.id, instance.base_url)
        job_status = "failed"
        job_error = breaker_message
        job_records = 0
        job_context = {
            **(job_context or {}),
            "skipped": "breaker_open",
            "health_status": health_row.status if health_row is not None else None,
            "breaker_open_until_at": health_row.breaker_open_until_at if health_row is not None else None,
        }
        sync_run.status = "failed"
        sync_run.finished_at = _now()
        sync_run.error_message = breaker_message
        sync_run.summary_json = make_json_safe(job_context)
        instance.last_sync_at = _now()
        instance.last_sync_status = "failed"
        instance.last_sync_message = breaker_message
        session.add(instance)
        session.add(sync_run)
        session.commit()
        return MetabaseSyncRunOut.model_validate(sync_run, from_attributes=True)

    try:
        health_started_at = _now()
        with MetabaseClient(
            MetabaseClientConfig(
                base_url=instance.base_url,
                auth_type=instance.auth_type,
                auth_username=instance.auth_username,
                auth_secret=_instance_secret(instance),
                timeout_seconds=instance.timeout_seconds,
            )
        ) as client:
            client.authenticate()
            collections = _collection_metadata_items(client.list_collections()) if instance.sync_collections else []
            dashboards = client.list_dashboards() if instance.sync_dashboards else []
            cards = client.list_cards() if instance.sync_questions or instance.sync_dashboards else []
            database_metadata_cache: dict[str, dict[str, Any]] = {}
            table_lookup_cache: dict[str, dict[str, dict[str, Any]]] = {}
            collection_objects: dict[str, MetabaseObject] = {}
            for card in cards:
                db_id = card.get("database_id") or card.get("databaseId")
                if db_id is None:
                    continue
                db_key = str(db_id)
                if db_key in database_metadata_cache:
                    continue
                try:
                    database_metadata_cache[db_key] = client.get_database_metadata(db_id)
                except MetabaseClientError:
                    database_metadata_cache[db_key] = {}
                table_lookup_cache[db_key] = _database_table_lookup(database_metadata_cache[db_key])

            card_by_id: dict[str, dict[str, Any]] = {}
            card_links: dict[str, list[MetabaseObjectLink]] = defaultdict(list)
            for collection in collections:
                with session.begin_nested():
                    collection_object = _upsert_collection_object(session, instance, collection)
                    collection_objects[collection_object.external_id] = collection_object
                    collections_count += 1

            for card in cards:
                card_id = _normalize_external_id(card.get("id"))
                try:
                    card_detail = client.get_card(card_id)
                except MetabaseClientError:
                    card_detail = card
                db_id = card_detail.get("database_id") or card_detail.get("databaseId") or card.get("database_id") or card.get("databaseId")
                if db_id is not None:
                    card_detail = dict(card_detail)
                    card_detail["database_metadata"] = database_metadata_cache.get(str(db_id), {})
                card_by_id[card_id] = card_detail
                if not instance.sync_questions:
                    continue
                try:
                    with session.begin_nested():
                        question = _upsert_question_object(session, instance, card_detail)
                        q_refs, _q_sql = _parse_dataset_query(
                            card_detail.get("dataset_query") if isinstance(card_detail.get("dataset_query"), dict) else None
                        )
                        question.referenced_tables_json = _resolve_referenced_tables(
                            q_refs, table_lookup_cache.get(str(db_id), {})
                        )
                        matched, unresolved = _ensure_object_links_for_card(
                            session,
                            instance=instance,
                            object_row=question,
                            card=card,
                            card_detail=card_detail,
                            card_title=question.title,
                        )
                        questions_count += 1
                        links_count += matched
                        unresolved_count += unresolved
                        question_links = session.scalars(
                            select(MetabaseObjectLink).where(MetabaseObjectLink.metabase_object_id == question.id)
                        ).all()
                        card_links[card_id].extend(question_links)
                        collection_id = str(
                            card_detail.get("collection_id")
                            or card_detail.get("collectionId")
                            or card_detail.get("collection")
                            or ""
                        ).strip()
                        if collection_id and collection_id in collection_objects:
                            links_for_collection = _collection_consumes_table_links(
                                session,
                                instance=instance,
                                collection_object=collection_objects[collection_id],
                                table_ids=[link.table_id for link in question_links],
                                reason=f"Collection linked to question {question.title}",
                            )
                            links_count += links_for_collection
                except SQLAlchemyError:
                    session.rollback()
                    warnings_count += 1
                    logger.exception("metabase question sync failed instance_id=%s card_id=%s", instance.id, card_id)
                except Exception:
                    warnings_count += 1
                    logger.exception("metabase question sync failed instance_id=%s card_id=%s", instance.id, card_id)

            for dashboard in dashboards:
                dashboard_id = _normalize_external_id(dashboard.get("id"))
                try:
                    dashboard_detail = client.get_dashboard(dashboard_id)
                except MetabaseClientError:
                    dashboard_detail = dashboard
                if not instance.sync_dashboards:
                    continue
                try:
                    with session.begin_nested():
                        dash = _upsert_dashboard_object(session, instance, dashboard_detail)
                        dash_refs: list[dict[str, Any]] = []
                        dash_seen: set[str] = set()
                        for member_card_id in _extract_cards_from_dashboard(dashboard_detail):
                            member_detail = card_by_id.get(_normalize_external_id(member_card_id))
                            if not isinstance(member_detail, dict):
                                continue
                            member_db_id = (
                                member_detail.get("database_id")
                                or member_detail.get("databaseId")
                            )
                            member_refs, _member_sql = _parse_dataset_query(
                                member_detail.get("dataset_query")
                                if isinstance(member_detail.get("dataset_query"), dict)
                                else None
                            )
                            for entry in _resolve_referenced_tables(
                                member_refs, table_lookup_cache.get(str(member_db_id), {})
                            ):
                                key = str(entry.get("full_name") or "").lower()
                                if key and key not in dash_seen:
                                    dash_seen.add(key)
                                    dash_refs.append(entry)
                        dash.referenced_tables_json = dash_refs
                        matched, unresolved = _dashboard_consumes_question_links(
                            session,
                            instance=instance,
                            dashboard_object=dash,
                            dashboard=dashboard_detail,
                            card_links=card_links,
                        )
                        dashboards_count += 1
                        links_count += matched
                        unresolved_count += unresolved
                        collection_id = str(
                            dashboard_detail.get("collection_id")
                            or dashboard_detail.get("collectionId")
                            or dashboard_detail.get("collection")
                            or ""
                        ).strip()
                        if collection_id and collection_id in collection_objects:
                            related_table_ids = [
                                link.table_id
                                for card_id in _extract_cards_from_dashboard(dashboard_detail)
                                for link in card_links.get(card_id, [])
                            ]
                            links_for_collection = _collection_consumes_table_links(
                                session,
                                instance=instance,
                                collection_object=collection_objects[collection_id],
                                table_ids=related_table_ids,
                                reason=f"Collection linked to dashboard {dash.title}",
                            )
                            links_count += links_for_collection
                except SQLAlchemyError:
                    session.rollback()
                    warnings_count += 1
                    logger.exception("metabase dashboard sync failed instance_id=%s dashboard_id=%s", instance.id, dashboard_id)
                except Exception:
                    warnings_count += 1
                    logger.exception("metabase dashboard sync failed instance_id=%s dashboard_id=%s", instance.id, dashboard_id)

            summary = {
                "dashboards": dashboards_count,
                "questions": questions_count,
                "collections": collections_count,
                "links": links_count,
                "unresolved": unresolved_count,
                "warnings": warnings_count,
            }
            job_records = dashboards_count + questions_count + collections_count + links_count
            job_context = {
                **(job_context or {}),
                **summary,
            }
            instance.last_sync_at = _now()
            instance.last_sync_status = "success"
            instance.last_sync_message = None
            instance.last_sync_dashboards = dashboards_count
            instance.last_sync_questions = questions_count
            instance.last_sync_collections = collections_count
            instance.last_sync_links = links_count
            instance.last_sync_unresolved = unresolved_count
            instance.last_sync_warnings = warnings_count
            sync_run.status = "success"
            sync_run.finished_at = _now()
            sync_run.dashboards_count = dashboards_count
            sync_run.questions_count = questions_count
            sync_run.collections_count = collections_count
            sync_run.links_count = links_count
            sync_run.unresolved_count = unresolved_count
            sync_run.warnings_count = warnings_count
            sync_run.summary_json = make_json_safe(summary)
            if dashboards_count == 0 and questions_count == 0 and collections_count == 0:
                health_status = "empty"
                health_message = "A integração está conectada, aguardando artefatos sincronizados."
                health_category = "consumption"
            elif unresolved_count > 0 or warnings_count > 0:
                health_status = "degraded"
                health_message = "Sincronização parcial detectada."
                health_category = "sync"
            else:
                health_status = "healthy"
                health_message = "Sincronização do Metabase concluída com sucesso."
                health_category = "operation"
            health_snapshot = _metabase_sync_health_snapshot(
                instance=instance,
                health_row=health_row,
                status=health_status,
                status_message=health_message,
                category=health_category,
                checked_at=health_started_at,
                latency_ms=int(((_now() - health_started_at).total_seconds()) * 1000),
                details_json={
                    "sync_status": "success",
                    "last_sync_at": instance.last_sync_at,
                    "last_sync_status": instance.last_sync_status,
                    "last_sync_message": instance.last_sync_message,
                    "dashboards_count": dashboards_count,
                    "questions_count": questions_count,
                    "collections_count": collections_count,
                    "direct_links_count": links_count,
                    "total_links_count": links_count,
                    "unresolved_count": unresolved_count,
                    "warnings_count": warnings_count,
                },
            )
            health_snapshot = close_breaker(health_snapshot)
            upsert_integration_health(session, health_snapshot)
            from t2c_data.features.metabase.impact import sync_metabase_impact_index

            try:
                with session.begin_nested():
                    sync_metabase_impact_index(session, instance.id, commit=False)
            except Exception:
                logger.exception("metabase impact sync skipped instance_id=%s", instance.id)
            try:
                from t2c_data.features.lineage.metabase_bridge import sync_metabase_lineage

                lineage_summary = sync_metabase_lineage(session, instance=instance, commit=False)
                logger.info(
                    "metabase lineage bridge instance_id=%s artifacts=%s edges=%s",
                    instance.id,
                    lineage_summary.get("artifacts"),
                    lineage_summary.get("edges_created"),
                )
            except Exception:
                logger.exception("metabase lineage bridge skipped instance_id=%s", instance.id)
            session.add(instance)
            session.add(sync_run)
            session.commit()
            logger.info(
                "metabase sync completed instance_id=%s dashboards=%s questions=%s collections=%s links=%s unresolved=%s warnings=%s",
                instance.id,
                dashboards_count,
                questions_count,
                collections_count,
                links_count,
                unresolved_count,
                warnings_count,
            )
    except MetabaseClientError as exc:
        session.rollback()
        job_status = "failed"
        job_error = str(exc)
        job_records = 0
        job_context = {
            **(job_context or {}),
            "error": str(exc),
        }
        health_row = get_integration_health(session, "metabase")
        instance.last_sync_at = _now()
        instance.last_sync_status = "failed"
        instance.last_sync_message = str(exc)
        sync_run.status = "failed"
        sync_run.finished_at = _now()
        sync_run.error_message = str(exc)
        sync_run.summary_json = make_json_safe(summary or {"error": str(exc)})
        classification = classify_integration_issue(exc, integration_name="metabase", phase="sync")
        current_failures = (health_row.consecutive_failures if health_row is not None else 0) + 1
        health_snapshot = _metabase_sync_health_snapshot(
            instance=instance,
            health_row=health_row,
            status=classification["status"],
            status_message=classification["message"],
            category=classification["category"],
            checked_at=_now(),
            last_failure_at=_now(),
            consecutive_failures=current_failures,
            failure_count=(health_row.failure_count if health_row is not None else 0) + 1,
            error_type=classification["error_type"],
            error_summary=classification["message"],
            details_json=make_json_safe(summary or {"error": str(exc)}),
        )
        if classification["retryable"]:
            health_snapshot = open_breaker(
                health_snapshot,
                threshold=DEFAULT_BREAKER_THRESHOLD,
                open_seconds=DEFAULT_BREAKER_OPEN_SECONDS,
            )
        else:
            health_snapshot = close_breaker(health_snapshot)
        upsert_integration_health(session, health_snapshot)
        session.add(instance)
        session.add(sync_run)
        session.commit()
        logger.warning("metabase sync failed instance_id=%s base_url=%s error=%s", instance.id, instance.base_url, exc)
    except SQLAlchemyError as exc:
        session.rollback()
        job_status = "failed"
        job_error = str(exc)
        job_records = 0
        job_context = {
            **(job_context or {}),
            "error": str(exc),
        }
        health_row = get_integration_health(session, "metabase")
        instance.last_sync_at = _now()
        instance.last_sync_status = "failed"
        instance.last_sync_message = str(exc)
        sync_run.status = "failed"
        sync_run.finished_at = _now()
        sync_run.error_message = str(exc)
        sync_run.summary_json = make_json_safe(summary or {"error": str(exc)})
        classification = classify_integration_issue(exc, integration_name="metabase", phase="sync")
        current_failures = (health_row.consecutive_failures if health_row is not None else 0) + 1
        health_snapshot = _metabase_sync_health_snapshot(
            instance=instance,
            health_row=health_row,
            status=classification["status"],
            status_message=classification["message"],
            category=classification["category"],
            checked_at=_now(),
            last_failure_at=_now(),
            consecutive_failures=current_failures,
            failure_count=(health_row.failure_count if health_row is not None else 0) + 1,
            error_type=classification["error_type"],
            error_summary=classification["message"],
            details_json=make_json_safe(summary or {"error": str(exc)}),
        )
        if classification["retryable"]:
            health_snapshot = open_breaker(
                health_snapshot,
                threshold=DEFAULT_BREAKER_THRESHOLD,
                open_seconds=DEFAULT_BREAKER_OPEN_SECONDS,
            )
        else:
            health_snapshot = close_breaker(health_snapshot)
        upsert_integration_health(session, health_snapshot)
        session.add(instance)
        session.add(sync_run)
        session.commit()
        logger.exception("metabase sync db failure instance_id=%s base_url=%s", instance.id, instance.base_url)
    except Exception as exc:
        session.rollback()
        job_status = "failed"
        job_error = str(exc)
        job_records = 0
        job_context = {
            **(job_context or {}),
            "error": str(exc),
        }
        health_row = get_integration_health(session, "metabase")
        instance.last_sync_at = _now()
        instance.last_sync_status = "failed"
        instance.last_sync_message = str(exc)
        sync_run.status = "failed"
        sync_run.finished_at = _now()
        sync_run.error_message = str(exc)
        sync_run.summary_json = make_json_safe(summary or {"error": str(exc)})
        classification = classify_integration_issue(exc, integration_name="metabase", phase="sync")
        current_failures = (health_row.consecutive_failures if health_row is not None else 0) + 1
        health_snapshot = _metabase_sync_health_snapshot(
            instance=instance,
            health_row=health_row,
            status=classification["status"],
            status_message=classification["message"],
            category=classification["category"],
            checked_at=_now(),
            last_failure_at=_now(),
            consecutive_failures=current_failures,
            failure_count=(health_row.failure_count if health_row is not None else 0) + 1,
            error_type=classification["error_type"],
            error_summary=classification["message"],
            details_json=make_json_safe(summary or {"error": str(exc)}),
        )
        if classification["retryable"]:
            health_snapshot = open_breaker(
                health_snapshot,
                threshold=DEFAULT_BREAKER_THRESHOLD,
                open_seconds=DEFAULT_BREAKER_OPEN_SECONDS,
            )
        else:
            health_snapshot = close_breaker(health_snapshot)
        upsert_integration_health(session, health_snapshot)
        session.add(instance)
        session.add(sync_run)
        session.commit()
        logger.exception("metabase sync unexpected failure instance_id=%s base_url=%s", instance.id, instance.base_url)

    try:
        return MetabaseSyncRunOut.model_validate(sync_run, from_attributes=True)
    finally:
        if integration_job is not None:
            finish_integration_job_record(
                session,
                integration_job,
                status=job_status,
                records_processed=job_records,
                error=job_error,
                context_json=job_context,
                result_summary_json=summary or (job_context if isinstance(job_context, dict) else None),
                progress_pct=100.0,
            )
        else:
            finish_integration_job(
                session,
                job_handle,
                status=job_status,
                records_processed=job_records,
                error=job_error,
                context_json=job_context,
            )
        if audit_kwargs is not None:
            try:
                write_audit_log_sync(
                    session,
                    action="integration.metabase.sync_now",
                    entity_type="metabase_instance",
                    entity_id=instance_id_value,
                    metadata={
                        "instance_id": instance_id_value,
                        "instance_name": instance_name,
                        "force": force,
                        "job_status": job_status,
                        "job_records": job_records,
                        "job_error": job_error,
                        "sync_run_id": sync_run.id,
                        "sync_run_status": sync_run.status,
                    },
                    **audit_kwargs,
                )
                session.commit()
            except Exception:  # noqa: BLE001
                session.rollback()
                logger.exception("metabase sync audit log failed instance_id=%s", instance_id_value)


def _metabase_instance_or_none(session: Session) -> MetabaseInstance | None:
    return session.scalar(select(MetabaseInstance).where(MetabaseInstance.enabled.is_(True)).order_by(MetabaseInstance.updated_at.desc(), MetabaseInstance.id.desc()))


def get_table_metabase_consumption(session: Session, table_id: int) -> MetabaseConsumptionSummaryOut:
    table = session.get(TableEntity, table_id)
    if table is None:
        logger.info("metabase consumption requested for missing table_id=%s", table_id)
        return MetabaseConsumptionSummaryOut(
            table_id=table_id,
            table_fqn=str(table_id),
            available=False,
            configured=False,
            enabled=False,
            instance_id=None,
            instance_name=None,
            instance_base_url=None,
            message="Tabela não encontrada.",
        )
    table_fqn = f"{table.schema.database.datasource.name}.{table.schema.database.name}.{table.schema.name}.{table.name}"
    ensure_metabase_instance_from_settings(session)
    instance = _metabase_instance_or_none(session)
    if instance is None:
        logger.info("metabase consumption requested table_id=%s without enabled instance", table_id)
        return MetabaseConsumptionSummaryOut(
            table_id=table.id,
            table_fqn=table_fqn,
            available=False,
            configured=False,
            enabled=False,
            instance_id=None,
            instance_name=None,
            instance_base_url=None,
            message="Nenhuma instância do Metabase está configurada.",
        )

    dashboards = _consume_object_link_items(session, table_id=table.id, object_type="dashboard")
    questions = _consume_object_link_items(session, table_id=table.id, object_type="question")
    collections = _consume_collection_items(session, table_id=table.id)
    all_items = [*dashboards, *questions, *collections]
    direct_count = sum(1 for item in all_items if item.match_method in {"confirmed", "direct", "sql", "inferred"})
    indirect_count = sum(1 for item in all_items if item.match_method in {"indirect_view", "indirect_lineage", "lineage_indirect"})
    if direct_count > 0 and indirect_count > 0:
        match_state = "mixed"
    elif indirect_count > 0:
        match_state = "indirect"
    elif direct_count > 0:
        match_state = "direct"
    else:
        match_state = "none"
    logger.info(
        "metabase consumption resolved table_id=%s dashboards=%s questions=%s collections=%s direct=%s indirect=%s instance_id=%s",
        table.id,
        len(dashboards),
        len(questions),
        len(collections),
        direct_count,
        indirect_count,
        instance.id,
    )
    counts = Counter(item.confidence_level for item in all_items)
    return MetabaseConsumptionSummaryOut(
        table_id=table.id,
        table_fqn=table_fqn,
        available=True,
        configured=True,
        enabled=instance.enabled,
        instance_id=instance.id,
        instance_name=instance.name,
        instance_base_url=instance.base_url,
        message=instance.last_sync_message,
        last_sync_at=instance.last_sync_at,
        last_sync_status=instance.last_sync_status,
        last_sync_message=instance.last_sync_message,
        dashboards_count=len(dashboards),
        questions_count=len(questions),
        collections_count=len(collections),
        confirmed_count=int(counts.get("confirmed", 0)),
        inferred_count=int(counts.get("inferred", 0)),
        partial_count=int(counts.get("partial", 0)),
        direct_count=direct_count,
        indirect_count=indirect_count,
        match_state=match_state,
        unresolved_count=instance.last_sync_unresolved,
        dashboards=dashboards,
        questions=questions,
        collections=collections,
    )


__all__ = [
    "create_metabase_instance",
    "get_metabase_instance",
    "get_table_metabase_consumption",
    "list_metabase_instances",
    "list_metabase_sync_runs",
    "serialize_metabase_instance",
    "run_metabase_instance_sync",
    "update_metabase_instance",
]
