from __future__ import annotations

from datetime import datetime, timezone
from urllib.parse import quote

from sqlalchemy import delete, desc, func, select
from sqlalchemy.orm import Session

from t2c_data.features.search.global_search import CATEGORY_LABELS, _apply_visibility, _load_records, normalize_search_text
from t2c_data.models.auth import User
from t2c_data.models.catalog import TableEntity
from t2c_data.models.platform import PlatformUsageEvent
from t2c_data.models.search import SearchFavoriteAsset, SearchQueryHistory, SearchResultClick

MAX_RECENT_PER_USER = 12
CRITICALITY_LABELS = {"critical": "Crítica", "high": "Alta", "medium": "Média", "low": "Baixa"}

def track_recent_query(session: Session, *, user: User, query: str) -> None:
    normalized = normalize_search_text(query)
    raw_query = query.strip()
    if len(normalized) < 2 or not raw_query:
        return

    try:
        existing = session.scalar(
            select(SearchQueryHistory).where(
                SearchQueryHistory.user_id == user.id,
                SearchQueryHistory.normalized_query == normalized,
            )
        )
    except Exception:  # noqa: BLE001
        return
    now = datetime.now(timezone.utc)
    if existing:
        existing.raw_query = raw_query
        existing.search_count += 1
        existing.last_searched_at = now
        session.add(existing)
    else:
        session.add(
            SearchQueryHistory(
                user_id=user.id,
                raw_query=raw_query,
                normalized_query=normalized,
                search_count=1,
                last_searched_at=now,
            )
        )
    session.flush()

    try:
        stale_ids = session.scalars(
            select(SearchQueryHistory.id)
            .where(SearchQueryHistory.user_id == user.id)
            .order_by(SearchQueryHistory.last_searched_at.desc(), SearchQueryHistory.id.desc())
            .offset(MAX_RECENT_PER_USER)
        ).all()
    except Exception:  # noqa: BLE001
        return
    if stale_ids:
        session.execute(delete(SearchQueryHistory).where(SearchQueryHistory.id.in_(stale_ids)))


def track_result_click(
    session: Session,
    *,
    user: User | None,
    entity_type: str,
    entity_id: int,
    query_text: str | None,
    target_url: str | None,
) -> None:
    try:
        session.add(
            SearchResultClick(
                user_id=user.id if user else None,
                entity_type=entity_type,
                entity_id=entity_id,
                query_text=(query_text or "").strip() or None,
                normalized_query=normalize_search_text(query_text),
                target_url=target_url,
            )
        )
    except Exception:  # noqa: BLE001
        return


def get_recent_searches(session: Session, *, user: User, limit: int = 8) -> dict[str, object]:
    try:
        rows = session.scalars(
            select(SearchQueryHistory)
            .where(SearchQueryHistory.user_id == user.id)
            .order_by(SearchQueryHistory.last_searched_at.desc(), SearchQueryHistory.id.desc())
            .limit(limit)
        ).all()
    except Exception:  # noqa: BLE001
        return {"enabled": False, "items": []}
    return {
        "enabled": True,
        "items": [
            {
                "label": row.raw_query,
                "target_url": f"/search?q={quote(row.raw_query)}",
                "entity_type": "recent_query",
                "entity_id": row.id,
                "category": "Recentes",
                "subtitle": f"Usada {row.search_count} vez(es)",
                "context_path": row.last_searched_at.isoformat() if row.last_searched_at else None,
            }
            for row in rows
        ],
    }


def get_popular_results(session: Session, *, limit: int = 8, user: User | None = None) -> dict[str, object]:
    try:
        rows = session.execute(
        select(
            SearchResultClick.entity_type,
            SearchResultClick.entity_id,
            func.count(SearchResultClick.id).label("click_count"),
            func.max(SearchResultClick.created_at).label("last_clicked_at"),
        )
        .group_by(SearchResultClick.entity_type, SearchResultClick.entity_id)
            .order_by(desc("click_count"), desc("last_clicked_at"))
    ).all()
    except Exception:  # noqa: BLE001
        return {"enabled": False, "items": []}
    if not rows:
        return {"enabled": True, "items": []}

    records = _load_records(session)
    if user is not None:
        records = _apply_visibility(session, records, user)
    record_index = {(record.entity_type, record.entity_id): record for record in records}
    items: list[dict[str, object]] = []
    for row in rows:
        record = record_index.get((str(row.entity_type), int(row.entity_id)))
        if not record:
            continue
        items.append(
            {
                "label": record.title,
                "target_url": record.target_url,
                "entity_type": record.entity_type,
                "entity_id": record.entity_id,
                "category": CATEGORY_LABELS.get(record.entity_type, record.entity_type),
                "subtitle": record.subtitle,
                "context_path": record.context_path,
                "description": record.description,
                "count": int(row.click_count or 0),
            }
        )
        if len(items) >= limit:
            break
    return {"enabled": True, "items": items}


def _visible_record_index(session: Session, *, user: User | None = None) -> dict[tuple[str, int], object]:
    records = _load_records(session)
    if user is not None:
        records = _apply_visibility(session, records, user)
    return {(record.entity_type, record.entity_id): record for record in records}


def get_favorite_results(session: Session, *, user: User, limit: int = 12) -> dict[str, object]:
    try:
        rows = session.scalars(
            select(SearchFavoriteAsset)
            .where(SearchFavoriteAsset.user_id == user.id)
            .order_by(SearchFavoriteAsset.updated_at.desc(), SearchFavoriteAsset.id.desc())
            .limit(limit)
        ).all()
    except Exception:  # noqa: BLE001
        return {"enabled": False, "items": []}
    if not rows:
        return {"enabled": True, "items": []}

    record_index = _visible_record_index(session, user=user)
    items: list[dict[str, object]] = []
    for row in rows:
        record = record_index.get((row.entity_type, row.entity_id))
        if record:
            items.append(
                {
                    "label": record.title,
                    "target_url": record.target_url,
                    "entity_type": record.entity_type,
                    "entity_id": record.entity_id,
                    "category": CATEGORY_LABELS.get(record.entity_type, record.entity_type),
                    "subtitle": record.subtitle,
                    "context_path": record.context_path,
                    "description": record.description,
                }
            )
            continue
        if row.entity_type in {"table", "column", "datasource", "database", "schema", "classification", "glossary_term", "tag", "owner"}:
            continue
        items.append(
            {
                "label": row.label,
                "target_url": row.target_url,
                "entity_type": row.entity_type,
                "entity_id": row.entity_id,
                "category": row.category or CATEGORY_LABELS.get(row.entity_type, row.entity_type),
                "subtitle": row.subtitle,
                "context_path": row.context_path,
                "description": None,
            }
        )
    return {"enabled": True, "items": items}


def get_critical_results(session: Session, *, user: User, limit: int = 8) -> dict[str, object]:
    try:
        rows = session.execute(
            select(TableEntity.id, TableEntity.certification_criticality)
            .where(TableEntity.certification_criticality.in_(["critical", "high"]))
            .limit(max(limit * 3, limit))
        ).all()
    except Exception:  # noqa: BLE001
        return {"enabled": False, "items": []}
    if not rows:
        return {"enabled": True, "items": []}

    record_index = _visible_record_index(session, user=user)
    priority = {"critical": 0, "high": 1}
    sorted_rows = sorted(rows, key=lambda row: (priority.get(str(row.certification_criticality), 9), int(row.id)))
    items: list[dict[str, object]] = []
    for row in sorted_rows:
        record = record_index.get(("table", int(row.id)))
        if not record:
            continue
        criticality = str(row.certification_criticality or "")
        label = CRITICALITY_LABELS.get(criticality, criticality or "Não avaliada")
        items.append(
            {
                "label": record.title,
                "target_url": record.target_url,
                "entity_type": record.entity_type,
                "entity_id": record.entity_id,
                "category": "Tabela crítica",
                "subtitle": f"Criticidade {label}",
                "context_path": record.context_path,
                "description": record.description,
            }
        )
        if len(items) >= limit:
            break
    return {"enabled": True, "items": items}


def get_recent_asset_results(session: Session, *, user: User, limit: int = 8) -> dict[str, object]:
    try:
        rows = session.execute(
            select(
                PlatformUsageEvent.entity_type,
                PlatformUsageEvent.entity_id,
                func.max(PlatformUsageEvent.created_at).label("last_viewed_at"),
            )
            .where(
                PlatformUsageEvent.user_id == user.id,
                PlatformUsageEvent.module_name == "explorer",
                PlatformUsageEvent.event_name == "page_view",
                PlatformUsageEvent.entity_type == "table",
                PlatformUsageEvent.entity_id.is_not(None),
            )
            .group_by(PlatformUsageEvent.entity_type, PlatformUsageEvent.entity_id)
            .order_by(desc("last_viewed_at"))
            .limit(max(limit * 3, limit))
        ).all()
    except Exception:  # noqa: BLE001
        return {"enabled": False, "items": []}
    if not rows:
        return {"enabled": True, "items": []}

    record_index = _visible_record_index(session, user=user)
    items: list[dict[str, object]] = []
    for row in rows:
        if row.entity_type is None or row.entity_id is None:
            continue
        record = record_index.get((str(row.entity_type), int(row.entity_id)))
        if not record:
            continue
        items.append(
            {
                "label": record.title,
                "target_url": record.target_url,
                "entity_type": record.entity_type,
                "entity_id": record.entity_id,
                "category": CATEGORY_LABELS.get(record.entity_type, record.entity_type),
                "subtitle": record.subtitle,
                "context_path": record.context_path,
                "description": record.description,
                "count": None,
            }
        )
        if len(items) >= limit:
            break
    return {"enabled": True, "items": items}


def is_favorite_asset(session: Session, *, user: User, entity_type: str, entity_id: int) -> bool:
    try:
        favorite_id = session.scalar(
            select(SearchFavoriteAsset.id).where(
                SearchFavoriteAsset.user_id == user.id,
                SearchFavoriteAsset.entity_type == entity_type,
                SearchFavoriteAsset.entity_id == entity_id,
            )
        )
    except Exception:  # noqa: BLE001
        return False
    return favorite_id is not None


def upsert_favorite_asset(
    session: Session,
    *,
    user: User,
    entity_type: str,
    entity_id: int,
    label: str,
    target_url: str | None = None,
    category: str | None = None,
    subtitle: str | None = None,
    context_path: str | None = None,
    metadata: dict | list | None = None,
) -> None:
    normalized_type = entity_type.strip().lower()
    safe_label = label.strip()
    if not normalized_type or not safe_label:
        raise ValueError("entity_type and label are required")

    now = datetime.now(timezone.utc)
    row = session.scalar(
        select(SearchFavoriteAsset).where(
            SearchFavoriteAsset.user_id == user.id,
            SearchFavoriteAsset.entity_type == normalized_type,
            SearchFavoriteAsset.entity_id == entity_id,
        )
    )
    if row is None:
        row = SearchFavoriteAsset(
            user_id=user.id,
            entity_type=normalized_type,
            entity_id=entity_id,
            label=safe_label,
        )
    row.label = safe_label
    row.target_url = target_url
    row.category = category
    row.subtitle = subtitle
    row.context_path = context_path
    row.metadata_json = metadata
    row.updated_at = now
    session.add(row)
    session.flush()


def delete_favorite_asset(session: Session, *, user: User, entity_type: str, entity_id: int) -> None:
    session.execute(
        delete(SearchFavoriteAsset).where(
            SearchFavoriteAsset.user_id == user.id,
            SearchFavoriteAsset.entity_type == entity_type.strip().lower(),
            SearchFavoriteAsset.entity_id == entity_id,
        )
    )
