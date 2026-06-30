from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from t2c_data.models.catalog import ColumnEntity, TableEntity
from t2c_data.models.metabase import MetabaseInstance, MetabaseObject, MetabaseObjectLink
from t2c_data.models.metabase_impact import MetabaseAsset, MetabaseFieldDependency, MetabaseImpactSnapshot, MetabaseTableDependency
from t2c_data.schemas.metabase import MetabaseImpactDependencyOut, MetabaseImpactSummaryOut
from t2c_data.features.metabase.bootstrap import ensure_metabase_instance_from_settings

logger = logging.getLogger(__name__)


_DASHBOARD_TYPES = {"dashboard"}
_QUESTION_TYPES = {"question"}
_MODEL_TYPES = {"model"}
_IMPACT_ASSET_TYPES = _DASHBOARD_TYPES | _QUESTION_TYPES | _MODEL_TYPES


@dataclass(slots=True)
class _ImpactLinkCandidate:
    link: MetabaseObjectLink
    object: MetabaseObject


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_risk(value: str | None) -> str:
    normalized = (value or "").strip().lower()
    if normalized in {"high", "medium", "low", "none"}:
        return normalized
    return "medium"


def _confidence_to_risk(confidence: str | None) -> str:
    normalized = (confidence or "").strip().lower()
    if normalized in {"confirmed", "high"}:
        return "high"
    if normalized in {"inferred", "medium"}:
        return "medium"
    if normalized in {"partial", "low"}:
        return "low"
    return "medium"


def _dependency_type_from_method(match_method: str | None) -> str:
    normalized = (match_method or "").strip().lower()
    if normalized in {"confirmed", "direct"}:
        return "direct"
    if normalized == "sql":
        return "sql_native"
    if normalized in {"indirect_view", "indirect_lineage", "lineage_indirect"}:
        return "indirect"
    if normalized == "dashboard_card":
        return "dashboard_card"
    if normalized == "collection_membership":
        return "collection_membership"
    return "unknown"


def _risk_profile(dependency_type: str, asset_type: str, confidence_level: str) -> tuple[str, str]:
    normalized_asset_type = (asset_type or "").strip().lower()
    normalized_dependency_type = (dependency_type or "").strip().lower()
    confidence_risk = _confidence_to_risk(confidence_level)

    if normalized_asset_type not in _IMPACT_ASSET_TYPES:
        return "low", "low"
    if normalized_dependency_type in {"direct", "sql_native"}:
        return "high", "high"
    if normalized_dependency_type in {"dashboard_card", "indirect"}:
        if confidence_risk == "low":
            return "medium", "medium"
        return "high", "medium"
    if normalized_dependency_type == "collection_membership":
        return "low", "low"
    return "medium", "medium"


def _priority_for_link(candidate: _ImpactLinkCandidate) -> tuple[int, int, int]:
    method = (candidate.link.match_method or "").strip().lower()
    if method in {"confirmed", "direct"}:
        group = 0
    elif method == "sql":
        group = 1
    elif method in {"indirect_view", "indirect_lineage", "lineage_indirect"}:
        group = 2
    elif method == "dashboard_card":
        group = 3
    elif method == "collection_membership":
        group = 4
    else:
        group = 5
    return (group, candidate.object.id, candidate.link.id)


def _table_fqn(table: TableEntity) -> str:
    schema = table.schema
    database = schema.database
    datasource = database.datasource
    return f"{datasource.name}.{database.name}.{schema.name}.{table.name}"


def _object_asset_type(object_type: str | None) -> str:
    normalized = (object_type or "").strip().lower()
    if normalized in _IMPACT_ASSET_TYPES:
        return normalized
    return normalized or "unknown"


def _upsert_asset(
    session: Session,
    *,
    instance_id: int,
    metabase_object: MetabaseObject,
    now: datetime,
) -> MetabaseAsset:
    external_id = str(metabase_object.external_id)
    asset = session.scalar(
        select(MetabaseAsset).where(
            MetabaseAsset.instance_id == instance_id,
            MetabaseAsset.metabase_object_id == metabase_object.id,
        )
    )
    asset_type = _object_asset_type(metabase_object.object_type)
    if asset is None:
        asset = MetabaseAsset(
            instance_id=instance_id,
            metabase_object_id=metabase_object.id,
            metabase_id=external_id,
            asset_type=asset_type,
            name=metabase_object.title,
            collection_name=metabase_object.collection_name,
            collection_external_id=metabase_object.collection_external_id,
            url=metabase_object.url,
            archived=bool(metabase_object.archived),
            source_updated_at=metabase_object.remote_updated_at,
            last_synced_at=now,
            last_verified_at=now,
            metadata_json=metabase_object.metadata_json,
        )
        session.add(asset)
    else:
        asset.metabase_id = external_id
        asset.asset_type = asset_type
        asset.name = metabase_object.title
        asset.collection_name = metabase_object.collection_name
        asset.collection_external_id = metabase_object.collection_external_id
        asset.url = metabase_object.url
        asset.archived = bool(metabase_object.archived)
        asset.source_updated_at = metabase_object.remote_updated_at
        asset.last_synced_at = now
        asset.last_verified_at = now
        asset.metadata_json = metabase_object.metadata_json
    return asset


def _field_name_for_link(session: Session, link: MetabaseObjectLink, column_name_cache: dict[int, str]) -> str | None:
    if link.source_column_name and link.source_column_name.strip():
        return link.source_column_name.strip()
    if link.column_id is None:
        return None
    if link.column_id not in column_name_cache:
        column_name = session.scalar(select(ColumnEntity.name).where(ColumnEntity.id == link.column_id))
        if column_name:
            column_name_cache[link.column_id] = str(column_name)
    return column_name_cache.get(link.column_id)


def sync_metabase_impact_index(session: Session, instance_id: int, *, commit: bool = False) -> dict[str, int]:
    instance = session.get(MetabaseInstance, instance_id)
    if instance is None:
        raise KeyError(instance_id)

    now = _now()
    objects = session.scalars(
        select(MetabaseObject).where(MetabaseObject.instance_id == instance.id, MetabaseObject.archived.is_(False))
    ).all()
    links = session.execute(
        select(MetabaseObjectLink, MetabaseObject)
        .join(MetabaseObject, MetabaseObjectLink.metabase_object_id == MetabaseObject.id)
        .where(MetabaseObjectLink.instance_id == instance.id, MetabaseObjectLink.is_active.is_(True))
        .order_by(MetabaseObject.updated_at.desc(), MetabaseObject.id.desc(), MetabaseObjectLink.id.asc())
    ).all()

    assets_by_object_id: dict[int, MetabaseAsset] = {}
    for metabase_object in objects:
        asset = _upsert_asset(session, instance_id=instance.id, metabase_object=metabase_object, now=now)
        assets_by_object_id[metabase_object.id] = asset

    session.flush()

    current_dependency_keys: set[tuple[int, int]] = set()
    current_field_dependency_keys: set[tuple[int, int | None, str, int]] = set()
    column_name_cache: dict[int, str] = {}
    grouped_links: dict[tuple[int, int], list[_ImpactLinkCandidate]] = defaultdict(list)
    for link, metabase_object in links:
        grouped_links[(metabase_object.id, link.table_id)].append(_ImpactLinkCandidate(link=link, object=metabase_object))

    for (object_id, table_id), candidates in grouped_links.items():
        best = min(candidates, key=_priority_for_link)
        asset = assets_by_object_id.get(object_id)
        if asset is None:
            continue
        if asset.asset_type not in _IMPACT_ASSET_TYPES:
            continue
        dependency_type = _dependency_type_from_method(best.link.match_method)
        confidence_level = _confidence_to_risk(best.link.confidence_level)
        break_risk_on_drop, break_risk_on_change = _risk_profile(dependency_type, asset.asset_type, confidence_level)
        details_json = {
            "match_method": best.link.match_method,
            "confidence_reason": best.link.confidence_reason,
            "source_table_name": best.link.source_table_name,
            "source_schema_name": best.link.source_schema_name,
            "source_database_name": best.link.source_database_name,
            "source_column_name": best.link.source_column_name,
            "link_count": len(candidates),
            "table_link_ids": [candidate.link.id for candidate in candidates],
        }
        dependency = session.scalar(
            select(MetabaseTableDependency).where(
                MetabaseTableDependency.instance_id == instance.id,
                MetabaseTableDependency.table_id == table_id,
                MetabaseTableDependency.metabase_asset_id == asset.id,
            )
        )
        if dependency is None:
            dependency = MetabaseTableDependency(
                instance_id=instance.id,
                table_id=table_id,
                metabase_asset_id=asset.id,
                dependency_type=dependency_type,
                confidence_level=confidence_level,
                break_risk_on_drop=break_risk_on_drop,
                break_risk_on_change=break_risk_on_change,
                details_json=details_json,
                last_verified_at=now,
                is_active=True,
            )
            session.add(dependency)
        else:
            dependency.dependency_type = dependency_type
            dependency.confidence_level = confidence_level
            dependency.break_risk_on_drop = break_risk_on_drop
            dependency.break_risk_on_change = break_risk_on_change
            dependency.details_json = details_json
            dependency.last_verified_at = now
            dependency.is_active = True
        current_dependency_keys.add((table_id, asset.id))

        field_name = _field_name_for_link(session, best.link, column_name_cache)
        if field_name:
            field_dependency = session.scalar(
                select(MetabaseFieldDependency).where(
                    MetabaseFieldDependency.instance_id == instance.id,
                    MetabaseFieldDependency.table_id == table_id,
                    MetabaseFieldDependency.column_id == best.link.column_id,
                    MetabaseFieldDependency.field_name == field_name,
                    MetabaseFieldDependency.metabase_asset_id == asset.id,
                )
            )
            if field_dependency is None:
                field_dependency = MetabaseFieldDependency(
                    instance_id=instance.id,
                    table_id=table_id,
                    column_id=best.link.column_id,
                    field_name=field_name,
                    metabase_asset_id=asset.id,
                    dependency_type=dependency_type,
                    confidence_level=confidence_level,
                    break_risk_on_drop=break_risk_on_drop,
                    break_risk_on_change=break_risk_on_change,
                    details_json=details_json,
                    last_verified_at=now,
                    is_active=True,
                )
                session.add(field_dependency)
            else:
                field_dependency.dependency_type = dependency_type
                field_dependency.confidence_level = confidence_level
                field_dependency.break_risk_on_drop = break_risk_on_drop
                field_dependency.break_risk_on_change = break_risk_on_change
                field_dependency.details_json = details_json
                field_dependency.last_verified_at = now
                field_dependency.is_active = True
            current_field_dependency_keys.add((table_id, best.link.column_id, field_name, asset.id))

    active_dependency_rows = session.execute(
        select(MetabaseTableDependency.table_id, MetabaseTableDependency.metabase_asset_id)
        .where(
            MetabaseTableDependency.instance_id == instance.id,
            MetabaseTableDependency.is_active.is_(True),
        )
    ).all()
    for table_id, asset_id in active_dependency_rows:
        if (int(table_id), int(asset_id)) not in current_dependency_keys:
            dependency = session.scalar(
                select(MetabaseTableDependency).where(
                    MetabaseTableDependency.instance_id == instance.id,
                    MetabaseTableDependency.table_id == table_id,
                    MetabaseTableDependency.metabase_asset_id == asset_id,
                )
            )
            if dependency is not None:
                dependency.is_active = False
                dependency.last_verified_at = now

    active_field_rows = session.execute(
        select(
            MetabaseFieldDependency.table_id,
            MetabaseFieldDependency.column_id,
            MetabaseFieldDependency.field_name,
            MetabaseFieldDependency.metabase_asset_id,
        ).where(
            MetabaseFieldDependency.instance_id == instance.id,
            MetabaseFieldDependency.is_active.is_(True),
        )
    ).all()
    for table_id, column_id, field_name, asset_id in active_field_rows:
        key = (int(table_id), column_id if column_id is None else int(column_id), str(field_name), int(asset_id))
        if key not in current_field_dependency_keys:
            field_dependency = session.scalar(
                select(MetabaseFieldDependency).where(
                    MetabaseFieldDependency.instance_id == instance.id,
                    MetabaseFieldDependency.table_id == table_id,
                    MetabaseFieldDependency.column_id == column_id,
                    MetabaseFieldDependency.field_name == field_name,
                    MetabaseFieldDependency.metabase_asset_id == asset_id,
                )
                )
            if field_dependency is not None:
                field_dependency.is_active = False
                field_dependency.last_verified_at = now

    session.flush()

    touched_tables = set(table_id for table_id, _ in current_dependency_keys)
    touched_tables.update(
        int(table_id)
        for table_id in session.scalars(
            select(MetabaseImpactSnapshot.table_id).where(MetabaseImpactSnapshot.instance_id == instance.id)
        ).all()
    )

    for table_id in touched_tables:
        active_rows = session.execute(
            select(MetabaseTableDependency, MetabaseAsset)
            .join(MetabaseAsset, MetabaseTableDependency.metabase_asset_id == MetabaseAsset.id)
            .where(
                MetabaseTableDependency.instance_id == instance.id,
                MetabaseTableDependency.table_id == table_id,
                MetabaseTableDependency.is_active.is_(True),
                MetabaseAsset.asset_type.in_(list(_IMPACT_ASSET_TYPES)),
            )
        ).all()
        dashboard_count = 0
        question_count = 0
        model_count = 0
        max_risk_drop = "none"
        max_risk_change = "none"
        for dependency, asset in active_rows:
            if asset.asset_type == "dashboard":
                dashboard_count += 1
            elif asset.asset_type == "question":
                question_count += 1
            elif asset.asset_type == "model":
                model_count += 1
            max_risk_drop = _max_risk(max_risk_drop, dependency.break_risk_on_drop)
            max_risk_change = _max_risk(max_risk_change, dependency.break_risk_on_change)

        asset_count = dashboard_count + question_count + model_count
        snapshot = MetabaseImpactSnapshot(
            instance_id=instance.id,
            table_id=table_id,
            dashboard_count=dashboard_count,
            question_count=question_count,
            model_count=model_count,
            asset_count=asset_count,
            break_risk_on_drop=max_risk_drop,
            break_risk_on_change=max_risk_change,
            last_verified_at=now,
            summary_json={
                "dashboard_count": dashboard_count,
                "question_count": question_count,
                "model_count": model_count,
                "asset_count": asset_count,
                "break_risk_on_drop": max_risk_drop,
                "break_risk_on_change": max_risk_change,
            },
        )
        session.add(snapshot)

    if commit:
        session.commit()
    else:
        session.flush()
    return {
        "assets": len(assets_by_object_id),
        "dependencies": len(current_dependency_keys),
        "field_dependencies": len(current_field_dependency_keys),
        "snapshots": len(touched_tables),
    }


def _max_risk(current: str, candidate: str) -> str:
    order = {"none": 0, "low": 1, "medium": 2, "high": 3}
    current_level = order.get(_normalize_risk(current), 0)
    candidate_level = order.get(_normalize_risk(candidate), 0)
    return candidate if candidate_level >= current_level else current


def _latest_snapshot_for_table(session: Session, *, instance_id: int, table_id: int) -> MetabaseImpactSnapshot | None:
    return session.scalar(
        select(MetabaseImpactSnapshot)
        .where(
            MetabaseImpactSnapshot.instance_id == instance_id,
            MetabaseImpactSnapshot.table_id == table_id,
        )
        .order_by(MetabaseImpactSnapshot.last_verified_at.desc().nullslast(), MetabaseImpactSnapshot.created_at.desc(), MetabaseImpactSnapshot.id.desc())
    )


def get_table_metabase_impact(session: Session, table_id: int) -> MetabaseImpactSummaryOut:
    table = session.get(TableEntity, table_id)
    if table is None:
        return MetabaseImpactSummaryOut(
            table_id=table_id,
            table_fqn=str(table_id),
            available=False,
            configured=False,
            enabled=False,
            message="Tabela não encontrada.",
        )

    table_fqn = _table_fqn(table)
    ensure_metabase_instance_from_settings(session)
    instance = session.scalar(
        select(MetabaseInstance)
        .where(MetabaseInstance.enabled.is_(True))
        .order_by(MetabaseInstance.updated_at.desc(), MetabaseInstance.id.desc())
    )
    if instance is None:
        return MetabaseImpactSummaryOut(
            table_id=table.id,
            table_fqn=table_fqn,
            available=False,
            configured=False,
            enabled=False,
            message="Nenhuma instância do Metabase está configurada.",
        )

    snapshot = _latest_snapshot_for_table(session, instance_id=instance.id, table_id=table.id)
    rows = session.execute(
        select(MetabaseTableDependency, MetabaseAsset)
        .join(MetabaseAsset, MetabaseTableDependency.metabase_asset_id == MetabaseAsset.id)
        .where(
            MetabaseTableDependency.instance_id == instance.id,
            MetabaseTableDependency.table_id == table.id,
            MetabaseTableDependency.is_active.is_(True),
            MetabaseAsset.asset_type.in_(list(_IMPACT_ASSET_TYPES)),
        )
        .order_by(MetabaseAsset.asset_type.asc(), MetabaseAsset.name.asc(), MetabaseTableDependency.id.asc())
    ).all()

    dependencies: list[MetabaseImpactDependencyOut] = []
    for dependency, asset in rows:
        dependencies.append(
            MetabaseImpactDependencyOut(
                metabase_asset_id=asset.id,
                metabase_id=asset.metabase_id,
                asset_type=asset.asset_type,  # type: ignore[arg-type]
                name=asset.name,
                collection_name=asset.collection_name,
                url=asset.url,
                dependency_type=dependency.dependency_type,  # type: ignore[arg-type]
                confidence_level=dependency.confidence_level,  # type: ignore[arg-type]
                break_risk_on_drop=dependency.break_risk_on_drop,  # type: ignore[arg-type]
                break_risk_on_change=dependency.break_risk_on_change,  # type: ignore[arg-type]
                last_verified_at=dependency.last_verified_at,
                details_json=dependency.details_json,
            )
        )

    dashboard_count = sum(1 for item in dependencies if item.asset_type == "dashboard")
    question_count = sum(1 for item in dependencies if item.asset_type == "question")
    model_count = sum(1 for item in dependencies if item.asset_type == "model")
    asset_count = dashboard_count + question_count + model_count

    last_verified_at = snapshot.last_verified_at if snapshot is not None else None
    if last_verified_at is None:
        last_verified_at = max((item.last_verified_at for item in dependencies if item.last_verified_at is not None), default=None)

    break_risk_on_drop = snapshot.break_risk_on_drop if snapshot is not None else "none"
    break_risk_on_change = snapshot.break_risk_on_change if snapshot is not None else "none"
    if snapshot is None:
        for item in dependencies:
            break_risk_on_drop = _max_risk(break_risk_on_drop, item.break_risk_on_drop)
            break_risk_on_change = _max_risk(break_risk_on_change, item.break_risk_on_change)

    message = None
    available = True
    if not dependencies:
        message = "Sem dependências indexadas para esta tabela."

    return MetabaseImpactSummaryOut(
        table_id=table.id,
        table_fqn=table_fqn,
        available=available,
        configured=True,
        enabled=instance.enabled,
        instance_id=instance.id,
        instance_name=instance.name,
        instance_base_url=instance.base_url,
        message=message,
        last_verified_at=last_verified_at,
        dashboard_count=dashboard_count,
        question_count=question_count,
        model_count=model_count,
        asset_count=asset_count,
        break_risk_on_drop=break_risk_on_drop,  # type: ignore[arg-type]
        break_risk_on_change=break_risk_on_change,  # type: ignore[arg-type]
        dependencies=dependencies,
    )


__all__ = ["get_table_metabase_impact", "sync_metabase_impact_index"]
