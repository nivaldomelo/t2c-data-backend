from __future__ import annotations

from collections import Counter

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from t2c_data.features.lineage.openlineage_support import build_job_summary
from t2c_data.features.lineage.shared import (
    LINEAGE_SUPPORTED_DATABASE_ENGINES,
    canonical_lineage_asset_key,
    infer_engine_from_namespace,
    normalize_database_engine,
    normalized_relation_origin,
    serialize_asset_ref,
)
from t2c_data.features.lineage.visibility import relation_visible_to_user
from t2c_data.features.lineage.versioning import confidence_tier
from t2c_data.models.auth import User
from t2c_data.models.catalog import DataSource
from t2c_data.models.lineage import LineageAsset, LineageRelation
from t2c_data.schemas.lineage import (
    LineageAssetProcessOut,
    LineageAssetSummaryOut,
    LineageGraphEdgeOut,
    LineageGraphNodeOut,
    LineageImpactOut,
)

INTERNAL_LINEAGE_LABEL = "Linhagem interna"


def _collect_relation_trail(
    db: Session,
    *,
    asset_id: int,
    current_user: User | None,
    direction: str,
    max_depth: int,
) -> tuple[list[LineageRelation], list[LineageAsset], int]:
    if max_depth <= 0:
        return [], [], 0

    frontier = {asset_id}
    visited_assets = {asset_id}
    relations_by_id: dict[int, LineageRelation] = {}
    related_assets_by_id: dict[int, LineageAsset] = {}
    total_relations = 0

    for _depth in range(max_depth):
        if not frontier:
            break
        if direction == "upstream":
            stmt = (
                select(LineageRelation)
                .where(LineageRelation.is_active.is_(True), LineageRelation.target_asset_id.in_(sorted(frontier)))
                .order_by(LineageRelation.updated_at.desc(), LineageRelation.id.desc())
            )
        else:
            stmt = (
                select(LineageRelation)
                .where(LineageRelation.is_active.is_(True), LineageRelation.source_asset_id.in_(sorted(frontier)))
                .order_by(LineageRelation.updated_at.desc(), LineageRelation.id.desc())
            )
        relations = db.scalars(stmt).all()
        if current_user is not None:
            relations = [relation for relation in relations if relation_visible_to_user(db, current_user, relation)]

        next_frontier: set[int] = set()
        for relation in relations:
            total_relations += 1
            if relation.id not in relations_by_id:
                relations_by_id[relation.id] = relation
            related_asset = relation.source_asset if direction == "upstream" else relation.target_asset
            if related_asset.id not in related_assets_by_id:
                related_assets_by_id[related_asset.id] = related_asset
            if related_asset.id not in visited_assets:
                next_frontier.add(related_asset.id)
        visited_assets.update(next_frontier)
        frontier = next_frontier

    return list(relations_by_id.values()), list(related_assets_by_id.values()), total_relations


def collect_asset_summary(
    db: Session,
    asset: LineageAsset,
    *,
    current_user: User | None = None,
    max_relations: int | None = None,
    max_depth: int = 1,
) -> LineageAssetSummaryOut:
    if max_depth > 1:
        upstream_relations, upstream_assets, _upstream_relation_total = _collect_relation_trail(
            db,
            asset_id=asset.id,
            current_user=current_user,
            direction="upstream",
            max_depth=max_depth,
        )
        downstream_relations, downstream_assets, _downstream_relation_total = _collect_relation_trail(
            db,
            asset_id=asset.id,
            current_user=current_user,
            direction="downstream",
            max_depth=max_depth,
        )
        upstream_total = len(upstream_assets)
        downstream_total = len(downstream_assets)
    else:
        upstream_stmt = (
            select(LineageRelation)
            .where(LineageRelation.is_active.is_(True), LineageRelation.target_asset_id == asset.id)
            .order_by(LineageRelation.updated_at.desc())
        )
        downstream_stmt = (
            select(LineageRelation)
            .where(LineageRelation.is_active.is_(True), LineageRelation.source_asset_id == asset.id)
            .order_by(LineageRelation.updated_at.desc())
        )
        upstream_total = None
        downstream_total = None
        if max_relations is not None:
            upstream_total = int(
                db.scalar(
                    select(func.count(LineageRelation.id)).where(
                        LineageRelation.is_active.is_(True),
                        LineageRelation.target_asset_id == asset.id,
                    )
                )
                or 0
            )
            downstream_total = int(
                db.scalar(
                    select(func.count(LineageRelation.id)).where(
                        LineageRelation.is_active.is_(True),
                        LineageRelation.source_asset_id == asset.id,
                    )
                )
                or 0
            )
            upstream_stmt = upstream_stmt.limit(max_relations)
            downstream_stmt = downstream_stmt.limit(max_relations)

        upstream_relations = db.scalars(upstream_stmt).all()
        downstream_relations = db.scalars(downstream_stmt).all()
        if current_user is not None:
            upstream_relations = [relation for relation in upstream_relations if relation_visible_to_user(db, current_user, relation)]
            downstream_relations = [relation for relation in downstream_relations if relation_visible_to_user(db, current_user, relation)]

    def _dedupe_assets(items: list[LineageAsset]) -> list[LineageAsset]:
        seen: set[str] = set()
        deduped: list[LineageAsset] = []
        for item in items:
            key = canonical_lineage_asset_key(item)
            if key in seen:
                continue
            seen.add(key)
            deduped.append(item)
        return deduped

    upstream_assets = _dedupe_assets([relation.source_asset for relation in upstream_relations if relation.source_asset.is_active])
    downstream_assets = _dedupe_assets([relation.target_asset for relation in downstream_relations if relation.target_asset.is_active])
    dashboards = [item for item in downstream_assets if item.asset_type in {"dashboard", "question"}]

    process_counter: Counter[tuple[str, str | None, str]] = Counter()
    for relation in [*upstream_relations, *downstream_relations]:
        if relation.process_name:
            process_counter[(relation.process_name, relation.process_type, relation.relation_type)] += 1

    related_processes = [
        LineageAssetProcessOut(
            process_name=name,
            process_type=process_type,
            relation_type=relation_type,
            count=count,
        )
        for (name, process_type, relation_type), count in process_counter.items()
    ]

    notes = [relation.notes for relation in [*upstream_relations, *downstream_relations] if relation.notes]
    related_jobs_map = {
        relation.lineage_job.id: relation.lineage_job
        for relation in [*upstream_relations, *downstream_relations]
        if relation.lineage_job is not None
    }
    related_jobs = [build_job_summary(job) for job in related_jobs_map.values()]
    recent_runs = []
    for job in related_jobs:
        recent_runs.extend(job.recent_runs)
    recent_runs = sorted(
        recent_runs,
        key=lambda item: item.started_at or item.ended_at or item.nominal_start_time or "",
        reverse=True,
    )[:5]
    lineage_sources = [INTERNAL_LINEAGE_LABEL] if [*upstream_relations, *downstream_relations] else []
    origin_values = {
        normalized_relation_origin(relation.discovery_method) for relation in [*upstream_relations, *downstream_relations]
    }
    lineage_origin = "manual"
    if "merged" in origin_values or len(origin_values) > 1:
        lineage_origin = "merged"
    elif "automatic" in origin_values:
        lineage_origin = "automatic"

    impact_level = "low"
    if len(dashboards) >= 3 or len(downstream_assets) >= 5:
        impact_level = "high"
    elif len(dashboards) >= 1 or len(downstream_assets) >= 2:
        impact_level = "medium"

    upstream_count = upstream_total if upstream_total is not None else len(upstream_assets)
    downstream_count = downstream_total if downstream_total is not None else len(downstream_assets)
    graph_truncated = False
    if max_relations is not None:
        graph_truncated = (
            (upstream_total is not None and upstream_total > len(upstream_relations))
            or (downstream_total is not None and downstream_total > len(downstream_relations))
        )

    datasource_ids = {
        item.datasource_id
        for item in [asset, *upstream_assets, *downstream_assets]
        if item.datasource_id is not None
    }
    datasource_engine_map: dict[int, str] = {}
    if datasource_ids:
        datasource_engine_map = {
            row.id: (row.db_type or "").lower()
            for row in db.scalars(select(DataSource).where(DataSource.id.in_(datasource_ids))).all()
        }

    graph_nodes_map: dict[str, LineageGraphNodeOut] = {}
    graph_edges_map: dict[str, LineageGraphEdgeOut] = {}

    def _asset_node_kind(item: LineageAsset, *, direction: str, is_current: bool = False) -> str:
        if is_current:
            return "current"
        if item.asset_type in {"dashboard", "question"} or item.layer == "dashboard":
            return "dashboard"
        if direction == "upstream" or item.asset_type in {"source", "api"} or item.layer == "source":
            return "source"
        return "target"

    def _database_engine_for_asset(item: LineageAsset) -> str | None:
        if item.datasource_id and item.datasource_id in datasource_engine_map:
            value = datasource_engine_map[item.datasource_id]
            normalized = normalize_database_engine(value)
            return normalized if normalized in LINEAGE_SUPPORTED_DATABASE_ENGINES else None
        inferred_from_namespace = infer_engine_from_namespace(item.external_namespace)
        if inferred_from_namespace in LINEAGE_SUPPORTED_DATABASE_ENGINES:
            return inferred_from_namespace
        inferred_from_external_type = normalize_database_engine(item.external_type)
        if inferred_from_external_type in LINEAGE_SUPPORTED_DATABASE_ENGINES:
            return inferred_from_external_type
        inferred_from_system_name = normalize_database_engine(item.system_name)
        if inferred_from_system_name in LINEAGE_SUPPORTED_DATABASE_ENGINES:
            return inferred_from_system_name
        if item.system_name:
            lowered = item.system_name.lower()
            if lowered in {"api", "s3", "file"}:
                return None
        return None

    def _source_type_for_asset(item: LineageAsset) -> str | None:
        namespace = (item.external_namespace or "").lower()
        if namespace.startswith("s3://"):
            return "s3"
        if namespace.startswith("http://") or namespace.startswith("https://"):
            return "api"
        if namespace.startswith("file://"):
            return "file"
        if item.external_type:
            lowered_external_type = item.external_type.lower()
            if lowered_external_type in {"api", "file", "s3"}:
                return lowered_external_type
        if item.asset_type == "source":
            if item.system_name:
                system = item.system_name.lower()
                if "s3" in system:
                    return "s3"
                if "api" in system:
                    return "api"
                if "file" in system:
                    return "file"
            if item.external_name:
                external_name = item.external_name.lower()
                if external_name.startswith("s3://"):
                    return "s3"
                if external_name.startswith("http://") or external_name.startswith("https://"):
                    return "api"
            return "source"
        return None

    def _upsert_asset_node(
        item: LineageAsset,
        *,
        direction: str,
        is_current: bool = False,
        relation_origin: str | None = None,
    ) -> str:
        node_id = canonical_lineage_asset_key(item)
        origin_value = lineage_origin if is_current else relation_origin or "manual"
        existing = graph_nodes_map.get(node_id)
        if existing is not None:
            if existing.lineage_origin != origin_value:
                existing.lineage_origin = "merged"
            return node_id
        graph_nodes_map[node_id] = LineageGraphNodeOut(
            id=node_id,
            label=item.asset_name,
            kind=_asset_node_kind(item, direction=direction, is_current=is_current),
            asset_id=item.id if item.id else None,
            catalog_table_id=item.catalog_table_id,
            node_type=item.asset_type,
            asset_type=item.asset_type,
            layer=item.layer,
            subtitle=item.system_name or item.schema_name,
            database_engine=_database_engine_for_asset(item),
            source_type=_source_type_for_asset(item),
            process_type=None,
            lineage_origin=origin_value,
        )
        return node_id

    def _upsert_process_node(relation: LineageRelation) -> str | None:
        if relation.lineage_job is None and not relation.process_name and not relation.process_type:
            return None
        node_id = f"process-{relation.id}"
        graph_nodes_map.setdefault(
            node_id,
            LineageGraphNodeOut(
                id=node_id,
                label=relation.lineage_job.display_name if relation.lineage_job else relation.process_name or "Process",
                kind="process",
                asset_id=None,
                catalog_table_id=None,
                node_type="job" if relation.lineage_job else "process",
                asset_type="job" if relation.lineage_job else "source",
                layer="mart" if relation.lineage_job else None,
                subtitle=relation.lineage_job.namespace if relation.lineage_job else relation.process_type,
                database_engine=None,
                source_type=None,
                process_type=(relation.process_type or relation.lineage_job.job_type or ("airflow" if relation.lineage_job else "process")).lower(),
                lineage_origin=normalized_relation_origin(relation.discovery_method),
            ),
        )
        return node_id

    def _upsert_edge(edge_id: str, *, source: str, target: str, relation: LineageRelation, relation_type: str) -> None:
        graph_edges_map.setdefault(
            edge_id,
            LineageGraphEdgeOut(
                id=edge_id,
                source=source,
                target=target,
                relation_type=relation_type,
                confidence_score=int(relation.confidence_score or 0),
                confidence_tier=confidence_tier(relation.confidence_score, is_verified=bool(relation.is_verified)),
                is_verified=bool(relation.is_verified),
                version=int(relation.version or 1),
                evidence=relation.evidence,
            ),
        )

    current_node_id = _upsert_asset_node(asset, direction="current", is_current=True)

    for relation in upstream_relations:
        relation_origin = normalized_relation_origin(relation.discovery_method)
        source_node_id = _upsert_asset_node(relation.source_asset, direction="upstream", relation_origin=relation_origin)
        process_node_id = _upsert_process_node(relation)
        if process_node_id:
            _upsert_edge(
                f"edge-up-{relation.id}-source-process",
                source=source_node_id,
                target=process_node_id,
                relation=relation,
                relation_type=relation.relation_type,
            )
            _upsert_edge(
                f"edge-up-{relation.id}-process-current",
                source=process_node_id,
                target=current_node_id,
                relation=relation,
                relation_type=relation.relation_type,
            )
        else:
            _upsert_edge(
                f"edge-up-{relation.id}",
                source=source_node_id,
                target=current_node_id,
                relation=relation,
                relation_type=relation.relation_type,
            )

    for relation in downstream_relations:
        relation_origin = normalized_relation_origin(relation.discovery_method)
        target_node_id = _upsert_asset_node(relation.target_asset, direction="downstream", relation_origin=relation_origin)
        process_node_id = _upsert_process_node(relation)
        if process_node_id:
            _upsert_edge(
                f"edge-down-{relation.id}-current-process",
                source=current_node_id,
                target=process_node_id,
                relation=relation,
                relation_type=relation.relation_type,
            )
            _upsert_edge(
                f"edge-down-{relation.id}-process-target",
                source=process_node_id,
                target=target_node_id,
                relation=relation,
                relation_type=relation.relation_type,
            )
        else:
            _upsert_edge(
                f"edge-down-{relation.id}",
                source=current_node_id,
                target=target_node_id,
                relation=relation,
                relation_type=relation.relation_type,
            )

    return LineageAssetSummaryOut(
        asset=serialize_asset_ref(asset),
        upstream=[serialize_asset_ref(item) for item in upstream_assets],
        downstream=[serialize_asset_ref(item) for item in downstream_assets],
        related_processes=related_processes,
        related_dashboards=[serialize_asset_ref(item) for item in dashboards],
        related_jobs=related_jobs,
        lineage_origin=lineage_origin,
        lineage_sources=lineage_sources,
        recent_runs=recent_runs,
        impact=LineageImpactOut(
            upstream_count=upstream_count,
            downstream_count=downstream_count,
            process_count=len(related_processes),
            dashboard_count=len(dashboards),
            direct_dependencies_count=upstream_count + downstream_count,
            impact_level=impact_level,
        ),
        graph_nodes=list(graph_nodes_map.values()),
        graph_edges=list(graph_edges_map.values()),
        notes=notes,
        graph_truncated=graph_truncated,
        graph_limit=max_relations,
    )


__all__ = ["collect_asset_summary"]
