from __future__ import annotations

import json
import logging
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from fastapi import HTTPException, status
from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from t2c_data.features.lineage.openlineage_persistence import upsert_dataset_asset, upsert_relation
from t2c_data.features.lineage.openlineage_support import upsert_job, upsert_run
from t2c_data.features.lineage.source_configs import create_source_config, get_source_config, list_source_configs, serialize_source_config
from t2c_data.features.lineage.sql_lineage import extract_sql_column_lineage, extract_sql_table_lineage
from t2c_data.features.lineage.versioning import record_column_edge_version
from t2c_data.models.lineage import LineageAsset, LineageColumnEdge, LineageEventRaw, LineageJob, LineageSourceConfig, LineageSyncCheckpoint
from t2c_data.schemas.lineage import (
    LineageEventBulkIn,
    LineageEventIn,
    LineageEventIngestionOut,
    LineageEventsBulkOut,
    LineageSourceConfigCreate,
    LineageSourceSyncOut,
)

logger = logging.getLogger(__name__)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_namespace(value: str | None) -> str | None:
    raw = (value or "").strip()
    return raw or None


def _event_payload(raw: dict[str, Any]) -> dict[str, Any]:
    if isinstance(raw.get("payload"), dict):
        return raw["payload"]
    if isinstance(raw.get("event"), dict):
        return raw["event"]
    return raw


def _normalize_event_key(payload: dict[str, Any]) -> str:
    event = _event_payload(payload)
    job = event.get("job") if isinstance(event.get("job"), dict) else {}
    run = event.get("run") if isinstance(event.get("run"), dict) else {}
    namespace = _normalize_namespace(job.get("namespace") or event.get("namespace") or payload.get("namespace"))
    job_name = _normalize_namespace(job.get("name") or event.get("jobName") or payload.get("job_name"))
    run_id = _normalize_namespace(run.get("runId") or run.get("id") or payload.get("run_id"))
    event_type = _normalize_namespace(event.get("eventType") or payload.get("event_type")) or "event"
    event_time = _normalize_namespace(event.get("eventTime") or payload.get("event_time"))
    producer = _normalize_namespace(event.get("producer") or payload.get("producer"))
    return "|".join(part for part in [producer, namespace, job_name, run_id, event_type, event_time] if part) or f"event-{id(payload)}"


def _resolve_source(
    db: Session,
    *,
    source_id: int | None,
    payload: dict[str, Any],
) -> LineageSourceConfig:
    if source_id is not None:
        return get_source_config(db, source_id)

    candidates = list_source_configs(db)
    for candidate in candidates:
        if candidate.source_type != "openlineage":
            continue
        if candidate.name == "Linhagem automática interna":
            return candidate

    create_payload = LineageSourceConfigCreate(
        name="Linhagem automática interna",
        source_type="openlineage",
        base_url="internal://openlineage",
        default_namespace=None,
        auth_type="none",
        enabled=True,
    )
    source = create_source_config(db, create_payload)
    db.flush()
    return source


def _dataset_payload(item: dict[str, Any], *, default_namespace: str | None) -> dict[str, Any]:
    namespace = _normalize_namespace(item.get("namespace") or default_namespace)
    name = item.get("name") or item.get("dataset") or item.get("physicalName") or item.get("physical_name")
    facets = item.get("facets") if isinstance(item.get("facets"), dict) else {}
    schema_facet = facets.get("schema") if isinstance(facets.get("schema"), dict) else {}
    aliases = item.get("aliases") if isinstance(item.get("aliases"), list) else []
    if not aliases and isinstance(schema_facet.get("fields"), list):
        aliases = [str(value) for value in schema_facet.get("fields", []) if str(value).strip()]
    return {
        "node_id": item.get("id") or item.get("nodeId"),
        "namespace": namespace,
        "name": name,
        "display_name": item.get("displayName") or item.get("dataset") or name,
        "physical_name": item.get("physicalName") or item.get("physical_name") or name,
        "aliases": aliases,
        "facets": facets,
        "kind": item.get("type") or item.get("dataSourceType") or item.get("kind"),
    }


def _job_payload(payload: dict[str, Any]) -> dict[str, Any]:
    event = _event_payload(payload)
    job = event.get("job") if isinstance(event.get("job"), dict) else {}
    job_facets = job.get("facets") if isinstance(job.get("facets"), dict) else {}
    t2c_job_facet = job_facets.get("t2cJob") if isinstance(job_facets.get("t2cJob"), dict) else {}
    namespace = _normalize_namespace(job.get("namespace") or event.get("namespace") or payload.get("namespace"))
    name = job.get("name") or payload.get("job_name")
    display_name = (
        t2c_job_facet.get("displayName")
        or t2c_job_facet.get("pipelineName")
        or job.get("displayName")
        or job.get("name")
        or name
    )
    return {
        "id": event.get("job", {}).get("namespace") if isinstance(event.get("job"), dict) else None,
        "nodeId": event.get("job", {}).get("namespace") if isinstance(event.get("job"), dict) else None,
        "namespace": namespace,
        "name": name,
        "displayName": display_name,
        "type": t2c_job_facet.get("jobType") or job.get("type") or job.get("jobType") or payload.get("job_type"),
        "location": job.get("facets", {}).get("documentation", {}).get("description") if isinstance(job.get("facets"), dict) else None,
    }


def _run_payload(payload: dict[str, Any]) -> dict[str, Any]:
    event = _event_payload(payload)
    run = event.get("run") if isinstance(event.get("run"), dict) else {}
    event_type = _normalize_namespace(event.get("eventType") or payload.get("event_type")) or "UNKNOWN"
    event_time = _normalize_namespace(event.get("eventTime") or payload.get("event_time"))
    return {
        "id": run.get("runId") or run.get("id") or payload.get("run_id") or event_time or f"run-{id(payload)}",
        "runId": run.get("runId") or run.get("id") or payload.get("run_id") or event_time or f"run-{id(payload)}",
        "state": event_type,
        "startedAt": event_time,
        "endedAt": event_time,
        "nominalStartTime": event_time,
    }


def _relation_key(source_asset: LineageAsset, target_asset: LineageAsset, *, job: LineageJob | None, relation_type: str) -> str:
    job_key = job.id if job else "no-job"
    return f"{source_asset.id}|{job_key}|{target_asset.id}|{relation_type}"


def _upsert_column_edge(
    db: Session,
    *,
    source_asset: LineageAsset,
    target_asset: LineageAsset,
    source_column_name: str,
    target_column_name: str,
    source: LineageSourceConfig,
    job: LineageJob | None,
    external_edge_key: str,
    relation_type: str = "transformation",
    evidence_source: str = "openlineage",
    transform_expression: str | None = None,
    notes: str | None = None,
) -> tuple[LineageColumnEdge, bool]:
    now = datetime.now(timezone.utc)
    edge = db.scalar(
        select(LineageColumnEdge).where(
            LineageColumnEdge.source_asset_id == source_asset.id,
            LineageColumnEdge.target_asset_id == target_asset.id,
            LineageColumnEdge.source_column_name == source_column_name,
            LineageColumnEdge.target_column_name == target_column_name,
            LineageColumnEdge.relation_type == relation_type,
        )
    )
    created = edge is None
    previous_state = None
    if not edge:
        edge = LineageColumnEdge(
            source_asset_id=source_asset.id,
            target_asset_id=target_asset.id,
            source_column_name=source_column_name,
            target_column_name=target_column_name,
            relation_type=relation_type,
            discovery_method="automatic",
            confidence_score=100,
            evidence_source=evidence_source,
            evidence=notes or external_edge_key or evidence_source,
            transform_expression=transform_expression,
            notes=notes,
            external_edge_key=external_edge_key,
            lineage_source_id=source.id,
            lineage_job_id=job.id if job else None,
            is_active=True,
            is_verified=False,
        )
        db.add(edge)
    else:
        previous_state = {
            "lineage_source_id": edge.lineage_source_id,
            "lineage_job_id": edge.lineage_job_id,
            "source_asset_id": edge.source_asset_id,
            "target_asset_id": edge.target_asset_id,
            "source_column_name": edge.source_column_name,
            "target_column_name": edge.target_column_name,
            "relation_type": edge.relation_type,
            "discovery_method": edge.discovery_method,
            "confidence_score": edge.confidence_score,
            "evidence_source": edge.evidence_source,
            "evidence": edge.evidence,
            "transform_expression": edge.transform_expression,
            "notes": edge.notes,
            "external_edge_key": edge.external_edge_key,
            "is_verified": edge.is_verified,
            "is_active": edge.is_active,
            "created_by_user_id": edge.created_by_user_id,
            "updated_by_user_id": edge.updated_by_user_id,
        }
        edge.external_edge_key = external_edge_key or edge.external_edge_key
        edge.lineage_source_id = source.id
        edge.lineage_job_id = job.id if job else edge.lineage_job_id
        edge.discovery_method = "automatic"
        edge.evidence_source = evidence_source or edge.evidence_source or "openlineage"
        edge.evidence = notes or edge.evidence or external_edge_key or evidence_source
        edge.transform_expression = transform_expression if transform_expression is not None else edge.transform_expression
        edge.notes = notes if notes is not None else edge.notes
        edge.is_active = True
        edge.is_verified = bool(edge.is_verified)
    db.flush()
    edge.last_seen_at = now
    record_column_edge_version(db, edge, force_version=created, previous_state=previous_state)
    return edge, created


def _process_openlineage_event(db: Session, source: LineageSourceConfig, payload: dict[str, Any]) -> LineageEventIngestionOut:
    event = _event_payload(payload)
    event_key = _normalize_event_key(payload)
    event_type = _normalize_namespace(event.get("eventType") or payload.get("event_type"))
    namespace = _normalize_namespace((event.get("job") or {}).get("namespace") if isinstance(event.get("job"), dict) else None)
    namespace = namespace or _normalize_namespace(event.get("namespace") or payload.get("namespace") or source.default_namespace)
    job_name = _normalize_namespace((event.get("job") or {}).get("name") if isinstance(event.get("job"), dict) else None)
    job_name = job_name or _normalize_namespace(payload.get("job_name"))
    run_id = _normalize_namespace((event.get("run") or {}).get("runId") if isinstance(event.get("run"), dict) else None)
    run_id = run_id or _normalize_namespace((event.get("run") or {}).get("id") if isinstance(event.get("run"), dict) else None)
    producer = _normalize_namespace(event.get("producer") or payload.get("producer"))

    raw = db.scalar(
        select(LineageEventRaw).where(
            LineageEventRaw.lineage_source_id == source.id,
            LineageEventRaw.event_key == event_key,
        )
    )
    if not raw:
        raw = LineageEventRaw(
            lineage_source_id=source.id,
            event_key=event_key,
            payload_json=json.dumps(payload, ensure_ascii=False),
            is_processed=False,
        )
        db.add(raw)
    else:
        raw.payload_json = json.dumps(payload, ensure_ascii=False)
        raw.error_message = None
    raw.event_type = event_type
    raw.producer = producer
    raw.namespace = namespace
    raw.job_name = job_name
    raw.run_id = run_id
    raw.event_time = _normalize_namespace(event.get("eventTime") or payload.get("event_time"))
    raw.status = event_type

    jobs_synced = 0
    runs_synced = 0
    datasets_synced = 0
    relations_created = 0
    relations_updated = 0
    column_edges_created = 0
    matched_catalog_assets = 0
    unmatched_assets_created = 0
    warnings: list[str] = []

    try:
        job_meta = _job_payload(payload)
        job = None
        if namespace and job_name:
            job, job_created = upsert_job(db, source, job_meta)
            if job:
                jobs_synced += 1 if job_created else 0
                run_meta = _run_payload(payload)
                run, run_created = upsert_run(db, job, run_meta)
                if run:
                    runs_synced += 1 if run_created else 0
        inputs = [item for item in event.get("inputs", []) if isinstance(item, dict)]
        outputs = [item for item in event.get("outputs", []) if isinstance(item, dict)]
        all_datasets = inputs + outputs
        dataset_map: dict[str, LineageAsset] = {}
        for raw_dataset in all_datasets:
            meta = _dataset_payload(raw_dataset, default_namespace=namespace)
            asset, created, matched = upsert_dataset_asset(db, source=source, meta=meta)
            if asset:
                datasets_synced += 1
                matched_catalog_assets += 1 if matched else 0
                unmatched_assets_created += 1 if created and not matched else 0
                key_candidates = {
                    str(raw_dataset.get("id") or raw_dataset.get("nodeId") or ""),
                    str(meta["name"] or ""),
                    str(meta["physical_name"] or ""),
                }
                for key in key_candidates:
                    if key:
                        dataset_map[key] = asset
                if meta["namespace"] and meta["name"]:
                    dataset_map[f"{meta['namespace']}::{meta['name']}"] = asset
                if matched and created:
                    warnings.append("A dataset was matched to a catalog table during normalization.")

        sql_text = (
            event.get("query")
            or event.get("sql")
            or (event.get("facets") or {}).get("sql")
            or (event.get("facets") or {}).get("query")
        )
        sql_outputs = extract_sql_table_lineage(str(sql_text)) if sql_text else []
        if sql_text and sql_outputs:
            warnings.append("SQL lineage enrichment detected table dependencies from event SQL.")
            if not outputs:
                for table_name in sql_outputs:
                    warnings.append(f"SQL references table {table_name}")

        for source_dataset in inputs:
            source_meta = _dataset_payload(source_dataset, default_namespace=namespace)
            source_asset = dataset_map.get(str(source_dataset.get("id") or source_dataset.get("nodeId") or ""))
            if not source_asset and source_meta["namespace"] and source_meta["name"]:
                source_asset = dataset_map.get(f"{source_meta['namespace']}::{source_meta['name']}")
            for target_dataset in outputs or []:
                target_meta = _dataset_payload(target_dataset, default_namespace=namespace)
                target_asset = dataset_map.get(str(target_dataset.get("id") or target_dataset.get("nodeId") or ""))
                if not target_asset and target_meta["namespace"] and target_meta["name"]:
                    target_asset = dataset_map.get(f"{target_meta['namespace']}::{target_meta['name']}")
                if not source_asset or not target_asset:
                    continue
                relation, created_relation, _merged = upsert_relation(
                    db,
                    source=source,
                    source_asset=source_asset,
                    target_asset=target_asset,
                    relation_type="transformation",
                    process_name=job.display_name if job else job_name,
                    process_type=(job.job_type if job else "openlineage"),
                    discovery_method="automatic",
                    lineage_job=job,
                    external_edge_key=_relation_key(source_asset, target_asset, job=job, relation_type="transformation"),
                )
                relations_created += 1 if created_relation else 0
                relations_updated += 0 if created_relation else 1
                if not relation.is_active:
                    relation.is_active = True

        if outputs:
            for target_dataset in outputs:
                target_meta = _dataset_payload(target_dataset, default_namespace=namespace)
                target_asset = dataset_map.get(str(target_dataset.get("id") or target_dataset.get("nodeId") or ""))
                if not target_asset and target_meta["namespace"] and target_meta["name"]:
                    target_asset = dataset_map.get(f"{target_meta['namespace']}::{target_meta['name']}")
                if not target_asset:
                    continue
                facets = target_dataset.get("facets") if isinstance(target_dataset.get("facets"), dict) else {}
                column_lineage = facets.get("columnLineage") if isinstance(facets.get("columnLineage"), dict) else None
                if not column_lineage:
                    continue
                fields = column_lineage.get("fields") if isinstance(column_lineage.get("fields"), dict) else {}
                for target_column, spec in fields.items():
                    if not isinstance(spec, dict):
                        continue
                    input_fields = spec.get("inputFields") or []
                    for input_field in input_fields:
                        if not isinstance(input_field, dict):
                            continue
                        source_namespace = _normalize_namespace(input_field.get("namespace") or namespace)
                        source_name = _normalize_namespace(input_field.get("name"))
                        source_column = _normalize_namespace(input_field.get("field"))
                        if not source_namespace or not source_name or not source_column:
                            continue
                        source_asset = dataset_map.get(f"{source_namespace}::{source_name}")
                        if not source_asset:
                            continue
                        _edge, created_edge = _upsert_column_edge(
                            db,
                            source_asset=source_asset,
                            target_asset=target_asset,
                            source_column_name=source_column,
                            target_column_name=str(target_column),
                            source=source,
                            job=job,
                            external_edge_key=f"{source_asset.id}|{target_asset.id}|{source_column}|{target_column}",
                        )
                        column_edges_created += 1 if created_edge else 0

        if sql_text and len(inputs) == 1 and len(outputs) == 1 and not any(
            isinstance((target_dataset.get("facets") or {}).get("columnLineage"), dict) for target_dataset in outputs
        ):
            inferred_columns = extract_sql_column_lineage(str(sql_text))
            source_asset = dataset_map.get(str(inputs[0].get("id") or inputs[0].get("nodeId") or ""))
            if not source_asset:
                source_meta = _dataset_payload(inputs[0], default_namespace=namespace)
                if source_meta["namespace"] and source_meta["name"]:
                    source_asset = dataset_map.get(f"{source_meta['namespace']}::{source_meta['name']}")
            target_asset = dataset_map.get(str(outputs[0].get("id") or outputs[0].get("nodeId") or ""))
            if not target_asset:
                target_meta = _dataset_payload(outputs[0], default_namespace=namespace)
                if target_meta["namespace"] and target_meta["name"]:
                    target_asset = dataset_map.get(f"{target_meta['namespace']}::{target_meta['name']}")
            if source_asset and target_asset and inferred_columns:
                warnings.append("SQL lineage enrichment detected column dependencies from event SQL.")
                for column_pair in inferred_columns:
                    source_column = _normalize_namespace(column_pair.get("source_column"))
                    target_column = _normalize_namespace(column_pair.get("target_column"))
                    if not source_column or not target_column:
                        continue
                    _edge, created_edge = _upsert_column_edge(
                        db,
                        source_asset=source_asset,
                        target_asset=target_asset,
                        source_column_name=source_column,
                        target_column_name=target_column,
                        source=source,
                        job=job,
                        external_edge_key=f"{source_asset.id}|{target_asset.id}|{source_column}|{target_column}|sql",
                        evidence_source="inferred_sql",
                        transform_expression=str(sql_text),
                        notes="Inferido a partir do SQL do evento.",
                    )
                    column_edges_created += 1 if created_edge else 0

        raw.is_processed = True
        raw.processed_at = datetime.now(timezone.utc)
        raw.error_message = None
    except Exception as exc:  # pragma: no cover - defensive, surfaced via API/tests
        raw.is_processed = False
        raw.processed_at = datetime.now(timezone.utc)
        raw.error_message = str(exc)
        logger.exception("openlineage_event_processing_failed", extra={"source_id": source.id, "event_key": event_key})
        raise

    checkpoint = db.scalar(
        select(LineageSyncCheckpoint).where(
            LineageSyncCheckpoint.lineage_source_id == source.id,
            LineageSyncCheckpoint.checkpoint_type == "openlineage",
        )
    )
    if not checkpoint:
        checkpoint = LineageSyncCheckpoint(lineage_source_id=source.id, checkpoint_type="openlineage")
        db.add(checkpoint)
    checkpoint.last_event_raw_id = raw.id
    checkpoint.last_processed_at = datetime.now(timezone.utc)
    checkpoint.last_status = "success"
    checkpoint.message = f"{datasets_synced} datasets, {relations_created + relations_updated} relations processed"
    checkpoint.cursor_value = event_key
    db.flush()

    source.last_sync_at = _now_iso()
    source.last_sync_status = "success"
    source.last_sync_message = checkpoint.message
    db.flush()

    return LineageEventIngestionOut(
        source_id=source.id,
        source_name=source.name,
        event_raw_id=raw.id,
        event_key=event_key,
        event_type=event_type,
        processed=True,
        jobs_synced=jobs_synced,
        runs_synced=runs_synced,
        datasets_synced=datasets_synced,
        relations_created=relations_created,
        relations_updated=relations_updated,
        column_edges_created=column_edges_created,
        matched_catalog_assets=matched_catalog_assets,
        unmatched_assets_created=unmatched_assets_created,
        warnings=warnings,
    )


def ingest_openlineage_event(
    db: Session,
    *,
    payload: LineageEventIn,
) -> LineageEventIngestionOut:
    source = _resolve_source(db, source_id=payload.source_id, payload=payload.payload)
    result = _process_openlineage_event(db, source, payload.payload)
    db.commit()
    return result


def ingest_openlineage_events_bulk(
    db: Session,
    *,
    payload: LineageEventBulkIn,
) -> LineageEventsBulkOut:
    items: list[LineageEventIngestionOut] = []
    warnings: list[str] = []
    for event in payload.events:
        try:
            items.append(ingest_openlineage_event(db, payload=event))
        except Exception as exc:  # pragma: no cover - surfaced to caller
            warnings.append(str(exc))
    return LineageEventsBulkOut(items=items, processed=len(items), warnings=warnings)


def rebuild_openlineage_source(
    db: Session,
    *,
    source: LineageSourceConfig,
    depth: int = 1,
    namespace: str | None = None,
    node_id: str | None = None,
    table_id: int | None = None,
) -> LineageSourceSyncOut:
    events_query = select(LineageEventRaw).where(LineageEventRaw.lineage_source_id == source.id).order_by(LineageEventRaw.created_at.asc(), LineageEventRaw.id.asc())
    if namespace:
        events_query = events_query.where(LineageEventRaw.namespace == namespace)
    if node_id:
        events_query = events_query.where(or_(LineageEventRaw.event_key == node_id, LineageEventRaw.run_id == node_id))
    events = db.scalars(events_query).all()
    counts: dict[str, int] = defaultdict(int)
    warnings: list[str] = []
    for raw in events:
        try:
            payload = json.loads(raw.payload_json)
        except json.JSONDecodeError as exc:
            warnings.append(f"Invalid lineage payload stored for event {raw.event_key}: {exc}")
            continue
        result = _process_openlineage_event(db, source, payload)
        counts["datasets_synced"] += result.datasets_synced
        counts["jobs_synced"] += result.jobs_synced
        counts["runs_synced"] += result.runs_synced
        counts["assets_created"] += result.datasets_synced
        counts["assets_updated"] += 0
        counts["relations_created"] += result.relations_created
        counts["relations_updated"] += result.relations_updated
        counts["matched_catalog_assets"] += result.matched_catalog_assets
        counts["unmatched_assets_created"] += result.unmatched_assets_created
        warnings.extend(result.warnings)

    source.last_sync_at = _now_iso()
    source.last_sync_status = "success" if events else "empty"
    source.last_sync_message = f"{counts['datasets_synced']} datasets, {counts['relations_created'] + counts['relations_updated']} relações processadas"
    db.flush()
    db.commit()
    return LineageSourceSyncOut(
        source=serialize_source_config(source),
        namespace=namespace or source.default_namespace,
        node_id=node_id,
        depth=depth,
        datasets_synced=counts["datasets_synced"],
        jobs_synced=counts["jobs_synced"],
        runs_synced=counts["runs_synced"],
        assets_created=counts["assets_created"],
        assets_updated=counts["assets_updated"],
        relations_created=counts["relations_created"],
        relations_updated=counts["relations_updated"],
        matched_catalog_assets=counts["matched_catalog_assets"],
        unmatched_assets_created=counts["unmatched_assets_created"],
        warnings=warnings,
    )


def rebuild_openlineage_source_for_table(db: Session, *, table_id: int, depth: int = 1) -> LineageSourceSyncOut:
    asset = db.scalar(select(LineageAsset).where(LineageAsset.catalog_table_id == table_id))
    if not asset or not asset.lineage_source_id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No lineage source associated with this table")
    source = get_source_config(db, asset.lineage_source_id)
    if source.source_type != "openlineage":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Lineage source is not eligible for rebuild")
    return rebuild_openlineage_source(db, source=source, depth=depth, table_id=table_id)


__all__ = [
    "ingest_openlineage_event",
    "ingest_openlineage_events_bulk",
    "rebuild_openlineage_source",
    "rebuild_openlineage_source_for_table",
]
