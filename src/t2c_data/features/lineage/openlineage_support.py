from __future__ import annotations

import json
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from t2c_data.models.lineage import LineageJob, LineageRun, LineageSourceConfig
from t2c_data.schemas.lineage import LineageJobRunOut, LineageJobSummaryOut


def normalize_list(value: str | list[str] | None, *, split_pattern) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if not isinstance(value, str):
        return [str(value).strip()] if str(value).strip() else []
    chunks = [chunk.strip() for chunk in split_pattern.split(value) if chunk.strip()]
    return chunks or ([value.strip()] if value.strip() else [])


def normalize_node_id(raw_id: Any, *, kind: str, namespace: str | None, name: str | None) -> str | None:
    if isinstance(raw_id, str) and raw_id.strip():
        return raw_id.strip()
    if isinstance(raw_id, dict):
        raw_namespace = raw_id.get("namespace") or namespace
        raw_name = raw_id.get("name") or name
        if raw_namespace and raw_name:
            return f"{kind}:{raw_namespace}:{raw_name}"
    if namespace and name:
        return f"{kind}:{namespace}:{name}"
    return None


def dataset_meta(raw: dict[str, Any], namespace: str | None = None, *, split_pattern) -> dict[str, Any]:
    facets = raw.get("facets") if isinstance(raw.get("facets"), dict) else {}
    schema_facet = facets.get("schema") if isinstance(facets.get("schema"), dict) else {}
    data = raw.get("data") if isinstance(raw.get("data"), dict) else {}
    dataset_name = (
        raw.get("name")
        or raw.get("dataset")
        or data.get("name")
        or raw.get("physicalName")
        or data.get("physicalName")
    )
    node_namespace = raw.get("namespace") or data.get("namespace") or namespace
    physical_name = raw.get("physicalName") or data.get("physicalName") or None
    aliases = normalize_list(raw.get("aliases") or data.get("aliases") or schema_facet.get("fields"), split_pattern=split_pattern)
    return {
        "node_id": normalize_node_id(
            raw.get("id") or raw.get("nodeId"),
            kind="dataset",
            namespace=node_namespace,
            name=dataset_name,
        ),
        "namespace": node_namespace,
        "name": dataset_name,
        "display_name": raw.get("displayName") or data.get("displayName") or dataset_name,
        "physical_name": physical_name,
        "aliases": aliases,
    }


def job_meta(raw: dict[str, Any], namespace: str | None = None) -> dict[str, Any]:
    data = raw.get("data") if isinstance(raw.get("data"), dict) else {}
    name = raw.get("name") or raw.get("job") or data.get("name")
    job_namespace = raw.get("namespace") or data.get("namespace") or namespace
    job_type = raw.get("type") or data.get("type") or raw.get("jobType") or data.get("jobType")
    return {
        "node_id": normalize_node_id(
            raw.get("id") or raw.get("nodeId"),
            kind="job",
            namespace=job_namespace,
            name=name,
        ),
        "namespace": job_namespace,
        "name": name,
        "display_name": raw.get("displayName") or data.get("displayName") or name,
        "job_type": str(job_type).lower() if job_type else None,
        "location": raw.get("location") or data.get("location") or None,
    }


def upsert_job(db: Session, source: LineageSourceConfig, raw_job: dict[str, Any]) -> tuple[LineageJob | None, bool]:
    meta = job_meta(raw_job, source.default_namespace)
    if not meta["namespace"] or not meta["name"]:
        return None, False
    job = db.scalar(
        select(LineageJob).where(
            LineageJob.lineage_source_id == source.id,
            LineageJob.namespace == meta["namespace"],
            LineageJob.job_name == meta["name"],
        )
    )
    created = job is None
    if not job:
        job = LineageJob(
            lineage_source_id=source.id,
            namespace=meta["namespace"],
            job_name=meta["name"],
            display_name=meta["display_name"] or meta["name"],
            is_active=True,
        )
        db.add(job)
    job.display_name = meta["display_name"] or meta["name"]
    job.job_type = meta["job_type"]
    job.location = meta["location"]
    job.raw_json = json.dumps(raw_job, ensure_ascii=False)
    job.is_active = True
    db.flush()
    return job, created


def upsert_run(db: Session, job: LineageJob, raw_run: dict[str, Any]) -> tuple[LineageRun | None, bool]:
    external_run_id = raw_run.get("id") or raw_run.get("runId") or raw_run.get("name")
    if not external_run_id:
        return None, False
    run = db.scalar(
        select(LineageRun).where(
            LineageRun.lineage_job_id == job.id,
            LineageRun.external_run_id == str(external_run_id),
        )
    )
    created = run is None
    if not run:
        run = LineageRun(lineage_job_id=job.id, external_run_id=str(external_run_id))
        db.add(run)
    run.status = raw_run.get("state") or raw_run.get("status")
    run.started_at = raw_run.get("startedAt") or raw_run.get("startTime")
    run.ended_at = raw_run.get("endedAt") or raw_run.get("endTime")
    run.nominal_start_time = raw_run.get("nominalStartTime")
    run.raw_json = json.dumps(raw_run, ensure_ascii=False)
    db.flush()
    if run.started_at or run.ended_at:
        job.latest_run_id = run.external_run_id
        job.latest_run_status = run.status
        job.latest_run_at = run.ended_at or run.started_at
    return run, created


def build_job_summary(job: LineageJob, limit_runs: int = 3) -> LineageJobSummaryOut:
    recent_runs = sorted(job.runs, key=lambda item: item.started_at or item.created_at.isoformat(), reverse=True)[:limit_runs]
    return LineageJobSummaryOut(
        id=job.id,
        namespace=job.namespace,
        job_name=job.job_name,
        display_name=job.display_name,
        job_type=job.job_type,
        latest_run_id=job.latest_run_id,
        latest_run_status=job.latest_run_status,
        latest_run_at=job.latest_run_at,
        recent_runs=[
            LineageJobRunOut(
                external_run_id=run.external_run_id,
                status=run.status,
                started_at=run.started_at,
                ended_at=run.ended_at,
                nominal_start_time=run.nominal_start_time,
            )
            for run in recent_runs
        ],
    )


__all__ = [
    "build_job_summary",
    "dataset_meta",
    "job_meta",
    "normalize_list",
    "normalize_node_id",
    "upsert_job",
    "upsert_run",
]
