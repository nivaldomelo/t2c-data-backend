from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import inspect, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from t2c_data.models.dq import DQJobRun, DQRule, DQRuleLatestRun, DQRuleRun


DQ_SCHEMA = "t2c_data"


def latest_snapshot_support_ready(session: Session) -> bool:
    bind = session.get_bind()
    if bind is None:
        return False
    try:
        return inspect(bind).has_table("dq_rule_latest_runs", schema=DQ_SCHEMA)
    except Exception:  # noqa: BLE001
        return False


def _rule_run_sort_key(run: DQRuleRun | None) -> tuple[datetime, int]:
    if run is None:
        return (datetime.min.replace(tzinfo=timezone.utc), -1)
    created_at = run.created_at or datetime.min.replace(tzinfo=timezone.utc)
    return (created_at, int(run.id or -1))


def _job_run_sort_key(job: DQJobRun | None) -> tuple[datetime, int]:
    if job is None:
        return (datetime.min.replace(tzinfo=timezone.utc), -1)
    created_at = job.created_at or datetime.min.replace(tzinfo=timezone.utc)
    return (created_at, int(job.id or -1))


def requested_rule_ids_from_job(job: DQJobRun | None) -> list[int]:
    if job is None or not isinstance(job.result_json, dict):
        return []
    raw_ids = job.result_json.get("requested_rule_ids")
    if not isinstance(raw_ids, list):
        return []
    normalized: list[int] = []
    seen: set[int] = set()
    for raw_id in raw_ids:
        try:
            rule_id = int(raw_id)
        except Exception:  # noqa: BLE001
            continue
        if rule_id in seen:
            continue
        seen.add(rule_id)
        normalized.append(rule_id)
    return normalized


def get_latest_rule_snapshots(session: Session, rule_ids: Iterable[int]) -> dict[int, DQRuleLatestRun]:
    if not latest_snapshot_support_ready(session):
        return {}
    normalized_ids = sorted({int(rule_id) for rule_id in rule_ids if rule_id is not None})
    if not normalized_ids:
        return {}
    try:
        rows = session.scalars(select(DQRuleLatestRun).where(DQRuleLatestRun.rule_id.in_(normalized_ids))).all()
    except SQLAlchemyError:
        return {}
    return {row.rule_id: row for row in rows}


def _ensure_snapshot(session: Session, *, rule_id: int, table_id: int | None) -> DQRuleLatestRun:
    if not latest_snapshot_support_ready(session):
        raise RuntimeError("dq latest snapshot table unavailable")
    snapshot = session.get(DQRuleLatestRun, rule_id)
    if snapshot is None:
        snapshot = DQRuleLatestRun(rule_id=rule_id, table_id=table_id)
    elif table_id is not None:
        snapshot.table_id = table_id
    session.add(snapshot)
    session.flush()
    return snapshot


def sync_latest_snapshot_for_rule_run(
    session: Session,
    *,
    rule_run: DQRuleRun,
    rule: DQRule | None = None,
) -> DQRuleLatestRun | None:
    if not latest_snapshot_support_ready(session):
        return None
    resolved_rule = rule or session.get(DQRule, rule_run.rule_id)
    snapshot = _ensure_snapshot(
        session,
        rule_id=rule_run.rule_id,
        table_id=getattr(resolved_rule, "table_id", None),
    )
    current = session.get(DQRuleRun, snapshot.latest_rule_run_id) if snapshot.latest_rule_run_id is not None else None
    if snapshot.latest_rule_run_id is None or _rule_run_sort_key(rule_run) >= _rule_run_sort_key(current):
        snapshot.latest_rule_run_id = rule_run.id
        if getattr(resolved_rule, "table_id", None) is not None:
            snapshot.table_id = resolved_rule.table_id
        session.add(snapshot)
        session.flush()
    return snapshot


def sync_latest_snapshot_for_job(
    session: Session,
    *,
    job_run: DQJobRun,
    rule_ids: Iterable[int],
    table_id: int | None = None,
) -> None:
    if not latest_snapshot_support_ready(session):
        return
    normalized_ids = sorted({int(rule_id) for rule_id in rule_ids if rule_id is not None})
    if not normalized_ids:
        return
    existing = get_latest_rule_snapshots(session, normalized_ids)
    current_job_ids = [row.latest_job_run_id for row in existing.values() if row.latest_job_run_id is not None]
    current_jobs = {
        row.id: row
        for row in session.scalars(select(DQJobRun).where(DQJobRun.id.in_(current_job_ids))).all()
    } if current_job_ids else {}

    rules_by_id = {
        row.id: row
        for row in session.scalars(select(DQRule).where(DQRule.id.in_(normalized_ids))).all()
    }

    for rule_id in normalized_ids:
        snapshot = existing.get(rule_id) or _ensure_snapshot(
            session,
            rule_id=rule_id,
            table_id=table_id if table_id is not None else getattr(rules_by_id.get(rule_id), "table_id", None),
        )
        current_job = current_jobs.get(snapshot.latest_job_run_id) if snapshot.latest_job_run_id is not None else None
        if snapshot.latest_job_run_id is None or _job_run_sort_key(job_run) >= _job_run_sort_key(current_job):
            snapshot.latest_job_run_id = job_run.id
            if table_id is not None:
                snapshot.table_id = table_id
            elif getattr(rules_by_id.get(rule_id), "table_id", None) is not None:
                snapshot.table_id = rules_by_id[rule_id].table_id
            session.add(snapshot)
    session.flush()


def backfill_latest_rule_runs(session: Session) -> dict[str, int]:
    if not latest_snapshot_support_ready(session):
        return {
            "rules_total": 0,
            "created": 0,
            "updated": 0,
            "latest_rule_runs": 0,
            "latest_jobs": 0,
        }
    rules = session.scalars(select(DQRule).order_by(DQRule.id.asc())).all()
    latest_rule_runs: dict[int, DQRuleRun] = {}
    for run in session.scalars(
        select(DQRuleRun).order_by(DQRuleRun.rule_id.asc(), DQRuleRun.created_at.desc(), DQRuleRun.id.desc())
    ).all():
        latest_rule_runs.setdefault(run.rule_id, run)

    latest_jobs_by_rule: dict[int, DQJobRun] = {}
    for job in session.scalars(
        select(DQJobRun).where(DQJobRun.job_type == "rules").order_by(DQJobRun.id.desc())
    ).all():
        for rule_id in requested_rule_ids_from_job(job):
            latest_jobs_by_rule.setdefault(rule_id, job)

    created = 0
    updated = 0
    errors = 0
    for rule in rules:
        try:
            with session.begin_nested():
                snapshot = session.get(DQRuleLatestRun, rule.id)
                is_new = snapshot is None
                snapshot = _ensure_snapshot(session, rule_id=rule.id, table_id=rule.table_id)
                rule_run = latest_rule_runs.get(rule.id)
                if rule_run is not None:
                    snapshot.latest_rule_run_id = rule_run.id
                job = latest_jobs_by_rule.get(rule.id)
                if job is not None:
                    snapshot.latest_job_run_id = job.id
                snapshot.table_id = rule.table_id
                session.add(snapshot)
                if is_new:
                    created += 1
                else:
                    updated += 1
        except Exception:  # noqa: BLE001
            errors += 1

    session.flush()
    return {
        "rules_total": len(rules),
        "created": created,
        "updated": updated,
        "latest_rule_runs": len(latest_rule_runs),
        "latest_jobs": len(latest_jobs_by_rule),
        "errors": errors,
    }


__all__ = [
    "backfill_latest_rule_runs",
    "get_latest_rule_snapshots",
    "requested_rule_ids_from_job",
    "sync_latest_snapshot_for_job",
    "sync_latest_snapshot_for_rule_run",
]
