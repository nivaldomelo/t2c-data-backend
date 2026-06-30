from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from t2c_data.features.dashboard.profile_loader import load_table_profiles
from t2c_data.features.governance.settings import get_governance_settings_snapshot
from t2c_data.features.governance.trust_score import build_trust_score_for_profile
from t2c_data.models.governance import GovernanceTrustSnapshot


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _bucket_date(now: datetime | None = None) -> datetime:
    current = now or _now()
    return current.replace(hour=0, minute=0, second=0, microsecond=0)


def refresh_governance_trust_snapshots(session: Session, *, retention_days: int = 180) -> dict[str, Any]:
    now = _now()
    bucket = _bucket_date(now)
    settings_snapshot = get_governance_settings_snapshot(session)
    profiles = load_table_profiles(session, now)
    existing = {
        snapshot.table_id: snapshot
        for snapshot in session.scalars(
            select(GovernanceTrustSnapshot).where(GovernanceTrustSnapshot.bucket_date == bucket)
        ).all()
    }
    created = 0
    updated = 0
    for profile in profiles:
        trust_payload = build_trust_score_for_profile(profile, settings_snapshot=settings_snapshot)
        snapshot = existing.get(profile.table_id)
        if snapshot is None:
            snapshot = GovernanceTrustSnapshot(
                table_id=profile.table_id,
                datasource_id=profile.datasource_id,
                bucket_date=bucket,
            )
            session.add(snapshot)
            created += 1
        else:
            updated += 1
        snapshot.datasource_id = profile.datasource_id
        snapshot.owner_name = profile.owner_name
        snapshot.domain_label = profile.domain_name
        snapshot.score = int(trust_payload.score)
        snapshot.label = trust_payload.label
        snapshot.tone = trust_payload.tone
        snapshot.readiness_score = int(trust_payload.readiness_score)
        snapshot.governance_score = int(trust_payload.governance_score)
        snapshot.operational_score = int(trust_payload.operational_score)
        snapshot.dq_score = float(profile.dq_score) if profile.dq_score is not None else None
        snapshot.open_incidents = int(profile.open_incidents or 0)
        snapshot.critical_open_incidents = int(profile.critical_open_incidents or 0)
        snapshot.active_dq_violation = bool(profile.active_dq_violation)
        snapshot.recent_dq_failure_runs_30d = int(profile.recent_dq_failure_runs_30d or 0)
        snapshot.trust_context_json = trust_payload.context
    cutoff = bucket - timedelta(days=max(retention_days, 30))
    stale_rows = session.scalars(
        select(GovernanceTrustSnapshot).where(GovernanceTrustSnapshot.bucket_date < cutoff)
    ).all()
    purged = len(stale_rows)
    for row in stale_rows:
        session.delete(row)
    session.flush()
    return {
        "generated_at": now.isoformat(),
        "bucket_date": bucket.isoformat(),
        "created": created,
        "updated": updated,
        "purged": purged,
        "retention_days": max(retention_days, 30),
    }


def get_table_governance_trust_history(session: Session, *, table_id: int, limit: int = 30) -> list[dict[str, Any]]:
    rows = session.scalars(
        select(GovernanceTrustSnapshot)
        .where(GovernanceTrustSnapshot.table_id == table_id)
        .order_by(GovernanceTrustSnapshot.bucket_date.desc())
        .limit(max(1, min(limit, 180)))
    ).all()
    return [
        {
            "bucket_date": row.bucket_date,
            "score": int(row.score),
            "label": row.label,
            "tone": row.tone,
            "readiness_score": int(row.readiness_score),
            "governance_score": int(row.governance_score),
            "operational_score": int(row.operational_score),
            "dq_score": float(row.dq_score) if row.dq_score is not None else None,
            "open_incidents": int(row.open_incidents or 0),
            "critical_open_incidents": int(row.critical_open_incidents or 0),
            "active_dq_violation": bool(row.active_dq_violation),
            "recent_dq_failure_runs_30d": int(row.recent_dq_failure_runs_30d or 0),
            "trust_context": row.trust_context_json or {},
        }
        for row in reversed(rows)
    ]


__all__ = [
    "get_table_governance_trust_history",
    "refresh_governance_trust_snapshots",
]
