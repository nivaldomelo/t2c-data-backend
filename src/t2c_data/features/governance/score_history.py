from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from t2c_data.features.dashboard.profile_loader import load_table_profiles
from t2c_data.features.governance.scoring import build_governance_score_for_profile
from t2c_data.features.governance.settings import get_governance_settings_snapshot
from t2c_data.models.governance import GovernanceScoreSnapshot


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _bucket_date(now: datetime | None = None) -> datetime:
    current = now or _now()
    return current.replace(hour=0, minute=0, second=0, microsecond=0)


def refresh_governance_score_snapshots(session: Session, *, retention_days: int = 180) -> dict[str, Any]:
    now = _now()
    bucket = _bucket_date(now)
    settings_snapshot = get_governance_settings_snapshot(session)
    profiles = load_table_profiles(session, now)
    existing = {
        snapshot.table_id: snapshot
        for snapshot in session.scalars(
            select(GovernanceScoreSnapshot).where(GovernanceScoreSnapshot.bucket_date == bucket)
        ).all()
    }
    created = 0
    updated = 0
    for profile in profiles:
        score_payload = build_governance_score_for_profile(profile, settings_snapshot=settings_snapshot)
        snapshot = existing.get(profile.table_id)
        if snapshot is None:
            snapshot = GovernanceScoreSnapshot(
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
        snapshot.score = int(score_payload["score"])
        snapshot.label = str(score_payload["label"])
        snapshot.tone = str(score_payload["tone"])
        snapshot.dq_score = float(profile.dq_score) if profile.dq_score is not None else None
        snapshot.open_incidents = int(profile.open_incidents or 0)
    cutoff = bucket - timedelta(days=max(retention_days, 30))
    stale_rows = session.scalars(
        select(GovernanceScoreSnapshot).where(GovernanceScoreSnapshot.bucket_date < cutoff)
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


def get_table_governance_score_history(session: Session, *, table_id: int, limit: int = 30) -> list[dict[str, Any]]:
    rows = session.scalars(
        select(GovernanceScoreSnapshot)
        .where(GovernanceScoreSnapshot.table_id == table_id)
        .order_by(GovernanceScoreSnapshot.bucket_date.desc())
        .limit(max(1, min(limit, 180)))
    ).all()
    return [
        {
            "bucket_date": row.bucket_date,
            "score": int(row.score),
            "label": row.label,
            "tone": row.tone,
            "dq_score": float(row.dq_score) if row.dq_score is not None else None,
            "open_incidents": int(row.open_incidents or 0),
        }
        for row in reversed(rows)
    ]


def summarize_table_governance_score_trend(session: Session, *, table_id: int, limit: int = 14) -> dict[str, Any] | None:
    history = get_table_governance_score_history(session, table_id=table_id, limit=limit)
    if not history:
        return None
    latest = history[-1]
    baseline = history[0]
    delta = int(latest["score"]) - int(baseline["score"])
    if delta > 0:
        direction = "up"
        label = "Em evolução"
        tone = "success"
    elif delta < 0:
        direction = "down"
        label = "Em queda"
        tone = "warning"
    else:
        direction = "flat"
        label = "Estável"
        tone = "neutral"
    return {
        "current_score": int(latest["score"]),
        "baseline_score": int(baseline["score"]),
        "delta": delta,
        "direction": direction,
        "label": label,
        "tone": tone,
        "history": history,
    }


def summarize_governance_score_trend(session: Session, *, days: int = 14) -> dict[str, Any]:
    limit = max(2, min(days, 60))
    rows = session.scalars(
        select(GovernanceScoreSnapshot)
        .order_by(GovernanceScoreSnapshot.bucket_date.desc())
    ).all()
    buckets: dict[datetime, list[int]] = {}
    for row in rows:
        buckets.setdefault(row.bucket_date, []).append(int(row.score))
    ordered_dates = sorted(buckets.keys())[-limit:]
    series = [
        {
            "bucket_date": bucket_date,
            "avg_score": round(sum(scores) / len(scores), 1) if scores else 0.0,
            "assets": len(scores),
        }
        for bucket_date in ordered_dates
        if (scores := buckets.get(bucket_date))
    ]
    if not series:
        return {"history": [], "delta": 0.0, "direction": "flat", "label": "Sem histórico", "tone": "neutral"}
    baseline = series[0]["avg_score"]
    current = series[-1]["avg_score"]
    delta = round(float(current) - float(baseline), 1)
    if delta > 0:
        direction = "up"
        label = "Em evolução"
        tone = "success"
    elif delta < 0:
        direction = "down"
        label = "Em queda"
        tone = "warning"
    else:
        direction = "flat"
        label = "Estável"
        tone = "neutral"
    return {
        "history": series,
        "delta": delta,
        "direction": direction,
        "label": label,
        "tone": tone,
    }
