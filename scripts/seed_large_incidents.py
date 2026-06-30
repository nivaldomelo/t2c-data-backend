"""Seed large synthetic incidents, audit history, and row-count snapshots for scale validation.

This script is intentionally manual and should not be executed from unit tests.

Example:
    python backend/scripts/seed_large_incidents.py --incidents 100000 --audit-events 1000000 --snapshots-per-table 8
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from itertools import cycle

from sqlalchemy import func, insert, select

from t2c_data.core.db import SessionLocal
from t2c_data.models.audit import AuditLog
from t2c_data.models.catalog import Schema, TableEntity
from t2c_data.models.incident import Incident
from t2c_data.models.platform import AssetRowCountSnapshot


def _chunked(items: list[dict], size: int):
    for idx in range(0, len(items), size):
        yield items[idx : idx + size]


def _table_rows(session):
    return session.execute(
        select(
            TableEntity.id,
            TableEntity.name,
            Schema.name,
            Schema.database_id,
        )
        .join(Schema, TableEntity.schema_id == Schema.id)
        .where(TableEntity.name.like("scale_table_%"))
        .order_by(TableEntity.id.asc())
    ).all()


def _seed_incidents(session, *, tables: list[tuple[int, str, str, int]], incidents: int, now: datetime, chunk_size: int) -> int:
    existing = int(
        session.scalar(
            select(func.count()).select_from(Incident).where(Incident.title.like("Scale incident %"))
        )
        or 0
    )
    remaining = max(incidents - existing, 0)
    if remaining == 0:
        return 0

    statuses = cycle(["open", "investigating", "mitigated", "resolved", "closed", "reopened", "recurring"])
    severities = cycle(["sev1", "sev2", "sev3", "sev4"])
    source_types = cycle(["dq_rule", "dq_profile", "platform_job", "manual"])
    rows: list[dict] = []
    for idx in range(existing + 1, existing + remaining + 1):
        table_id, table_name, schema_name, database_id = tables[(idx - 1) % len(tables)]
        status = next(statuses)
        severity = next(severities)
        source_type = next(source_types)
        rows.append(
            {
                "title": f"Scale incident {idx:07d}",
                "description": f"Synthetic incident {idx} for scale validation.",
                "entity_type": "table",
                "table_fqn": f"scale-benchmark.{schema_name}.{table_name}",
                "detected_at": now - timedelta(minutes=idx % 525600),
                "last_seen_at": now - timedelta(minutes=idx % 1440) if status in {"open", "investigating", "recurring"} else None,
                "status": status,
                "severity": severity,
                "source_type": source_type,
                "source_ref_id": (idx % 5000) + 1,
                "evidence_json": {
                    "severity": severity,
                    "source_type": source_type,
                    "table_id": table_id,
                    "database_id": database_id,
                    "row_index": idx,
                },
                "technical_origin_json": {
                    "module": "scale_seed",
                    "source": source_type,
                },
                "impact_json": {
                    "impact": "Synthetic operational impact for scale validation.",
                },
                "mitigation_json": {
                    "mitigation": "Synthetic mitigation path.",
                },
                "root_cause": "Synthetic load test condition",
                "impact_summary": "Synthetic impact summary for benchmark data.",
                "mitigation_summary": "Synthetic mitigation summary for benchmark data.",
                "postmortem_summary": None if status in {"open", "investigating"} else "Synthetic closure note.",
                "domain_name": f"domain-{(idx % 12) + 1:02d}",
                "owner_team": f"team-{(idx % 40) + 1:02d}",
                "squad_name": f"squad-{(idx % 18) + 1:02d}",
                "recurrence_count": idx % 9,
                "occurrences": 1 + (idx % 4),
                "tags": ["scale", "benchmark"],
                "created_at": now,
                "updated_at": now,
            }
        )
        if len(rows) >= chunk_size:
            session.execute(insert(Incident), rows)
            session.commit()
            rows = []
    if rows:
        session.execute(insert(Incident), rows)
        session.commit()
    return remaining


def _seed_audit_events(
    session,
    *,
    events: int,
    now: datetime,
    chunk_size: int,
    tables: list[tuple[int, str, str, int]],
) -> int:
    existing = int(
        session.scalar(
            select(func.count()).select_from(AuditLog).where(AuditLog.request_id.like("scale-request-%"))
        )
        or 0
    )
    remaining = max(events - existing, 0)
    if remaining == 0:
        return 0

    actions = cycle(
        [
            "table.update",
            "table.review",
            "dq.rule.evaluate",
            "incident.update",
            "privacy.review",
            "owner.reassign",
            "platform.job.finish",
            "audit.export",
        ]
    )
    source_modules = cycle(["catalog", "data_quality", "incidents", "privacy_access", "governance", "platform", "audit"])
    rows: list[dict] = []
    for idx in range(existing + 1, existing + remaining + 1):
        table_id, table_name, schema_name, _database_id = tables[(idx - 1) % len(tables)]
        action = next(actions)
        source_module = next(source_modules)
        rows.append(
            {
                "created_at": now - timedelta(seconds=idx % 2592000),
                "user_id": None,
                "actor_name": f"Scale Actor {((idx - 1) % 250) + 1:03d}",
                "user_email": f"scale-actor-{((idx - 1) % 250) + 1:03d}@example.local",
                "ip": f"10.{idx % 250}.{(idx // 250) % 250}.{idx % 250}",
                "user_agent": "scale-benchmark/1.0",
                "action": action,
                "entity_type": "table" if idx % 3 else "incident",
                "entity_id": str(table_id if idx % 3 else (idx % 100000) + 1),
                "parent_entity_type": "schema" if idx % 5 == 0 else None,
                "parent_entity_id": str(schema_name) if idx % 5 == 0 else None,
                "change_set_id": f"scale-change-{idx % 250000:06d}",
                "change_type": "update" if idx % 2 else "create",
                "field_name": "description" if idx % 4 else "certification_status",
                "source_module": source_module,
                "is_sensitive_change": idx % 7 == 0,
                "sensitive_category": "export" if action == "audit.export" else "governance" if idx % 5 == 0 else None,
                "route": f"/api/v1/scale/{idx % 50}",
                "method": "POST" if idx % 2 else "PATCH",
                "status_code": 200 if idx % 17 else 403,
                "request_id": f"scale-request-{idx:07d}",
                "before_json": {"value": idx - 1, "table": table_name} if idx % 3 == 0 else None,
                "after_json": {"value": idx, "table": table_name} if idx % 3 == 0 else None,
                "metadata_json": {
                    "table_id": table_id,
                    "schema_name": schema_name,
                    "benchmark": True,
                },
            }
        )
        if len(rows) >= chunk_size:
            session.execute(insert(AuditLog), rows)
            session.commit()
            rows = []
    if rows:
        session.execute(insert(AuditLog), rows)
        session.commit()
    return remaining


def _seed_row_count_snapshots(
    session,
    *,
    tables: list[tuple[int, str, str, int]],
    snapshots_per_table: int,
    now: datetime,
    chunk_size: int,
) -> int:
    target_total = len(tables) * max(int(snapshots_per_table), 0)
    existing = int(
        session.scalar(
            select(func.count())
            .select_from(AssetRowCountSnapshot)
            .where(AssetRowCountSnapshot.asset_fqn.like("scale-benchmark.%"))
        )
        or 0
    )
    if existing >= target_total:
        return 0

    measurement_sources = cycle(["catalog_profile", "manual", "postgres_count", "sql_count"])
    rows: list[dict] = []
    total = 0
    skip_existing = existing
    for table_index, (table_id, table_name, schema_name, _database_id) in enumerate(tables, start=1):
        for snapshot_index in range(1, snapshots_per_table + 1):
            if skip_existing > 0:
                skip_existing -= 1
                continue
            source = next(measurement_sources)
            observed_at = now - timedelta(hours=(table_index % 720) + snapshot_index)
            row_count = table_index * 1000 + snapshot_index * 17
            rows.append(
                {
                    "asset_type": "table",
                    "asset_id": table_id,
                    "asset_name": table_name,
                    "asset_fqn": f"scale-benchmark.{schema_name}.{table_name}",
                    "source": source,
                    "observed_at": observed_at,
                    "row_count": row_count,
                    "row_count_method": "count" if source in {"postgres_count", "sql_count"} else "profile",
                    "row_count_confidence": "high" if source in {"postgres_count", "sql_count"} else "medium",
                    "integration_sync_job_id": None,
                    "context_json": {
                        "table_id": table_id,
                        "schema_name": schema_name,
                        "snapshot_index": snapshot_index,
                        "benchmark": True,
                    },
                    "created_at": observed_at,
                    "updated_at": observed_at,
                }
            )
            total += 1
            if len(rows) >= chunk_size:
                session.execute(insert(AssetRowCountSnapshot), rows)
                session.commit()
                rows = []
    if rows:
        session.execute(insert(AssetRowCountSnapshot), rows)
        session.commit()
    return total


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed large synthetic incidents, audit history, and row-count snapshots.")
    parser.add_argument("--incidents", type=int, default=100000)
    parser.add_argument("--audit-events", type=int, default=1000000)
    parser.add_argument("--snapshots-per-table", type=int, default=8)
    parser.add_argument("--chunk-size", type=int, default=5000)
    args = parser.parse_args()

    now = datetime.now(timezone.utc)
    session = SessionLocal()
    try:
        tables = [
            (int(table_id), str(table_name), str(schema_name), int(database_id))
            for table_id, table_name, schema_name, database_id in _table_rows(session)
        ]
        if not tables:
            raise SystemExit("No scale tables found. Run seed_large_catalog.py first.")

        incidents_seeded = _seed_incidents(
            session,
            tables=tables,
            incidents=args.incidents,
            now=now,
            chunk_size=args.chunk_size,
        )
        audit_events_seeded = _seed_audit_events(
            session,
            events=args.audit_events,
            now=now,
            chunk_size=args.chunk_size,
            tables=tables,
        )
        snapshots_seeded = _seed_row_count_snapshots(
            session,
            tables=tables,
            snapshots_per_table=args.snapshots_per_table,
            now=now,
            chunk_size=args.chunk_size,
        )

        print(
            {
                "tables_found": len(tables),
                "incidents_seeded": incidents_seeded,
                "audit_events_seeded": audit_events_seeded,
                "row_count_snapshots_seeded": snapshots_seeded,
                "snapshots_per_table": args.snapshots_per_table,
            }
        )
    finally:
        session.close()


if __name__ == "__main__":
    main()
