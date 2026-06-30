from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import desc, func, select
from sqlalchemy.orm import Session

from t2c_data.core.telemetry import runtime_metrics
from t2c_data.features.governance.settings import get_governance_settings_snapshot
from t2c_data.features.dashboard.executive_scoring import compute_priority_score
from t2c_data.features.catalog.correlation import build_correlation_priority_payload
from t2c_data.features.ingestion import load_ingestion_operational_overview_from_source
from t2c_data.features.governance.rules import certification_review_due, owner_review_due, privacy_review_due
from t2c_data.features.operations.failures import failure_summary
from t2c_data.features.operations.backups import backup_health_snapshot
from t2c_data.features.platform.analytics import analytics_summary
from t2c_data.features.platform.read_models import load_dashboard_profiles_with_fallback
from t2c_data.models.catalog import DataSource, Schema, TableEntity
from t2c_data.models.dq import DQJobRun
from t2c_data.models.incident import Incident
from t2c_data.models.scan import ScanRun


def cockpit_summary(session: Session, current_user=None) -> dict[str, object]:
    now = datetime.now(timezone.utc)
    settings_snapshot = get_governance_settings_snapshot(session)
    tables, _source = load_dashboard_profiles_with_fallback(session, now, current_user=current_user)

    last_scan_failures = session.execute(
        select(ScanRun.id, ScanRun.datasource_id, ScanRun.status, ScanRun.created_at)
        .where(~ScanRun.status.in_(["success", "succeeded"]))
        .order_by(desc(ScanRun.created_at))
        .limit(5)
    ).all()
    dq_job_failures = session.execute(
        select(DQJobRun.id, DQJobRun.job_type, DQJobRun.table_fqn, DQJobRun.status, DQJobRun.created_at)
        .where(DQJobRun.status.in_(["failed", "error"]))
        .order_by(desc(DQJobRun.created_at))
        .limit(5)
    ).all()
    dq_failure_table_ids = {
        table_fqn: table_id
        for table_id, table_fqn in session.execute(
            select(TableEntity.id, (Schema.name + "." + TableEntity.name))
            .join(Schema, TableEntity.schema_id == Schema.id)
            .where((Schema.name + "." + TableEntity.name).in_([row.table_fqn for row in dq_job_failures if row.table_fqn] or ["__none__"]))
        ).all()
        if table_fqn
    }
    critical_incidents = int(
        session.scalar(
            select(func.count(Incident.id)).where(
                Incident.status.in_(["open", "investigating"]),
                Incident.severity == "sev1",
            )
        )
        or 0
    )
    datasource_total = int(session.scalar(select(func.count(DataSource.id))) or 0)
    inactive_datasources = int(session.scalar(select(func.count(DataSource.id)).where(DataSource.is_active.is_(False))) or 0)
    analytics_payload = analytics_summary(session, days=30, current_user=current_user)
    failure_snapshot = failure_summary(session, limit=12)
    backup_snapshot = backup_health_snapshot(session)

    def _item_key(item: dict[str, object]) -> str | None:
        table_id = item.get("table_id")
        if table_id is not None:
            return f"id:{int(table_id)}"
        table_fqn = str(item.get("table_fqn") or "").strip()
        if table_fqn:
            return f"fqn:{table_fqn.lower()}"
        schema_name = str(item.get("schema_name") or "").strip()
        table_name = str(item.get("table_name") or "").strip()
        if schema_name and table_name:
            return f"fqn:{schema_name.lower()}.{table_name.lower()}"
        return None

    top_asset_clicks = {
        int(item["asset_id"]): int(item.get("total_clicks") or 0)
        for item in list(analytics_payload.get("top_assets") or [])
        if item.get("asset_id") is not None
    }

    critical_without_owner = [
        table for table in tables if not table.owner_defined and (table.critical_open_incidents > 0 or (table.dq_score or 100) < 70)
    ][:8]
    sensitive_without_classification = [
        table for table in tables if (table.has_personal_data or table.has_sensitive_personal_data) and not table.classification_defined
    ][:8]
    overdue_reviews = [
        table
        for table in tables
        if owner_review_due(table, now=now, settings_snapshot=settings_snapshot)
        or privacy_review_due(table, now=now, settings_snapshot=settings_snapshot)
        or certification_review_due(table, now=now, settings_snapshot=settings_snapshot)
    ][:8]
    ingestion = load_ingestion_operational_overview_from_source(
        session,
        table_refs=[
            {
                "table_id": table.table_id,
                "table_name": table.table_name,
                "table_fqn": table.table_fqn,
                "schema_name": table.schema_name,
                "criticality_score": compute_priority_score(
                    table,
                    recent_incident_count=table.open_incidents,
                    recent_occurrences=table.open_incidents,
                )[0],
            }
            for table in tables
        ],
        limit=8,
        high_volume_threshold_rows=settings_snapshot.operational_high_volume_threshold_rows,
        stale_threshold_hours=settings_snapshot.platform_recent_success_window_hours,
        airflow_ui_base_url=settings_snapshot.airflow_ui_base_url,
    )
    ingestion_keys_failed = {_item_key(item) for item in list(ingestion.get("failed_items") or [])}
    ingestion_keys_degraded = {_item_key(item) for item in list(ingestion.get("degraded_items") or [])}
    ingestion_keys_stale = {_item_key(item) for item in list(ingestion.get("critical_stale_items") or [])}
    ingestion_keys_high_volume_failed = {_item_key(item) for item in list(ingestion.get("high_volume_failed_items") or [])}
    failed_operational_by_table_id: dict[int, dict[str, object]] = {
        int(item["table_id"]): item
        for item in list(ingestion.get("failed_items") or [])
        if item.get("table_id") is not None
    }
    ingestion_items_by_key: dict[str, dict[str, object]] = {}
    for item in list(ingestion.get("items") or []):
        key = _item_key(item)
        if key:
            ingestion_items_by_key[key] = item

    correlation_priority = []
    combined_failure_dq_incident = []
    for table in tables:
        table_key = f"id:{table.table_id}"
        qualified_name = f"{table.datasource_name}.{table.database_name}.{table.schema_name}.{table.table_name}"
        ingestion_item = ingestion_items_by_key.get(table_key) or ingestion_items_by_key.get(f"fqn:{table.schema_name.lower()}.{table.table_name.lower()}")
        has_operational_failure = bool(
            table_key in ingestion_keys_failed
            or table_key in ingestion_keys_stale
            or table_key in ingestion_keys_high_volume_failed
            or table_key in ingestion_keys_degraded
            or (
                ingestion_item
                and (
                    str(ingestion_item.get("latest_status_label") or "").strip() == "Falha"
                    or str(ingestion_item.get("last_status") or "").strip().lower() in {"failed", "error"}
                    or bool((ingestion_item.get("last_error") or "").strip())
                )
            )
        )
        has_dq_degradation = bool(table.dq_score is not None and table.dq_score < 90)
        has_open_incident = table.open_incidents > 0
        if not (has_operational_failure or has_dq_degradation or has_open_incident):
            continue
        payload = build_correlation_priority_payload(
            table_id=table.table_id,
            asset_name=table.table_name,
            qualified_name=qualified_name,
            schema_name=table.schema_name,
            source_name=table.datasource_name,
            has_operational_failure=has_operational_failure,
            has_dq_degradation=has_dq_degradation,
            has_open_incident=has_open_incident,
            access_clicks=top_asset_clicks.get(table.table_id, 0),
            total_clicks=top_asset_clicks.get(table.table_id, 0),
            table_fqn=f"{table.schema_name}.{table.table_name}",
        )
        correlation_priority.append(payload)
        if table.table_id in failed_operational_by_table_id:
            dq_score = float(table.dq_score or 0)
            if dq_score < 90 and table.open_incidents > 0:
                failed_item = failed_operational_by_table_id[table.table_id]
                combined_failure_dq_incident.append(
                    {
                        **failed_item,
                        "hint": (
                            f"DQ {dq_score:.0f} pts · {table.open_incidents} incidente(s) aberto(s) · "
                            "falha operacional recente no pipeline associado."
                        ),
                    }
                )
    correlation_priority.sort(
        key=lambda item: (
            -int(item["priority_score"]),
            -int(1 if item["has_operational_failure"] else 0),
            -int(1 if item["has_dq_degradation"] else 0),
            -int(1 if item["has_open_incident"] else 0),
            -int(item.get("total_clicks") or 0),
            str(item["qualified_name"]),
        )
    )
    return {
        "generated_at": now.isoformat(),
        "runtime": runtime_metrics.snapshot(),
        "health": {
            "datasources_total": datasource_total,
            "inactive_datasources": inactive_datasources,
            "critical_incidents": critical_incidents,
            "scan_failures_last_24h": sum(
                1 for row in last_scan_failures if row.created_at and row.created_at >= now - timedelta(hours=24)
            ),
            "dq_failures_last_24h": sum(
                1 for row in dq_job_failures if row.created_at and row.created_at >= now - timedelta(hours=24)
            ),
        },
        "queues": {
            "critical_without_owner": [
                {"table_id": table.table_id, "table_name": table.table_name, "table_fqn": table.table_fqn, "target_url": f"/explorer?tableId={table.table_id}"}
                for table in critical_without_owner
            ],
            "sensitive_without_classification": [
                {"table_id": table.table_id, "table_name": table.table_name, "table_fqn": table.table_fqn, "target_url": f"/privacy-access?tableId={table.table_id}"}
                for table in sensitive_without_classification
            ],
            "overdue_reviews": [
                {"table_id": table.table_id, "table_name": table.table_name, "table_fqn": table.table_fqn, "target_url": f"/dashboard?tableId={table.table_id}"}
                for table in overdue_reviews
            ],
            "sem_pipeline_mapeado": list(ingestion.get("unmapped_items") or []),
            "pipeline_degradado": list(ingestion.get("degraded_items") or []),
            "falha_operacional": list(ingestion.get("failed_items") or []),
            "falha_operacional_alto_consumo": list(ingestion.get("high_volume_failed_items") or []),
            "falha_dq_incidente": combined_failure_dq_incident[:8],
            "criticos_sem_sucesso_recente": list(ingestion.get("critical_stale_items") or []),
        },
        "correlation_priority": correlation_priority[:8],
        "recent_failures": {
            "scan_runs": [
                {
                    "id": int(row.id),
                    "datasource_id": int(row.datasource_id) if row.datasource_id is not None else None,
                    "status": row.status,
                    "created_at": row.created_at.isoformat() if row.created_at else None,
                    "target_url": f"/ops/cockpit?datasourceId={int(row.datasource_id)}" if row.datasource_id is not None else None,
                }
                for row in last_scan_failures
            ],
            "dq_jobs": [
                {
                    "id": int(row.id),
                    "table_id": dq_failure_table_ids.get(str(row.table_fqn)),
                    "job_type": row.job_type,
                    "table_fqn": row.table_fqn,
                    "status": row.status,
                    "created_at": row.created_at.isoformat() if row.created_at else None,
                    "target_url": (
                        f"/data-quality?tableId={int(dq_failure_table_ids[str(row.table_fqn)])}"
                        if row.table_fqn and dq_failure_table_ids.get(str(row.table_fqn)) is not None
                        else None
                    ),
                }
                for row in dq_job_failures
            ],
        },
        "ingestion": ingestion,
        "operational_failures": failure_snapshot,
        "backup": backup_snapshot,
        "analytics": analytics_payload,
    }
