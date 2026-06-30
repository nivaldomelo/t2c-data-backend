from __future__ import annotations

from fastapi import APIRouter, Depends
from fastapi.responses import PlainTextResponse
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from t2c_data.core.db import get_db
from t2c_data.core.deps import require_roles
from t2c_data.core.telemetry import runtime_metrics
from t2c_data.models.auth import User
from t2c_data.models.catalog import ColumnEntity, DataSource, Schema, TableEntity
from t2c_data.models.dq import DQJobRun, DQRuleRun
from t2c_data.models.glossary import GlossaryTerm
from t2c_data.models.incident import Incident
from t2c_data.models.platform import DashboardAssetReadModel, IntegrationSyncJob
from t2c_data.models.scan import ScanRun
from t2c_data.models.tag import Tag
from t2c_data.schemas.metrics import MetricsSummaryOut

router = APIRouter(prefix="/metrics", tags=["metrics"])
_ACTIVE_INCIDENT_STATUSES = ("open", "investigating", "mitigated", "reopened", "recurring")


@router.get("/summary", response_model=MetricsSummaryOut)
def metrics_summary(
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "viewer")),
) -> MetricsSummaryOut:
    return MetricsSummaryOut(
        datasources=int(db.scalar(select(func.count(DataSource.id))) or 0),
        schemas=int(db.scalar(select(func.count(Schema.id))) or 0),
        tables=int(db.scalar(select(func.count(TableEntity.id))) or 0),
        columns=int(db.scalar(select(func.count(ColumnEntity.id))) or 0),
        tags=int(db.scalar(select(func.count(Tag.id))) or 0),
        glossary_terms=int(db.scalar(select(func.count(GlossaryTerm.id))) or 0),
        last_scan_at=db.scalar(select(func.max(ScanRun.created_at))),
        requests=runtime_metrics.snapshot(),
    )


@router.get("/export", response_class=PlainTextResponse)
def metrics_export(
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "viewer")),
) -> PlainTextResponse:
    lines = [runtime_metrics.export_prometheus().rstrip()]
    integration_job_counts = {
        (str(source), str(status)): int(count)
        for source, status, count in db.execute(
            select(IntegrationSyncJob.source, IntegrationSyncJob.status, func.count(IntegrationSyncJob.id))
            .group_by(IntegrationSyncJob.source, IntegrationSyncJob.status)
        ).all()
    }
    metrics = {
        "scan_runs_total": int(db.scalar(select(func.count(ScanRun.id))) or 0),
        "scan_runs_failed_total": int(db.scalar(select(func.count(ScanRun.id)).where(ScanRun.status == "failed")) or 0),
        "scan_runs_partial_total": int(db.scalar(select(func.count(ScanRun.id)).where(ScanRun.status == "partial_success")) or 0),
        "dq_rule_runs_total": int(db.scalar(select(func.count(DQRuleRun.id))) or 0),
        "dq_jobs_queued_total": int(db.scalar(select(func.count(DQJobRun.id)).where(DQJobRun.status == "queued")) or 0),
        "dq_jobs_running_total": int(db.scalar(select(func.count(DQJobRun.id)).where(DQJobRun.status == "running")) or 0),
        "dq_jobs_failed_total": int(db.scalar(select(func.count(DQJobRun.id)).where(DQJobRun.status == "failed")) or 0),
        "dq_rule_violations_total": int(db.scalar(select(func.coalesce(func.sum(DQRuleRun.violations_count), 0))) or 0),
        "incidents_open_total": int(
            db.scalar(select(func.count(Incident.id)).where(Incident.status.in_(_ACTIVE_INCIDENT_STATUSES))) or 0
        ),
        "metabase_sync_jobs_total": int(
            db.scalar(select(func.count(IntegrationSyncJob.id)).where(IntegrationSyncJob.source == "metabase")) or 0
        ),
        "integration_job_failures_total": int(
            db.scalar(select(func.count(IntegrationSyncJob.id)).where(IntegrationSyncJob.status == "failed")) or 0
        ),
        "freshness_delayed_assets_total": int(
            db.scalar(
                select(func.count(DashboardAssetReadModel.table_id)).where(
                    DashboardAssetReadModel.freshness_seconds.is_not(None),
                    DashboardAssetReadModel.freshness_seconds > 24 * 3600,
                )
            )
            or 0
        ),
    }
    lines.extend(
        [
            "# HELP t2c_scan_runs_total Total de execucoes de scan persistidas.",
            "# TYPE t2c_scan_runs_total gauge",
            f"t2c_scan_runs_total {metrics['scan_runs_total']}",
            "# HELP t2c_scan_runs_failed_total Total de scans com falha.",
            "# TYPE t2c_scan_runs_failed_total gauge",
            f"t2c_scan_runs_failed_total {metrics['scan_runs_failed_total']}",
            "# HELP t2c_scan_runs_partial_total Total de scans com sucesso parcial.",
            "# TYPE t2c_scan_runs_partial_total gauge",
            f"t2c_scan_runs_partial_total {metrics['scan_runs_partial_total']}",
            "# HELP t2c_dq_rule_runs_total Total de execucoes de regras DQ.",
            "# TYPE t2c_dq_rule_runs_total gauge",
            f"t2c_dq_rule_runs_total {metrics['dq_rule_runs_total']}",
            "# HELP t2c_dq_jobs_queued_total Total de jobs DQ enfileirados.",
            "# TYPE t2c_dq_jobs_queued_total gauge",
            f"t2c_dq_jobs_queued_total {metrics['dq_jobs_queued_total']}",
            "# HELP t2c_dq_jobs_running_total Total de jobs DQ em execucao.",
            "# TYPE t2c_dq_jobs_running_total gauge",
            f"t2c_dq_jobs_running_total {metrics['dq_jobs_running_total']}",
            "# HELP t2c_dq_jobs_failed_total Total de jobs DQ com falha.",
            "# TYPE t2c_dq_jobs_failed_total gauge",
            f"t2c_dq_jobs_failed_total {metrics['dq_jobs_failed_total']}",
            "# HELP t2c_dq_rule_violations_total Total acumulado de violacoes DQ persistidas.",
            "# TYPE t2c_dq_rule_violations_total gauge",
            f"t2c_dq_rule_violations_total {metrics['dq_rule_violations_total']}",
            "# HELP t2c_incidents_open_total Total de incidentes ativos.",
            "# TYPE t2c_incidents_open_total gauge",
            f"t2c_incidents_open_total {metrics['incidents_open_total']}",
            "# HELP t2c_metabase_sync_jobs_total Total de syncs do Metabase registrados.",
            "# TYPE t2c_metabase_sync_jobs_total gauge",
            f"t2c_metabase_sync_jobs_total {metrics['metabase_sync_jobs_total']}",
            "# HELP t2c_integration_job_failures_total Total de jobs de integracao com falha.",
            "# TYPE t2c_integration_job_failures_total gauge",
            f"t2c_integration_job_failures_total {metrics['integration_job_failures_total']}",
            "# HELP t2c_freshness_delayed_assets_total Total de ativos com freshness acima de 24h.",
            "# TYPE t2c_freshness_delayed_assets_total gauge",
            f"t2c_freshness_delayed_assets_total {metrics['freshness_delayed_assets_total']}",
            "# HELP t2c_integration_jobs_by_source_status_total Total de jobs de integração por origem e status.",
            "# TYPE t2c_integration_jobs_by_source_status_total gauge",
        ]
    )
    for (source, status), count in sorted(integration_job_counts.items()):
        lines.append(
            't2c_integration_jobs_by_source_status_total'
            f'{{source="{source}",status="{status}"}} {count}'
        )
    return PlainTextResponse("\n".join(lines) + "\n")
