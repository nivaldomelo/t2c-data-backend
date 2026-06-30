from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from t2c_data.features.integrations.data_lake import get_data_lake_connection_or_404
from t2c_data.features.integrations.data_lake_inventory import serialize_data_lake_scan_run
from t2c_data.models.platform import DataLakeInventoryScanRun, DataLakeInventoryTable, DataLakeTableObservation
from t2c_data.schemas.integrations import (
    DataLakeConnectionOperationalLayerOut,
    DataLakeOperationalIssueOut,
    DataLakeOperationsSummaryOut,
    DataLakeTroubleshootingOut,
    DataLakeInventoryScanRunOut,
)


def _latest_observations_by_table(session: Session, connection_id: int) -> dict[int, DataLakeTableObservation]:
    observations = session.scalars(
        select(DataLakeTableObservation)
        .join(DataLakeInventoryTable, DataLakeInventoryTable.id == DataLakeTableObservation.table_id)
        .where(DataLakeInventoryTable.connection_id == connection_id)
        .order_by(DataLakeTableObservation.table_id.asc(), DataLakeTableObservation.created_at.desc(), DataLakeTableObservation.id.desc())
    ).all()
    latest: dict[int, DataLakeTableObservation] = {}
    for observation in observations:
        if observation.table_id not in latest:
            latest[observation.table_id] = observation
    return latest


def _issue(
    key: str,
    label: str,
    *,
    tone: str = "neutral",
    detail: str | None = None,
    recommended_action: str | None = None,
    table_id: int | None = None,
    table_name: str | None = None,
) -> DataLakeOperationalIssueOut:
    return DataLakeOperationalIssueOut(
        key=key,
        label=label,
        tone=tone,
        detail=detail,
        recommended_action=recommended_action,
        table_id=table_id,
        table_name=table_name,
    )


def load_data_lake_operations_summary(session: Session, connection_id: int) -> DataLakeOperationsSummaryOut:
    connection = get_data_lake_connection_or_404(session, connection_id)
    tables = session.scalars(
        select(DataLakeInventoryTable).where(DataLakeInventoryTable.connection_id == connection.id).order_by(DataLakeInventoryTable.layer.asc(), DataLakeInventoryTable.table_name.asc())
    ).all()
    latest_scan = session.scalar(
        select(DataLakeInventoryScanRun)
        .where(DataLakeInventoryScanRun.connection_id == connection.id)
        .order_by(DataLakeInventoryScanRun.created_at.desc(), DataLakeInventoryScanRun.id.desc())
        .limit(1)
    )
    recent_runs = session.scalars(
        select(DataLakeInventoryScanRun)
        .where(DataLakeInventoryScanRun.connection_id == connection.id)
        .order_by(DataLakeInventoryScanRun.created_at.desc(), DataLakeInventoryScanRun.id.desc())
        .limit(5)
    ).all()
    latest_observations = _latest_observations_by_table(session, connection.id)

    tables_without_parquet = [table for table in tables if table.parquet_files_count <= 0]
    tables_with_error = [table for table in tables if table.status_scan == "error" or bool(table.error_message)]
    stale_tables = [
        table
        for table in tables
        if (latest_observations.get(table.id).freshness_status if latest_observations.get(table.id) else "unknown") != "fresh"
    ]
    tables_with_drift = [table for table in tables if latest_observations.get(table.id) and latest_observations[table.id].drift_detected]

    layer_buckets: dict[str, list[float]] = defaultdict(list)
    layer_table_count: dict[str, int] = defaultdict(int)
    layer_stale_count: dict[str, int] = defaultdict(int)
    for table in tables:
        layer_table_count[table.layer] += 1
        observation = latest_observations.get(table.id)
        if observation and observation.quality_score is not None:
            layer_buckets[table.layer].append(float(observation.quality_score))
        if observation and observation.freshness_status != "fresh":
            layer_stale_count[table.layer] += 1

    issues: list[DataLakeOperationalIssueOut] = []
    if connection.last_test_status and connection.last_test_status != "success":
        issues.append(
            _issue(
                "connection_test",
                "Conexão com atenção",
                tone="warning",
                detail=connection.last_test_message or "Último teste não retornou sucesso.",
                recommended_action="Revalidar credenciais e acesso ao bucket/prefixo.",
            )
        )
    if latest_scan and latest_scan.status == "error":
        issues.append(
            _issue(
                "latest_scan_error",
                "Última varredura falhou",
                tone="danger",
                detail=latest_scan.error_message or "O scan mais recente terminou com erro.",
                recommended_action="Reexecutar o scan e revisar logs da integração.",
            )
        )
    if tables_without_parquet:
        first = tables_without_parquet[0]
        issues.append(
            _issue(
                "no_parquet",
                "Tabelas sem parquet válido",
                tone="warning",
                detail=f"{len(tables_without_parquet)} tabela(s) não têm parquet válido.",
                recommended_action="Verificar o prefixo e a organização das pastas.",
                table_id=first.id,
                table_name=first.table_name,
            )
        )
    if stale_tables:
        first = stale_tables[0]
        issues.append(
            _issue(
                "stale_tables",
                "Dados sem atualização recente",
                tone="warning",
                detail=f"{len(stale_tables)} tabela(s) estão fora do freshness esperado.",
                recommended_action="Reescanear a conexão e revisar o SLA de freshness.",
                table_id=first.id,
                table_name=first.table_name,
            )
        )
    if tables_with_error:
        first = tables_with_error[0]
        issues.append(
            _issue(
                "table_error",
                "Erros de leitura detectados",
                tone="danger",
                detail=f"{len(tables_with_error)} tabela(s) registraram falha de leitura ou scan.",
                recommended_action="Abrir o detalhe da tabela e revisar o erro técnico.",
                table_id=first.id,
                table_name=first.table_name,
            )
        )
    if tables_with_drift:
        first = tables_with_drift[0]
        issues.append(
            _issue(
                "schema_drift",
                "Drift de schema detectado",
                tone="warning",
                detail=f"{len(tables_with_drift)} tabela(s) apresentam drift ou variação entre amostras.",
                recommended_action="Validar compatibilidade de schema antes de consumir o ativo.",
                table_id=first.id,
                table_name=first.table_name,
            )
        )

    average_quality_score = None
    if tables:
        quality_scores = [float(obs.quality_score) for obs in latest_observations.values() if obs.quality_score is not None]
        if quality_scores:
            average_quality_score = round(sum(quality_scores) / len(quality_scores), 1)

    layer_summaries = [
        DataLakeConnectionOperationalLayerOut(
            layer=layer,
            tables_count=layer_table_count.get(layer, 0),
            average_quality_score=round(sum(layer_buckets[layer]) / len(layer_buckets[layer]), 1) if layer_buckets[layer] else None,
            tables_without_recent_update=layer_stale_count.get(layer, 0),
            stale_tables_count=layer_stale_count.get(layer, 0),
        )
        for layer in ("bronze", "silver", "gold")
    ]

    last_scan_duration_seconds = None
    if latest_scan and latest_scan.started_at and latest_scan.finished_at:
        last_scan_duration_seconds = max(0, int((latest_scan.finished_at - latest_scan.started_at).total_seconds()))

    return DataLakeOperationsSummaryOut(
        connection_id=connection.id,
        connection_name=connection.name,
        last_scan_at=latest_scan.finished_at if latest_scan else None,
        last_scan_duration_seconds=last_scan_duration_seconds,
        last_scan_status=latest_scan.status if latest_scan else None,
        last_scan_error=latest_scan.error_message if latest_scan else None,
        tables_total=len(tables),
        tables_scanned=len([table for table in tables if table.data_last_scan_at is not None]),
        tables_with_error=len(tables_with_error),
        tables_without_parquet=len(tables_without_parquet),
        tables_without_recent_update=len(stale_tables),
        tables_with_drift=len(tables_with_drift),
        average_quality_score=average_quality_score,
        layer_summaries=layer_summaries,
        recent_scan_runs=[DataLakeInventoryScanRunOut.model_validate(serialize_data_lake_scan_run(run)) for run in recent_runs],
        issues=issues,
    )


def load_data_lake_troubleshooting(session: Session, connection_id: int) -> DataLakeTroubleshootingOut:
    summary = load_data_lake_operations_summary(session, connection_id)
    status = "ok" if not summary.issues else "attention"
    text = "Sem alertas relevantes no momento." if not summary.issues else f"{len(summary.issues)} sinal(is) exigem atenção."
    return DataLakeTroubleshootingOut(
        connection_id=summary.connection_id,
        connection_name=summary.connection_name,
        status=status,
        summary=text,
        items=summary.issues,
    )
