from __future__ import annotations

from datetime import datetime, timezone

from fastapi import HTTPException, status
from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from t2c_data.features.catalog.correlation import build_table_correlation_summary
from t2c_data.features.data_quality.spark_launch_commands import launch_spark_profiling_run
from t2c_data.features.incidents.api_support import create_incident_model
from t2c_data.features.platform.analytics import track_usage_event
from t2c_data.features.scanner.application import enqueue_datasource_scan
from t2c_data.models.auth import User
from t2c_data.models.catalog import DataSource, Schema, TableEntity
from t2c_data.models.incident import Incident
from t2c_data.schemas.dq import DQSparkProfilingRunRequest
from t2c_data.schemas.incident import IncidentCreate
from t2c_data.services.audit import write_audit_log_sync


def reprocess_datasource_scan(session: Session, *, datasource_id: int, current_user: User, audit_kwargs: dict) -> dict[str, object]:
    datasource = session.get(DataSource, datasource_id)
    if datasource is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Datasource não encontrada.")
    run, job = enqueue_datasource_scan(session, datasource=datasource, started_by=current_user.id, trigger_mode="manual")
    write_audit_log_sync(
        session,
        action="platform.cockpit.scan.reprocess",
        entity_type="scan_run",
        entity_id=run.id,
        parent_entity_type="datasource",
        parent_entity_id=datasource_id,
        source_module="platform.cockpit",
        metadata={"datasource_id": datasource_id},
        **audit_kwargs,
    )
    track_usage_event(
        session,
        user=current_user,
        event_name="reprocess_scan",
        module_name="ops_cockpit",
        entity_type="datasource",
        entity_id=datasource_id,
        metadata={"scan_run_id": run.id, "integration_job_id": job.id if job is not None else None},
    )
    session.commit()
    return {"ok": True, "message": "Scan reenfileirado com sucesso.", "target_id": run.id}


def rerun_table_profiling(session: Session, *, table_id: int, current_user: User, audit_kwargs: dict) -> dict[str, object]:
    table = session.get(TableEntity, table_id)
    if table is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Ativo não encontrado.")
    launched = launch_spark_profiling_run(
        db=session,
        payload=DQSparkProfilingRunRequest(scope="table", table_id=table_id, limit=200, concurrency=5),
        current_user=current_user,
        audit_kwargs=audit_kwargs,
    )
    track_usage_event(
        session,
        user=current_user,
        event_name="rerun_profiling",
        module_name="ops_cockpit",
        entity_type="table",
        entity_id=table_id,
        metadata={"dq_run_id": launched.run_id, "job_run_id": launched.job_run_id},
    )
    session.commit()
    return {"ok": True, "message": "Profiling reenfileirado com sucesso.", "target_id": launched.run_id}


def open_operational_incident(
    session: Session,
    *,
    table_id: int,
    current_user: User,
    audit_kwargs: dict,
    mode: str = "manual",
) -> dict[str, object]:
    row = session.execute(
        select(TableEntity, Schema.name.label("schema_name"))
        .join(Schema, TableEntity.schema_id == Schema.id)
        .where(TableEntity.id == table_id)
    ).first()
    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Ativo não encontrado.")
    table, schema_name = row
    table_fqn = f"{schema_name}.{table.name}"
    existing = session.scalar(
        select(Incident)
        .where(
            Incident.entity_type == "table",
            Incident.table_fqn == table_fqn,
            Incident.status.in_(["open", "investigating"]),
        )
        .order_by(desc(Incident.updated_at))
    )
    if existing is not None:
        track_usage_event(
            session,
            user=current_user,
            event_name="open_existing_incident",
            module_name="ops_cockpit",
            entity_type="incident",
            entity_id=existing.id,
            metadata={"table_id": table_id},
        )
        session.commit()
        return {"ok": True, "message": "Já existe um incidente aberto para este ativo.", "target_id": existing.id}

    correlation_summary = build_table_correlation_summary(db=session, table_id=table_id, current_user=current_user)
    if mode == "auto_if_missing":
        if not (correlation_summary.signals.operational_failure or correlation_summary.signals.stale_pipeline):
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Não há falha operacional elegível para abertura automática neste ativo.",
            )
        if correlation_summary.incident_prefill is None:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="A correlação operacional do ativo ainda não gerou evidência suficiente para abertura automática.",
            )
        prefill = correlation_summary.incident_prefill
        severity = "sev2" if correlation_summary.signals.dq_below_threshold else "sev3"
        payload = IncidentCreate(
            title=prefill.title,
            description=prefill.description,
            entity_type="table",
            table_fqn=table_fqn,
            detected_at=datetime.now(timezone.utc),
            status="open",
            severity=severity,
            source_type=prefill.source_type,
            source_ref_id=prefill.source_ref_id,
            evidence_json={**prefill.evidence_json, "opened_via": "platform_action_auto"},
            tags=["ops", "automatic"],
        )
        success_message = "Incidente operacional criado automaticamente a partir da correlação do ativo."
        audit_action = "platform.cockpit.incident.auto_open"
        event_name = "open_incident_auto"
    else:
        evidence_json = {"origin": "ops_cockpit", "table_id": table_id}
        if correlation_summary.incident_prefill is not None:
            evidence_json = {**correlation_summary.incident_prefill.evidence_json, "opened_via": "platform_action_manual"}
        payload = IncidentCreate(
            title=f"Tratamento operacional necessário em {table.name}",
            description="Incidente aberto a partir do cockpit operacional para tratamento do ativo.",
            entity_type="table",
            table_fqn=table_fqn,
            detected_at=datetime.now(timezone.utc),
            status="open",
            severity="sev3",
            source_type="platform_ops",
            source_ref_id=table_id,
            evidence_json=evidence_json,
            tags=["ops", "manual"],
        )
        success_message = "Incidente operacional aberto com sucesso."
        audit_action = "platform.cockpit.incident.open"
        event_name = "open_incident"

    incident = create_incident_model(payload, reporter_user_id=current_user.id)
    session.add(incident)
    session.flush()
    write_audit_log_sync(
        session,
        action=audit_action,
        entity_type="incident",
        entity_id=incident.id,
        parent_entity_type="table",
        parent_entity_id=table_id,
        source_module="platform.cockpit",
        metadata={"table_fqn": table_fqn, "mode": mode},
        **audit_kwargs,
    )
    track_usage_event(
        session,
        user=current_user,
        event_name=event_name,
        module_name="ops_cockpit",
        entity_type="incident",
        entity_id=incident.id,
        metadata={"table_id": table_id, "mode": mode},
    )
    session.commit()
    return {"ok": True, "message": success_message, "target_id": incident.id}
