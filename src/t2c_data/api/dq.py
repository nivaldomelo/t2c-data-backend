from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request, status
from sqlalchemy.orm import Session

from t2c_data.core.db import get_db
from t2c_data.core.deps import require_roles
from t2c_data.features.data_quality.incident_signals import evaluate_table_dq_incident_signals
from t2c_data.features.data_quality.application import (
    build_dq_job_out,
    create_rule_with_audit,
    delete_rule_with_audit,
    get_latest_metrics_by_fqn,
    get_latest_metrics_by_table_id,
    get_rule_detail,
    launch_bulk_dq_run,
    launch_spark_batch_profiling_run,
    launch_spark_profiling_run,
    launch_single_rule_run,
    launch_spark_rules_run,
    list_rule_runs_history,
    list_rule_table_options,
    list_rules_with_filters_page,
    require_spark_dq_engine,
    update_rule_with_audit,
    validate_rule_structure_for_spark,
)
from t2c_data.features.data_quality.profiling_schedules import (
    delete_profiling_schedule,
    get_profiling_schedule,
    list_profiling_schedules,
    upsert_profiling_schedule,
    set_profiling_schedule_enabled,
    update_profiling_schedule,
)
from t2c_data.features.data_quality.api_support import (
    build_dq_run_items_out,
    build_dq_tree,
    build_dq_tree_datasource,
    build_dq_tree_tables,
    get_dq_job_run_or_404,
    get_dq_profiling_run_or_404,
)
from t2c_data.features.data_quality.profiling_executions import (
    get_profiling_execution_detail,
    list_profiling_executions,
)
from t2c_data.features.data_quality.profiling_settings import (
    get_profiling_table_setting,
    upsert_profiling_table_setting,
)
from t2c_data.features.data_quality.rule_management import search_rule_notification_users
from t2c_data.features.data_quality.rule_management import rule_builder_options
from t2c_data.features.data_quality.scheduler import scheduler_status_snapshot
from t2c_data.features.data_quality.profiling_scheduler import (
    run_profiling_schedule_now,
    scheduler_status_snapshot as profiling_scheduler_status_snapshot,
)
from t2c_data.features.data_quality.observability_store import load_filtered_observability_artifacts
from t2c_data.features.data_quality.scorecards import build_dq_platform_scorecard_summary
from t2c_data.features.data_quality.spark_runs import get_dq_job_runs
from t2c_data.features.data_observability.service import build_observability_asset_detail, build_observability_overview
from t2c_data.features.certification.api_support import get_table_certification_or_404
from t2c_data.models.auth import User
from t2c_data.schemas.asset_context import DQIncidentSignalsOut
from t2c_data.schemas.dq import (
    DQJobRunOut,
    DQObservabilityHistoryFiltersOut,
    DQObservabilityHistoryOut,
    DQProfilingLaunchOut,
    DQProfilingExecutionDetailOut,
    DQProfilingExecutionPageOut,
    DQProfilingExecutionSummaryOut,
    DQProfilingTableSettingIn,
    DQProfilingTableSettingOut,
    DQProfilingScheduleCreate,
    DQProfilingScheduleOut,
    DQProfilingSchedulerStatusOut,
    DQRunItemOut,
    DQRunProgressOut,
    DQRunOut,
    DQRunRequest,
    DQSparkBatchProfilingRunRequest,
    DQSparkProfilingRunRequest,
    DQSparkRulesRunRequest,
    DQTableLatestOut,
    DQTreeDatasourceChildrenOut,
    DQTreeDatasourceOut,
    DQTreeTableOut,
    DQPlatformScorecardSummaryOut,
)
from t2c_data.schemas.observability import ObservabilityAssetDetailOut, ObservabilityOverviewOut
from t2c_data.schemas.dq_rules import (
    DQRuleCreate,
    DQRuleBuilderOptionsOut,
    DQRuleOut,
    DQRuleRunOut,
    DQRuleTableOption,
    DQRuleTestOut,
    DQRuleRunRequest,
    DQRuleUpdate,
    DQSchedulerStatusOut,
    DQUserOption,
)
from t2c_data.schemas.pagination import PageOut
from t2c_data.services.audit import request_audit_kwargs
from t2c_data.services.audit import write_audit_log_sync

router = APIRouter(prefix="/dq", tags=["data-quality"])


@router.get("/observability/overview", response_model=ObservabilityOverviewOut)
def dq_observability_overview(
    datasource_id: int = Query(..., ge=1),
    schema: str | None = Query(default=None),
    table: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=10, ge=1, le=50),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> ObservabilityOverviewOut:
    try:
        return build_observability_overview(
            db,
            datasource_id=datasource_id,
            current_user=current_user,
            schema_name=schema,
            table_name=table,
            page=page,
            page_size=page_size,
        )
    except KeyError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Data source not found")


@router.get("/observability/assets/{table_id}", response_model=ObservabilityAssetDetailOut)
def dq_observability_asset_detail(
    table_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> ObservabilityAssetDetailOut:
    try:
        return build_observability_asset_detail(db, table_id=table_id, current_user=current_user)
    except KeyError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Tabela não encontrada")


@router.post("/profiling/run", response_model=DQProfilingLaunchOut, status_code=status.HTTP_202_ACCEPTED)
def dq_spark_profiling_run(
    payload: DQSparkProfilingRunRequest,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin")),
) -> DQProfilingLaunchOut:
    return launch_spark_profiling_run(
        db=db,
        payload=payload,
        current_user=current_user,
        audit_kwargs=request_audit_kwargs(request, current_user),
    )


@router.post("/profiling/batch/run", response_model=DQProfilingLaunchOut, status_code=status.HTTP_202_ACCEPTED)
def dq_spark_batch_profiling_run(
    payload: DQSparkBatchProfilingRunRequest,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin")),
) -> DQProfilingLaunchOut:
    return launch_spark_batch_profiling_run(
        db=db,
        payload=payload,
        current_user=current_user,
        audit_kwargs=request_audit_kwargs(request, current_user),
    )


@router.post("/rules/run", response_model=DQJobRunOut, status_code=status.HTTP_202_ACCEPTED)
def dq_spark_rules_run(
    payload: DQSparkRulesRunRequest,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin")),
) -> DQJobRunOut:
    return launch_spark_rules_run(
        db=db,
        payload=payload,
        current_user=current_user,
        audit_kwargs=request_audit_kwargs(request, current_user),
    )


@router.get("/runs", response_model=list[DQJobRunOut])
def dq_runs_list(
    limit: int = Query(default=100, ge=1, le=500),
    table_id: int | None = Query(default=None, ge=1),
    dq_run_id: int | None = Query(default=None, ge=1),
    job_type: str | None = Query(default=None),
    status: str | None = Query(default=None),
    execution_engine: str | None = Query(default=None),
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "viewer")),
) -> list[DQJobRunOut]:
    return [
        build_dq_job_out(item, db)
        for item in get_dq_job_runs(
            limit,
            table_id=table_id,
            dq_run_id=dq_run_id,
            job_type=job_type,
            status=status,
            execution_engine=execution_engine,
        )
    ]


@router.get("/runs/{run_id}", response_model=DQJobRunOut)
def dq_runs_get(
    run_id: int,
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "viewer")),
) -> DQJobRunOut:
    return get_dq_job_run_or_404(run_id, db)


@router.get("/profiling/runs/{run_id}", response_model=DQRunProgressOut)
def dq_profiling_run_get(
    run_id: int,
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "viewer")),
) -> DQRunProgressOut:
    return get_dq_profiling_run_or_404(run_id, db)


@router.get("/profiling/runs/{run_id}/items", response_model=list[DQRunItemOut])
def dq_profiling_run_items(
    run_id: int,
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "viewer")),
) -> list[DQRunItemOut]:
    return build_dq_run_items_out(run_id, db)


@router.get("/runs/{run_id}/items", response_model=list[DQRunItemOut])
def dq_run_items_alias(
    run_id: int,
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "viewer")),
) -> list[DQRunItemOut]:
    return dq_profiling_run_items(run_id=run_id, db=db, _=_)


@router.get("/profiling/executions", response_model=DQProfilingExecutionPageOut)
def dq_profiling_executions(
    limit: int = Query(default=10, ge=1, le=50),
    offset: int = Query(default=0, ge=0, le=5000),
    datasource_id: int | None = Query(default=None, ge=1),
    schema: str | None = Query(default=None),
    table_id: int | None = Query(default=None, ge=1),
    status: str | None = Query(default=None),
    scope: str | None = Query(default=None),
    search: str | None = Query(default=None),
    started_from: date | None = Query(default=None),
    started_to: date | None = Query(default=None),
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "viewer")),
) -> DQProfilingExecutionPageOut:
    return list_profiling_executions(
        db,
        datasource_id=datasource_id,
        schema_name=schema,
        table_id=table_id,
        status=status,
        scope=scope,
        search=search,
        started_from=started_from,
        started_to=started_to,
        limit=limit,
        offset=offset,
    )


@router.get("/profiling/executions/{run_id}", response_model=DQProfilingExecutionDetailOut)
def dq_profiling_execution_detail(
    run_id: int,
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "viewer")),
) -> DQProfilingExecutionDetailOut:
    detail = get_profiling_execution_detail(db, run_id)
    if detail is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="DQ profiling execution not found")
    return detail


@router.get("/profiling/table-settings", response_model=DQProfilingTableSettingOut)
def dq_profiling_table_setting_get(
    table_id: int = Query(ge=1),
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "viewer")),
) -> DQProfilingTableSettingOut:
    return get_profiling_table_setting(db, table_id=table_id)


@router.put("/profiling/table-settings", response_model=DQProfilingTableSettingOut)
def dq_profiling_table_setting_save(
    payload: DQProfilingTableSettingIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor")),
) -> DQProfilingTableSettingOut:
    return upsert_profiling_table_setting(db, payload=payload, current_user=current_user)


@router.get("/tree", response_model=list[DQTreeDatasourceOut])
def dq_tree(
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> list[DQTreeDatasourceOut]:
    return build_dq_tree(db, current_user)


@router.get("/tree/datasources/{datasource_id}", response_model=DQTreeDatasourceChildrenOut)
def dq_tree_datasource(
    datasource_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> DQTreeDatasourceChildrenOut:
    return build_dq_tree_datasource(datasource_id, db, current_user)


@router.get("/tree/schemas/{schema_id}/tables", response_model=list[DQTreeTableOut])
def dq_tree_tables(
    schema_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> list[DQTreeTableOut]:
    return build_dq_tree_tables(schema_id, db, current_user)


@router.post("/run", response_model=DQRunOut)
def dq_run(
    payload: DQRunRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin")),
) -> DQRunOut:
    return launch_bulk_dq_run(db=db, payload=payload, current_user=current_user)


@router.get("/tables/{table_fqn}/latest", response_model=DQTableLatestOut)
def dq_latest_by_fqn(
    table_fqn: str,
    history_runs: int = Query(default=14, ge=2, le=30),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> DQTableLatestOut:
    return DQTableLatestOut(
        **get_latest_metrics_by_fqn(
            db=db,
            table_fqn=table_fqn,
            history_runs=history_runs,
            current_user=current_user,
        )
    )


@router.get("/tables/id/{table_id}/latest", response_model=DQTableLatestOut)
def dq_latest_by_table_id(
    table_id: int,
    history_runs: int = Query(default=14, ge=2, le=30),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> DQTableLatestOut:
    return DQTableLatestOut(
        **get_latest_metrics_by_table_id(
            db=db,
            table_id=table_id,
            history_runs=history_runs,
            current_user=current_user,
        )
    )


@router.get("/scorecards/summary", response_model=DQPlatformScorecardSummaryOut)
def dq_scorecards_summary(
    domain: str | None = Query(default=None),
    owner: str | None = Query(default=None),
    criticality: str | None = Query(default=None),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> DQPlatformScorecardSummaryOut:
    return DQPlatformScorecardSummaryOut.model_validate(
        build_dq_platform_scorecard_summary(
            db,
            current_user=current_user,
            scope_domain=domain,
            scope_owner=owner,
            scope_criticality=criticality,
        )
    )


@router.get("/tables/id/{table_id}/observability/history", response_model=DQObservabilityHistoryOut)
def dq_observability_history_by_table_id(
    table_id: int,
    artifact_type: str = Query(default="all"),
    limit: int = Query(default=10, ge=1, le=50),
    metric_key: str | None = Query(default=None),
    column_name: str | None = Query(default=None),
    dimension_key: str | None = Query(default=None),
    event_type: str | None = Query(default=None),
    severity: str | None = Query(default=None),
    evidence_type: str | None = Query(default=None),
    origin: str | None = Query(default=None),
    status: str | None = Query(default=None),
    dq_run_id: int | None = Query(default=None, ge=1),
    rule_run_id: int | None = Query(default=None, ge=1),
    rule_id: int | None = Query(default=None, ge=1),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> DQObservabilityHistoryOut:
    table = get_table_certification_or_404(db, table_id)
    artifacts = load_filtered_observability_artifacts(
        db,
        table_id=table.id,
        limit=limit,
        artifact_type=artifact_type,
        metric_key=metric_key,
        column_name=column_name,
        dimension_key=dimension_key,
        event_type=event_type,
        severity=severity,
        evidence_type=evidence_type,
        origin=origin,
        status=status,
        dq_run_id=dq_run_id,
        rule_run_id=rule_run_id,
        rule_id=rule_id,
    )
    return DQObservabilityHistoryOut(
        table_id=table.id,
        table_fqn=f"{table.schema.name}.{table.name}",
        filters=DQObservabilityHistoryFiltersOut(
            artifact_type=artifact_type,
            limit=limit,
            metric_key=metric_key,
            column_name=column_name,
            dimension_key=dimension_key,
            event_type=event_type,
            severity=severity,
            evidence_type=evidence_type,
            origin=origin,
            status=status,
            dq_run_id=dq_run_id,
            rule_run_id=rule_run_id,
            rule_id=rule_id,
        ),
        baselines=artifacts["baselines"],
        events=artifacts["events"],
        evidence_samples=artifacts["evidence_samples"],
    )


@router.get("/tables/id/{table_id}/incident-signals", response_model=DQIncidentSignalsOut)
def dq_incident_signals_by_table_id(
    table_id: int,
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "viewer")),
) -> DQIncidentSignalsOut:
    return DQIncidentSignalsOut.model_validate(
        evaluate_table_dq_incident_signals(
            db,
            table_id=table_id,
        )
    )


@router.get("/rules/table-options", response_model=list[DQRuleTableOption])
def dq_rule_table_options(
    q: str = Query(default="", min_length=0, max_length=200),
    limit: int = Query(default=30, ge=1, le=200),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> list[DQRuleTableOption]:
    return list_rule_table_options(db=db, q=q, limit=limit, current_user=current_user)


@router.get("/users", response_model=list[DQUserOption])
def dq_rule_users(
    q: str = Query(default="", min_length=0, max_length=200),
    limit: int = Query(default=20, ge=1, le=100),
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "viewer")),
) -> list[DQUserOption]:
    return search_rule_notification_users(db=db, q=q, limit=limit)


@router.get("/rule-builder/options", response_model=DQRuleBuilderOptionsOut)
def dq_rule_builder_options(
    _: User = Depends(require_roles("admin", "editor", "viewer")),
) -> DQRuleBuilderOptionsOut:
    return rule_builder_options()


@router.get("/scheduler/status", response_model=DQSchedulerStatusOut)
def dq_scheduler_status(
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "viewer")),
) -> DQSchedulerStatusOut:
    return DQSchedulerStatusOut.model_validate(
        scheduler_status_snapshot(db)
    )


@router.get("/profiling/scheduler/status", response_model=DQProfilingSchedulerStatusOut)
def dq_profiling_scheduler_status(
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "viewer")),
) -> DQProfilingSchedulerStatusOut:
    return DQProfilingSchedulerStatusOut.model_validate(
        profiling_scheduler_status_snapshot(db)
    )


@router.get("/profiling/schedules", response_model=list[DQProfilingScheduleOut])
def dq_profiling_schedule_list(
    scope: str | None = Query(default=None),
    table_id: int | None = Query(default=None),
    datasource_id: int | None = Query(default=None),
    schema_name: str | None = Query(default=None),
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "viewer")),
) -> list[DQProfilingScheduleOut]:
    return list_profiling_schedules(
        db,
        scope=scope,
        table_id=table_id,
        datasource_id=datasource_id,
        schema_name=schema_name,
    )


@router.get("/profiling/schedules/{schedule_id}", response_model=DQProfilingScheduleOut)
def dq_profiling_schedule_get(
    schedule_id: int,
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "viewer")),
) -> DQProfilingScheduleOut:
    schedule = get_profiling_schedule(db, schedule_id)
    if schedule is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Profiling schedule not found")
    return schedule


@router.post("/profiling/schedules", response_model=DQProfilingScheduleOut, status_code=status.HTTP_201_CREATED)
def dq_profiling_schedule_save(
    payload: DQProfilingScheduleCreate,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor")),
) -> DQProfilingScheduleOut:
    try:
        result = upsert_profiling_schedule(db, payload)
        write_audit_log_sync(
            db,
            action="dq.profiling.schedule.create",
            entity_type="dq_profiling_schedule",
            entity_id=result.id,
            metadata={"scope_type": result.scope, "datasource_id": result.datasource_id, "schema_name": result.schema_name, "table_ids": result.table_ids},
            **request_audit_kwargs(request, current_user),
        )
        return result
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc


@router.put("/profiling/schedules/{schedule_id}", response_model=DQProfilingScheduleOut)
def dq_profiling_schedule_update(
    schedule_id: int,
    payload: DQProfilingScheduleCreate,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor")),
) -> DQProfilingScheduleOut:
    try:
        result = update_profiling_schedule(db, schedule_id, payload)
        write_audit_log_sync(
            db,
            action="dq.profiling.schedule.update",
            entity_type="dq_profiling_schedule",
            entity_id=result.id,
            metadata={"scope_type": result.scope, "datasource_id": result.datasource_id, "schema_name": result.schema_name, "table_ids": result.table_ids},
            **request_audit_kwargs(request, current_user),
        )
        return result
    except ValueError as exc:
        detail = str(exc)
        status_code = status.HTTP_404_NOT_FOUND if "not found" in detail.lower() else status.HTTP_422_UNPROCESSABLE_ENTITY
        raise HTTPException(status_code=status_code, detail=detail) from exc


@router.post("/profiling/schedules/{schedule_id}/pause", response_model=DQProfilingScheduleOut)
def dq_profiling_schedule_pause(
    schedule_id: int,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor")),
) -> DQProfilingScheduleOut:
    try:
        result = set_profiling_schedule_enabled(db, schedule_id, enabled=False)
        write_audit_log_sync(
            db,
            action="dq.profiling.schedule.pause",
            entity_type="dq_profiling_schedule",
            entity_id=result.id,
            metadata={"scope_type": result.scope},
            **request_audit_kwargs(request, current_user),
        )
        return result
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc


@router.post("/profiling/schedules/{schedule_id}/resume", response_model=DQProfilingScheduleOut)
def dq_profiling_schedule_resume(
    schedule_id: int,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor")),
) -> DQProfilingScheduleOut:
    try:
        result = set_profiling_schedule_enabled(db, schedule_id, enabled=True)
        write_audit_log_sync(
            db,
            action="dq.profiling.schedule.resume",
            entity_type="dq_profiling_schedule",
            entity_id=result.id,
            metadata={"scope_type": result.scope},
            **request_audit_kwargs(request, current_user),
        )
        return result
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc


@router.post("/profiling/schedules/{schedule_id}/run-now", response_model=DQProfilingLaunchOut, status_code=status.HTTP_202_ACCEPTED)
def dq_profiling_schedule_run_now(
    schedule_id: int,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin")),
) -> DQProfilingLaunchOut:
    try:
        result = run_profiling_schedule_now(db, schedule_id=schedule_id, requested_by_user_id=current_user.id)
    except HTTPException:
        raise
    except ValueError as exc:
        detail = str(exc)
        status_code = status.HTTP_404_NOT_FOUND if "not found" in detail.lower() else status.HTTP_422_UNPROCESSABLE_ENTITY
        raise HTTPException(status_code=status_code, detail=detail) from exc
    write_audit_log_sync(
        db,
        action="dq.profiling.schedule.run_now",
        entity_type="dq_profiling_schedule",
        entity_id=schedule_id,
        metadata={
            "scope_type": result.scope,
            "tables_total": result.tables_total,
        },
        **request_audit_kwargs(request, current_user),
    )
    return result


@router.delete("/profiling/schedules/{schedule_id}", status_code=status.HTTP_204_NO_CONTENT, response_model=None)
def dq_profiling_schedule_delete(
    schedule_id: int,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor")),
) -> None:
    write_audit_log_sync(
        db,
        action="dq.profiling.schedule.delete",
        entity_type="dq_profiling_schedule",
        entity_id=schedule_id,
        metadata={},
        **request_audit_kwargs(request, current_user),
    )
    delete_profiling_schedule(db, schedule_id)
    return None


@router.get("/rules", response_model=PageOut[DQRuleOut])
def dq_rules_list(
    rule_id: int | None = Query(default=None),
    q: str | None = Query(default=None),
    table_id: int | None = Query(default=None, ge=1),
    table_fqn: str | None = Query(default=None),
    is_active: bool | None = Query(default=None),
    severity: str | None = Query(default=None),
    last_status: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=500),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> PageOut[DQRuleOut]:
    return list_rules_with_filters_page(
        db=db,
        rule_id=rule_id,
        q=q,
        table_id=table_id,
        table_fqn=table_fqn,
        is_active=is_active,
        severity=severity,
        last_status=last_status,
        page=page,
        page_size=page_size,
        current_user=current_user,
    )


@router.post("/rules", response_model=DQRuleOut, status_code=status.HTTP_201_CREATED)
def dq_rule_create(
    payload: DQRuleCreate,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor")),
) -> DQRuleOut:
    return create_rule_with_audit(
        db=db,
        payload=payload,
        audit_kwargs=request_audit_kwargs(request, current_user),
        current_user=current_user,
    )


@router.get("/rules/{rule_id}", response_model=DQRuleOut)
def dq_rule_get(
    rule_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> DQRuleOut:
    return get_rule_detail(db=db, rule_id=rule_id, current_user=current_user)


@router.put("/rules/{rule_id}", response_model=DQRuleOut)
def dq_rule_update(
    rule_id: int,
    payload: DQRuleUpdate,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor")),
) -> DQRuleOut:
    return update_rule_with_audit(
        db=db,
        rule_id=rule_id,
        payload=payload,
        audit_kwargs=request_audit_kwargs(request, current_user),
        current_user=current_user,
    )


@router.delete("/rules/{rule_id}", status_code=status.HTTP_204_NO_CONTENT, response_model=None)
def dq_rule_delete(
    rule_id: int,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor")),
) -> None:
    delete_rule_with_audit(
        db=db,
        rule_id=rule_id,
        audit_kwargs=request_audit_kwargs(request, current_user),
        current_user=current_user,
    )
    return None


@router.post("/rules/{rule_id}/test", response_model=DQRuleTestOut)
def dq_rule_test(
    rule_id: int,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor")),
) -> DQRuleTestOut:
    require_spark_dq_engine()
    result = validate_rule_structure_for_spark(db=db, rule_id=rule_id, current_user=current_user)
    write_audit_log_sync(
        db,
        action="dq_rule.test",
        entity_type="dq_rule",
        entity_id=rule_id,
        metadata={
            "mode": "structure_validation",
            "execution_engine": "spark",
            "valid": result.valid,
            "status": result.status,
        },
        **request_audit_kwargs(request, current_user),
    )
    db.commit()
    return result


@router.post("/rules/{rule_id}/validate", response_model=DQRuleTestOut)
def dq_rule_validate(
    rule_id: int,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor")),
) -> DQRuleTestOut:
    return dq_rule_test(rule_id=rule_id, request=request, db=db, current_user=current_user)


@router.post("/rules/{rule_id}/run", response_model=DQJobRunOut, status_code=status.HTTP_202_ACCEPTED)
def dq_rule_run(
    rule_id: int,
    request: Request,
    payload: DQRuleRunRequest = Body(default_factory=DQRuleRunRequest),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin")),
) -> DQJobRunOut:
    get_rule_detail(db=db, rule_id=rule_id, current_user=current_user)
    return launch_single_rule_run(
        db=db,
        rule_id=rule_id,
        current_user=current_user,
        execution_engine=payload.execution_engine,
        audit_kwargs=request_audit_kwargs(request, current_user),
    )


@router.get("/rules/{rule_id}/runs", response_model=list[DQRuleRunOut])
def dq_rule_runs_history(
    rule_id: int,
    limit: int = Query(default=20, ge=1, le=200),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> list[DQRuleRunOut]:
    runs = list_rule_runs_history(db=db, rule_id=rule_id, limit=limit, current_user=current_user)
    return [
        DQRuleRunOut(
            id=run.id,
            rule_id=run.rule_id,
            status=run.status,
            execution_engine=getattr(run, "execution_engine", "spark"),
            violations_count=run.violations_count,
            sample_rows_json=run.sample_rows_json,
            error_message=run.error_message,
            created_at=run.created_at,
            updated_at=run.updated_at,
        )
        for run in runs
    ]
