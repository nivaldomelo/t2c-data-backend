from __future__ import annotations

from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from t2c_data.core.config import settings
from t2c_data.features.data_quality.contracts import DefaultDQExecutionGateway, DQExecutionGateway
from t2c_data.features.data_quality.queries import resolve_table_context_by_fqn
from t2c_data.features.data_quality.rule_builder import summarize_rule_definition
from t2c_data.features.data_quality.run_outputs import build_dq_job_out
from t2c_data.features.data_quality.spark_runs import update_dq_run_fields
from t2c_data.models.auth import User
from t2c_data.models.catalog import Database, Schema, TableEntity
from t2c_data.models.dq import DQRule
from t2c_data.schemas.dq import (
    DQJobRunOut,
    DQProfilingLaunchOut,
    DQRunOut,
    DQRunRequest,
    DQSparkBatchProfilingRunRequest,
    DQSparkProfilingRunRequest,
    DQSparkRulesRunRequest,
)
from t2c_data.services.audit import write_audit_log_sync
from t2c_data.services.data_quality import (
    configured_execution_mode,
    ensure_spark_execution_engine,
    local_execution_disabled_message,
    spark_only_execution_message,
)


def require_spark_dq_engine() -> None:
    if (settings.dq_execution_engine or "spark").strip().lower() != "spark":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=spark_only_execution_message())
    if configured_execution_mode() not in {"spark_only", "local_disabled"}:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=local_execution_disabled_message())


def _batch_table_targets(
    db: Session,
    *,
    datasource_id: int,
    scope: str,
    schema_name: str | None,
    table_ids: list[int],
    limit: int,
) -> list[dict[str, object]]:
    if scope == "datasource":
        query = (
            select(TableEntity, Schema, Database)
            .join(Schema, TableEntity.schema_id == Schema.id)
            .join(Database, Schema.database_id == Database.id)
            .where(Database.datasource_id == datasource_id)
            .where(TableEntity.table_type == "table")
        )
    elif scope == "schema":
        if not schema_name:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="schema is required for scope=schema")
        query = (
            select(TableEntity, Schema, Database)
            .join(Schema, TableEntity.schema_id == Schema.id)
            .join(Database, Schema.database_id == Database.id)
            .where(Database.datasource_id == datasource_id)
            .where(Schema.name == schema_name)
            .where(TableEntity.table_type == "table")
        )
    elif scope == "tables":
        normalized_ids = [table_id for table_id in table_ids if table_id > 0]
        if not normalized_ids:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="table_ids are required for scope=tables")
        query = (
            select(TableEntity, Schema, Database)
            .join(Schema, TableEntity.schema_id == Schema.id)
            .join(Database, Schema.database_id == Database.id)
            .where(TableEntity.id.in_(normalized_ids))
            .where(Database.datasource_id == datasource_id)
            .where(TableEntity.table_type == "table")
        )
        if schema_name:
            query = query.where(Schema.name == schema_name)
    else:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=f"unsupported batch profiling scope '{scope}'")

    rows = db.execute(query.order_by(Schema.name, TableEntity.name)).all()
    table_targets: list[dict[str, object]] = []
    for table, schema, database in rows:
        table_targets.append(
            {
                "table_id": table.id,
                "table_fqn": f"{schema.name}.{table.name}",
                "schema_name": schema.name,
                "datasource_id": database.datasource_id,
            }
        )
        if len(table_targets) >= max(limit, 1):
            break
    return table_targets


def _batch_scope_empty_message(scope: str, *, schema_name: str | None) -> str:
    if scope == "datasource":
        return "Nenhuma tabela elegível encontrada para este Data Source."
    if scope == "schema":
        if schema_name:
            return f"Nenhuma tabela elegível encontrada para o schema {schema_name}."
        return "Nenhuma tabela elegível encontrada para este schema."
    if scope == "tables":
        return "Nenhuma tabela elegível encontrada para as tabelas selecionadas."
    return "Nenhuma tabela elegível encontrada para este escopo."


def launch_spark_batch_profiling_run(
    *,
    db: Session,
    payload: DQSparkBatchProfilingRunRequest,
    current_user: User,
    audit_kwargs: dict,
    execution_gateway: DQExecutionGateway | None = None,
) -> DQProfilingLaunchOut:
    engine = ensure_spark_execution_engine(getattr(payload, "execution_engine", None))
    gateway = execution_gateway or DefaultDQExecutionGateway()
    require_spark_dq_engine()
    scope = (payload.scope or "schema").lower()
    datasource_id = payload.datasource_id
    if datasource_id is None:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="datasource_id is required")

    schema_name = (payload.schema_name or "").strip() or None
    table_targets = _batch_table_targets(
        db,
        datasource_id=datasource_id,
        scope=scope,
        schema_name=schema_name,
        table_ids=payload.table_ids,
        limit=payload.limit,
    )
    if not table_targets:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=_batch_scope_empty_message(scope, schema_name=schema_name),
        )

    if scope == "datasource":
        parent_run = gateway.create_batch_run(
            datasource_id=datasource_id,
            scope="datasource",
            schema_name=None,
            execution_engine=engine,
        )
    elif scope == "schema":
        if not schema_name:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="schema is required for scope=schema")
        parent_run = gateway.create_schema_run(
            datasource_id=datasource_id,
            schema_name=schema_name,
            execution_engine=engine,
        )
    else:
        parent_run = gateway.create_batch_run(
            datasource_id=datasource_id,
            scope="tables",
            schema_name=schema_name,
            execution_engine=engine,
        )

    update_dq_run_fields(
        parent_run.id,
        profile_payload_json={
            "trigger_source": "manual",
            "scope_type": scope,
            "datasource_id": datasource_id,
            "schema_name": schema_name,
            "table_ids": payload.table_ids,
        },
    )
    gateway.enqueue_schema_profiling(
        parent_run_id=parent_run.id,
        table_targets=table_targets,
        requested_by_user_id=current_user.id,
        concurrency=payload.concurrency,
        sample_fraction=payload.sample_fraction,
        columns=payload.columns,
        execution_engine=engine,
    )
    write_audit_log_sync(
        db,
        action="dq.profiling.batch_run.start",
        entity_type="dq_run",
        entity_id=parent_run.id,
        metadata={
            "scope_type": scope,
            "datasource_id": datasource_id,
            "schema_name": schema_name,
            "table_ids": payload.table_ids,
            "tables_total": len(table_targets),
            "concurrency": payload.concurrency,
            "limit": payload.limit,
            "trigger_source": "manual",
        },
        **audit_kwargs,
    )
    db.commit()
    return DQProfilingLaunchOut(
        run_id=parent_run.id,
        scope=scope,
        schema=schema_name,
        tables_total=len(table_targets),
        status="queued",
        execution_engine=engine,
        job_run_id=None,
    )


def launch_spark_profiling_run(
    *,
    db: Session,
    payload: DQSparkProfilingRunRequest,
    current_user: User,
    audit_kwargs: dict,
    execution_gateway: DQExecutionGateway | None = None,
) -> DQProfilingLaunchOut:
    engine = ensure_spark_execution_engine(getattr(payload, "execution_engine", None))
    gateway = execution_gateway or DefaultDQExecutionGateway()
    require_spark_dq_engine()
    if (payload.scope or "table").lower() == "schema":
        schema_name = (payload.schema_name or "").strip()
        if not schema_name:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="schema is required for scope=schema")
        query = (
            select(TableEntity, Schema, Database)
            .join(Schema, TableEntity.schema_id == Schema.id)
            .join(Database, Schema.database_id == Database.id)
            .where(Schema.name == schema_name)
            .where(TableEntity.table_type == "table")
        )
        if payload.datasource_id is not None:
            query = query.where(Database.datasource_id == payload.datasource_id)
        rows = db.execute(query.order_by(TableEntity.name)).all()
        include_set = {name.strip() for name in payload.include_tables if name.strip()}
        exclude_set = {name.strip() for name in payload.exclude_tables if name.strip()}
        table_targets: list[dict] = []
        datasource_id_for_parent: int | None = payload.datasource_id
        for table, schema, database in rows:
            table_name = table.name
            if include_set and table_name not in include_set:
                continue
            if table_name in exclude_set:
                continue
            if datasource_id_for_parent is None:
                datasource_id_for_parent = database.datasource_id
            table_targets.append(
                {
                    "table_id": table.id,
                    "table_fqn": f"{schema.name}.{table.name}",
                    "schema_name": schema.name,
                    "datasource_id": database.datasource_id,
                }
            )
            if len(table_targets) >= payload.limit:
                break
        if not table_targets:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No tables found for schema profiling")

        parent_run = gateway.create_schema_run(
            datasource_id=datasource_id_for_parent,
            schema_name=schema_name,
            execution_engine=engine,
        )
        gateway.enqueue_schema_profiling(
            parent_run_id=parent_run.id,
            table_targets=table_targets,
            requested_by_user_id=current_user.id,
            concurrency=payload.concurrency,
            sample_fraction=payload.sample_fraction,
            columns=payload.columns,
            execution_engine=engine,
        )
        write_audit_log_sync(
            db,
            action="dq.profiling.schema_run.start",
            entity_type="dq_run",
            entity_id=parent_run.id,
            metadata={
                "scope": "schema",
                "schema": schema_name,
                "tables_total": len(table_targets),
                "concurrency": payload.concurrency,
                "limit": payload.limit,
                "include_tables": payload.include_tables,
                "exclude_tables": payload.exclude_tables,
            },
            **audit_kwargs,
        )
        db.commit()
        return DQProfilingLaunchOut(
            run_id=parent_run.id,
            scope="schema",
            schema=schema_name,
            tables_total=len(table_targets),
            status="queued",
            execution_engine=engine,
        )

    resolved_table_id = payload.table_id
    resolved_table_fqn = payload.table_fqn
    if payload.table_id is None and payload.table_fqn:
        table, schema, _database, _ds = resolve_table_context_by_fqn(db, payload.table_fqn)
        resolved_table_id = table.id
        resolved_table_fqn = f"{schema.name}.{table.name}"
    dq_run = gateway.create_table_run(table_id=resolved_table_id, table_fqn=resolved_table_fqn, execution_engine=engine)
    job = gateway.enqueue_profiling(
        table_id=resolved_table_id,
        table_fqn=resolved_table_fqn,
        columns=payload.columns,
        sample_fraction=payload.sample_fraction,
        requested_by_user_id=current_user.id,
        dq_run_id=dq_run.id,
        execution_engine=engine,
    )
    write_audit_log_sync(
        db,
        action="dq.profiling.run.start",
        entity_type="dq_run",
        entity_id=dq_run.id,
        metadata={"job_run_id": job.id, "table_fqn": resolved_table_fqn},
        **audit_kwargs,
    )
    db.commit()
    return DQProfilingLaunchOut(
        run_id=dq_run.id,
        scope="table",
        table_fqn=resolved_table_fqn,
        tables_total=1,
        status="queued",
        execution_engine=engine,
        job_run_id=job.id,
    )


def launch_spark_rules_run(
    *,
    db: Session,
    payload: DQSparkRulesRunRequest,
    current_user: User,
    audit_kwargs: dict,
    execution_gateway: DQExecutionGateway | None = None,
) -> DQJobRunOut:
    engine = ensure_spark_execution_engine(getattr(payload, "execution_engine", None))
    gateway = execution_gateway or DefaultDQExecutionGateway()
    require_spark_dq_engine()
    resolved_table_id = payload.table_id
    resolved_table_fqn = payload.table_fqn
    if payload.table_id is None and payload.table_fqn:
        table, schema, _database, _ds = resolve_table_context_by_fqn(db, payload.table_fqn)
        resolved_table_id = table.id
        resolved_table_fqn = f"{schema.name}.{table.name}"
    dq_run = gateway.create_table_run(table_id=resolved_table_id, table_fqn=resolved_table_fqn, execution_engine=engine)
    job = gateway.enqueue_rules(
        table_id=resolved_table_id,
        table_fqn=resolved_table_fqn,
        rule_ids=payload.rule_ids,
        requested_by_user_id=current_user.id,
        dq_run_id=dq_run.id,
        execution_engine=engine,
    )
    write_audit_log_sync(
        db,
        action="dq.rules.run.start",
        entity_type="dq_run",
        entity_id=dq_run.id,
        metadata={"job_run_id": job.id, "table_fqn": resolved_table_fqn, "rule_ids": payload.rule_ids},
        **audit_kwargs,
    )
    db.commit()
    return build_dq_job_out(job, db)


def launch_bulk_dq_run(
    *,
    db: Session,
    payload: DQRunRequest,
    current_user: User,
    execution_gateway: DQExecutionGateway | None = None,
) -> DQRunOut:
    engine = ensure_spark_execution_engine(getattr(payload, "execution_engine", None))
    require_spark_dq_engine()
    gateway = execution_gateway or DefaultDQExecutionGateway()
    query = select(TableEntity).join(Schema, TableEntity.schema_id == Schema.id).join(Database, Schema.database_id == Database.id)
    if payload.table_id is not None:
        query = query.where(TableEntity.id == payload.table_id)
    if payload.datasource_id is not None:
        query = query.where(Database.datasource_id == payload.datasource_id)
    tables = db.scalars(query.order_by(TableEntity.id).limit(max(payload.max_tables, 1))).all()
    if not tables:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No tables found for DQ run")

    dq_run_ids: list[int] = []
    for table in tables:
        schema = db.get(Schema, table.schema_id)
        if not schema:
            continue
        dq_run = gateway.create_table_run(table_id=table.id, table_fqn=f"{schema.name}.{table.name}", execution_engine=engine)
        gateway.enqueue_profiling(
            table_id=table.id,
            table_fqn=f"{schema.name}.{table.name}",
            columns=[],
            sample_fraction=None,
            requested_by_user_id=current_user.id,
            dq_run_id=dq_run.id,
            execution_engine=engine,
        )
        dq_run_ids.append(dq_run.id)
    if not dq_run_ids:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No tables queued for DQ run")
    return DQRunOut(run_ids=dq_run_ids, processed_tables=len(dq_run_ids), status="queued", execution_engine=engine)


def launch_single_rule_run(
    *,
    db: Session,
    rule_id: int,
    current_user: User,
    execution_engine: str | None = None,
    audit_kwargs: dict,
    execution_gateway: DQExecutionGateway | None = None,
) -> DQJobRunOut:
    rule = db.get(DQRule, rule_id)
    if not rule:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Rule not found")
    if getattr(rule, "archived", False):
        raise HTTPException(status_code=status.HTTP_410_GONE, detail="Esta regra foi arquivada e não pode mais ser executada.")
    if not rule.is_active:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Rule is inactive")
    if not isinstance(getattr(rule, "rule_definition_json", None), dict):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Esta regra não usa o novo construtor visual e não pode mais ser executada. Crie uma regra estruturada no builder visual.",
        )
    engine = ensure_spark_execution_engine(execution_engine or getattr(rule, "execution_engine", None))
    gateway = execution_gateway or DefaultDQExecutionGateway()
    require_spark_dq_engine()

    dq_run = gateway.create_table_run(table_id=rule.table_id, table_fqn=rule.table_fqn, execution_engine=engine)
    job = gateway.enqueue_rules(
        table_id=rule.table_id,
        table_fqn=rule.table_fqn,
        rule_ids=[rule.id],
        requested_by_user_id=current_user.id,
        dq_run_id=dq_run.id,
        execution_engine=engine,
    )
    write_audit_log_sync(
        db,
        action="dq_rule.run.start",
        entity_type="dq_rule",
        entity_id=rule.id,
        metadata={
            "dq_run_id": dq_run.id,
            "job_run_id": job.id,
            "table_fqn": rule.table_fqn,
            "rule_summary": summarize_rule_definition(rule.rule_definition_json),
        },
        **audit_kwargs,
    )
    db.commit()
    return build_dq_job_out(job, db)


__all__ = [
    "launch_bulk_dq_run",
    "launch_single_rule_run",
    "launch_spark_batch_profiling_run",
    "launch_spark_profiling_run",
    "launch_spark_rules_run",
    "require_spark_dq_engine",
]
