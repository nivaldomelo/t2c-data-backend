from __future__ import annotations

import logging

from fastapi import HTTPException, status
from sqlalchemy import MetaData, Table, func, inspect, or_, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, noload, selectinload

from t2c_data.core.config import settings
from t2c_data.features.data_quality.engines import normalize_execution_engine
from t2c_data.features.pagination import normalize_page_params
from t2c_data.features.data_quality.queries import resolve_table_context_by_fqn, table_fqn_candidates, table_fqn_candidates_for_table
from t2c_data.features.data_quality.schedule_utils import validate_schedule_payload
from t2c_data.features.data_quality.rule_builder import (
    build_rule_definition,
    builder_options_payload,
    reject_legacy_sql_payload,
    summarize_rule_definition,
)
from t2c_data.features.data_quality.latest_runs import get_latest_rule_snapshots, latest_snapshot_support_ready
from t2c_data.features.data_quality.run_outputs import (
    build_dq_job_out,
    build_dq_job_out_map,
    build_rule_out,
    load_rule_audit_payloads,
    load_rule_notification_recipients,
    map_rule_out,
    open_incidents_for_rule_ids,
)
from t2c_data.features.access_control.policy import can_view_table
from t2c_data.models.auth import User
from t2c_data.models.catalog import ColumnEntity, DataSource, Database, Schema, TableEntity
from t2c_data.models.dq import DQJobRun, DQRule, DQRuleLatestRun, DQRuleRun
from t2c_data.schemas.pagination import PageOut
from t2c_data.schemas.dq_rules import DQRuleBuilderOptionsOut, DQRuleOut, DQRuleTableOption, DQUserOption
from t2c_data.schemas.dq_rules import DQRuleTestOut
from t2c_data.services.audit import serialize_model, write_audit_log_sync
from t2c_data.services.data_quality import configured_execution_engine


logger = logging.getLogger(__name__)
DQ_SCHEMA = getattr(settings, "db_schema", "t2c_data") or "t2c_data"
DQ_RULES_MAX_PAGE_SIZE = 100


def _has_table(db: Session, table_name: str) -> bool:
    bind = db.get_bind()
    if bind is None:
        return False
    try:
        return inspect(bind).has_table(table_name, schema=DQ_SCHEMA)
    except Exception:  # noqa: BLE001
        return False


def _dq_rules_schema_ready(db: Session) -> bool:
    if not _has_table(db, "dq_rules"):
        return False
    bind = db.get_bind()
    if bind is None:
        return False
    try:
        columns = {column["name"] for column in inspect(bind).get_columns("dq_rules", schema=DQ_SCHEMA)}
    except Exception:  # noqa: BLE001
        return False
    required = {
        "execution_engine",
        "schedule_mode",
        "schedule_enabled",
        "schedule_every_minutes",
        "schedule_time",
        "schedule_day_of_week",
        "schedule_day_of_month",
        "schedule_anchor_date",
        "schedule_last_run_at",
        "rule_builder_version",
        "rule_definition_json",
        "archived",
    }
    return required.issubset(columns)


def _dq_rules_table_ready(db: Session) -> bool:
    return _has_table(db, "dq_rules")


def _reflect_dq_rules_table(db: Session) -> Table | None:
    bind = db.get_bind()
    if bind is None:
        return None
    try:
        return Table("dq_rules", MetaData(), schema=DQ_SCHEMA, autoload_with=bind)
    except Exception:  # noqa: BLE001
        return None


def _legacy_rule_proxy(row, table: Table) -> DQRule:
    proxy = DQRule()
    row_mapping = dict(row._mapping)
    for column in table.columns:
        if column.name in row_mapping:
            setattr(proxy, column.name, row_mapping[column.name])
    # Preserve visibility for legacy rows even if the new schedule columns
    # are absent from the database schema.
    if "schedule_mode" not in row_mapping:
        proxy.schedule_mode = "manual"
    if "schedule_enabled" not in row_mapping:
        proxy.schedule_enabled = False
    if "schedule_every_minutes" not in row_mapping:
        proxy.schedule_every_minutes = None
    if "execution_engine" not in row_mapping:
        proxy.execution_engine = configured_execution_engine()
    if "schedule_time" not in row_mapping:
        proxy.schedule_time = None
    if "schedule_day_of_week" not in row_mapping:
        proxy.schedule_day_of_week = None
    if "schedule_day_of_month" not in row_mapping:
        proxy.schedule_day_of_month = None
    if "schedule_anchor_date" not in row_mapping:
        proxy.schedule_anchor_date = None
    if "schedule_last_run_at" not in row_mapping:
        proxy.schedule_last_run_at = None
    if "notification_recipient_user_id" not in row_mapping:
        proxy.notification_recipient_user_id = None
    return proxy


def list_rule_table_options(*, db: Session, q: str, limit: int, current_user: User | None = None) -> list[DQRuleTableOption]:
    from t2c_data.models.catalog import DataSource

    pattern = f"%{q.strip()}%"
    rows = db.execute(
        select(
            TableEntity,
            TableEntity.id,
            DataSource.name,
            Schema.name,
            TableEntity.name,
        )
        .join(Schema, TableEntity.schema_id == Schema.id)
        .join(Database, Schema.database_id == Database.id)
        .join(DataSource, Database.datasource_id == DataSource.id)
        .where(
            (TableEntity.name.ilike(pattern))
            | (Schema.name.ilike(pattern))
            | (DataSource.name.ilike(pattern))
        )
        .order_by(DataSource.name, Schema.name, TableEntity.name)
    ).all()
    options: list[DQRuleTableOption] = []
    for table, table_id, datasource_name, schema_name, table_name in rows:
        if current_user is not None and not can_view_table(current_user, table):
            continue
        if len(options) >= limit:
            break
        options.append(DQRuleTableOption(table_id=table_id, table_fqn=f"{datasource_name}.{schema_name}.{table_name}"))
    return options


def search_rule_notification_users(*, db: Session, q: str, limit: int) -> list[DQUserOption]:
    pattern = f"%{q.strip()}%" if q.strip() else None
    query = select(User).where(User.is_active.is_(True))
    if pattern:
        query = query.where((User.name.ilike(pattern)) | (User.full_name.ilike(pattern)) | (User.email.ilike(pattern)))
    users = db.scalars(query.order_by(User.name.asc().nullslast(), User.email.asc()).limit(limit)).all()
    return [
        DQUserOption(
            id=user.id,
            display_name=(user.name or user.full_name or user.email).strip() or user.email,
            email=user.email,
        )
        for user in users
    ]


def _normalize_rule_recipient_ids(
    db: Session,
    *,
    user_id: int | None = None,
    user_ids: list[int] | None = None,
) -> list[int]:
    raw_ids = list(user_ids or [])
    if not raw_ids and user_id is not None:
        raw_ids = [user_id]
    normalized: list[int] = []
    seen: set[int] = set()
    for raw_id in raw_ids:
        try:
            candidate_id = int(raw_id)
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="notification recipient ids must be integers",
            ) from exc
        if candidate_id in seen:
            continue
        user = db.get(User, candidate_id)
        if user is None or not user.is_active:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"notification_recipient_user_id {candidate_id} must point to an active user",
            )
        seen.add(candidate_id)
        normalized.append(candidate_id)
    return normalized


def _load_rule_recipients(db: Session, recipient_ids: list[int]) -> list[User]:
    if not recipient_ids:
        return []
    users = db.scalars(
        select(User)
        .where(User.id.in_(recipient_ids), User.is_active.is_(True))
        .order_by(User.name.asc().nullslast(), User.email.asc())
    ).all()
    by_id = {user.id: user for user in users}
    return [by_id[user_id] for user_id in recipient_ids if user_id in by_id]


def _rule_table_visible(db: Session, rule: DQRule, current_user: User | None) -> bool:
    if current_user is None:
        return True
    if rule.table_id is not None:
        table = db.get(TableEntity, int(rule.table_id))
        if table is not None:
            return can_view_table(current_user, table)
    try:
        table, _schema, _database, _datasource = resolve_table_context_by_fqn(db, rule.table_fqn)
    except Exception:  # noqa: BLE001
        return False
    return can_view_table(current_user, table)


def _rule_scope_filters(
    db: Session,
    *,
    table_id: int | None,
    table_fqn: str | None,
) -> list:
    filters = []
    if table_id is not None:
        candidates = table_fqn_candidates_for_table(db, table_id)
        if candidates:
            filters.append(
                or_(
                    DQRule.table_id == table_id,
                    DQRule.table_fqn.in_(candidates),
                )
            )
        else:
            filters.append(DQRule.table_id == table_id)
    elif table_fqn:
        candidates = table_fqn_candidates(table_fqn)
        if candidates:
            filters.append(DQRule.table_fqn.in_(candidates))
        else:
            filters.append(DQRule.table_fqn.ilike(f"%{table_fqn.strip()}%"))
    return filters


def rule_builder_options() -> DQRuleBuilderOptionsOut:
    return DQRuleBuilderOptionsOut.model_validate(builder_options_payload())


def _non_archived_rule_filters():
    return [DQRule.archived.is_(False)]


def _resolve_target_table(
    db: Session,
    *,
    table_id: int | None,
    table_fqn: str | None,
) -> tuple[TableEntity, Schema, Database, DataSource]:
    if table_id is not None:
        row = db.execute(
            select(TableEntity, Schema, Database, DataSource)
            .join(Schema, TableEntity.schema_id == Schema.id)
            .join(Database, Schema.database_id == Database.id)
            .join(DataSource, Database.datasource_id == DataSource.id)
            .where(TableEntity.id == table_id)
            .limit(1)
        ).first()
        if not row:
            raise ValueError("Tabela não encontrada no catálogo para a regra")
        return row[0], row[1], row[2], row[3]
    if table_fqn:
        return resolve_table_context_by_fqn(db, table_fqn)
    raise ValueError("A regra precisa informar a tabela alvo.")


def _columns_by_name_for_table(db: Session, table_id: int) -> dict[str, ColumnEntity]:
    columns = db.scalars(select(ColumnEntity).where(ColumnEntity.table_id == table_id).order_by(ColumnEntity.ordinal_position.asc())).all()
    return {column.name: column for column in columns}


def _ensure_rule_is_available(rule: DQRule) -> None:
    if getattr(rule, "archived", False):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Rule not found")


def _collect_rule_matches(
    *,
    db: Session,
    rule_id: int | None,
    q: str | None,
    table_id: int | None,
    table_fqn: str | None,
    is_active: bool | None,
    severity: str | None,
    last_status: str | None,
    current_user: User | None = None,
) -> list[tuple[DQRule, DQRuleRun | None, DQJobRun | None, object | None]]:
    return _collect_rule_matches_impl(
        db=db,
        rule_id=rule_id,
        q=q,
        table_id=table_id,
        table_fqn=table_fqn,
        is_active=is_active,
        severity=severity,
        last_status=last_status,
        current_user=current_user,
    )



def _collect_rule_matches_impl(
    *,
    db: Session,
    rule_id: int | None,
    q: str | None,
    table_id: int | None,
    table_fqn: str | None,
    is_active: bool | None,
    severity: str | None,
    last_status: str | None,
    current_user: User | None = None,
) -> list[tuple[DQRule, DQRuleRun | None, DQJobRun | None, object | None]]:
    if not _dq_rules_table_ready(db):
        logger.warning("dq rules table unavailable schema=%s table=dq_rules", DQ_SCHEMA)
        return []

    rules: list[DQRule]
    if _dq_rules_schema_ready(db):
        filters = _non_archived_rule_filters()
        if rule_id is not None:
            filters.append(DQRule.id == rule_id)
        filters.extend(_rule_scope_filters(db, table_id=table_id, table_fqn=table_fqn))
        if q:
            pattern = f"%{q.strip()}%"
            filters.append(
                (DQRule.name.ilike(pattern)) | (DQRule.description.ilike(pattern)) | (DQRule.table_fqn.ilike(pattern))
            )
        if is_active is not None:
            filters.append(DQRule.is_active == is_active)
        if severity:
            filters.append(DQRule.severity == severity.strip().lower())

        query_options = [selectinload(DQRule.notification_recipient_user), noload(DQRule.notification_recipients)]
        query = select(DQRule).options(*query_options)
        if filters:
            query = query.where(*filters)

        try:
            rules = db.scalars(query.order_by(DQRule.updated_at.desc(), DQRule.id.desc())).all()
        except SQLAlchemyError as exc:
            if len(query_options) > 1:
                logger.warning(
                    "dq rules list retrying without recipient eager load schema=%s error=%s",
                    DQ_SCHEMA,
                    exc.__class__.__name__,
                )
                fallback_query = select(DQRule).options(
                    selectinload(DQRule.notification_recipient_user),
                    noload(DQRule.notification_recipients),
                )
                if filters:
                    fallback_query = fallback_query.where(*filters)
                try:
                    rules = db.scalars(fallback_query.order_by(DQRule.updated_at.desc(), DQRule.id.desc())).all()
                except SQLAlchemyError as fallback_exc:
                    raise HTTPException(
                        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                        detail="Não foi possível carregar as regras de Data Quality no momento.",
                    ) from fallback_exc
            else:
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail="Não foi possível carregar as regras de Data Quality no momento.",
                ) from exc
    else:
        legacy_table = _reflect_dq_rules_table(db)
        if legacy_table is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Não foi possível carregar as regras de Data Quality no momento.",
            )
        query = select(*legacy_table.c)
        if rule_id is not None:
            query = query.where(legacy_table.c.id == rule_id)
        legacy_scope_filters = []
        if table_id is not None:
            table_filters = []
            if "table_id" in legacy_table.c:
                table_filters.append(legacy_table.c.table_id == table_id)
            candidates = table_fqn_candidates_for_table(db, table_id)
            if candidates and "table_fqn" in legacy_table.c:
                table_filters.append(legacy_table.c.table_fqn.in_(candidates))
            if table_filters:
                legacy_scope_filters.append(or_(*table_filters))
        elif table_fqn:
            candidates = table_fqn_candidates(table_fqn)
            table_filters = []
            if "table_fqn" in legacy_table.c:
                if candidates:
                    table_filters.append(legacy_table.c.table_fqn.in_(candidates))
                else:
                    table_filters.append(legacy_table.c.table_fqn.ilike(f"%{table_fqn.strip()}%"))
            if table_filters:
                legacy_scope_filters.append(or_(*table_filters))
        if legacy_scope_filters:
            query = query.where(*legacy_scope_filters)
        if q:
            pattern = f"%{q.strip()}%"
            query = query.where(
                (legacy_table.c.name.ilike(pattern))
                | (legacy_table.c.description.ilike(pattern))
                | (legacy_table.c.table_fqn.ilike(pattern))
            )
        if is_active is not None:
            query = query.where(legacy_table.c.is_active == is_active)
        if severity:
            query = query.where(legacy_table.c.severity == severity.strip().lower())
        try:
            legacy_rows = db.execute(query.order_by(legacy_table.c.updated_at.desc(), legacy_table.c.id.desc())).all()
        except SQLAlchemyError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Não foi possível carregar as regras de Data Quality no momento.",
            ) from exc
        rules = [_legacy_rule_proxy(row, legacy_table) for row in legacy_rows]
    if not rules:
        return []

    latest_by_rule: dict[int, DQRuleRun] = {}
    latest_job_by_rule: dict[int, DQJobRun] = {}
    latest_job_out_by_rule: dict[int, object] = {}
    rule_ids = [rule.id for rule in rules]

    if latest_snapshot_support_ready(db):
        snapshots = get_latest_rule_snapshots(db, rule_ids)
        latest_run_ids = sorted(
            {
                int(snapshot.latest_rule_run_id)
                for snapshot in snapshots.values()
                if snapshot.latest_rule_run_id is not None
            }
        )
        latest_job_ids = sorted(
            {
                int(snapshot.latest_job_run_id)
                for snapshot in snapshots.values()
                if snapshot.latest_job_run_id is not None
            }
        )
        latest_runs_by_id = (
            {
                row.id: row
                for row in db.scalars(select(DQRuleRun).where(DQRuleRun.id.in_(latest_run_ids))).all()
            }
            if latest_run_ids
            else {}
        )
        latest_jobs_by_id = (
            {
                row.id: row
                for row in db.scalars(select(DQJobRun).where(DQJobRun.id.in_(latest_job_ids))).all()
            }
            if latest_job_ids
            else {}
        )
        latest_job_outs_by_id = build_dq_job_out_map(db, list(latest_jobs_by_id.values()))
        for rule in rules:
            snapshot = snapshots.get(rule.id)
            if snapshot is None:
                continue
            if snapshot.latest_rule_run_id is not None:
                latest = latest_runs_by_id.get(int(snapshot.latest_rule_run_id))
                if latest is not None:
                    latest_by_rule[rule.id] = latest
            if snapshot.latest_job_run_id is not None:
                latest_job = latest_jobs_by_id.get(int(snapshot.latest_job_run_id))
                if latest_job is not None:
                    latest_job_by_rule[rule.id] = latest_job
                    latest_job_out = latest_job_outs_by_id.get(latest_job.id)
                    if latest_job_out is not None:
                        latest_job_out_by_rule[rule.id] = latest_job_out
        missing_rule_ids = [rule.id for rule in rules if rule.id not in latest_by_rule]
        if missing_rule_ids and _has_table(db, "dq_rule_runs"):
            try:
                fallback_runs = db.scalars(
                    select(DQRuleRun)
                    .where(DQRuleRun.rule_id.in_(missing_rule_ids))
                    .order_by(DQRuleRun.rule_id, DQRuleRun.created_at.desc())
                ).all()
            except SQLAlchemyError:
                fallback_runs = []
            for item in fallback_runs:
                latest_by_rule.setdefault(item.rule_id, item)

        missing_job_rule_ids = [rule.id for rule in rules if rule.id not in latest_job_by_rule]
        if missing_job_rule_ids and _has_table(db, "dq_job_runs"):
            missing_job_rule_ids_set = set(missing_job_rule_ids)
            try:
                for job in db.scalars(select(DQJobRun).where(DQJobRun.job_type == "rules").order_by(DQJobRun.id.desc())).all():
                    payload = job.result_json if isinstance(job.result_json, dict) else None
                    requested_rule_ids = payload.get("requested_rule_ids") if isinstance(payload, dict) else None
                    if not isinstance(requested_rule_ids, list):
                        continue
                    for raw_id in requested_rule_ids:
                        try:
                            rid = int(raw_id)
                        except Exception:
                            continue
                        if rid not in missing_job_rule_ids_set or rid in latest_job_by_rule:
                            continue
                        latest_job_by_rule[rid] = job
                        latest_job_out_by_rule[rid] = build_dq_job_out(job, db)
                    if len({rule_id for rule_id in missing_job_rule_ids_set if rule_id in latest_job_by_rule}) >= len(missing_job_rule_ids_set):
                        break
            except SQLAlchemyError:
                pass
    else:
        if _has_table(db, "dq_rule_runs"):
            try:
                latest_runs = db.scalars(
                    select(DQRuleRun)
                    .where(DQRuleRun.rule_id.in_(rule_ids))
                    .order_by(DQRuleRun.rule_id, DQRuleRun.created_at.desc())
                ).all()
            except SQLAlchemyError:
                latest_runs = []
            for item in latest_runs:
                latest_by_rule.setdefault(item.rule_id, item)

        rule_ids_set = set(rule_ids)
        if _has_table(db, "dq_job_runs"):
            try:
                for job in db.scalars(select(DQJobRun).where(DQJobRun.job_type == "rules").order_by(DQJobRun.id.desc())).all():
                    payload = job.result_json if isinstance(job.result_json, dict) else None
                    requested_rule_ids = payload.get("requested_rule_ids") if isinstance(payload, dict) else None
                    if not isinstance(requested_rule_ids, list):
                        continue
                    for raw_id in requested_rule_ids:
                        try:
                            rid = int(raw_id)
                        except Exception:
                            continue
                        if rid not in rule_ids_set or rid in latest_job_by_rule:
                            continue
                        latest_job_by_rule[rid] = job
                        latest_job_out_by_rule[rid] = build_dq_job_out(job, db)
                    if len(latest_job_by_rule) >= len(rule_ids_set):
                        break
            except SQLAlchemyError:
                latest_job_by_rule = {}
                latest_job_out_by_rule = {}

    matches: list[tuple[DQRule, DQRuleRun | None, DQJobRun | None, object | None]] = []
    for rule in rules:
        latest = latest_by_rule.get(rule.id)
        latest_job = latest_job_by_rule.get(rule.id)
        latest_job_out = latest_job_out_by_rule.get(rule.id)
        if not _rule_table_visible(db, rule, current_user):
            continue
        if last_status:
            current_exec_status = build_rule_out(rule, latest, latest_job, latest_job_out).last_run_status if latest else None
            if not current_exec_status or current_exec_status != last_status:
                continue
        matches.append((rule, latest, latest_job, latest_job_out))
    return matches


def _collect_rule_page_rules(
    *,
    db: Session,
    rule_id: int | None,
    q: str | None,
    table_id: int | None,
    table_fqn: str | None,
    is_active: bool | None,
    severity: str | None,
    last_status: str | None,
    page: int,
    page_size: int,
    current_user: User | None = None,
) -> tuple[list[DQRule], int, int, int]:
    normalized_page, normalized_page_size = normalize_page_params(
        page=page,
        page_size=page_size,
        default_page_size=25,
        max_page_size=DQ_RULES_MAX_PAGE_SIZE,
    )
    role_names = {
        str(getattr(role, "name", "")).strip().lower()
        for role in getattr(current_user, "roles", []) or []
        if str(getattr(role, "name", "")).strip()
    }
    if current_user is not None and "admin" not in role_names:
        matches = _collect_rule_matches(
            db=db,
            rule_id=rule_id,
            q=q,
            table_id=table_id,
            table_fqn=table_fqn,
            is_active=is_active,
            severity=severity,
            last_status=last_status,
            current_user=current_user,
        )
        start = max((normalized_page - 1) * normalized_page_size, 0)
        page_matches = matches[start : start + normalized_page_size]
        return [rule for rule, *_rest in page_matches], len(matches), normalized_page, normalized_page_size
    if not _dq_rules_table_ready(db):
        logger.warning("dq rules table unavailable schema=%s table=dq_rules", DQ_SCHEMA)
        return [], 0, normalized_page, normalized_page_size

    if _dq_rules_schema_ready(db):
        filters = _non_archived_rule_filters()
        if rule_id is not None:
            filters.append(DQRule.id == rule_id)
        filters.extend(_rule_scope_filters(db, table_id=table_id, table_fqn=table_fqn))
        if q:
            pattern = f"%{q.strip()}%"
            filters.append(
                (DQRule.name.ilike(pattern)) | (DQRule.description.ilike(pattern)) | (DQRule.table_fqn.ilike(pattern))
            )
        if is_active is not None:
            filters.append(DQRule.is_active == is_active)
        if severity:
            filters.append(DQRule.severity == severity.strip().lower())

        query_options = [selectinload(DQRule.notification_recipient_user), noload(DQRule.notification_recipients)]
        query = select(DQRule).options(*query_options)
        if filters:
            query = query.where(*filters)

        if last_status and latest_snapshot_support_ready(db):
            latest_status_sq = (
                select(
                    DQRuleLatestRun.rule_id.label("rule_id"),
                    DQRuleRun.status.label("last_run_status"),
                )
                .join(DQRuleRun, DQRuleLatestRun.latest_rule_run_id == DQRuleRun.id)
                .subquery()
            )
            query = query.join(latest_status_sq, latest_status_sq.c.rule_id == DQRule.id).where(
                latest_status_sq.c.last_run_status == last_status
            )

        try:
            total = int(db.scalar(select(func.count()).select_from(query.order_by(None).subquery())) or 0)
            rules = db.scalars(
                query.order_by(DQRule.updated_at.desc(), DQRule.id.desc())
                .offset((normalized_page - 1) * normalized_page_size)
                .limit(normalized_page_size)
            ).all()
        except SQLAlchemyError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Não foi possível carregar as regras de Data Quality no momento.",
            ) from exc
        return rules, total, normalized_page, normalized_page_size

    legacy_table = _reflect_dq_rules_table(db)
    if legacy_table is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Não foi possível carregar as regras de Data Quality no momento.",
        )
    query = select(*legacy_table.c)
    if rule_id is not None:
        query = query.where(legacy_table.c.id == rule_id)
    legacy_scope_filters = []
    if table_id is not None:
        table_filters = []
        if "table_id" in legacy_table.c:
            table_filters.append(legacy_table.c.table_id == table_id)
        candidates = table_fqn_candidates_for_table(db, table_id)
        if candidates and "table_fqn" in legacy_table.c:
            table_filters.append(legacy_table.c.table_fqn.in_(candidates))
        if table_filters:
            legacy_scope_filters.append(or_(*table_filters))
    elif table_fqn:
        candidates = table_fqn_candidates(table_fqn)
        table_filters = []
        if "table_fqn" in legacy_table.c:
            if candidates:
                table_filters.append(legacy_table.c.table_fqn.in_(candidates))
            else:
                table_filters.append(legacy_table.c.table_fqn.ilike(f"%{table_fqn.strip()}%"))
        if table_filters:
            legacy_scope_filters.append(or_(*table_filters))
    if legacy_scope_filters:
        query = query.where(*legacy_scope_filters)
    if q:
        pattern = f"%{q.strip()}%"
        query = query.where(
            (legacy_table.c.name.ilike(pattern))
            | (legacy_table.c.description.ilike(pattern))
            | (legacy_table.c.table_fqn.ilike(pattern))
        )
    if is_active is not None:
        query = query.where(legacy_table.c.is_active == is_active)
    if severity:
        query = query.where(legacy_table.c.severity == severity.strip().lower())
    try:
        total = int(db.scalar(select(func.count()).select_from(query.order_by(None).subquery())) or 0)
        legacy_rows = db.execute(
            query.order_by(legacy_table.c.updated_at.desc(), legacy_table.c.id.desc())
            .offset((normalized_page - 1) * normalized_page_size)
            .limit(normalized_page_size)
        ).all()
    except SQLAlchemyError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Não foi possível carregar as regras de Data Quality no momento.",
        ) from exc
    rules = [_legacy_rule_proxy(row, legacy_table) for row in legacy_rows]
    return rules, total, normalized_page, normalized_page_size


def list_rules_with_filters(
    *,
    db: Session,
    rule_id: int | None,
    q: str | None,
    table_id: int | None,
    table_fqn: str | None,
    is_active: bool | None,
    severity: str | None,
    last_status: str | None,
    current_user: User | None = None,
) -> list[DQRuleOut]:
    matches = _collect_rule_matches(
        db=db,
        rule_id=rule_id,
        q=q,
        table_id=table_id,
        table_fqn=table_fqn,
        is_active=is_active,
        severity=severity,
        last_status=last_status,
        current_user=current_user,
    )
    if not matches:
        return []

    rule_ids = [rule.id for rule, *_rest in matches]
    incidents_by_rule = open_incidents_for_rule_ids(db, rule_ids)
    audit_payloads_by_rule = load_rule_audit_payloads(db, rule_ids)
    recipients_by_rule = load_rule_notification_recipients(db, rule_ids)
    return [
        map_rule_out(
            db,
            rule,
            latest,
            latest_job,
            latest_job_out,
            incident=incidents_by_rule.get(rule.id),
            audit_payload=audit_payloads_by_rule.get(rule.id),
            notification_recipients=recipients_by_rule.get(rule.id),
        )
        for rule, latest, latest_job, latest_job_out in matches
    ]


def list_rules_with_filters_page(
    *,
    db: Session,
    rule_id: int | None,
    q: str | None,
    table_id: int | None,
    table_fqn: str | None,
    is_active: bool | None,
    severity: str | None,
    last_status: str | None,
    page: int,
    page_size: int,
    current_user: User | None = None,
) -> PageOut[DQRuleOut]:
    rules, total, normalized_page, normalized_page_size = _collect_rule_page_rules(
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
    if not rules:
        return PageOut[DQRuleOut](
            page=normalized_page,
            page_size=normalized_page_size,
            total=total,
            total_pages=max(1, (total + normalized_page_size - 1) // normalized_page_size) if total > 0 else 0,
            has_more=normalized_page * normalized_page_size < total,
            items=[],
        )

    page_rule_ids = [rule.id for rule in rules]
    incidents_by_rule = open_incidents_for_rule_ids(db, page_rule_ids)
    audit_payloads_by_rule = load_rule_audit_payloads(db, page_rule_ids)
    recipients_by_rule = load_rule_notification_recipients(db, page_rule_ids)
    items = [
        map_rule_out(
            db,
            rule,
            latest,
            latest_job,
            latest_job_out,
            incident=incidents_by_rule.get(rule.id),
            audit_payload=audit_payloads_by_rule.get(rule.id),
            notification_recipients=recipients_by_rule.get(rule.id),
        )
        for rule, latest, latest_job, latest_job_out in [
            (rule, None, None, None) for rule in rules
        ]
    ]
    if latest_snapshot_support_ready(db):
        snapshots = get_latest_rule_snapshots(db, page_rule_ids)
        latest_run_ids = sorted(
            {
                int(snapshot.latest_rule_run_id)
                for snapshot in snapshots.values()
                if snapshot.latest_rule_run_id is not None
            }
        )
        latest_job_ids = sorted(
            {
                int(snapshot.latest_job_run_id)
                for snapshot in snapshots.values()
                if snapshot.latest_job_run_id is not None
            }
        )
        latest_runs_by_id = (
            {
                row.id: row
                for row in db.scalars(select(DQRuleRun).where(DQRuleRun.id.in_(latest_run_ids))).all()
            }
            if latest_run_ids
            else {}
        )
        latest_jobs_by_id = (
            {
                row.id: row
                for row in db.scalars(select(DQJobRun).where(DQJobRun.id.in_(latest_job_ids))).all()
            }
            if latest_job_ids
            else {}
        )
        latest_job_outs_by_id = build_dq_job_out_map(db, list(latest_jobs_by_id.values()))
        items = [
            map_rule_out(
                db,
                rule,
                latest_runs_by_id.get(int(snapshot.latest_rule_run_id)) if snapshot and snapshot.latest_rule_run_id is not None else None,
                latest_jobs_by_id.get(int(snapshot.latest_job_run_id)) if snapshot and snapshot.latest_job_run_id is not None else None,
                latest_job_outs_by_id.get(int(snapshot.latest_job_run_id)) if snapshot and snapshot.latest_job_run_id is not None else None,
                incident=incidents_by_rule.get(rule.id),
                audit_payload=audit_payloads_by_rule.get(rule.id),
                notification_recipients=recipients_by_rule.get(rule.id),
            )
            for rule in rules
            for snapshot in [snapshots.get(rule.id)]
        ]
    else:
        items = [
            map_rule_out(
                db,
                rule,
                incident=incidents_by_rule.get(rule.id),
                audit_payload=audit_payloads_by_rule.get(rule.id),
                notification_recipients=recipients_by_rule.get(rule.id),
            )
            for rule in rules
        ]
    return PageOut[DQRuleOut](
        page=normalized_page,
        page_size=normalized_page_size,
        total=total,
        total_pages=max(1, (total + normalized_page_size - 1) // normalized_page_size) if total > 0 else 0,
        has_more=normalized_page * normalized_page_size < total,
        items=items,
    )


def get_rule_detail(*, db: Session, rule_id: int, current_user: User | None = None) -> DQRuleOut:
    if not _dq_rules_table_ready(db):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Não foi possível carregar as regras de Data Quality no momento.",
        )
    if _dq_rules_schema_ready(db):
        try:
            rule = db.scalar(
                select(DQRule)
                .options(
                    selectinload(DQRule.notification_recipient_user),
                    noload(DQRule.notification_recipients),
                )
                .where(DQRule.id == rule_id, DQRule.archived.is_(False))
            )
        except SQLAlchemyError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Não foi possível carregar as regras de Data Quality no momento.",
            ) from exc
    else:
        legacy_table = _reflect_dq_rules_table(db)
        if legacy_table is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Não foi possível carregar as regras de Data Quality no momento.",
            )
        try:
            legacy_row = db.execute(select(*legacy_table.c).where(legacy_table.c.id == rule_id)).first()
        except SQLAlchemyError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Não foi possível carregar as regras de Data Quality no momento.",
            ) from exc
        rule = _legacy_rule_proxy(legacy_row, legacy_table) if legacy_row else None
    if not rule:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Rule not found")
    _ensure_rule_is_available(rule)
    if not _rule_table_visible(db, rule, current_user):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Rule not found")
    latest = None
    latest_job = None
    latest_job_out = None
    if latest_snapshot_support_ready(db):
        snapshot = get_latest_rule_snapshots(db, [rule.id]).get(rule.id)
        if snapshot is not None:
            if snapshot.latest_rule_run_id is not None:
                latest = db.get(DQRuleRun, int(snapshot.latest_rule_run_id))
            if snapshot.latest_job_run_id is not None:
                latest_job = db.get(DQJobRun, int(snapshot.latest_job_run_id))
                if latest_job is not None:
                    latest_job_out = build_dq_job_out(latest_job, db)
        if latest is None and _has_table(db, "dq_rule_runs"):
            try:
                latest = db.scalar(select(DQRuleRun).where(DQRuleRun.rule_id == rule.id).order_by(DQRuleRun.created_at.desc()).limit(1))
            except SQLAlchemyError:
                latest = None
        if latest_job is None and _has_table(db, "dq_job_runs"):
            try:
                for job in db.scalars(select(DQJobRun).where(DQJobRun.job_type == "rules").order_by(DQJobRun.id.desc())).all():
                    payload = job.result_json if isinstance(job.result_json, dict) else None
                    requested_rule_ids = payload.get("requested_rule_ids") if isinstance(payload, dict) else None
                    if not isinstance(requested_rule_ids, list):
                        continue
                    if any(int(raw_id) == rule.id for raw_id in requested_rule_ids if str(raw_id).isdigit()):
                        latest_job = job
                        latest_job_out = build_dq_job_out(job, db)
                        break
            except SQLAlchemyError:
                latest_job = None
                latest_job_out = None
    else:
        if _has_table(db, "dq_rule_runs"):
            try:
                latest = db.scalar(select(DQRuleRun).where(DQRuleRun.rule_id == rule.id).order_by(DQRuleRun.created_at.desc()).limit(1))
            except SQLAlchemyError:
                latest = None
        if _has_table(db, "dq_job_runs"):
            try:
                for job in db.scalars(select(DQJobRun).where(DQJobRun.job_type == "rules").order_by(DQJobRun.id.desc())).all():
                    payload = job.result_json if isinstance(job.result_json, dict) else None
                    requested_rule_ids = payload.get("requested_rule_ids") if isinstance(payload, dict) else None
                    if not isinstance(requested_rule_ids, list):
                        continue
                    if any(int(raw_id) == rule.id for raw_id in requested_rule_ids if str(raw_id).isdigit()):
                        latest_job = job
                        latest_job_out = build_dq_job_out(job, db)
                        break
            except SQLAlchemyError:
                latest_job = None
                latest_job_out = None
    return map_rule_out(db, rule, latest, latest_job, latest_job_out)


def validate_rule_structure_for_spark(*, db: Session, rule_id: int, current_user: User | None = None) -> DQRuleTestOut:
    if not _dq_rules_table_ready(db):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Nao foi possivel validar a regra de Data Quality no momento.",
        )

    rule = db.get(DQRule, rule_id)
    if not rule:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Rule not found")
    _ensure_rule_is_available(rule)
    if not _rule_table_visible(db, rule, current_user):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Rule not found")

    try:
        table, schema, _database, datasource = _resolve_target_table(db, table_id=rule.table_id, table_fqn=rule.table_fqn)
        definition = rule.rule_definition_json if isinstance(rule.rule_definition_json, dict) else None
        if definition is None:
            raise ValueError("A regra não possui definição estruturada.")
        build_rule_definition(
            datasource=datasource,
            schema=schema,
            table=table,
            rule_type=str(rule.rule_type),
            logic=str(definition.get("logic") or "AND"),
            conditions=list(definition.get("conditions") or []),
            columns_by_name=_columns_by_name_for_table(db, table.id),
            quality_dimension=str(definition.get("dimension") or ""),
            rule_category=str(definition.get("category") or ""),
            template_key=str(definition.get("template_key") or ""),
            unique_columns=list(definition.get("unique_columns") or []),
            comparison_target=definition.get("comparison") if isinstance(definition.get("comparison"), dict) else None,
        )
        if rule.table_id is not None and table.id != rule.table_id:
            raise ValueError("A regra aponta para uma tabela divergente do cadastro atual.")
    except ValueError as exc:
        return DQRuleTestOut(
            valid=False,
            status="error",
            violations_count=0,
            preview_rows=[],
            error_message=str(exc),
        )

    return DQRuleTestOut(
        valid=True,
        status="pass",
        violations_count=0,
        preview_rows=[],
        error_message=None,
    )



def create_rule_with_audit(*, db: Session, payload, audit_kwargs: dict, current_user: User | None = None) -> DQRuleOut:
    if not _dq_rules_schema_ready(db):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Não foi possível salvar as regras de Data Quality no momento.",
        )
    reject_legacy_sql_payload(payload)
    try:
        table, schema, _database, datasource = _resolve_target_table(
            db,
            table_id=getattr(payload, "table_id", None),
            table_fqn=getattr(payload, "table_fqn", None),
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc
    if current_user is not None and not can_view_table(current_user, table):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Table not found")
    try:
        definition = build_rule_definition(
            datasource=datasource,
            schema=schema,
            table=table,
            rule_type=payload.rule_type,
            logic=payload.logic,
            conditions=[item.model_dump() for item in payload.conditions],
            columns_by_name=_columns_by_name_for_table(db, table.id),
            quality_dimension=getattr(payload, "quality_dimension", None),
            rule_category=getattr(payload, "rule_category", None),
            template_key=getattr(payload, "template_key", None),
            unique_columns=list(getattr(payload, "unique_columns", None) or []),
            comparison_target=(getattr(payload, "comparison_target", None).model_dump() if getattr(payload, "comparison_target", None) is not None else None),
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc

    recipient_ids = _normalize_rule_recipient_ids(
        db,
        user_id=getattr(payload, "notification_recipient_user_id", None),
        user_ids=getattr(payload, "notification_recipient_user_ids", None),
    )
    schedule_payload = validate_schedule_payload(payload.model_dump(), existing=None)
    rule = DQRule(
        table_id=table.id,
        execution_engine=configured_execution_engine(getattr(payload, "execution_engine", None)),
        notification_recipient_user_id=recipient_ids[0] if recipient_ids else None,
        table_fqn=f"{datasource.name}.{schema.name}.{table.name}",
        name=payload.name.strip(),
        description=payload.description,
        rule_type=payload.rule_type,
        severity=payload.severity,
        rule_builder_version=1,
        rule_definition_json=definition,
        legacy_rule_type=None,
        archived=False,
        archived_reason=None,
        archived_at=None,
        is_active=payload.is_active,
        schedule_mode=schedule_payload["schedule_mode"],
        schedule_enabled=schedule_payload["schedule_enabled"],
        schedule_every_minutes=schedule_payload.get("schedule_every_minutes"),
        schedule_time=schedule_payload.get("schedule_time"),
        schedule_day_of_week=schedule_payload.get("schedule_day_of_week"),
        schedule_day_of_month=schedule_payload.get("schedule_day_of_month"),
        schedule_anchor_date=schedule_payload.get("schedule_anchor_date"),
    )
    rule.notification_recipients = _load_rule_recipients(db, recipient_ids)
    db.add(rule)
    db.commit()
    db.refresh(rule)
    write_audit_log_sync(
        db,
        action="dq_rule.create",
        entity_type="dq_rule",
        entity_id=rule.id,
        after=serialize_model(rule),
        **audit_kwargs,
    )
    db.commit()
    return map_rule_out(db, rule)



def update_rule_with_audit(*, db: Session, rule_id: int, payload, audit_kwargs: dict, current_user: User | None = None) -> DQRuleOut:
    if not _dq_rules_schema_ready(db):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Não foi possível atualizar as regras de Data Quality no momento.",
        )
    rule = db.get(DQRule, rule_id)
    if not rule:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Rule not found")
    _ensure_rule_is_available(rule)
    if not _rule_table_visible(db, rule, current_user):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Rule not found")
    before = serialize_model(rule)

    updates = payload.model_dump(exclude_unset=True)
    reject_legacy_sql_payload(updates)
    if "name" in updates and (updates["name"] is None or not str(updates["name"]).strip()):
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="name is required")
    try:
        table, schema, _database, datasource = _resolve_target_table(
            db,
            table_id=int(updates.get("table_id") or rule.table_id or 0) or None,
            table_fqn=updates.get("table_fqn") or rule.table_fqn,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc
    definition_seed = rule.rule_definition_json if isinstance(rule.rule_definition_json, dict) else {"logic": "AND", "conditions": []}
    next_conditions = updates.get("conditions")
    serialized_conditions = (
        [item.model_dump() if hasattr(item, "model_dump") else item for item in next_conditions]
        if next_conditions is not None
        else list(definition_seed.get("conditions") or [])
    )
    next_logic = updates.get("logic") or str(definition_seed.get("logic") or "AND")
    next_rule_type = updates.get("rule_type") or rule.rule_type
    try:
        definition = build_rule_definition(
            datasource=datasource,
            schema=schema,
            table=table,
            rule_type=next_rule_type,
            logic=next_logic,
            conditions=serialized_conditions,
            columns_by_name=_columns_by_name_for_table(db, table.id),
            quality_dimension=updates.get("quality_dimension") or definition_seed.get("dimension"),
            rule_category=updates.get("rule_category") or definition_seed.get("category"),
            template_key=updates.get("template_key") or definition_seed.get("template_key"),
            unique_columns=list(updates.get("unique_columns") or definition_seed.get("unique_columns") or []),
            comparison_target=(
                (updates.get("comparison_target") or definition_seed.get("comparison"))
                if isinstance(updates.get("comparison_target") or definition_seed.get("comparison"), dict)
                else None
            ),
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc
    if "execution_engine" in updates and updates["execution_engine"] is not None:
        updates["execution_engine"] = configured_execution_engine(str(updates["execution_engine"]))
    recipient_ids: list[int] | None = None
    if "notification_recipient_user_ids" in updates or "notification_recipient_user_id" in updates:
        recipient_ids = _normalize_rule_recipient_ids(
            db,
            user_id=updates.get("notification_recipient_user_id"),
            user_ids=updates.get("notification_recipient_user_ids"),
        )
        updates["notification_recipient_user_id"] = recipient_ids[0] if recipient_ids else None
    if any(
        key in updates
        for key in {
            "schedule_mode",
            "schedule_enabled",
            "schedule_every_minutes",
            "schedule_time",
            "schedule_day_of_week",
            "schedule_day_of_month",
            "schedule_anchor_date",
        }
    ):
        normalized_schedule = validate_schedule_payload(updates, existing=serialize_model(rule))
        updates.update(normalized_schedule)

    for key, value in updates.items():
        if key in {"notification_recipient_user_ids", "conditions", "logic"}:
            continue
        if key in {
            "schedule_mode",
            "schedule_enabled",
            "schedule_every_minutes",
            "schedule_time",
            "schedule_day_of_week",
            "schedule_day_of_month",
            "schedule_anchor_date",
        }:
            setattr(rule, key, value)
            continue
        setattr(rule, key, value.strip() if isinstance(value, str) else value)
    rule.table_id = table.id
    rule.table_fqn = f"{datasource.name}.{schema.name}.{table.name}"
    rule.rule_definition_json = definition
    rule.rule_builder_version = 1

    if recipient_ids is not None:
        rule.notification_recipients = _load_rule_recipients(db, recipient_ids)

    db.add(rule)
    db.commit()
    db.refresh(rule)
    write_audit_log_sync(
        db,
        action="dq_rule.update",
        entity_type="dq_rule",
        entity_id=rule.id,
        before=before,
        after=serialize_model(rule),
        **audit_kwargs,
    )
    db.commit()
    latest = db.scalar(select(DQRuleRun).where(DQRuleRun.rule_id == rule.id).order_by(DQRuleRun.created_at.desc()).limit(1))
    return map_rule_out(db, rule, latest)



def delete_rule_with_audit(*, db: Session, rule_id: int, audit_kwargs: dict, current_user: User | None = None) -> None:
    if not _dq_rules_table_ready(db):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Não foi possível excluir as regras de Data Quality no momento.",
        )
    rule = db.get(DQRule, rule_id)
    if not rule:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Rule not found")
    _ensure_rule_is_available(rule)
    if not _rule_table_visible(db, rule, current_user):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Rule not found")
    before = serialize_model(rule)
    db.delete(rule)
    db.commit()
    write_audit_log_sync(
        db,
        action="dq_rule.delete",
        entity_type="dq_rule",
        entity_id=rule_id,
        before=before,
        **audit_kwargs,
    )
    db.commit()



def list_rule_runs_history(*, db: Session, rule_id: int, limit: int, current_user: User | None = None) -> list:
    if not _dq_rules_table_ready(db):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Não foi possível carregar o histórico da regra no momento.",
        )
    rule = db.get(DQRule, rule_id)
    if not rule:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Rule not found")
    _ensure_rule_is_available(rule)
    if not _rule_table_visible(db, rule, current_user):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Rule not found")
    return db.scalars(
        select(DQRuleRun).where(DQRuleRun.rule_id == rule_id).order_by(DQRuleRun.created_at.desc()).limit(limit)
    ).all()
