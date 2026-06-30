from __future__ import annotations

import logging
from threading import Lock
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import quote, urlencode

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from t2c_data.core.config import settings
from t2c_data.core.sql_utils import safe_identifier, safe_relation
from t2c_data.features.shared_cache import safe_connection_fingerprint
from t2c_data.features.operations.failures import classify_operational_error, record_operational_failure

CONTROL_SCHEMA = (settings.operational_db_schema or "controle").strip() or "controle"
CATALOG_SCHEMA = "t2c_data"
SUMMARY_VIEW = "vw_t2c_ingestao_operacional"
CANONICAL_SUMMARY_TABLE = "t2c_controle_ingestao"
PIPELINE_TABLE = "t2c_controle_pipeline_mysql_pg"
EXECUTION_TABLE = "vw_t2c_historico_operacional"
CANONICAL_EXECUTION_TABLE = "etl_watermark"
LOG_TABLE = "vw_t2c_log_operacional"
CANONICAL_LOG_TABLE = "t2c_log_execucao_ingestao"
LEGACY_SUMMARY_VIEW = "vw_t2c_ingestao_tabelas"
LEGACY_EXECUTION_TABLE = "t2c_execucao_pipeline_mysql_pg"
LEGACY_LOG_TABLE = "t2c_log_pipeline_mysql_pg"
SUMMARY_RELATION_CANDIDATES = (SUMMARY_VIEW, CANONICAL_SUMMARY_TABLE, LEGACY_SUMMARY_VIEW, PIPELINE_TABLE)
EXECUTION_RELATION_CANDIDATES = (EXECUTION_TABLE, CANONICAL_EXECUTION_TABLE, LEGACY_EXECUTION_TABLE)
LOG_RELATION_CANDIDATES = (LOG_TABLE, CANONICAL_LOG_TABLE, LEGACY_LOG_TABLE)
SNAPSHOT_TABLE = "operational_stability_snapshots"
STALE_SUCCESS_THRESHOLD_HOURS = 72
HIGH_VOLUME_ROWS_THRESHOLD = 100000

logger = logging.getLogger(__name__)
_COLUMN_MAP_CACHE_LOCK = Lock()
_COLUMN_MAP_CACHE_TTL_SECONDS = 300

SAFE_CONTROL_SCHEMA = safe_identifier(CONTROL_SCHEMA, label="schema")
SAFE_CATALOG_SCHEMA = safe_identifier(CATALOG_SCHEMA, label="schema")


@dataclass
class ColumnMapCacheEntry:
    checked_at: datetime
    column_map: ColumnMap | None
    unavailable_message: str | None = None
    unavailable_log_emitted: bool = False


class IngestionIntegrationUnavailable(RuntimeError):
    pass


@dataclass(frozen=True)
class TableIngestionKey:
    schema_name: str
    table_name: str


@dataclass(frozen=True)
class ColumnMap:
    summary_relation: str
    execution_relation: str | None
    log_relation: str | None
    summary_columns: set[str]
    execution_columns: set[str]
    log_columns: set[str]


_COLUMN_MAP_CACHE: dict[str, ColumnMapCacheEntry] = {}


def _normalize_token(value: Any) -> str | None:
    if value is None:
        return None
    text_value = str(value).strip()
    return text_value or None


def _normalize_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _normalize_dt(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value))
    except ValueError:
        return None


def _sortable_dt(value: Any) -> datetime:
    normalized = _normalize_dt(value)
    if normalized is None:
        return datetime.min.replace(tzinfo=timezone.utc)
    if normalized.tzinfo is None:
        return normalized.replace(tzinfo=timezone.utc)
    return normalized.astimezone(timezone.utc)


def _safe_identifier_or_none(value: str | None, *, label: str) -> str | None:
    if value is None:
        return None
    return safe_identifier(value, label=label)


def _column_map_cache_key(session: Session) -> str:
    get_bind = getattr(session, "get_bind", None)
    if not callable(get_bind):
        return "bind:unknown"
    bind = get_bind()
    if bind is None:
        return "bind:unknown"
    engine = getattr(bind, "engine", bind)
    url = getattr(engine, "url", None)
    if url is None:
        return f"bind:{id(engine)}"
    return f"{CONTROL_SCHEMA}|{safe_connection_fingerprint(engine)}"


def _safe_rollback(session: Session, *, context: str) -> None:
    try:
        session.rollback()
    except Exception:  # noqa: BLE001
        logger.debug("ingestion rollback failed context=%s", context, exc_info=True)


def _record_ingestion_failure(
    session: Session,
    *,
    exc: Exception,
    source: str,
    schema_name: str | None = None,
    table_name: str | None = None,
    context: dict[str, object] | None = None,
) -> None:
    try:
        category, severity, retryable = classify_operational_error(exc, source=source)
        record_operational_failure(
            session,
            source=source,
            message=str(exc),
            category_code=category,
            severity=severity,
            retryable=retryable,
            context={
                "schema": schema_name,
                "table": table_name,
                **(context or {}),
            },
        )
        session.commit()
    except Exception:  # noqa: BLE001
        session.rollback()


def _pick_column(columns: set[str], candidates: tuple[str, ...]) -> str | None:
    for candidate in candidates:
        if candidate in columns:
            return candidate
    return None


def _pick_value(row: dict[str, Any], candidates: tuple[str, ...]) -> Any:
    for candidate in candidates:
        if candidate in row and row[candidate] not in (None, ""):
            return row[candidate]
    return None


def _list_columns(session: Session, table_name: str) -> set[str]:
    safe_table = safe_identifier(table_name, label="relation")
    rows = session.execute(
        text(
            """
            select column_name
            from information_schema.columns
            where table_schema = :schema_name and table_name = :table_name
            order by ordinal_position
            """
        ),
        {"schema_name": SAFE_CONTROL_SCHEMA, "table_name": safe_table},
    ).scalars().all()
    return {str(item) for item in rows}


def _relation_exists(session: Session, relation_name: str) -> bool:
    regclass = session.execute(
        text("SELECT to_regclass(:regname)"),
        {"regname": safe_relation(SAFE_CONTROL_SCHEMA, relation_name, label="relation")},
    ).scalar_one()
    return regclass is not None


def _existing_relations(session: Session, candidates: tuple[str, ...]) -> list[str]:
    return [relation_name for relation_name in candidates if _relation_exists(session, relation_name)]


def _resolve_relation_columns(session: Session, candidates: tuple[str, ...]) -> tuple[str, set[str]] | None:
    for relation_name in candidates:
        if not _relation_exists(session, relation_name):
            continue
        columns = _list_columns(session, relation_name)
        if columns:
            return relation_name, columns
    return None


def _discover_column_map(session: Session) -> ColumnMap:
    summary_result = _resolve_relation_columns(session, SUMMARY_RELATION_CANDIDATES)
    if summary_result is None:
        expected = ", ".join(SUMMARY_RELATION_CANDIDATES)
        found = ", ".join(_existing_relations(session, SUMMARY_RELATION_CANDIDATES)) or "nenhuma relação"
        raise IngestionIntegrationUnavailable(
            f"A visão operacional de ingestão não está disponível no schema {CONTROL_SCHEMA}. Esperado: {expected}. Encontrado: {found}."
        )
    execution_result = _resolve_relation_columns(session, EXECUTION_RELATION_CANDIDATES)
    log_result = _resolve_relation_columns(session, LOG_RELATION_CANDIDATES)
    summary_relation, summary_columns = summary_result
    execution_relation, execution_columns = execution_result if execution_result is not None else (None, set())
    log_relation, log_columns = log_result if log_result is not None else (None, set())
    return ColumnMap(
        summary_relation=summary_relation,
        execution_relation=execution_relation,
        log_relation=log_relation,
        summary_columns=summary_columns,
        execution_columns=execution_columns,
        log_columns=log_columns,
    )


def _load_column_map(session: Session) -> ColumnMap:
    now = datetime.now(timezone.utc)
    cache_key = _column_map_cache_key(session)
    with _COLUMN_MAP_CACHE_LOCK:
        cache = _COLUMN_MAP_CACHE.get(cache_key)
        if cache is not None and (now - cache.checked_at).total_seconds() < _COLUMN_MAP_CACHE_TTL_SECONDS:
            if cache.column_map is not None:
                return cache.column_map
            raise IngestionIntegrationUnavailable(cache.unavailable_message or "A visão operacional de ingestão não está disponível neste ambiente.")

    previous_cache = cache
    try:
        column_map = _discover_column_map(session)
    except IngestionIntegrationUnavailable as exc:
        message = str(exc)
        should_log = (
            previous_cache is None
            or previous_cache.column_map is not None
            or previous_cache.unavailable_message != message
            or not previous_cache.unavailable_log_emitted
        )
        if should_log:
            logger.warning(message)
        with _COLUMN_MAP_CACHE_LOCK:
            _COLUMN_MAP_CACHE[cache_key] = ColumnMapCacheEntry(
                checked_at=now,
                column_map=None,
                unavailable_message=message,
                unavailable_log_emitted=should_log,
            )
        raise

    if previous_cache is not None and previous_cache.column_map is None:
        logger.info(
            "ingestion operational relation available again schema=%s summary=%s execution=%s log=%s",
            CONTROL_SCHEMA,
            column_map.summary_relation,
            column_map.execution_relation or "none",
            column_map.log_relation or "none",
        )
    with _COLUMN_MAP_CACHE_LOCK:
        _COLUMN_MAP_CACHE[cache_key] = ColumnMapCacheEntry(
            checked_at=now,
            column_map=column_map,
            unavailable_message=None,
            unavailable_log_emitted=False,
        )
    return column_map


def _empty_overview_payload(*, now: datetime, message: str) -> dict[str, Any]:
    return {
        "available": False,
        "message": message,
        "generated_at": now,
        "pipelines_total": 0,
        "linked_tables": 0,
        "unmapped": 0,
        "degraded": 0,
        "failed": 0,
        "running": 0,
        "pending": 0,
        "stale": 0,
        "critical_stale": 0,
        "high_volume_failed": 0,
        "high_volume_failed_threshold_rows": HIGH_VOLUME_ROWS_THRESHOLD,
        "stale_threshold_hours": STALE_SUCCESS_THRESHOLD_HOURS,
        "items": [],
        "unmapped_items": [],
        "degraded_items": [],
        "failed_items": [],
        "critical_stale_items": [],
        "high_volume_failed_items": [],
    }


def _load_snapshot_rows(session: Session) -> list[dict[str, Any]]:
    snapshot_relation = safe_relation(SAFE_CATALOG_SCHEMA, SNAPSHOT_TABLE, label="snapshot table")
    rows = session.execute(
        text(
            f"""
            select *
            from {snapshot_relation}
            order by bucket_start_at desc, id desc
            """
        )
    ).mappings().all()
    latest_by_table: dict[int, dict[str, Any]] = {}
    for row in rows:
        row_dict = dict(row)
        table_id = _normalize_int(row_dict.get("table_id"))
        if table_id is None or table_id in latest_by_table:
            continue
        latest_by_table[table_id] = row_dict
    return list(latest_by_table.values())


def _load_raw_pipeline_rows(session: Session) -> list[dict[str, Any]]:
    for relation_name in (CANONICAL_SUMMARY_TABLE, PIPELINE_TABLE, SUMMARY_VIEW, LEGACY_SUMMARY_VIEW):
        if not _relation_exists(session, relation_name):
            continue
        safe_relation_name = safe_relation(SAFE_CONTROL_SCHEMA, relation_name, label="summary relation")
        rows = session.execute(
            text(
                f"""
                select *
                from {safe_relation_name}
                """
            )
        ).mappings().all()
        if rows:
            return [dict(row) for row in rows]
    return []


def _build_ingestion_overview_from_rows(
    rows: list[dict[str, Any]],
    *,
    table_refs: list[dict[str, Any]] | None = None,
    limit: int = 8,
    high_volume_threshold_rows: int = HIGH_VOLUME_ROWS_THRESHOLD,
    stale_threshold_hours: int = STALE_SUCCESS_THRESHOLD_HOURS,
    airflow_ui_base_url: str | None = None,
    now: datetime | None = None,
    fallback_message: str | None = None,
) -> dict[str, Any]:
    current_time = now or datetime.now(timezone.utc)
    if not rows:
        return _empty_overview_payload(now=current_time, message=fallback_message or "Não há dados operacionais de ingestão neste ambiente.")

    allowed_pairs: set[tuple[str, str]] | None = None
    ref_map: dict[tuple[str, str], dict[str, Any]] = {}
    if table_refs:
        allowed_pairs = set()
        for item in table_refs:
            schema_name = (_normalize_token(item.get("schema_name")) or "").lower()
            table_name = (_normalize_token(item.get("table_name")) or "").lower()
            if not schema_name or not table_name:
                continue
            key = (schema_name, table_name)
            allowed_pairs.add(key)
            ref_map[key] = item

    ranked_by_table: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        target_schema = (_normalize_token(_pick_value(row, TARGET_SCHEMA_CANDIDATES)) or "").lower()
        target_table = (_normalize_token(_pick_value(row, TARGET_TABLE_CANDIDATES)) or "").lower()
        if target_schema and target_table:
            key = (target_schema, target_table)
            if allowed_pairs is not None and key not in allowed_pairs:
                continue
            current = ranked_by_table.get(key)
            if current is None or _row_priority(row) > _row_priority(current):
                ranked_by_table[key] = row
            continue

        pipeline_id = (_normalize_token(_pick_value(row, PIPELINE_ID_CANDIDATES)) or "").lower()
        dag_id = (_normalize_token(_pick_value(row, DAG_ID_CANDIDATES)) or "").lower()
        pipeline_name = (_normalize_token(_pick_value(row, PIPELINE_NAME_CANDIDATES)) or "").lower()
        identity_key: tuple[str, str] | None = None
        if pipeline_id:
            identity_key = ("pipeline_id", pipeline_id)
        elif dag_id:
            identity_key = ("dag_id", dag_id)
        elif pipeline_name:
            identity_key = ("pipeline_name", pipeline_name)
        if identity_key is None:
            continue
        current = ranked_by_table.get(identity_key)
        if current is None or _row_priority(row) > _row_priority(current):
            ranked_by_table[identity_key] = row

    if not ranked_by_table:
        return _empty_overview_payload(now=current_time, message=fallback_message or "Não foi possível consolidar a visão operacional de ingestão.")

    ranked_rows = sorted(ranked_by_table.items(), key=lambda item: _row_priority(item[1]), reverse=True)
    payload_by_key: dict[tuple[str, str], dict[str, Any]] = {
        key: _serialize_pipeline_row(row, is_primary=True, airflow_ui_base_url=airflow_ui_base_url) for key, row in ranked_rows
    }
    failed = 0
    degraded = 0
    running = 0
    pending = 0
    stale = 0
    critical_stale = 0
    degraded_items: list[dict[str, Any]] = []
    failed_items: list[dict[str, Any]] = []
    critical_stale_items: list[dict[str, Any]] = []
    high_volume_failed_items: list[dict[str, Any]] = []
    for key, row in ranked_rows:
        status_label = _status_label(_pick_value(row, LATEST_STATUS_CANDIDATES))
        payload = payload_by_key[key]
        table_ref = ref_map.get(key, {})
        threshold_hours = max(int(stale_threshold_hours or STALE_SUCCESS_THRESHOLD_HOURS), 1)
        is_stale = _is_stale_pipeline(payload, now=current_time, threshold_hours=threshold_hours)
        criticality_score = _normalize_int(table_ref.get("criticality_score")) or 0
        if status_label == "Falha":
            failed += 1
            degraded += 1
            if table_ref:
                degraded_items.append(
                    _overview_focus_item(
                        table_ref,
                        payload=payload,
                        hint="Falha operacional registrada. Vale abrir o ativo e revisar o histórico do pipeline.",
                    )
                )
                failed_items.append(
                    _overview_focus_item(
                        table_ref,
                        payload=payload,
                        hint="Falha operacional ativa no pipeline associado a este ativo.",
                    )
                )
                rows_processed = _normalize_int(payload.get("rows_processed")) or 0
                if rows_processed >= max(int(high_volume_threshold_rows or HIGH_VOLUME_ROWS_THRESHOLD), 1):
                    high_volume_failed_items.append(
                        _overview_focus_item(
                            table_ref,
                            payload=payload,
                            hint=(
                                f"Falha operacional em ativo com volume recente de "
                                f"{rows_processed:,}".replace(",", ".")
                                + " linhas processadas."
                            ),
                        )
                    )
        elif status_label == "Em execução":
            running += 1
        elif status_label == "Pendente":
            pending += 1
            degraded += 1
            if table_ref:
                degraded_items.append(
                    _overview_focus_item(
                        table_ref,
                        payload=payload,
                        hint="Pipeline vinculado, mas ainda pendente de execução ou processamento.",
                    )
                )
        if is_stale:
            stale += 1
            if criticality_score >= 75 and table_ref:
                critical_stale += 1
                critical_stale_items.append(
                    _overview_focus_item(
                        table_ref,
                        payload=payload,
                        hint=f"Ativo crítico sem sucesso operacional recente nas últimas {STALE_SUCCESS_THRESHOLD_HOURS}h.",
                    )
                )

    items = []
    for (schema_name, table_name), row in ranked_rows[:limit]:
        payload = payload_by_key[(schema_name, table_name)]
        table_ref = ref_map.get((schema_name, table_name), {})
        table_id = _normalize_int(table_ref.get("table_id"))
        schema_ref_name = _normalize_token(table_ref.get("schema_name")) or _normalize_token(payload.get("schema_name")) or _normalize_token(payload.get("target_schema")) or schema_name
        items.append(
            {
                "table_id": table_id,
                "table_name": _normalize_token(table_ref.get("table_name")) or payload.get("target_table") or table_name,
                "table_fqn": _normalize_token(table_ref.get("table_fqn")) or f"{schema_name}.{table_name}",
                "schema_name": schema_ref_name,
                "pipeline_name": payload.get("pipeline_name"),
                "dag_id": payload.get("dag_id"),
                "task_name": payload.get("task_name"),
                "load_type": payload.get("load_type"),
                "load_type_label": payload.get("load_type_label"),
                "latest_status_label": payload.get("latest_status_label"),
                "last_status": payload.get("last_status"),
                "last_success_at": payload.get("last_success_at"),
                "last_execution_finished_at": payload.get("last_execution_finished_at"),
                "last_run_started_at": payload.get("last_run_started_at"),
                "last_run_finished_at": payload.get("last_run_finished_at"),
                "last_watermark": payload.get("last_watermark"),
                "watermark_value": payload.get("watermark_value"),
                "records_processed": payload.get("records_processed"),
                "rows_processed": payload.get("rows_processed"),
                "observacao": payload.get("observacao"),
                "last_error": payload.get("last_error"),
                "pipeline_history_href": payload.get("pipeline_history_href"),
                "airflow_dag_href": payload.get("airflow_dag_href"),
                "airflow_task_href": payload.get("airflow_task_href"),
                "target_url": f"/explorer?tableId={table_id}" if table_id is not None else None,
            }
        )

    unmapped_items: list[dict[str, Any]] = []
    if allowed_pairs is not None:
        missing_pairs = sorted(key for key in allowed_pairs if key not in ranked_by_table)
        for key in missing_pairs[:limit]:
            table_ref = ref_map.get(key)
            if table_ref:
                unmapped_items.append(
                    _overview_focus_item(
                        table_ref,
                        hint="O ativo está no catálogo, mas ainda não há pipeline Airflow mapeado na camada operacional.",
                    )
                )

    return {
        "available": True,
        "message": None,
        "generated_at": current_time,
        "pipelines_total": len(rows),
        "linked_tables": len(ranked_by_table),
        "unmapped": max(len(allowed_pairs or []) - len(ranked_by_table), 0) if allowed_pairs is not None else 0,
        "degraded": degraded,
        "failed": failed,
        "running": running,
        "pending": pending,
        "stale": stale,
        "critical_stale": critical_stale,
        "high_volume_failed": len(high_volume_failed_items),
        "high_volume_failed_threshold_rows": max(int(high_volume_threshold_rows or HIGH_VOLUME_ROWS_THRESHOLD), 1),
        "stale_threshold_hours": max(int(stale_threshold_hours or STALE_SUCCESS_THRESHOLD_HOURS), 1),
        "items": items,
        "unmapped_items": unmapped_items,
        "degraded_items": degraded_items[:limit],
        "failed_items": failed_items[:limit],
        "critical_stale_items": critical_stale_items[:limit],
        "high_volume_failed_items": high_volume_failed_items[:limit],
    }


TARGET_SCHEMA_CANDIDATES = ("target_schema", "schema_name", "schema_destino", "dest_schema", "target_schema_name")
TARGET_TABLE_CANDIDATES = ("target_table", "table_name", "tabela_destino", "dest_table", "target_table_name")
PIPELINE_ID_CANDIDATES = ("pipeline_id", "id_pipeline", "controle_pipeline_id", "pipeline_control_id")
PIPELINE_NAME_CANDIDATES = ("pipeline_name", "nome_pipeline", "pipeline", "pipeline_label")
DAG_ID_CANDIDATES = ("dag_id", "airflow_dag_id")
TASK_NAME_CANDIDATES = ("task_name", "task_id", "main_task", "tarefa_principal", "ultima_task_id")
LOAD_TYPE_CANDIDATES = ("load_type", "tipo_carga", "ingestion_type", "mode")
SOURCE_CONNECTION_CANDIDATES = ("source_connection", "conexao_origem", "origem_conexao", "source_name", "source_conn_id")
SOURCE_DATABASE_CANDIDATES = ("source_database", "base_origem", "database_origem", "source_db")
SOURCE_TABLE_CANDIDATES = ("source_table", "tabela_origem", "source_object")
SCHEMA_NAME_CANDIDATES = ("schema_name", "schema", "target_schema", "target_schema_name", "schema_destino")
LATEST_STATUS_CANDIDATES = ("last_status", "latest_status", "latest_status_label", "status_ultima_execucao", "status_execucao", "status", "ultima_execucao_status", "ultima_execucao_status_detalhe")
LAST_SUCCESS_CANDIDATES = ("last_success_at", "ultima_execucao_sucesso", "data_ultima_sucesso", "last_successful_at")
LAST_START_CANDIDATES = ("last_execution_started_at", "ultima_execucao_inicio", "started_at", "data_inicio", "ultima_execucao_started_at")
LAST_FINISH_CANDIDATES = ("last_execution_finished_at", "ultima_execucao_fim", "finished_at", "data_fim", "ended_at", "ultima_execucao_finished_at")
LAST_FAILURE_CANDIDATES = ("last_failure_at", "ultima_falha_em", "data_ultima_falha")
LAST_ERROR_CANDIDATES = ("last_error", "ultimo_erro", "error_message", "mensagem_erro", "ultimo_erro_mensagem")
WATERMARK_VALUE_CANDIDATES = ("watermark_value", "watermark_atual", "current_watermark")
WATERMARK_COLUMN_CANDIDATES = ("watermark_column", "coluna_watermark")
WATERMARK_TYPE_CANDIDATES = ("watermark_type", "tipo_watermark")
ROWS_PROCESSED_CANDIDATES = (
    "rows_processed",
    "records_processed",
    "qtd_rows_processadas",
    "rows_written",
    "rows_upserted",
    "linhas_gravadas_ultima",
    "linhas_extraidas_ultima",
    "linhas_upsert_ultima",
)
OBSERVACAO_CANDIDATES = ("observacao", "observations", "notes", "note", "comment", "last_error", "error_message", "mensagem_erro")

EXECUTION_ID_CANDIDATES = ("execution_id", "id_execucao", "execucao_id", "id", "ultima_execucao_id")
EXECUTION_RUN_ID_CANDIDATES = ("dag_run_id", "run_id", "airflow_run_id", "execution_id", "id_execucao", "execucao_id")
EXECUTION_STATUS_CANDIDATES = ("status", "status_execucao")
EXECUTION_START_CANDIDATES = ("started_at", "data_inicio", "inicio_execucao")
EXECUTION_FINISH_CANDIDATES = ("finished_at", "data_fim", "fim_execucao", "ended_at")
EXECUTION_DURATION_CANDIDATES = ("duration_seconds", "duracao_segundos", "duration_s")
ROWS_EXTRACTED_CANDIDATES = ("rows_extracted", "linhas_extraidas", "qtd_extraida", "linhas_extraidas_ultima")
ROWS_WRITTEN_CANDIDATES = ("rows_written", "linhas_gravadas", "qtd_gravada", "linhas_gravadas_ultima")
ROWS_UPSERTED_CANDIDATES = ("rows_upserted", "linhas_upsert", "qtd_upsert", "linhas_upsert_ultima")
WATERMARK_BEFORE_CANDIDATES = ("watermark_before", "watermark_anterior")
WATERMARK_AFTER_CANDIDATES = ("watermark_after", "watermark_posterior", "watermark_value", "ultima_watermark_after")
EXECUTION_ERROR_CANDIDATES = ("error_message", "mensagem_erro", "last_error", "ultimo_erro", "erro_mensagem", "ultimo_erro_mensagem")

LOG_ID_CANDIDATES = ("log_id", "id_log", "id")
LOG_EXECUTION_ID_CANDIDATES = ("execution_id", "id_execucao", "execucao_id")
LOG_TS_CANDIDATES = ("logged_at", "created_at", "timestamp", "data_hora", "occurred_at", "event_ts")
LOG_LEVEL_CANDIDATES = ("level", "nivel")
LOG_STEP_CANDIDATES = ("step", "etapa", "stage")
LOG_MESSAGE_CANDIDATES = ("message", "mensagem", "log_message")
LOG_STACKTRACE_CANDIDATES = ("stacktrace", "stack_trace", "traceback", "erro_detalhado")


def _pipeline_history_href(row: dict[str, Any]) -> str | None:
    dag_id = _normalize_token(_pick_value(row, DAG_ID_CANDIDATES))
    pipeline_id = _normalize_token(_pick_value(row, PIPELINE_ID_CANDIDATES))
    target_schema = _normalize_token(_pick_value(row, TARGET_SCHEMA_CANDIDATES))
    target_table = _normalize_token(_pick_value(row, TARGET_TABLE_CANDIDATES))
    if not dag_id and not pipeline_id:
        return None
    params: dict[str, str] = {}
    if dag_id:
        params["dagId"] = dag_id
    if pipeline_id:
        params["pipelineId"] = pipeline_id
    if target_schema:
        params["schema"] = target_schema
    if target_table:
        params["table"] = target_table
    return f"/ops/ingestion?{urlencode(params)}" if params else None


def _normalized_base_url(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip().rstrip("/")
    return normalized or None


def _airflow_dag_href(*, dag_id: str | None, base_url: str | None) -> str | None:
    normalized_base_url = _normalized_base_url(base_url)
    normalized_dag_id = _normalize_token(dag_id)
    if not normalized_base_url or not normalized_dag_id:
        return None
    return f"{normalized_base_url}/dags/{quote(normalized_dag_id, safe='')}/grid"


def _airflow_task_href(*, dag_id: str | None, task_name: str | None, base_url: str | None) -> str | None:
    dag_href = _airflow_dag_href(dag_id=dag_id, base_url=base_url)
    normalized_task_name = _normalize_token(task_name)
    if not dag_href or not normalized_task_name:
        return None
    return f"{dag_href}?task_id={quote(normalized_task_name, safe='')}"


def _airflow_run_href(*, dag_id: str | None, run_id: str | None, base_url: str | None) -> str | None:
    dag_href = _airflow_dag_href(dag_id=dag_id, base_url=base_url)
    normalized_run_id = _normalize_token(run_id)
    if not dag_href or not normalized_run_id:
        return None
    return f"{dag_href}?dag_run_id={quote(normalized_run_id, safe='')}"


def _status_label(value: Any) -> str:
    token = (_normalize_token(value) or "sem_execucao").lower()
    if token in {"success", "sucesso", "succeeded", "ok", "completed"}:
        return "Sucesso"
    if token in {"failed", "failure", "erro", "error"}:
        return "Falha"
    if token in {"running", "em_execucao", "em_andamento", "in_progress"}:
        return "Em execução"
    if token in {"queued", "scheduled", "pending", "pendente"}:
        return "Pendente"
    return "Sem execução"


def _load_type_label(value: Any) -> str | None:
    token = (_normalize_token(value) or "").lower()
    if not token:
        return None
    mapping = {
        "full_refresh": "Full refresh",
        "full": "Full refresh",
        "full_merge": "Full merge",
        "merge": "Full merge",
        "incremental": "Incremental",
        "incremental_ts": "Incremental por timestamp",
        "incremental_timestamp": "Incremental por timestamp",
        "incremental_by_timestamp": "Incremental por timestamp",
        "append": "Append only",
        "append_only": "Append only",
        "window_merge": "Window merge",
    }
    return mapping.get(token, token.replace("_", " ").title())


def _row_priority(row: dict[str, Any]) -> tuple[int, datetime, datetime, datetime]:
    status = (_normalize_token(_pick_value(row, LATEST_STATUS_CANDIDATES)) or "").lower()
    status_weight = 3 if status in {"running", "em_execucao", "em_andamento", "in_progress"} else 2 if status in {"success", "sucesso", "succeeded", "ok", "completed"} else 1 if status in {"failed", "failure", "erro", "error"} else 0
    last_success = _sortable_dt(_pick_value(row, LAST_SUCCESS_CANDIDATES))
    last_finish = _sortable_dt(_pick_value(row, LAST_FINISH_CANDIDATES))
    last_start = _sortable_dt(_pick_value(row, LAST_START_CANDIDATES))
    return (status_weight, last_success, last_finish, last_start)


def _serialize_pipeline_row(row: dict[str, Any], *, is_primary: bool, airflow_ui_base_url: str | None = None) -> dict[str, Any]:
    latest_status = _pick_value(row, LATEST_STATUS_CANDIDATES)
    latest_status_label = _status_label(latest_status)
    last_execution_started_at = _normalize_dt(_pick_value(row, LAST_START_CANDIDATES))
    last_execution_finished_at = _normalize_dt(_pick_value(row, LAST_FINISH_CANDIDATES))
    last_success_at = _normalize_dt(_pick_value(row, LAST_SUCCESS_CANDIDATES))
    if last_success_at is None and latest_status_label == "Sucesso":
        last_success_at = last_execution_finished_at or last_execution_started_at
    schema_name = _normalize_token(_pick_value(row, SCHEMA_NAME_CANDIDATES)) or _normalize_token(_pick_value(row, TARGET_SCHEMA_CANDIDATES))
    last_status = _normalize_token(_pick_value(row, ("last_status", "last_status_label", "status_ultima_execucao"))) or _normalize_token(latest_status)
    last_watermark = _normalize_token(_pick_value(row, WATERMARK_VALUE_CANDIDATES))
    records_processed = _normalize_int(_pick_value(row, ROWS_PROCESSED_CANDIDATES))
    return {
        "pipeline_id": _normalize_token(_pick_value(row, PIPELINE_ID_CANDIDATES)),
        "pipeline_name": _normalize_token(_pick_value(row, PIPELINE_NAME_CANDIDATES)),
        "dag_id": _normalize_token(_pick_value(row, DAG_ID_CANDIDATES)),
        "task_name": _normalize_token(_pick_value(row, TASK_NAME_CANDIDATES)),
        "load_type": _normalize_token(_pick_value(row, LOAD_TYPE_CANDIDATES)),
        "load_type_label": _load_type_label(_pick_value(row, LOAD_TYPE_CANDIDATES)),
        "source_connection": _normalize_token(_pick_value(row, SOURCE_CONNECTION_CANDIDATES)),
        "source_database": _normalize_token(_pick_value(row, SOURCE_DATABASE_CANDIDATES)),
        "source_table": _normalize_token(_pick_value(row, SOURCE_TABLE_CANDIDATES)),
        "schema_name": schema_name,
        "target_schema": _normalize_token(_pick_value(row, TARGET_SCHEMA_CANDIDATES)) or schema_name,
        "target_table": _normalize_token(_pick_value(row, TARGET_TABLE_CANDIDATES)),
        "latest_status": _normalize_token(latest_status),
        "latest_status_label": latest_status_label,
        "last_status": last_status,
        "watermark_value": last_watermark,
        "last_watermark": last_watermark,
        "watermark_column": _normalize_token(_pick_value(row, WATERMARK_COLUMN_CANDIDATES)),
        "watermark_type": _normalize_token(_pick_value(row, WATERMARK_TYPE_CANDIDATES)),
        "last_success_at": last_success_at,
        "last_execution_started_at": last_execution_started_at,
        "last_execution_finished_at": last_execution_finished_at,
        "last_run_started_at": last_execution_started_at,
        "last_run_finished_at": last_execution_finished_at,
        "last_failure_at": _normalize_dt(_pick_value(row, LAST_FAILURE_CANDIDATES)),
        "last_error": _normalize_token(_pick_value(row, LAST_ERROR_CANDIDATES)),
        "rows_processed": records_processed,
        "records_processed": records_processed,
        "observacao": _normalize_token(_pick_value(row, OBSERVACAO_CANDIDATES)),
        "pipeline_history_href": _pipeline_history_href(row),
        "airflow_dag_href": _airflow_dag_href(
            dag_id=_normalize_token(_pick_value(row, DAG_ID_CANDIDATES)),
            base_url=airflow_ui_base_url,
        ),
        "airflow_task_href": _airflow_task_href(
            dag_id=_normalize_token(_pick_value(row, DAG_ID_CANDIDATES)),
            task_name=_normalize_token(_pick_value(row, TASK_NAME_CANDIDATES)),
            base_url=airflow_ui_base_url,
        ),
        "is_primary": is_primary,
    }


def _is_stale_pipeline(payload: dict[str, Any], *, now: datetime, threshold_hours: int = STALE_SUCCESS_THRESHOLD_HOURS) -> bool:
    latest_status_label = _normalize_token(payload.get("latest_status_label"))
    if latest_status_label == "Em execução":
        return False
    last_success_at = payload.get("last_success_at")
    if not isinstance(last_success_at, datetime):
        return True
    success_dt = last_success_at if last_success_at.tzinfo is not None else last_success_at.replace(tzinfo=timezone.utc)
    return success_dt <= now - timedelta(hours=threshold_hours)


def _overview_focus_item(
    table_ref: dict[str, Any],
    *,
    payload: dict[str, Any] | None = None,
    hint: str | None = None,
) -> dict[str, Any]:
    payload_table_name = _normalize_token(payload.get("target_table")) if payload is not None else None
    payload_schema_name = _normalize_token(payload.get("schema_name") or payload.get("target_schema")) if payload is not None else None
    payload_table_fqn = _normalize_token(payload.get("table_fqn")) if payload is not None else None
    table_id = _normalize_int(table_ref.get("table_id")) if table_ref else None
    if table_id is None and payload is not None:
        table_id = _normalize_int(payload.get("table_id"))
    table_name = (
        _normalize_token(table_ref.get("table_name"))
        or payload_table_name
        or _normalize_token(payload.get("source_table") if payload is not None else None)
        or "Ativo sem nome"
    )
    table_fqn = (
        _normalize_token(table_ref.get("table_fqn"))
        or payload_table_fqn
        or (
            f"{payload_schema_name}.{table_name}"
            if payload_schema_name
            else table_name
        )
    )
    item = {
        "table_id": table_id,
        "table_name": table_name,
        "table_fqn": table_fqn,
        "schema_name": _normalize_token(table_ref.get("schema_name")) or payload_schema_name,
        "target_url": f"/explorer?tableId={table_id}" if table_id is not None else (f"/explorer?schema={payload_schema_name}&table={table_name}" if payload_schema_name else None),
        "hint": hint,
        "status_label": None,
        "last_success_at": None,
        "pipeline_history_href": None,
        "pipeline_name": None,
        "dag_id": None,
        "rows_processed": None,
    }
    if payload is not None:
        item["status_label"] = _normalize_token(payload.get("latest_status_label"))
        item["last_success_at"] = payload.get("last_success_at")
        item["pipeline_history_href"] = _normalize_token(payload.get("pipeline_history_href"))
        item["pipeline_name"] = _normalize_token(payload.get("pipeline_name"))
        item["dag_id"] = _normalize_token(payload.get("dag_id"))
        item["rows_processed"] = _normalize_int(payload.get("rows_processed"))
        item["load_type_label"] = _normalize_token(payload.get("load_type_label"))
    return item


def _load_summary_rows(session: Session, key: TableIngestionKey, column_map: ColumnMap) -> list[dict[str, Any]]:
    target_schema_column = _safe_identifier_or_none(
        _pick_column(column_map.summary_columns, TARGET_SCHEMA_CANDIDATES),
        label="summary target_schema",
    )
    target_table_column = _safe_identifier_or_none(
        _pick_column(column_map.summary_columns, TARGET_TABLE_CANDIDATES),
        label="summary target_table",
    )
    if not target_schema_column or not target_table_column:
        raise IngestionIntegrationUnavailable("A view operacional não expõe target_schema/target_table para vínculo com o catálogo.")
    summary_relation = safe_relation(SAFE_CONTROL_SCHEMA, column_map.summary_relation, label="summary relation")
    query = text(
        f"""
        select *
        from {summary_relation}
        where lower(cast({target_schema_column} as text)) = :target_schema
          and lower(cast({target_table_column} as text)) = :target_table
        """
    )
    return [dict(row) for row in session.execute(query, {"target_schema": key.schema_name.lower(), "target_table": key.table_name.lower()}).mappings().all()]


def load_table_ingestion_summary(
    session: Session,
    *,
    schema_name: str,
    table_name: str,
    airflow_ui_base_url: str | None = None,
) -> dict[str, Any]:
    key = TableIngestionKey(schema_name=schema_name, table_name=table_name)
    try:
        column_map = _load_column_map(session)
        rows = _load_summary_rows(session, key, column_map)
    except IngestionIntegrationUnavailable as exc:
        return {
            "linked": False,
            "state": "unavailable",
            "message": str(exc),
            "table_schema": schema_name,
            "table_name": table_name,
            "pipeline_count": 0,
            "primary_pipeline": None,
            "pipelines": [],
        }
    except SQLAlchemyError as exc:
        _safe_rollback(session, context="load_table_ingestion_summary")
        _record_ingestion_failure(
            session,
            exc=exc,
            source="ingestion.summary",
            schema_name=schema_name,
            table_name=table_name,
        )
        logger.warning(
            "ingestion summary query failed schema=%s table=%s error=%s",
            schema_name,
            table_name,
            exc,
        )
        return {
            "linked": False,
            "state": "unavailable",
            "message": "Não foi possível consultar a estrutura operacional de ingestão.",
            "table_schema": schema_name,
            "table_name": table_name,
            "pipeline_count": 0,
            "primary_pipeline": None,
            "pipelines": [],
        }

    if not rows:
        return {
            "linked": False,
            "state": "not_linked",
            "message": "Nenhum pipeline Airflow associado a esta tabela.",
            "table_schema": schema_name,
            "table_name": table_name,
            "pipeline_count": 0,
            "primary_pipeline": None,
            "pipelines": [],
        }

    ranked_rows = sorted(rows, key=_row_priority, reverse=True)
    pipelines = [
        _serialize_pipeline_row(row, is_primary=index == 0, airflow_ui_base_url=airflow_ui_base_url)
        for index, row in enumerate(ranked_rows)
    ]
    return {
        "linked": True,
        "state": "available",
        "message": None,
        "table_schema": schema_name,
        "table_name": table_name,
        "pipeline_count": len(pipelines),
        "primary_pipeline": pipelines[0],
        "pipelines": pipelines,
        }


def _degraded_summary_payload(*, schema_name: str, table_name: str, message: str) -> dict[str, Any]:
    return {
        "linked": False,
        "state": "unavailable",
        "message": message,
        "table_schema": schema_name,
        "table_name": table_name,
        "pipeline_count": 0,
        "primary_pipeline": None,
        "pipelines": [],
    }


def _degraded_detail_payload(*, schema_name: str, table_name: str, message: str, page: int, page_size: int) -> dict[str, Any]:
    summary = _degraded_summary_payload(schema_name=schema_name, table_name=table_name, message=message)
    executions = {
        "linked": False,
        "state": "unavailable",
        "message": message,
        "table_schema": schema_name,
        "table_name": table_name,
        "page": page,
        "page_size": page_size,
        "total": 0,
        "items": [],
    }
    return {
        "summary": summary,
        "executions": executions,
        "stability": None,
        "history": [],
    }


def _or_text_equals(column_name: str, values: list[str], prefix: str) -> tuple[str | None, dict[str, Any]]:
    unique_values = [value for value in dict.fromkeys(values) if value]
    if not unique_values:
        return None, {}
    clauses: list[str] = []
    params: dict[str, Any] = {}
    for index, value in enumerate(unique_values):
        param_name = f"{prefix}_{index}"
        clauses.append(f"cast({column_name} as text) = :{param_name}")
        params[param_name] = value
    return f"({' OR '.join(clauses)})", params


def list_table_ingestion_executions(
    session: Session,
    *,
    schema_name: str,
    table_name: str,
    page: int,
    page_size: int,
    airflow_ui_base_url: str | None = None,
) -> dict[str, Any]:
    key = TableIngestionKey(schema_name=schema_name, table_name=table_name)
    try:
        column_map = _load_column_map(session)
        summary_rows = _load_summary_rows(session, key, column_map)
    except IngestionIntegrationUnavailable:
        raise
    except SQLAlchemyError as exc:
        _safe_rollback(session, context="list_table_ingestion_executions")
        logger.warning("ingestion executions query failed schema=%s table=%s error=%s", schema_name, table_name, exc)
        return {
            "linked": False,
            "state": "unavailable",
            "message": "Não foi possível consultar a estrutura operacional de ingestão.",
            "table_schema": schema_name,
            "table_name": table_name,
            "page": page,
            "page_size": page_size,
            "total": 0,
            "items": [],
        }
    if not summary_rows:
        return {
            "linked": False,
            "state": "not_linked",
            "message": "Nenhum pipeline Airflow associado a esta tabela.",
            "table_schema": schema_name,
            "table_name": table_name,
            "page": page,
            "page_size": page_size,
            "total": 0,
            "items": [],
        }
    if not column_map.execution_relation or not column_map.execution_columns:
        return {
            "linked": True,
            "state": "available",
            "message": "O histórico operacional ainda não está disponível neste ambiente.",
            "table_schema": schema_name,
            "table_name": table_name,
            "page": page,
            "page_size": page_size,
            "total": 0,
            "items": [],
        }

    pipeline_id_column = _safe_identifier_or_none(
        _pick_column(column_map.execution_columns, PIPELINE_ID_CANDIDATES),
        label="execution pipeline_id",
    )
    dag_id_column = _safe_identifier_or_none(
        _pick_column(column_map.execution_columns, DAG_ID_CANDIDATES),
        label="execution dag_id",
    )
    pipeline_name_column = _safe_identifier_or_none(
        _pick_column(column_map.execution_columns, PIPELINE_NAME_CANDIDATES),
        label="execution pipeline_name",
    )
    order_column = _safe_identifier_or_none(
        _pick_column(column_map.execution_columns, EXECUTION_FINISH_CANDIDATES)
        or _pick_column(column_map.execution_columns, EXECUTION_START_CANDIDATES)
        or _pick_column(column_map.execution_columns, EXECUTION_ID_CANDIDATES),
        label="execution order",
    )
    if not order_column:
        raise IngestionIntegrationUnavailable("A tabela de execuções não possui coluna suficiente para ordenação operacional.")

    filter_clauses: list[str] = []
    params: dict[str, Any] = {}
    pipeline_ids = [_normalize_token(_pick_value(row, PIPELINE_ID_CANDIDATES)) for row in summary_rows]
    dag_ids = [_normalize_token(_pick_value(row, DAG_ID_CANDIDATES)) for row in summary_rows]
    pipeline_names = [_normalize_token(_pick_value(row, PIPELINE_NAME_CANDIDATES)) for row in summary_rows]
    clause, clause_params = _or_text_equals(pipeline_id_column, [value for value in pipeline_ids if value], "pipeline_id") if pipeline_id_column else (None, {})
    if clause:
        filter_clauses.append(clause)
        params.update(clause_params)
    clause, clause_params = _or_text_equals(dag_id_column, [value for value in dag_ids if value], "dag_id") if dag_id_column else (None, {})
    if clause:
        filter_clauses.append(clause)
        params.update(clause_params)
    clause, clause_params = _or_text_equals(pipeline_name_column, [value for value in pipeline_names if value], "pipeline_name") if pipeline_name_column else (None, {})
    if clause:
        filter_clauses.append(clause)
        params.update(clause_params)
    if not filter_clauses:
        return {
            "linked": True,
            "state": "available",
            "message": "Há pipeline vinculado, mas não foi possível localizar o vínculo técnico na tabela de execuções.",
            "table_schema": schema_name,
            "table_name": table_name,
            "page": page,
            "page_size": page_size,
            "total": 0,
            "items": [],
        }

    where_clause = " OR ".join(filter_clauses)
    execution_relation = safe_relation(SAFE_CONTROL_SCHEMA, column_map.execution_relation, label="execution relation")
    count_query = text(f"select count(*) from {execution_relation} where {where_clause}")
    total = int(session.execute(count_query, params).scalar_one() or 0)

    execution_id_column = _safe_identifier_or_none(
        _pick_column(column_map.execution_columns, EXECUTION_ID_CANDIDATES),
        label="execution id",
    )
    if not execution_id_column:
        raise IngestionIntegrationUnavailable("A tabela de execuções não possui identificador da execução.")
    query = text(
        f"""
        select *
        from {execution_relation}
        where {where_clause}
        order by {order_column} desc nulls last
        limit :limit_value offset :offset_value
        """
    )
    rows = session.execute(
        query,
        {**params, "limit_value": page_size, "offset_value": (page - 1) * page_size},
    ).mappings().all()
    items = []
    for row in rows:
        row_dict = dict(row)
        started_at = _normalize_dt(_pick_value(row_dict, EXECUTION_START_CANDIDATES))
        finished_at = _normalize_dt(_pick_value(row_dict, EXECUTION_FINISH_CANDIDATES))
        duration_seconds = _normalize_int(_pick_value(row_dict, EXECUTION_DURATION_CANDIDATES))
        if duration_seconds is None and started_at and finished_at:
            duration_seconds = max(int((finished_at - started_at).total_seconds()), 0)
        items.append(
            {
                "execution_id": _normalize_token(row_dict.get(execution_id_column)) or "-",
                "pipeline_id": _normalize_token(_pick_value(row_dict, PIPELINE_ID_CANDIDATES)),
                "pipeline_name": _normalize_token(_pick_value(row_dict, PIPELINE_NAME_CANDIDATES)),
                "dag_id": _normalize_token(_pick_value(row_dict, DAG_ID_CANDIDATES)),
                "airflow_dag_href": _airflow_dag_href(
                    dag_id=_normalize_token(_pick_value(row_dict, DAG_ID_CANDIDATES)),
                    base_url=airflow_ui_base_url,
                ),
                "airflow_run_href": _airflow_run_href(
                    dag_id=_normalize_token(_pick_value(row_dict, DAG_ID_CANDIDATES)),
                    run_id=_normalize_token(_pick_value(row_dict, EXECUTION_RUN_ID_CANDIDATES)) or _normalize_token(row_dict.get(execution_id_column)),
                    base_url=airflow_ui_base_url,
                ),
                "status": _normalize_token(_pick_value(row_dict, EXECUTION_STATUS_CANDIDATES)),
                "status_label": _status_label(_pick_value(row_dict, EXECUTION_STATUS_CANDIDATES)),
                "started_at": started_at,
                "finished_at": finished_at,
                "duration_seconds": duration_seconds,
                "rows_extracted": _normalize_int(_pick_value(row_dict, ROWS_EXTRACTED_CANDIDATES)),
                "rows_written": _normalize_int(_pick_value(row_dict, ROWS_WRITTEN_CANDIDATES)),
                "rows_upserted": _normalize_int(_pick_value(row_dict, ROWS_UPSERTED_CANDIDATES)),
                "watermark_before": _normalize_token(_pick_value(row_dict, WATERMARK_BEFORE_CANDIDATES)),
                "watermark_after": _normalize_token(_pick_value(row_dict, WATERMARK_AFTER_CANDIDATES)),
                "error_message": _normalize_token(_pick_value(row_dict, EXECUTION_ERROR_CANDIDATES)),
            }
        )

    return {
        "linked": True,
        "state": "available",
        "message": None,
        "table_schema": schema_name,
        "table_name": table_name,
        "page": page,
        "page_size": page_size,
        "total": total,
        "items": items,
    }


def list_table_ingestion_history(
    session: Session,
    *,
    schema_name: str,
    table_name: str,
    limit: int = 24,
) -> list[dict[str, Any]]:
    executions_payload = list_table_ingestion_executions(
        session,
        schema_name=schema_name,
        table_name=table_name,
        page=1,
        page_size=max(min(int(limit), 240), 1),
    )
    items = list(executions_payload.get("items", []))
    history: list[dict[str, Any]] = []
    consecutive_failures = 0
    for execution in items:
        status_label = _normalize_token(execution.get("status_label")) or "Sem execução"
        if status_label == "Falha":
            consecutive_failures += 1
        elif status_label == "Sucesso":
            consecutive_failures = 0
        bucket_start_at = execution.get("finished_at") or execution.get("started_at")
        rows_processed = (
            _normalize_int(execution.get("rows_written"))
            or _normalize_int(execution.get("rows_upserted"))
            or _normalize_int(execution.get("rows_extracted"))
        )
        success = status_label == "Sucesso"
        history.append(
            {
                "bucket_start_at": bucket_start_at,
                "pipeline_name": execution.get("pipeline_name"),
                "dag_id": execution.get("dag_id"),
                "task_name": None,
                "latest_status_label": status_label,
                "rows_processed": rows_processed,
                "last_success_at": execution.get("finished_at") if success else None,
                "last_execution_finished_at": execution.get("finished_at"),
                "window_runs": 1,
                "success_rate_pct": 100.0 if success else 0.0,
                "failed_runs": 1 if status_label == "Falha" else 0,
                "recurrent_degradation": consecutive_failures >= 2,
                "currently_stale": False,
            }
        )
    return history


def list_execution_logs(session: Session, *, execution_id: str, page: int, page_size: int) -> dict[str, Any]:
    column_map = _load_column_map(session)
    if not column_map.log_relation or not column_map.log_columns:
        return {
            "execution_id": execution_id,
            "page": page,
            "page_size": page_size,
            "total": 0,
            "items": [],
        }
    execution_fk_column = _safe_identifier_or_none(
        _pick_column(column_map.log_columns, LOG_EXECUTION_ID_CANDIDATES),
        label="log execution_id",
    )
    log_id_column = _safe_identifier_or_none(
        _pick_column(column_map.log_columns, LOG_ID_CANDIDATES),
        label="log id",
    )
    order_column = _safe_identifier_or_none(
        _pick_column(column_map.log_columns, LOG_TS_CANDIDATES) or log_id_column,
        label="log order",
    )
    if not execution_fk_column or not log_id_column:
        raise IngestionIntegrationUnavailable("A tabela de logs não possui vínculo suficiente com a execução operacional.")
    log_relation = safe_relation(SAFE_CONTROL_SCHEMA, column_map.log_relation, label="log relation")
    count_query = text(
        f"select count(*) from {log_relation} where cast({execution_fk_column} as text) = :execution_id"
    )
    total = int(session.execute(count_query, {"execution_id": execution_id}).scalar_one() or 0)
    query = text(
        f"""
        select *
        from {log_relation}
        where cast({execution_fk_column} as text) = :execution_id
        order by {order_column} asc nulls last
        limit :limit_value offset :offset_value
        """
    )
    rows = session.execute(
        query,
        {"execution_id": execution_id, "limit_value": page_size, "offset_value": (page - 1) * page_size},
    ).mappings().all()
    items = []
    for row in rows:
        row_dict = dict(row)
        items.append(
            {
                "log_id": _normalize_token(row_dict.get(log_id_column)) or "-",
                "execution_id": execution_id,
                "occurred_at": _normalize_dt(_pick_value(row_dict, LOG_TS_CANDIDATES)),
                "step": _normalize_token(_pick_value(row_dict, LOG_STEP_CANDIDATES)),
                "level": _normalize_token(_pick_value(row_dict, LOG_LEVEL_CANDIDATES)),
                "message": _normalize_token(_pick_value(row_dict, LOG_MESSAGE_CANDIDATES)),
                "stacktrace": _normalize_token(_pick_value(row_dict, LOG_STACKTRACE_CANDIDATES)),
            }
        )
    return {
        "execution_id": execution_id,
        "page": page,
        "page_size": page_size,
        "total": total,
        "items": items,
    }


def _build_stability_summary(
    executions: list[dict[str, Any]],
    *,
    primary_pipeline: dict[str, Any] | None,
    window: int = 10,
) -> dict[str, Any] | None:
    if not executions and not primary_pipeline:
        return None
    points = []
    window_items = list(executions[:window])
    success_count = 0
    failed_runs = 0
    consecutive_failures = 0
    for execution in window_items:
        status_label = str(execution.get("status_label") or "Sem execução")
        success = status_label == "Sucesso"
        if success:
            success_count += 1
            consecutive_failures = 0
        elif status_label == "Falha":
            failed_runs += 1
            consecutive_failures += 1
        points.append(
            {
                "execution_id": str(execution.get("execution_id") or "-"),
                "occurred_at": execution.get("finished_at") or execution.get("started_at"),
                "status_label": status_label,
                "success": success,
                "rows_written": execution.get("rows_written"),
            }
        )
    current_status_label = _normalize_token(primary_pipeline.get("latest_status_label")) if isinstance(primary_pipeline, dict) else None
    current_stale = False
    if isinstance(primary_pipeline, dict):
        last_success_at = _normalize_dt(primary_pipeline.get("last_success_at"))
        if current_status_label != "Em execução":
            current_stale = last_success_at is None or last_success_at <= datetime.now(timezone.utc) - timedelta(hours=STALE_SUCCESS_THRESHOLD_HOURS)
    recurrent_degradation = consecutive_failures >= 2 or (failed_runs >= 2) or (current_stale and failed_runs >= 1)
    window_runs = len(window_items)
    return {
        "window_runs": window_runs,
        "success_rate_pct": round((success_count / window_runs) * 100, 1) if window_runs else 0.0,
        "failed_runs": failed_runs,
        "recurrent_degradation": recurrent_degradation,
        "currently_stale": current_stale,
        "current_status_label": current_status_label,
        "points": points,
    }


def load_table_ingestion_detail(
    session: Session,
    *,
    schema_name: str,
    table_name: str,
    page: int,
    page_size: int,
    airflow_ui_base_url: str | None = None,
) -> dict[str, Any]:
    summary = load_table_ingestion_summary(
        session,
        schema_name=schema_name,
        table_name=table_name,
        airflow_ui_base_url=airflow_ui_base_url,
    )
    if summary.get("state") != "available":
        return {
            "summary": summary,
            "executions": {
                "linked": bool(summary.get("linked")),
                "state": summary.get("state") or "unavailable",
                "message": summary.get("message") or "A camada operacional de ingestão ainda não está disponível neste ambiente.",
                "table_schema": schema_name,
                "table_name": table_name,
                "page": page,
                "page_size": page_size,
                "total": 0,
                "items": [],
            },
            "stability": None,
            "history": [],
        }
    try:
        executions = list_table_ingestion_executions(
            session,
            schema_name=schema_name,
            table_name=table_name,
            page=page,
            page_size=page_size,
            airflow_ui_base_url=airflow_ui_base_url,
        )
        history = list_table_ingestion_history(
            session,
            schema_name=schema_name,
            table_name=table_name,
            limit=max(page_size, 24),
        )
    except IngestionIntegrationUnavailable:
        executions = {
            "linked": False,
            "state": "unavailable",
            "message": summary.get("message") or "A camada operacional de ingestão ainda não está disponível neste ambiente.",
            "table_schema": schema_name,
            "table_name": table_name,
            "page": page,
            "page_size": page_size,
            "total": 0,
            "items": [],
        }
        history = []
    except SQLAlchemyError as exc:
        _safe_rollback(session, context="load_table_ingestion_detail")
        logger.warning("ingestion detail query failed schema=%s table=%s error=%s", schema_name, table_name, exc)
        return _degraded_detail_payload(
            schema_name=schema_name,
            table_name=table_name,
            message="Não foi possível consultar a estrutura operacional de ingestão.",
            page=page,
            page_size=page_size,
        )
    stability = _build_stability_summary(executions.get("items", []), primary_pipeline=summary.get("primary_pipeline"))
    return {"summary": summary, "executions": executions, "stability": stability, "history": history}


def load_table_ingestion_summary_from_source(
    bind_session: Session,
    *,
    schema_name: str,
    table_name: str,
    airflow_ui_base_url: str | None = None,
) -> dict[str, Any]:
    from t2c_data.features.ingestion.runtime import operational_session

    try:
        with operational_session(bind_session) as operational_db:
            return load_table_ingestion_summary(
                operational_db,
                schema_name=schema_name,
                table_name=table_name,
                airflow_ui_base_url=airflow_ui_base_url,
            )
    except IngestionIntegrationUnavailable as exc:
        _safe_rollback(bind_session, context="load_table_ingestion_summary_from_source")
        _record_ingestion_failure(
            bind_session,
            exc=exc,
            source="ingestion.summary.operational_source",
            schema_name=schema_name,
            table_name=table_name,
            context={"flow": "summary"},
        )
        logger.warning("ingestion summary source unavailable schema=%s table=%s error=%s", schema_name, table_name, exc)
        return _degraded_summary_payload(schema_name=schema_name, table_name=table_name, message=str(exc))


def load_table_ingestion_detail_from_source(
    bind_session: Session,
    *,
    schema_name: str,
    table_name: str,
    page: int,
    page_size: int,
    airflow_ui_base_url: str | None = None,
) -> dict[str, Any]:
    from t2c_data.features.ingestion.runtime import operational_session

    try:
        with operational_session(bind_session) as operational_db:
            return load_table_ingestion_detail(
                operational_db,
                schema_name=schema_name,
                table_name=table_name,
                page=page,
                page_size=page_size,
                airflow_ui_base_url=airflow_ui_base_url,
            )
    except IngestionIntegrationUnavailable as exc:
        _safe_rollback(bind_session, context="load_table_ingestion_detail_from_source")
        _record_ingestion_failure(
            bind_session,
            exc=exc,
            source="ingestion.detail.operational_source",
            schema_name=schema_name,
            table_name=table_name,
            context={"flow": "detail"},
        )
        logger.warning("ingestion detail source unavailable schema=%s table=%s error=%s", schema_name, table_name, exc)
        return _degraded_detail_payload(
            schema_name=schema_name,
            table_name=table_name,
            message=str(exc),
            page=page,
            page_size=page_size,
        )


def list_table_ingestion_executions_from_source(
    bind_session: Session,
    *,
    schema_name: str,
    table_name: str,
    page: int,
    page_size: int,
    airflow_ui_base_url: str | None = None,
) -> dict[str, Any]:
    from t2c_data.features.ingestion.runtime import operational_session

    try:
        with operational_session(bind_session) as operational_db:
            return list_table_ingestion_executions(
                operational_db,
                schema_name=schema_name,
                table_name=table_name,
                page=page,
                page_size=page_size,
                airflow_ui_base_url=airflow_ui_base_url,
            )
    except IngestionIntegrationUnavailable as exc:
        _safe_rollback(bind_session, context="list_table_ingestion_executions_from_source")
        _record_ingestion_failure(
            bind_session,
            exc=exc,
            source="ingestion.executions.operational_source",
            schema_name=schema_name,
            table_name=table_name,
            context={"flow": "executions"},
        )
        logger.warning("ingestion executions source unavailable schema=%s table=%s error=%s", schema_name, table_name, exc)
        return {
            "linked": False,
            "state": "unavailable",
            "message": str(exc),
            "table_schema": schema_name,
            "table_name": table_name,
            "page": page,
            "page_size": page_size,
            "total": 0,
            "items": [],
        }


def list_execution_logs_from_source(
    bind_session: Session,
    *,
    execution_id: str,
    page: int,
    page_size: int,
) -> dict[str, Any]:
    from t2c_data.features.ingestion.runtime import operational_session

    try:
        with operational_session(bind_session) as operational_db:
            return list_execution_logs(
                operational_db,
                execution_id=execution_id,
                page=page,
                page_size=page_size,
            )
    except IngestionIntegrationUnavailable as exc:
        _safe_rollback(bind_session, context="list_execution_logs_from_source")
        _record_ingestion_failure(
            bind_session,
            exc=exc,
            source="ingestion.logs.operational_source",
            context={"flow": "logs", "execution_id": execution_id},
        )
        logger.warning("ingestion logs source unavailable execution_id=%s error=%s", execution_id, exc)
        return {
            "execution_id": execution_id,
            "page": page,
            "page_size": page_size,
            "total": 0,
            "items": [],
        }


def load_ingestion_operational_overview(
    session: Session,
    *,
    table_refs: list[dict[str, Any]] | None = None,
    limit: int = 8,
    high_volume_threshold_rows: int = HIGH_VOLUME_ROWS_THRESHOLD,
    stale_threshold_hours: int = STALE_SUCCESS_THRESHOLD_HOURS,
    airflow_ui_base_url: str | None = None,
) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    try:
        column_map = _load_column_map(session)
    except IngestionIntegrationUnavailable as exc:
        return {
            "available": False,
            "message": str(exc),
            "generated_at": now,
            "pipelines_total": 0,
            "linked_tables": 0,
            "unmapped": 0,
            "degraded": 0,
            "failed": 0,
            "running": 0,
            "pending": 0,
            "stale": 0,
            "critical_stale": 0,
            "stale_threshold_hours": max(int(stale_threshold_hours or STALE_SUCCESS_THRESHOLD_HOURS), 1),
            "items": [],
        }
    except SQLAlchemyError as exc:
        _safe_rollback(session, context="load_ingestion_operational_overview")
        _record_ingestion_failure(
            session,
            exc=exc,
            source="ingestion.overview",
        )
        logger.warning("ingestion overview query failed error=%s", exc)
        return {
            "available": False,
            "message": "Não foi possível consultar a camada operacional de ingestão.",
            "generated_at": now,
            "pipelines_total": 0,
            "linked_tables": 0,
            "unmapped": 0,
            "degraded": 0,
            "failed": 0,
            "running": 0,
            "pending": 0,
            "stale": 0,
            "critical_stale": 0,
            "stale_threshold_hours": max(int(stale_threshold_hours or STALE_SUCCESS_THRESHOLD_HOURS), 1),
            "items": [],
        }

    try:
        summary_relation = safe_relation(SAFE_CONTROL_SCHEMA, column_map.summary_relation, label="summary relation")
        summary_rows = [dict(row) for row in session.execute(text(f"select * from {summary_relation}")).mappings().all()]
        if summary_rows:
            return _build_ingestion_overview_from_rows(
                summary_rows,
                table_refs=table_refs,
                limit=limit,
                high_volume_threshold_rows=high_volume_threshold_rows,
                stale_threshold_hours=stale_threshold_hours,
                airflow_ui_base_url=airflow_ui_base_url,
                now=now,
            )
    except SQLAlchemyError as exc:
        _safe_rollback(session, context="load_ingestion_overview_summary_rows")
        _record_ingestion_failure(
            session,
            exc=exc,
            source="ingestion.overview.summary_rows",
        )
        logger.warning("ingestion overview summary query failed error=%s", exc)

    try:
        snapshot_rows = _load_snapshot_rows(session)
        if snapshot_rows:
            return _build_ingestion_overview_from_rows(
                snapshot_rows,
                table_refs=table_refs,
                limit=limit,
                high_volume_threshold_rows=high_volume_threshold_rows,
                stale_threshold_hours=stale_threshold_hours,
                airflow_ui_base_url=airflow_ui_base_url,
                now=now,
            )
    except SQLAlchemyError as exc:
        _safe_rollback(session, context="load_ingestion_overview_snapshot_rows")
        _record_ingestion_failure(
            session,
            exc=exc,
            source="ingestion.overview.snapshots",
        )
        logger.warning("ingestion overview snapshot query failed error=%s", exc)

    try:
        raw_pipeline_rows = _load_raw_pipeline_rows(session)
        if raw_pipeline_rows:
            return _build_ingestion_overview_from_rows(
                raw_pipeline_rows,
                table_refs=table_refs,
                limit=limit,
                high_volume_threshold_rows=high_volume_threshold_rows,
                stale_threshold_hours=stale_threshold_hours,
                airflow_ui_base_url=airflow_ui_base_url,
                now=now,
            )
    except SQLAlchemyError as exc:
        _safe_rollback(session, context="load_ingestion_overview_raw_pipeline_rows")
        _record_ingestion_failure(
            session,
            exc=exc,
            source="ingestion.overview.raw_rows",
        )
        logger.warning("ingestion overview raw pipeline query failed error=%s", exc)

    return _empty_overview_payload(
        now=now,
        message=f"Não há dados operacionais de ingestão disponíveis no schema {CONTROL_SCHEMA}.",
    )


def load_ingestion_operational_overview_from_source(
    bind_session: Session,
    *,
    table_refs: list[dict[str, Any]] | None = None,
    limit: int = 8,
    high_volume_threshold_rows: int = HIGH_VOLUME_ROWS_THRESHOLD,
    stale_threshold_hours: int = STALE_SUCCESS_THRESHOLD_HOURS,
    airflow_ui_base_url: str | None = None,
) -> dict[str, Any]:
    from t2c_data.features.ingestion.runtime import operational_session

    now = datetime.now(timezone.utc)
    try:
        with operational_session(bind_session) as operational_db:
            return load_ingestion_operational_overview(
                operational_db,
                table_refs=table_refs,
                limit=limit,
                high_volume_threshold_rows=high_volume_threshold_rows,
                stale_threshold_hours=stale_threshold_hours,
                airflow_ui_base_url=airflow_ui_base_url,
            )
    except IngestionIntegrationUnavailable as exc:
        return {
            "available": False,
            "message": str(exc),
            "generated_at": now,
            "pipelines_total": 0,
            "linked_tables": 0,
            "unmapped": 0,
            "degraded": 0,
            "failed": 0,
            "running": 0,
            "pending": 0,
            "stale": 0,
            "critical_stale": 0,
            "high_volume_failed": 0,
            "high_volume_failed_threshold_rows": max(int(high_volume_threshold_rows or HIGH_VOLUME_ROWS_THRESHOLD), 1),
            "stale_threshold_hours": max(int(stale_threshold_hours or STALE_SUCCESS_THRESHOLD_HOURS), 1),
            "items": [],
            "unmapped_items": [],
            "degraded_items": [],
            "failed_items": [],
            "critical_stale_items": [],
            "high_volume_failed_items": [],
        }
