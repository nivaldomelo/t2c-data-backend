from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from t2c_data.core.redaction import redact_value


def _normalize_status(value: str | None) -> str:
    return (value or "").strip().lower()


def _normalize_source(value: str | None) -> str:
    return (value or "").strip().lower()


def _coerce_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)
    return None


def _job_context(job: Any) -> dict[str, Any]:
    context = getattr(job, "context_json", None)
    return context if isinstance(context, dict) else {}


def _job_result_summary(job: Any) -> dict[str, Any]:
    summary = getattr(job, "result_summary_json", None)
    return summary if isinstance(summary, dict) else {}


def _format_duration_label(hours: float) -> str:
    if hours >= 24:
        days = max(1, int(hours // 24))
        return f"Travado há {days} dia{'s' if days > 1 else ''}"
    return "Possivelmente travado"


def _diagnostic_module(source: str) -> str:
    return {
        "datasource": "datasource_scan",
        "s3": "data_lake",
        "dq": "data_quality",
        "metabase": "metabase",
        "airflow": "airflow",
        "platform": "platform_maintenance",
    }.get(source, source or "platform")


def _runbook_path(source: str, status: str, probable_cause_code: str) -> str | None:
    if source == "datasource":
        if status == "partial_success":
            return "/docs/runbooks/scan-partial.md"
        if probable_cause_code == "row_count_timeout":
            return "/docs/runbooks/row-count-timeout.md"
        return "/docs/runbooks/scan-failed.md"
    if source == "s3":
        return "/docs/runbooks/data-lake-scan-failed.md"
    if source == "dq":
        return "/docs/runbooks/dq-failed.md"
    if source == "metabase":
        return "/docs/runbooks/metabase-sync-alert.md"
    if source == "airflow":
        return "/docs/runbooks/airflow-disconnected.md"
    if source == "platform":
        return "/docs/runbooks/platform-maintenance-failed.md"
    return None


def _build_evidence(job: Any, *, context: dict[str, Any], running_duration_seconds: int | None) -> str | None:
    parts: list[str] = []
    error_code = context.get("error_code")
    if error_code:
        parts.append(f"error_code={error_code}")
    if context.get("datasource_id") is not None:
        parts.append(f"datasource_id={context.get('datasource_id')}")
    if context.get("scan_run_id") is not None:
        parts.append(f"scan_run_id={context.get('scan_run_id')}")
    if context.get("schedule_id") is not None:
        parts.append(f"schedule_id={context.get('schedule_id')}")
    if context.get("scheduler_mode") is not None:
        parts.append(f"scheduler_mode={context.get('scheduler_mode')}")
    if context.get("failure_stage") is not None:
        parts.append(f"failure_stage={context.get('failure_stage')}")
    if context.get("spark_app_id") is not None:
        parts.append(f"spark_app_id={context.get('spark_app_id')}")
    if context.get("spark_driver_id") is not None:
        parts.append(f"spark_driver_id={context.get('spark_driver_id')}")
    row_counts = context.get("row_counts")
    if isinstance(row_counts, dict):
        failed = row_counts.get("failed")
        success = row_counts.get("success")
        if failed is not None or success is not None:
            parts.append(f"row_counts.failed={failed or 0}")
            parts.append(f"row_counts.success={success or 0}")
    discovery = context.get("discovery")
    if isinstance(discovery, dict):
        tables = discovery.get("tables")
        columns = discovery.get("columns")
        if tables is not None or columns is not None:
            parts.append(f"discovery.tables={tables or 0}")
            parts.append(f"discovery.columns={columns or 0}")
    if running_duration_seconds is not None and _normalize_status(getattr(job, "status", None)) == "running":
        parts.append(f"running_for_seconds={running_duration_seconds}")
    if getattr(job, "error", None):
        parts.append(f"error={str(redact_value(getattr(job, 'error')))[:180]}")
    elif isinstance(context.get("error"), str) and context.get("error"):
        parts.append(f"error={str(redact_value(context.get('error')))[:180]}")
    return " | ".join(parts) if parts else None


def _infer_probable_cause(job: Any, *, status: str, running_duration_seconds: int | None) -> tuple[str, str, str]:
    source = _normalize_source(getattr(job, "source", None))
    context = _job_context(job)
    error_code = str(context.get("error_code") or "").strip().lower()
    job_type = _normalize_status(getattr(job, "job_type", None))

    if status == "running" and running_duration_seconds is not None and running_duration_seconds > 0:
        if running_duration_seconds >= 24 * 3600:
            return (
                "stalled_execution",
                "Possível lock, worker sem heartbeat ou execução travada além da janela crítica.",
                "Revisar locks, worker dedicado, heartbeat e cancelar a execução com segurança se necessário.",
            )
        return (
            "long_running_execution",
            "Execução mais longa do que o padrão esperado para este job.",
            "Revisar progresso do worker, checkpoints e dependências externas antes de reprocessar.",
        )

    if source == "datasource":
        if status == "partial_success":
            row_counts = context.get("row_counts")
            if isinstance(row_counts, dict) and int(row_counts.get("failed") or 0) > 0:
                return (
                    "row_count_timeout",
                    "Parte do scan concluiu, mas houve falhas de row count ou tabelas inacessíveis.",
                    "Revisar tabelas com falha, reduzir custo de row count exato e validar timeouts do conector.",
                )
            return (
                "partial_scan",
                "O scan encontrou objetos válidos, mas não conseguiu concluir todo o inventário da fonte.",
                "Abrir o histórico do scan, revisar tabelas ignoradas e validar permissões da fonte.",
            )
        mapping = {
            "invalid_credentials": (
                "invalid_credentials",
                "Credenciais inválidas, expiradas ou revogadas para a fonte de dados.",
                "Testar conexão, revisar usuário/senha e validar rotação de segredo.",
            ),
            "invalid_host": (
                "invalid_host",
                "Host ou DNS da fonte de dados não está acessível a partir do runtime.",
                "Validar hostname, rede, DNS e firewall entre a aplicação e a fonte.",
            ),
            "timeout": (
                "timeout",
                "Timeout durante descoberta de schemas, tabelas ou row count.",
                "Rever timeouts, reduzir escopo do scan e evitar row count exato em fontes grandes.",
            ),
            "permission_denied": (
                "permission_denied",
                "Permissões insuficientes para listar ou consultar objetos da fonte.",
                "Revisar grants de leitura e acesso ao schema/tabela no banco de origem.",
            ),
            "scan_failed": (
                "scan_failed",
                "Falha operacional do conector ou objeto inacessível durante o scan.",
                "Inspecionar logs do conector, histórico do scan e testar a conexão novamente.",
            ),
        }
        if error_code in mapping:
            return mapping[error_code]
        return (
            "datasource_scan_failed",
            "Falha de scan sem classificação fina no conector ou na fonte de dados.",
            "Revisar logs do scan, testar conexão e validar permissões do schema alvo.",
        )

    if source == "s3":
        return (
            "data_lake_scan_failed",
            "Falha de acesso ao bucket, prefixo ou credencial AWS durante o inventário do Data Lake.",
            "Validar bucket, region, policy IAM, credencial e resposta do endpoint S3.",
        )

    if source == "metabase":
        return (
            "metabase_sync_failed",
            "Falha de autenticação, timeout ou indisponibilidade no sync com Metabase.",
            "Testar a conexão, revisar token/base URL e consultar o runbook de sync do Metabase.",
        )

    if source == "airflow":
        return (
            "airflow_disconnected",
            "Airflow indisponível, sem autenticação válida ou sem resposta do scheduler.",
            "Verificar saúde do Airflow, credenciais e conectividade com o endpoint configurado.",
        )

    if source == "platform":
        return (
            "platform_maintenance_failed",
            "Falha no ciclo de manutenção da plataforma, incluindo read models, automações ou limpeza operacional.",
            "Revisar scheduler da plataforma, worker dedicado, read models e logs do ciclo de manutenção.",
        )

    if source == "dq":
        if "profiling" in job_type:
            return (
                "dq_profiling_failed",
                "Falha no profiling DQ ou dependência indisponível durante o agendamento.",
                "Revisar scheduler de profiling, tabela alvo e logs do Spark/profiling.",
            )
        return (
            "dq_spark_failed",
            "Falha no submit do Spark, indisponibilidade do cluster ou erro durante o scheduler DQ.",
            "Revisar worker/scheduler DQ, Spark submit, cluster e logs do job de qualidade.",
        )

    return (
        "job_failed",
        "Falha operacional sem classificação específica para este módulo.",
        "Abrir histórico, correlacionar com logs e revisar a dependência externa mais próxima.",
    )


def _diagnostic_impact(source: str, status: str, probable_cause_code: str) -> str:
    if source == "datasource":
        if status == "partial_success" or probable_cause_code == "row_count_timeout":
            return "Parte do catálogo da fonte pode ficar incompleta ou desatualizada."
        return "O catálogo da fonte e os sinais associados podem ficar desatualizados."
    if source == "s3":
        return "O inventário do Data Lake pode ficar incompleto ou atrasado."
    if source == "dq":
        return "As regras e os incidentes de Data Quality podem atrasar a percepção de risco."
    if source == "metabase":
        return "A camada analítica pode exibir dados desatualizados ou incompletos."
    if source == "airflow":
        return "A leitura operacional de ingestão pode ficar cega até a reconciliação do scheduler."
    if source == "platform":
        return "Read models, automações e limpeza operacional podem ficar defasados."
    return "A execução operacional e os sinais associados podem ficar incompletos ou atrasados."


def _base_payload(
    *,
    job: Any,
    current_time: datetime,
    running_duration_seconds: int | None,
    is_overdue_next_run: bool,
    status_value: str,
    severity: str,
    label: str,
    description: str,
    impact: str,
    recommended_action: str,
    is_stalled: bool,
    recurrence_count: int | None = None,
) -> dict[str, object]:
    probable_cause_code, probable_cause, probable_action = _infer_probable_cause(
        job,
        status=status_value,
        running_duration_seconds=running_duration_seconds,
    )
    source = _normalize_source(getattr(job, "source", None))
    context = _job_context(job)
    payload = {
        "diagnostic_status": status_value,
        "diagnostic_severity": severity,
        "diagnostic_label": label,
        "diagnostic_description": description,
        "diagnostic_impact": impact,
        "diagnostic_recommended_action": recommended_action or probable_action,
        "diagnostic_module": _diagnostic_module(source),
        "diagnostic_probable_cause": probable_cause,
        "diagnostic_probable_cause_code": probable_cause_code,
        "diagnostic_evidence": _build_evidence(job, context=context, running_duration_seconds=running_duration_seconds),
        "diagnostic_runbook_url": _runbook_path(source, status_value, probable_cause_code),
        "diagnostic_correlation_id": getattr(job, "correlation_id", None),
        "diagnostic_generated_at": current_time,
        "diagnostic_recurrence_count": recurrence_count,
        "is_stalled": is_stalled,
        "is_overdue_next_run": is_overdue_next_run,
        "running_duration_seconds": running_duration_seconds,
    }
    return payload


def diagnose_integration_job(
    job: Any,
    *,
    now: datetime | None = None,
    attention_minutes: int = 120,
    critical_hours: int = 24,
    next_expected_delay_minutes: int = 60,
    recurrence_count: int | None = None,
) -> dict[str, object]:
    current_time = _coerce_datetime(now) or datetime.now(timezone.utc)
    source = _normalize_source(getattr(job, "source", None))
    started_at = _coerce_datetime(getattr(job, "started_at", None))
    finished_at = _coerce_datetime(getattr(job, "finished_at", None))
    next_expected_run_at = _coerce_datetime(getattr(job, "next_expected_run_at", None))
    status = _normalize_status(getattr(job, "status", None))

    running_duration_seconds: int | None = None
    if started_at is not None:
        reference_time = finished_at or current_time
        running_duration_seconds = max(int((reference_time - started_at).total_seconds()), 0)

    next_expected_lag_seconds: int | None = None
    is_overdue_next_run = False
    if next_expected_run_at is not None:
        next_expected_lag_seconds = max(int((current_time - next_expected_run_at).total_seconds()), 0)
        is_overdue_next_run = next_expected_lag_seconds >= max(int(next_expected_delay_minutes), 1) * 60

    attention_seconds = max(int(attention_minutes), 1) * 60
    critical_seconds = max(int(critical_hours), 1) * 3600

    if status in {"failed", "failure", "error"}:
        return _base_payload(
            job=job,
            current_time=current_time,
            running_duration_seconds=running_duration_seconds,
            is_overdue_next_run=False,
            status_value="critical",
            severity="critical",
            label="Falha",
            description="A última execução falhou e precisa de triagem técnica.",
            impact=_diagnostic_impact(source, "failed", "job_failed"),
            recommended_action="Abrir histórico, revisar logs e corrigir a causa raiz.",
            is_stalled=False,
            recurrence_count=recurrence_count,
        )

    if status == "partial_success":
        return _base_payload(
            job=job,
            current_time=current_time,
            running_duration_seconds=running_duration_seconds,
            is_overdue_next_run=False,
            status_value="partial_success",
            severity="warning",
            label="Sucesso parcial",
            description="A execução concluiu parcialmente e deixou sinais de degradação operacional.",
            impact=_diagnostic_impact(source, "partial_success", "partial_scan"),
            recommended_action="Revisar itens parciais, identificar a etapa degradada e reexecutar se necessário.",
            is_stalled=False,
            recurrence_count=recurrence_count,
        )

    if status in {"success", "succeeded", "completed", "ok"}:
        if is_overdue_next_run:
            return _base_payload(
                job=job,
                current_time=current_time,
                running_duration_seconds=running_duration_seconds,
                is_overdue_next_run=True,
                status_value="overdue_next_run",
                severity="warning",
                label="Execução prevista atrasada",
                description="A próxima execução registrada está no passado. Verifique o agendamento e os locks.",
                impact=_diagnostic_impact(source, "success", "overdue_next_run"),
                recommended_action="Revisar scheduler, heartbeat e próxima previsão.",
                is_stalled=False,
                recurrence_count=recurrence_count,
            )
        return _base_payload(
            job=job,
            current_time=current_time,
            running_duration_seconds=running_duration_seconds,
            is_overdue_next_run=False,
            status_value="healthy",
            severity="healthy",
            label="Saudável",
            description="Execução saudável na última leitura disponível.",
            impact=_diagnostic_impact(source, "success", "healthy"),
            recommended_action="Manter monitoramento.",
            is_stalled=False,
            recurrence_count=recurrence_count,
        )

    if status in {"skipped", "ignored", "ignore"}:
        return _base_payload(
            job=job,
            current_time=current_time,
            running_duration_seconds=running_duration_seconds,
            is_overdue_next_run=is_overdue_next_run,
            status_value="unknown",
            severity="info",
            label="Ignorado",
            description="A execução foi ignorada ou marcada como ignorada.",
            impact=_diagnostic_impact(source, "skipped", "ignored"),
            recommended_action="Revisar se a execução era esperada.",
            is_stalled=False,
            recurrence_count=recurrence_count,
        )

    if status == "running":
        if running_duration_seconds is not None and running_duration_seconds >= critical_seconds:
            return _base_payload(
                job=job,
                current_time=current_time,
                running_duration_seconds=running_duration_seconds,
                is_overdue_next_run=is_overdue_next_run,
                status_value="stalled",
                severity="critical",
                label=_format_duration_label(running_duration_seconds / 3600),
                description="Este job está em execução há muito tempo e merece revisão imediata.",
                impact=_diagnostic_impact(source, "running", "stalled_execution"),
                recommended_action="Revisar scheduler, locks e tentativa de encerramento seguro.",
                is_stalled=True,
                recurrence_count=recurrence_count,
            )
        if is_overdue_next_run:
            return _base_payload(
                job=job,
                current_time=current_time,
                running_duration_seconds=running_duration_seconds,
                is_overdue_next_run=True,
                status_value="overdue_next_run",
                severity="warning",
                label="Execução prevista atrasada",
                description="A próxima execução esperada está no passado ou não foi recalculada a tempo.",
                impact=_diagnostic_impact(source, "running", "overdue_next_run"),
                recommended_action="Verificar scheduler, lock e heartbeat da execução.",
                is_stalled=False,
                recurrence_count=recurrence_count,
            )
        if running_duration_seconds is not None and running_duration_seconds >= attention_seconds:
            return _base_payload(
                job=job,
                current_time=current_time,
                running_duration_seconds=running_duration_seconds,
                is_overdue_next_run=False,
                status_value="attention",
                severity="warning",
                label="Possivelmente travado",
                description="Este job está em execução há mais tempo do que o habitual.",
                impact=_diagnostic_impact(source, "running", "long_running_execution"),
                recommended_action="Validar scheduler, checkpoint e última atualização.",
                is_stalled=False,
                recurrence_count=recurrence_count,
            )
        return _base_payload(
            job=job,
            current_time=current_time,
            running_duration_seconds=running_duration_seconds,
            is_overdue_next_run=is_overdue_next_run,
            status_value="running",
            severity="info",
            label="Em execução",
            description="Execução ativa dentro da janela esperada.",
            impact=_diagnostic_impact(source, "running", "running"),
            recommended_action="Acompanhar conclusão.",
            is_stalled=False,
            recurrence_count=recurrence_count,
        )

    if is_overdue_next_run:
        return _base_payload(
            job=job,
            current_time=current_time,
            running_duration_seconds=running_duration_seconds,
            is_overdue_next_run=True,
            status_value="overdue_next_run",
            severity="warning",
            label="Execução prevista atrasada",
            description="A próxima execução registrada está atrasada. Verifique o agendamento.",
            impact=_diagnostic_impact(source, "success", "overdue_next_run"),
            recommended_action="Revisar scheduler, locks e última atualização.",
            is_stalled=False,
            recurrence_count=recurrence_count,
        )

    return _base_payload(
        job=job,
        current_time=current_time,
        running_duration_seconds=running_duration_seconds,
        is_overdue_next_run=False,
        status_value="unknown",
        severity="info",
        label="Sem diagnóstico",
        description="Não foi possível derivar um diagnóstico operacional para esta execução.",
        impact=_diagnostic_impact(source, status, "job_failed"),
        recommended_action="Abrir histórico e validar o estado da automação.",
        is_stalled=False,
        recurrence_count=recurrence_count,
    )
