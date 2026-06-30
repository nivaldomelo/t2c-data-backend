from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin, urlparse

from t2c_data.core.config import settings


_SEVERITY_META = {
    "critical": {"emoji": "🚨", "label": "CRITICAL"},
    "error": {"emoji": "🔥", "label": "INCIDENT"},
    "warning": {"emoji": "⚠️", "label": "WARNING"},
    "success": {"emoji": "✅", "label": "SUCCESS"},
    "info": {"emoji": "ℹ️", "label": "INFO"},
}


def _normalize_severity(value: str | None) -> str:
    normalized = (value or "").strip().lower()
    if normalized in {"critical", "error", "warning", "success", "info"}:
        return normalized
    if normalized == "medium":
        return "warning"
    if normalized == "high":
        return "critical"
    return "info"


def _format_datetime(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value.astimezone(timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M UTC")
    text = str(value).strip()
    return text or None


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _absolute_url(value: str | None) -> str | None:
    text = _string_or_none(value)
    if not text:
        return None
    parsed = urlparse(text)
    if parsed.scheme and parsed.netloc:
        return text
    base_url = settings.normalized_frontend_base_url
    if not base_url:
        return text
    return urljoin(f"{base_url.rstrip('/')}/", text.lstrip("/"))


def _context(notification) -> dict[str, Any]:
    context = getattr(notification, "context_json", None)
    if not isinstance(context, dict):
        context = {}
    metadata = context.get("metadata") if isinstance(context.get("metadata"), dict) else {}
    severity = _normalize_severity(getattr(notification, "severity", context.get("severity")))
    category = _string_or_none(getattr(notification, "category", None) or context.get("category")) or "platform"
    title = (
        _string_or_none(getattr(notification, "title", None))
        or _string_or_none(context.get("title"))
        or "Notificação t2c_data"
    )
    message = (
        _string_or_none(getattr(notification, "message", None))
        or _string_or_none(context.get("message"))
        or "Atualização operacional do t2c_data."
    )
    asset_name = (
        _string_or_none(context.get("asset_name"))
        or _string_or_none(context.get("table_name"))
        or _string_or_none(context.get("asset_display_name"))
        or _string_or_none(getattr(notification, "source_entity_id", None))
        or title
    )
    asset_type = (
        _string_or_none(context.get("asset_type"))
        or _string_or_none(getattr(notification, "source_entity_type", None))
        or "asset"
    )
    layer = _string_or_none(context.get("layer")) or _string_or_none(context.get("data_layer"))
    status = _string_or_none(getattr(notification, "status", None)) or _string_or_none(context.get("status"))
    owner = _string_or_none(context.get("owner")) or _string_or_none(context.get("owner_name"))
    impact = _string_or_none(context.get("impact")) or _string_or_none(context.get("business_impact"))
    action_url = _absolute_url(
        (
            _string_or_none(getattr(notification, "href", None))
            or _string_or_none(context.get("action_url"))
            or _string_or_none(context.get("href"))
            or _string_or_none(context.get("target_href"))
            or "/inbox"
        )
    )
    created_at = getattr(notification, "created_at", None) or context.get("created_at")

    return {
        "title": title,
        "message": message,
        "severity": severity,
        "category": category,
        "asset_name": asset_name,
        "asset_type": asset_type,
        "layer": layer,
        "status": status,
        "owner": owner,
        "impact": impact,
        "action_url": action_url,
        "created_at": created_at,
        "metadata": metadata,
        "context": context,
    }


def _severity_meta(severity: str | None) -> dict[str, str]:
    return _SEVERITY_META.get(_normalize_severity(severity), _SEVERITY_META["info"])


def _slack_field(label: str, value: str | None) -> dict[str, str] | None:
    if not value:
        return None
    return {"type": "mrkdwn", "text": f"*{label}*\n{value}"}


def _slack_fields(context: dict[str, Any]) -> list[dict[str, str]]:
    severity = context["severity"]
    metadata = context["metadata"]
    fields: list[dict[str, str]] = []

    ordered_items = [
        ("Severidade", severity.upper()),
        ("Categoria", context["category"]),
        ("Ativo", context["asset_name"]),
        ("Tipo", context["asset_type"]),
        ("Camada", context["layer"]),
        ("Status", context["status"]),
        ("Owner", context["owner"]),
        ("Impacto", context["impact"]),
    ]

    for label, value in ordered_items:
        field = _slack_field(label, _string_or_none(value))
        if field is not None:
            fields.append(field)

    if severity == "error" and isinstance(metadata, Mapping):
        dashboards = metadata.get("metabase_dashboards_count")
        impacted_users = metadata.get("impacted_users")
        if dashboards is not None:
            field = _slack_field("Dashboards afetados", _string_or_none(dashboards))
            if field is not None:
                fields.append(field)
        if impacted_users is not None:
            field = _slack_field("Usuários impactados", _string_or_none(impacted_users))
            if field is not None:
                fields.append(field)

    if severity == "warning" and isinstance(metadata, Mapping):
        sla = metadata.get("freshness_sla_hours") or metadata.get("sla_hours")
        freshness = metadata.get("freshness_age_hours") or metadata.get("age_hours")
        if sla is not None:
            field = _slack_field("SLA", _string_or_none(sla))
            if field is not None:
                fields.append(field)
        if freshness is not None:
            field = _slack_field("Freshness", _string_or_none(freshness))
            if field is not None:
                fields.append(field)

    if severity == "success" and isinstance(metadata, Mapping):
        records = metadata.get("records_processed") or metadata.get("records")
        duration = metadata.get("duration_seconds") or metadata.get("duration")
        if records is not None:
            field = _slack_field("Registros", _string_or_none(records))
            if field is not None:
                fields.append(field)
        if duration is not None:
            field = _slack_field("Duração", _string_or_none(duration))
            if field is not None:
                fields.append(field)

    if severity == "info" and isinstance(metadata, Mapping):
        rows = metadata.get("rows") or metadata.get("records_processed")
        if rows is not None:
            field = _slack_field("Registros", _string_or_none(rows))
            if field is not None:
                fields.append(field)

    return [field for field in fields if field]


def _slack_body_text(context: dict[str, Any], *, is_test: bool) -> str:
    if is_test:
        return "Este canal foi configurado com sucesso para receber notificações do t2c_data."

    severity = context["severity"]
    title = context["title"]
    message = context["message"]
    asset_name = context["asset_name"]
    layer = context["layer"]
    owner = context["owner"]
    impact = context["impact"]
    lines = [message]

    if severity == "critical":
        lines.append("O ativo requer atenção imediata.")
    elif severity == "error":
        lines.append("O evento afeta a operação ou consumo analítico.")
    elif severity == "warning":
        lines.append("Há risco de atraso ou degradação operacional.")
    elif severity == "success":
        lines.append("A rotina foi concluída com sucesso.")

    if asset_name:
        lines.append(f"Ativo: {asset_name}")
    if layer:
        lines.append(f"Camada: {layer}")
    if owner:
        lines.append(f"Owner: {owner}")
    if impact:
        lines.append(f"Impacto: {impact}")
    return "\n".join([line for line in lines if line]).strip() or title


def _slack_context_footer(context: dict[str, Any], *, is_test: bool) -> str:
    created_at = _format_datetime(context["created_at"])
    timestamp = "agora" if is_test else (created_at or "agora")
    return f"t2c_data • {timestamp}"


def build_slack_payload(notification, *, is_test: bool = False) -> dict[str, Any]:
    context = _context(notification)
    severity_meta = _severity_meta("info" if is_test else context["severity"])
    title = "Teste de notificação t2c_data" if is_test else context["title"]
    header_label = f"{severity_meta['emoji']} {severity_meta['label']} • {title}"
    blocks: list[dict[str, Any]] = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": header_label[:150],
                "emoji": True,
            },
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": _slack_body_text(context, is_test=is_test),
            },
        },
    ]

    fields = _slack_fields(context)
    if fields:
        blocks.append({"type": "section", "fields": fields[:10]})

    action_url = _absolute_url("/inbox" if is_test else context["action_url"])
    if action_url:
        blocks.append(
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "🔎 Ver no t2c_data"},
                        "url": action_url,
                    }
                ],
            }
        )

    blocks.append(
        {
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": _slack_context_footer(context, is_test=is_test)}],
        }
    )

    return {
        "text": f"{title}\n{context['message']}",
        "blocks": blocks,
    }


def build_teams_payload(notification, *, is_test: bool = False) -> dict[str, Any]:
    context = _context(notification)
    severity = "info" if is_test else context["severity"]
    severity_meta = _severity_meta(severity)
    title = "Teste de notificação t2c_data" if is_test else context["title"]
    facts: list[dict[str, str]] = []

    for label, value in [
        ("Severidade", severity.upper()),
        ("Categoria", context["category"]),
        ("Ativo", context["asset_name"]),
        ("Tipo", context["asset_type"]),
        ("Camada", context["layer"]),
        ("Status", context["status"]),
        ("Owner", context["owner"]),
    ]:
        if value:
            facts.append({"title": label, "value": str(value)})

    metadata = context["metadata"] if isinstance(context["metadata"], Mapping) else {}
    if severity == "error":
        dashboards = metadata.get("metabase_dashboards_count")
        impacted_users = metadata.get("impacted_users")
        if dashboards is not None:
            facts.append({"title": "Dashboards afetados", "value": str(dashboards)})
        if impacted_users is not None:
            facts.append({"title": "Usuários impactados", "value": str(impacted_users)})
    elif severity == "warning":
        sla = metadata.get("freshness_sla_hours") or metadata.get("sla_hours")
        freshness = metadata.get("freshness_age_hours") or metadata.get("age_hours")
        if sla is not None:
            facts.append({"title": "SLA", "value": str(sla)})
        if freshness is not None:
            facts.append({"title": "Freshness", "value": str(freshness)})
    elif severity == "success":
        records = metadata.get("records_processed") or metadata.get("records")
        duration = metadata.get("duration_seconds") or metadata.get("duration")
        if records is not None:
            facts.append({"title": "Registros", "value": str(records)})
        if duration is not None:
            facts.append({"title": "Duração", "value": str(duration)})

    body: list[dict[str, Any]] = [
        {
            "type": "TextBlock",
            "text": f"{severity_meta['emoji']} {severity_meta['label']} • {title}",
            "wrap": True,
            "weight": "Bolder",
            "size": "Large",
        },
        {
            "type": "TextBlock",
            "text": "Este canal foi configurado com sucesso para receber notificações do t2c_data." if is_test else context["message"],
            "wrap": True,
            "spacing": "Medium",
        },
    ]
    if facts:
        body.append({"type": "FactSet", "facts": facts[:10]})
    if severity == "critical" and not is_test:
        body.append(
            {
                "type": "TextBlock",
                "text": "O ativo requer atenção imediata e deve ser analisado o quanto antes.",
                "wrap": True,
                "spacing": "Medium",
            }
        )
    elif severity == "warning" and not is_test:
        body.append(
            {
                "type": "TextBlock",
                "text": "Há sinal de atraso ou degradação operacional. Vale revisar o freshness e o owner.",
                "wrap": True,
                "spacing": "Medium",
            }
        )
    elif severity == "success" and not is_test:
        body.append(
            {
                "type": "TextBlock",
                "text": "A rotina foi concluída com sucesso.",
                "wrap": True,
                "spacing": "Medium",
            }
        )

    body.append(
        {
            "type": "TextBlock",
            "text": "t2c_data • monitoramento automático" if not is_test else "t2c_data • notificação de teste",
            "wrap": True,
            "spacing": "Medium",
            "size": "Small",
            "isSubtle": True,
        }
    )

    action_url = _absolute_url("/inbox" if is_test else context["action_url"])
    actions: list[dict[str, Any]] = []
    if action_url:
        actions.append({"type": "Action.OpenUrl", "title": "Abrir no t2c_data", "url": action_url})

    card: dict[str, Any] = {
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "type": "AdaptiveCard",
        "version": "1.5",
        "msteams": {"width": "Full"},
        "body": body,
    }
    if actions:
        card["actions"] = actions

    return {
        "type": "message",
        "summary": title,
        "text": f"{title}\n{context['message']}",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": card,
            }
        ],
    }


def build_notification_payload(provider: str, notification, *, is_test: bool = False) -> dict[str, Any]:
    normalized = (provider or "").strip().lower()
    if normalized == "slack":
        return build_slack_payload(notification, is_test=is_test)
    if normalized == "teams":
        return build_teams_payload(notification, is_test=is_test)
    raise ValueError(f"Unsupported notification provider: {provider}")
