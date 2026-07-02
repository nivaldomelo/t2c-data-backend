from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Sequence

from fastapi import HTTPException, Request, status
from sqlalchemy.orm import Session

from t2c_data.core.rbac import is_admin_role, user_role_names
from t2c_data.core.telemetry import runtime_metrics
from t2c_data.features.platform.sensitive_data import redact_sensitive_metadata
from t2c_data.models.auth import User
from t2c_data.services.audit import request_audit_kwargs, write_audit_log_sync

DEFAULT_EXPORT_LIMIT = 5000


@dataclass(frozen=True)
class ExportPolicy:
    sensitivity: str
    limit: int


_EXPORT_LIMIT_BY_SENSITIVITY = {
    "low": DEFAULT_EXPORT_LIMIT,
    "medium": 2500,
    "high": 2000,
    "regulatory": 1000,
    "operational_critical": 2000,
}

_LOW_SENSITIVITY_MODULES = set()
_MEDIUM_SENSITIVITY_MODULES = {"datasource", "catalog", "glossary", "tags", "lineage"}
_HIGH_SENSITIVITY_MODULES = {"certification"}
_REGULATORY_SENSITIVITY_MODULES = {"audit", "privacy_access"}
_OPERATIONAL_CRITICAL_MODULES = {"platform", "ops", "governance", "owners", "io", "integrations", "dashboard"}


def classify_export_sensitivity(*, source_module: str, entity_type: str | None = None) -> str:
    normalized_module = (source_module or "").strip().lower()
    normalized_entity = (entity_type or "").strip().lower()
    if normalized_module in _REGULATORY_SENSITIVITY_MODULES or any(token in normalized_entity for token in ("audit", "privacy")):
        return "regulatory"
    if normalized_module in _HIGH_SENSITIVITY_MODULES or "certification" in normalized_entity:
        return "high"
    if normalized_module in _OPERATIONAL_CRITICAL_MODULES or any(token in normalized_entity for token in ("ownership", "governance", "ops", "platform", "bundle", "dashboard")):
        return "operational_critical"
    if normalized_module in _MEDIUM_SENSITIVITY_MODULES:
        return "medium"
    if normalized_module in _LOW_SENSITIVITY_MODULES:
        return "low"
    return "low"


def resolve_export_limit(*, source_module: str, entity_type: str | None = None) -> int:
    sensitivity = classify_export_sensitivity(source_module=source_module, entity_type=entity_type)
    return _EXPORT_LIMIT_BY_SENSITIVITY.get(sensitivity, DEFAULT_EXPORT_LIMIT)


def resolve_export_policy(*, source_module: str, entity_type: str | None = None) -> ExportPolicy:
    sensitivity = classify_export_sensitivity(source_module=source_module, entity_type=entity_type)
    return ExportPolicy(
        sensitivity=sensitivity,
        limit=resolve_export_limit(source_module=source_module, entity_type=entity_type),
    )


def enforce_export_permission(current_user: User, permission_name: str) -> User:
    raw_module_name = (permission_name.split(":", 1)[0] if permission_name else "unknown").strip().lower() or "unknown"
    module_name = raw_module_name.split(".", 1)[0]
    sensitivity = classify_export_sensitivity(source_module=module_name)
    role_names = user_role_names(current_user)
    if is_admin_role(role_names):
        runtime_metrics.export_event(module=module_name, outcome="granted", classification=sensitivity)
        return current_user

    granted = {perm.name for role in current_user.roles for perm in role.permissions}
    if permission_name in granted:
        runtime_metrics.export_event(module=module_name, outcome="granted", classification=sensitivity)
        return current_user

    runtime_metrics.export_event(module=module_name, outcome="denied", classification=sensitivity)
    raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Insufficient permission")


# Leading characters that spreadsheet apps (Excel/LibreOffice/Sheets) interpret as a
# formula. Any exported cell starting with one of these must be neutralized to prevent
# CSV/formula injection (e.g. =HYPERLINK/=WEBSERVICE/DDE) when the file is opened.
_FORMULA_PREFIXES = ("=", "+", "-", "@", "\t", "\r")


def neutralize_spreadsheet_formula(value: str) -> str:
    """Prefix a tab so the cell is treated as text, not a formula, by spreadsheet apps."""
    if value and value[0] in _FORMULA_PREFIXES:
        return "\t" + value
    return value


class SafeCsvWriter:
    """csv.writer wrapper que neutraliza CADA célula string (anti CSV/formula injection).

    Garante que identificadores de fontes externas (schema/tabela/coluna, telemetria) não
    sejam interpretados como fórmula ao abrir o CSV no Excel/Sheets."""

    def __init__(self, writer: Any) -> None:
        self._writer = writer

    def writerow(self, row: Any) -> Any:
        return self._writer.writerow(
            [neutralize_spreadsheet_formula(cell) if isinstance(cell, str) else cell for cell in row]
        )

    def writerows(self, rows: Any) -> None:
        for row in rows:
            self.writerow(row)


def safe_csv_writer(fileobj: Any) -> "SafeCsvWriter":
    return SafeCsvWriter(csv.writer(fileobj))


def safe_sheet_append(sheet: Any, row: Any) -> None:
    """openpyxl worksheet.append neutralizando cada célula string (anti-formula-injection em XLSX)."""
    sheet.append([neutralize_spreadsheet_formula(cell) if isinstance(cell, str) else cell for cell in row])


def redact_export_value(value: Any, *, field_name: str | None = None) -> str:
    normalized_field = (field_name or "").strip().lower().replace("-", "_")
    if normalized_field in {"owner", "owner_name", "owner_email", "user_email", "actor_email"}:
        return "[masked]"
    if field_name:
        sanitized = redact_sensitive_metadata({field_name: value}).get(field_name)
    else:
        sanitized = redact_sensitive_metadata(value)
    if sanitized is None:
        return ""
    if isinstance(sanitized, datetime):
        return sanitized.isoformat()
    if isinstance(sanitized, date):
        return sanitized.isoformat()
    if isinstance(sanitized, (dict, list)):
        return neutralize_spreadsheet_formula(json.dumps(sanitized, ensure_ascii=False, default=str))
    return neutralize_spreadsheet_formula(str(sanitized))


def redact_export_row(row: dict[str, Any], *, field_names: Sequence[str] | None = None) -> dict[str, str]:
    if field_names is None:
        return {key: redact_export_value(value, field_name=key) for key, value in row.items()}
    allowed = set(field_names)
    return {
        key: redact_export_value(value, field_name=key if key in allowed else None)
        for key, value in row.items()
    }


def enforce_export_limit[T](rows: Sequence[T], *, limit: int = DEFAULT_EXPORT_LIMIT) -> tuple[list[T], bool]:
    bounded_limit = max(1, min(int(limit or DEFAULT_EXPORT_LIMIT), DEFAULT_EXPORT_LIMIT))
    if len(rows) <= bounded_limit:
        return list(rows), False
    return list(rows[:bounded_limit]), True


def audit_export_event(
    session: Session,
    *,
    request: Request,
    current_user: User,
    action: str,
    entity_type: str,
    source_module: str,
    row_count: int,
    filters: dict[str, Any] | None = None,
    limit: int = DEFAULT_EXPORT_LIMIT,
    truncated: bool = False,
    export_format: str | None = None,
    permission_name: str | None = None,
    extra_metadata: dict[str, Any] | None = None,
) -> None:
    classification = classify_export_sensitivity(source_module=source_module, entity_type=entity_type)
    is_large_export = row_count >= max(int(limit or DEFAULT_EXPORT_LIMIT), 1)
    runtime_metrics.export_event(
        module=source_module,
        outcome="truncated" if truncated else "exported",
        classification=classification,
    )
    write_audit_log_sync(
        session,
        action=action,
        entity_type=entity_type,
        source_module=source_module,
        is_sensitive_change=True,
        sensitive_category="export",
        metadata={
            "endpoint": request.url.path,
            "http_method": request.method,
            "export_format": export_format or "unknown",
            "permission_name": permission_name,
            "classification": classification,
            "filters": redact_sensitive_metadata(filters or {}),
            "row_count": row_count,
            "limit": limit,
            "truncated": truncated,
            "is_large_export": is_large_export,
            **(extra_metadata or {}),
        },
        **request_audit_kwargs(request, current_user),
    )
    session.commit()


__all__ = [
    "DEFAULT_EXPORT_LIMIT",
    "audit_export_event",
    "classify_export_sensitivity",
    "enforce_export_permission",
    "enforce_export_limit",
    "neutralize_spreadsheet_formula",
    "resolve_export_limit",
    "resolve_export_policy",
    "redact_export_row",
    "redact_export_value",
]
