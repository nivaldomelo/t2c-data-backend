from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlalchemy.inspection import inspect as sa_inspect

from t2c_data.core.redaction import redact_sensitive_string
from t2c_data.core.network import get_request_client_ip
from t2c_data.features.platform.sensitive_data import redact_sensitive_metadata

SENSITIVE_KEY_RE = re.compile(
    r"(password|passwd|token|secret|jwt|api[_-]?key|access[_-]?token|refresh[_-]?token|connection[_-]?(uri|string)?|dsn)",
    re.IGNORECASE,
)
SECRET_IN_URI_RE = re.compile(r"(://[^:\s]+:)[^@\s]+(@)")
MAX_JSON_BYTES = 50_000


@dataclass(slots=True)
class AuditFieldChange:
    field_name: str
    before: Any
    after: Any
    change_type: str = "update"
    metadata: dict[str, Any] | None = None


SENSITIVE_FIELD_CATEGORIES: dict[str, str] = {
    "owner": "owner",
    "data_owner_id": "owner",
    "owner_reviewed_at": "owner",
    "certification_status": "certification",
    "certification_criticality": "certification",
    "certification_badges": "certification",
    "certification_notes": "certification",
    "certification_submitted_at": "certification",
    "certification_review_at": "certification",
    "certification_expires_at": "certification",
    "classification": "classification",
    "sensitivity_level": "classification",
    "has_personal_data": "classification",
    "has_sensitive_personal_data": "classification",
    "legal_basis": "classification",
    "privacy_purpose": "classification",
    "retention_policy": "classification",
    "is_masked": "classification",
    "external_sharing": "classification",
    "access_scope": "classification",
    "access_roles": "classification",
    "privacy_notes": "classification",
}
SENSITIVE_CHANGE_TYPE_CATEGORIES: dict[str, str] = {
    "certify": "certification",
    "decertify": "certification",
    "reclassify": "classification",
}


def safe_jsonable(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, bytes):
        return f"<bytes:{len(value)}>"
    if hasattr(value, "get_secret_value"):
        return "***"
    if isinstance(value, Mapping):
        return {str(k): safe_jsonable(v) for k, v in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [safe_jsonable(item) for item in value]
    return str(value)


def redact_string(value: str) -> str:
    return redact_sensitive_string(SECRET_IN_URI_RE.sub(r"\1***\2", value))


def redact(obj: Any) -> Any:
    return redact_sensitive_metadata(safe_jsonable(obj))


def truncate_json(obj: Any, limit_bytes: int = MAX_JSON_BYTES) -> Any:
    if obj is None:
        return None
    try:
        payload = json.dumps(obj, ensure_ascii=False, default=str)
    except Exception:
        payload = json.dumps(safe_jsonable(obj), ensure_ascii=False, default=str)
    if len(payload.encode("utf-8")) <= limit_bytes:
        return obj

    if isinstance(obj, Mapping):
        keys = list(obj.keys())
        return {
            "truncated": True,
            "keys_changed": [str(k) for k in keys[:100]],
            "approx_size_bytes": len(payload.encode("utf-8")),
            "keys_count": len(keys),
        }
    if isinstance(obj, list):
        return {
            "truncated": True,
            "items_count": len(obj),
            "approx_size_bytes": len(payload.encode("utf-8")),
        }
    return {"truncated": True, "approx_size_bytes": len(payload.encode("utf-8"))}


def serialize_model(entity: Any) -> dict[str, Any] | None:
    if entity is None:
        return None
    try:
        mapper = sa_inspect(entity.__class__)
        data: dict[str, Any] = {}
        for column in mapper.columns:
            data[column.key] = safe_jsonable(getattr(entity, column.key))
        return data
    except Exception:
        return None


def request_audit_kwargs(request: Any, user: Any = None) -> dict[str, Any]:
    return {
        "user_id": getattr(user, "id", None),
        "actor_name": getattr(user, "name", None) or getattr(user, "full_name", None),
        "user_email": getattr(user, "email", None),
        "ip": get_request_client_ip(request),
        "user_agent": request.headers.get("user-agent") if hasattr(request, "headers") else None,
        "route": getattr(getattr(request, "url", None), "path", None),
        "method": getattr(request, "method", None),
        "request_id": getattr(getattr(request, "state", None), "request_id", None),
    }


def finalize_audit_json(value: Any) -> Any:
    redacted = redact(safe_jsonable(value))
    return truncate_json(redacted)


def classify_sensitive_change(
    *,
    field_name: str | None,
    change_type: str | None,
    metadata: Mapping[str, Any] | None = None,
) -> tuple[bool, str | None]:
    if metadata:
        explicit = metadata.get("sensitive_category")
        if isinstance(explicit, str) and explicit.strip():
            return True, explicit.strip()
        if metadata.get("is_sensitive_change") is True:
            return True, "governance"

    normalized_field = (field_name or "").strip().lower()
    normalized_type = (change_type or "").strip().lower()

    if normalized_field in SENSITIVE_FIELD_CATEGORIES:
        return True, SENSITIVE_FIELD_CATEGORIES[normalized_field]
    if normalized_field.startswith("certification_"):
        return True, "certification"
    if normalized_field.startswith("classification") or normalized_field.startswith("privacy_"):
        return True, "classification"
    if normalized_type in SENSITIVE_CHANGE_TYPE_CATEGORIES:
        return True, SENSITIVE_CHANGE_TYPE_CATEGORIES[normalized_type]
    return False, None
