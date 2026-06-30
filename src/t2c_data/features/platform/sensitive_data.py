from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from typing import Any

from t2c_data.features.access_control.abac import can_access_resource
from t2c_data.core.redaction import redact_sensitive_string
from t2c_data.core.rbac import is_admin_role, user_role_names
from t2c_data.models.auth import User
from t2c_data.models.catalog import DataSource, TableEntity

_SECRET_FIELD_HINTS = (
    "password",
    "passwd",
    "pwd",
    "secret",
    "token",
    "api_key",
    "api-key",
    "access_key",
    "access-key",
    "secret_key",
    "secret-key",
    "aws_secret_access_key",
    "aws_session_token",
    "authorization",
    "webhook",
    "credential",
    "jdbc",
    "dsn",
    "connection_uri",
    "connection_string",
)

_SENSITIVE_FIELD_HINTS = (
    "cpf",
    "cnpj",
    "rg",
    "document",
    "documento",
    "doc",
    "email",
    "mail",
    "telefone",
    "phone",
    "celular",
    "mobile",
    "nome",
    "name",
    "endereco",
    "address",
    "cep",
    "zipcode",
    "renda",
    "salary",
    "salario",
    "income",
    "bank",
    "banco",
    "account",
    "conta",
    "agencia",
    "agency",
    "iban",
    "pix",
    "card",
    "cartao",
    "credit",
    "loan",
    "debt",
    "saldo",
    "amount",
    "valor",
    "price",
    "payment",
    "parcela",
    "installment",
    "fee",
    "contract",
    "contrato",
    "proposal",
    "proposta",
    "quota",
    "cota",
)

_PUBLIC_METADATA_KEYS = {
    "id",
    "row_number",
    "row_count",
    "count",
    "total",
    "status",
    "state",
    "severity",
    "tone",
    "type",
    "kind",
    "event_type",
    "action",
    "label",
    "message",
    "created_at",
    "updated_at",
    "detected_at",
    "calculated_at",
    "started_at",
    "last_seen_at",
    "ended_at",
    "expires_at",
}
_NON_PERSON_NAME_FIELDS = {
    "schema_name",
    "table_name",
    "database_name",
    "datasource_name",
    "column_name",
    "field_name",
    "resource_name",
    "route_name",
    "page_name",
    "metric_name",
    "event_name",
}
_DIRECT_PERSON_FIELDS = {
    "owner",
    "owner_name",
    "owner_email",
    "actor_name",
    "actor_email",
    "reviewer_name",
    "reviewer_email",
    "submitted_by_user_name",
    "submitted_by_user_email",
    "decided_by_user_name",
    "decided_by_user_email",
    "reviewed_by_user_name",
    "reviewed_by_user_email",
}
_PERSON_MAPPING_FIELDS = {
    "data_owner",
    "owner",
    "actor",
    "reviewer",
    "submitted_by_user",
    "decided_by_user",
    "reviewed_by_user",
}

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_CPF_RE = re.compile(r"^\d{3}\.?\d{3}\.?\d{3}-?\d{2}$")
_CNPJ_RE = re.compile(r"^\d{2}\.?\d{3}\.?\d{3}/?\d{4}-?\d{2}$")
_PHONE_RE = re.compile(r"^\+?\d[\d\s().-]{7,}$")
_IBAN_RE = re.compile(r"^[A-Z]{2}\d{2}[A-Z0-9]{10,30}$", re.IGNORECASE)
_DIGIT_RE = re.compile(r"\d")


def _normalize(value: str | None) -> str:
    return (value or "").strip().lower()


def _normalize_field_name(field_name: str | None) -> str:
    normalized = _normalize(field_name)
    return normalized.replace("-", "_").replace(" ", "_")


def _is_secret_field(field_name: str | None) -> bool:
    normalized = _normalize_field_name(field_name)
    return bool(normalized and any(token in normalized for token in _SECRET_FIELD_HINTS))


def _is_sensitive_field(field_name: str | None) -> bool:
    normalized = _normalize_field_name(field_name)
    if not normalized:
        return False
    segments = [segment for segment in re.split(r"[^a-z0-9]+", normalized) if segment]
    if normalized in _NON_PERSON_NAME_FIELDS:
        segments = [segment for segment in segments if segment != "name"]
    for token in _SENSITIVE_FIELD_HINTS:
        if token == "name":
            if "name" not in segments:
                continue
            if normalized in _NON_PERSON_NAME_FIELDS:
                continue
            if not any(context in segments for context in ("owner", "user", "actor", "person", "customer", "client", "contact", "full", "first", "last", "data_owner")):
                continue
            return True
        elif token in segments:
            return True
    return False


def _is_public_metadata_key(field_name: str | None) -> bool:
    normalized = _normalize_field_name(field_name)
    return normalized in _PUBLIC_METADATA_KEYS or normalized.endswith("_id")


def _looks_like_email(value: str) -> bool:
    return bool(_EMAIL_RE.match(value.strip()))


def _looks_like_document(value: str) -> bool:
    text = value.strip()
    return bool(_CPF_RE.match(text) or _CNPJ_RE.match(text))


def _looks_like_phone(value: str) -> bool:
    text = value.strip()
    return bool(len(text) >= 8 and _PHONE_RE.match(text))


def _looks_like_bank_identifier(value: str, *, field_name: str | None = None) -> bool:
    normalized_field = _normalize_field_name(field_name)
    if not any(token in normalized_field for token in ("bank", "banco", "conta", "account", "agencia", "agency", "iban", "pix", "card", "cartao", "credit")):
        return False
    text = value.strip().replace(" ", "")
    return bool(_DIGIT_RE.search(text) and (len(text) >= 8 or _IBAN_RE.match(text)))


def _looks_like_sensitive_value(value: Any, *, field_name: str | None = None) -> bool:
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return False
        if _looks_like_email(text) or _looks_like_document(text) or _looks_like_phone(text):
            return True
        if _looks_like_bank_identifier(text, field_name=field_name):
            return True
        normalized_field = _normalize_field_name(field_name)
        segments = [segment for segment in re.split(r"[^a-z0-9]+", normalized_field) if segment]
        if normalized_field not in _NON_PERSON_NAME_FIELDS and any(token in segments for token in ("email", "mail", "cpf", "cnpj", "telefone", "phone", "nome", "endereco", "address", "renda", "salary", "salario", "income", "bank", "banco", "account", "conta", "agencia", "agency", "iban", "pix", "card", "cartao", "credit", "loan", "debt", "saldo", "amount", "valor", "price", "payment", "parcela", "installment", "fee", "contract", "contrato", "proposal", "proposta", "quota", "cota")):
            return True
    return False


def can_view_sensitive_data(
    user: User | None,
    *,
    table: TableEntity | None = None,
    datasource: DataSource | None = None,
    action: str = "read",
    column_classification: Mapping[str, Any] | None = None,
) -> bool:
    if user is None:
        return False
    roles = user_role_names(user)
    if is_admin_role(roles):
        return True
    return can_access_resource(
        user,
        action=action,
        table=table,
        datasource=datasource,
        column_classification=column_classification,
    )


def mask_sensitive_value(
    value: Any,
    *,
    field_name: str | None = None,
    can_view_sensitive: bool = False,
    redact_token: str = "[redacted]",
    mask_token: str = "[masked]",
) -> Any:
    if value is None:
        return None
    if hasattr(value, "model_dump") and callable(getattr(value, "model_dump")):
        try:
            return mask_payload_by_policy(value.model_dump(mode="json"), can_view_sensitive=can_view_sensitive)
        except Exception:  # noqa: BLE001
            pass
    if hasattr(value, "__dict__") and not isinstance(value, type):
        try:
            data = {str(key): item for key, item in vars(value).items() if not str(key).startswith("_")}
            if data:
                return mask_payload_by_policy(data, can_view_sensitive=can_view_sensitive)
        except Exception:  # noqa: BLE001
            pass
    normalized_field = _normalize_field_name(field_name)
    if isinstance(value, Mapping) and not can_view_sensitive and normalized_field in _PERSON_MAPPING_FIELDS:
        masked_mapping: dict[str, Any] = {}
        for key, item in value.items():
            key_name = str(key)
            if key_name in {"name", "email", "owner", "owner_name", "owner_email"}:
                masked_mapping[key_name] = mask_token
                continue
            masked_mapping[key_name] = mask_sensitive_value(
                item,
                field_name=key_name,
                can_view_sensitive=can_view_sensitive,
                redact_token=redact_token,
                mask_token=mask_token,
            )
        return masked_mapping
    if isinstance(value, Mapping):
        return {
            str(key): mask_sensitive_value(
                item,
                field_name=str(key),
                can_view_sensitive=can_view_sensitive,
                redact_token=redact_token,
                mask_token=mask_token,
            )
            for key, item in value.items()
        }
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [
            mask_sensitive_value(
                item,
                field_name=field_name,
                can_view_sensitive=can_view_sensitive,
                redact_token=redact_token,
                mask_token=mask_token,
            )
            for item in value
        ]
    if _is_secret_field(field_name):
        return redact_token
    if isinstance(value, str):
        if not can_view_sensitive and normalized_field in _DIRECT_PERSON_FIELDS:
            return mask_token
        if not can_view_sensitive and (_is_sensitive_field(field_name) or _looks_like_sensitive_value(value, field_name=field_name)):
            return mask_token
        if _looks_like_sensitive_value(value, field_name=field_name) and can_view_sensitive:
            return value
        return redact_sensitive_string(value)
    if not can_view_sensitive and _is_sensitive_field(field_name):
        return mask_token
    return value


def mask_row_by_classification(
    row: Mapping[str, Any] | None,
    *,
    can_view_sensitive: bool = False,
    sensitivity_level: str | None = None,
    has_personal_data: bool = False,
    has_sensitive_personal_data: bool = False,
    column_classifications: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if not row:
        return {}
    if can_view_sensitive:
        return {str(key): value for key, value in row.items()}
    should_mask_all = has_personal_data or has_sensitive_personal_data or _normalize(sensitivity_level) in {"confidential", "restricted", "personal_data"}
    if column_classifications:
        masked: dict[str, Any] = {}
        for key, value in row.items():
            key_str = str(key)
            if should_mask_all and not _is_public_metadata_key(key_str):
                masked[key_str] = "[masked]"
                continue
            classification = column_classifications.get(key_str) if hasattr(column_classifications, "get") else None
            taxonomy_group = None
            sensitive_flag = False
            financial_flag = False
            if isinstance(classification, Mapping):
                taxonomy_group = classification.get("taxonomy_group")
                sensitive_flag = bool(classification.get("is_sensitive_data"))
                financial_flag = bool(classification.get("is_financial_data"))
            elif classification is not None:
                taxonomy_group = getattr(classification, "taxonomy_group", None)
                sensitive_flag = bool(getattr(classification, "is_sensitive_data", False))
                financial_flag = bool(getattr(classification, "is_financial_data", False))
            should_mask = (
                sensitive_flag
                or financial_flag
                or taxonomy_group in {"personal", "financial", "sensitive"}
                or _is_sensitive_field(key_str)
                or _looks_like_sensitive_value(value, field_name=key_str)
            )
            masked[key_str] = "[masked]" if should_mask else value
        return masked
    should_mask_all = not can_view_sensitive and should_mask_all
    masked: dict[str, Any] = {}
    for key, value in row.items():
        key_str = str(key)
        if should_mask_all and not _is_public_metadata_key(key_str):
            masked[key_str] = "[masked]"
            continue
        masked[key_str] = mask_sensitive_value(
            value,
            field_name=key_str,
            can_view_sensitive=can_view_sensitive,
        )
    return masked


def mask_payload_by_policy(
    payload: Any,
    *,
    can_view_sensitive: bool = False,
) -> Any:
    if isinstance(payload, Mapping):
        return {
            str(key): mask_sensitive_value(
                value,
                field_name=str(key),
                can_view_sensitive=can_view_sensitive,
            )
            for key, value in payload.items()
        }
    if isinstance(payload, Sequence) and not isinstance(payload, (str, bytes, bytearray)):
        return [mask_payload_by_policy(item, can_view_sensitive=can_view_sensitive) for item in payload]
    return mask_sensitive_value(payload, can_view_sensitive=can_view_sensitive)


def redact_sensitive_metadata(value: Any) -> Any:
    return mask_payload_by_policy(value, can_view_sensitive=False)


__all__ = [
    "can_view_sensitive_data",
    "mask_payload_by_policy",
    "mask_row_by_classification",
    "mask_sensitive_value",
    "redact_sensitive_metadata",
]
