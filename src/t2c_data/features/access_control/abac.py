from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping

from t2c_data.core.config import normalize_environment
from t2c_data.core.rbac import is_admin_role, user_role_names
from t2c_data.models.auth import User
from t2c_data.models.catalog import DataSource, TableEntity

SENSITIVE_READ_PERMISSIONS = {"sensitive:read", "data:sensitive:read", "sensitive_read", "admin.user_audit.sensitive_read"}
SENSITIVE_EXPORT_PERMISSIONS = {"sensitive:export", "data:sensitive:export", "sensitive_export"}
SENSITIVE_ACTIONS = {"read", "sensitive_read", "export", "update", "approve", "certify", "classify", "delete"}
ENVIRONMENT_ALIASES = {
    "production": "prod",
    "prod": "prod",
    "development": "dev",
    "dev": "dev",
    "local": "local",
    "test": "test",
    "qa": "qa",
    "hml": "hml",
    "sandbox": "sandbox",
    "shared": "shared",
}


@dataclass(slots=True)
class AccessSubject:
    action: str = "read"
    domain_name: str | None = None
    environment: str | None = None
    sensitivity_level: str | None = None
    has_personal_data: bool = False
    has_sensitive_data: bool = False
    has_financial_data: bool = False
    owner_email: str | None = None
    owner_user_id: int | None = None
    datasource_id: int | None = None
    table_id: int | None = None
    column_id: int | None = None
    column_classification: Mapping[str, Any] | None = field(default=None)


@dataclass(slots=True)
class AccessSubjectDecision:
    allowed: bool
    reason: str | None = None
    subject: AccessSubject | None = None


def _normalize_token(value: str | None) -> str:
    raw = normalize_environment(value)
    return ENVIRONMENT_ALIASES.get(raw, raw)


def _normalize_tokens(values: Iterable[str | None] | None) -> set[str]:
    return {token for token in (_normalize_token(value) for value in values or []) if token}


def _permission_names(user: User | None) -> set[str]:
    if user is None:
        return set()
    permissions: set[str] = set()
    for role in getattr(user, "roles", []) or []:
        for permission in getattr(role, "permissions", []) or []:
            name = str(getattr(permission, "name", "") or "").strip().lower()
            if name:
                permissions.add(name)
    return permissions


def _allowed_domains(user: User | None) -> set[str]:
    if user is None:
        return set()
    values = getattr(user, "allowed_domains", None)
    if isinstance(values, list):
        return _normalize_tokens(str(value) for value in values if value is not None)
    return set()


def _allowed_environments(user: User | None) -> set[str]:
    if user is None:
        return set()
    values = getattr(user, "allowed_environments", None)
    if isinstance(values, list):
        return _normalize_tokens(str(value) for value in values if value is not None)
    return set()


def _user_matches_owner(user: User | None, table: TableEntity | None) -> bool:
    if user is None or table is None or not user.email:
        return False
    owner_email = (getattr(table, "owner_email", None) or "").strip().lower()
    if owner_email and owner_email == user.email.strip().lower():
        return True
    data_owner = getattr(table, "data_owner", None)
    data_owner_email = (getattr(data_owner, "email", None) or "").strip().lower()
    return bool(data_owner_email and data_owner_email == user.email.strip().lower())


def _user_is_owner(user: User | None, table: TableEntity | None) -> bool:
    if table is None:
        return False
    roles = user_role_names(user)
    return "data_owner" in roles or _user_matches_owner(user, table)


def _user_is_steward(user: User | None) -> bool:
    return "stewardship" in user_role_names(user)


def _resource_domain_name(table: TableEntity | None) -> str | None:
    if table is None:
        return None
    data_owner = getattr(table, "data_owner", None)
    domain = getattr(data_owner, "area", None)
    domain_text = str(domain or "").strip()
    return domain_text or None


def _resource_environment(table: TableEntity | None = None, datasource: DataSource | None = None) -> str | None:
    if datasource is not None:
        return _normalize_token(getattr(datasource, "environment", None)) or None
    if table is None:
        return None
    datasource_obj = getattr(getattr(getattr(table, "schema", None), "database", None), "datasource", None)
    return _normalize_token(getattr(datasource_obj, "environment", None)) or None


def _resource_sensitivity_level(table: TableEntity | None, column_classification: Mapping[str, Any] | None = None) -> str | None:
    if column_classification is not None:
        taxonomy_group = str(column_classification.get("taxonomy_group") or "").strip().lower()
        if taxonomy_group in {"sensitive", "personal", "financial"}:
            return "restricted" if taxonomy_group == "sensitive" else "personal_data"
    if table is None:
        return None
    return _normalize_token(getattr(table, "sensitivity_level", None)) or None


def _resource_has_sensitive_flags(table: TableEntity | None, column_classification: Mapping[str, Any] | None = None) -> bool:
    if column_classification is not None:
        return bool(
            column_classification.get("is_personal_data")
            or column_classification.get("is_sensitive_data")
            or column_classification.get("is_financial_data")
        )
    if table is None:
        return False
    return bool(getattr(table, "has_personal_data", False) or getattr(table, "has_sensitive_personal_data", False))


def _resource_is_sensitive(table: TableEntity | None, column_classification: Mapping[str, Any] | None = None) -> bool:
    sensitivity = _resource_sensitivity_level(table, column_classification)
    if sensitivity in {"confidential", "restricted", "personal_data"}:
        return True
    return _resource_has_sensitive_flags(table, column_classification)


def _resource_domain_allowed(user: User | None, table: TableEntity | None) -> bool:
    domain = _resource_domain_name(table)
    if not domain:
        return True
    role_names = user_role_names(user)
    if is_admin_role(role_names) or _user_is_owner(user, table) or _user_is_steward(user):
        return True
    allowed_domains = _allowed_domains(user)
    if not allowed_domains:
        return True
    return _normalize_token(domain) in allowed_domains


def _resource_environment_allowed(user: User | None, table: TableEntity | None, datasource: DataSource | None = None) -> bool:
    environment = _resource_environment(table, datasource)
    if not environment:
        return True
    normalized = _normalize_token(environment)
    role_names = user_role_names(user)
    if is_admin_role(role_names) or _user_is_owner(user, table) or _user_is_steward(user):
        return True
    if normalized in {"dev", "local", "test", "shared", "sandbox", "hml", "qa"}:
        return True
    allowed_environments = _allowed_environments(user)
    if not allowed_environments:
        return False
    return normalized in allowed_environments


def _has_permission(user: User | None, permission_names: set[str]) -> bool:
    if user is None:
        return False
    return bool(_permission_names(user).intersection(permission_names))


def build_access_subject(
    *,
    action: str = "read",
    table: TableEntity | None = None,
    datasource: DataSource | None = None,
    column_classification: Mapping[str, Any] | None = None,
) -> AccessSubject:
    return AccessSubject(
        action=action,
        domain_name=_resource_domain_name(table),
        environment=_resource_environment(table, datasource),
        sensitivity_level=_resource_sensitivity_level(table, column_classification),
        has_personal_data=_resource_has_sensitive_flags(table, column_classification),
        has_sensitive_data=_resource_has_sensitive_flags(table, column_classification),
        has_financial_data=bool(column_classification.get("is_financial_data")) if column_classification else False,
        owner_email=getattr(table, "owner_email", None) if table is not None else None,
        owner_user_id=getattr(table, "data_owner_id", None) if table is not None else None,
        datasource_id=getattr(datasource, "id", None) if datasource is not None else getattr(getattr(getattr(table, "schema", None), "database", None), "datasource_id", None) if table is not None else None,
        table_id=getattr(table, "id", None),
        column_classification=column_classification,
    )


def can_access_resource(
    user: User | None,
    *,
    action: str = "read",
    table: TableEntity | None = None,
    datasource: DataSource | None = None,
    column_classification: Mapping[str, Any] | None = None,
) -> bool:
    if user is None:
        return False

    roles = user_role_names(user)
    if is_admin_role(roles):
        return True

    if not _resource_domain_allowed(user, table):
        return False
    if not _resource_environment_allowed(user, table, datasource=datasource):
        return False

    normalized_action = (action or "read").strip().lower()
    sensitive_resource = _resource_is_sensitive(table, column_classification)

    if normalized_action in {"read", "sensitive_read"}:
        if not sensitive_resource:
            return True
        if _has_permission(user, SENSITIVE_READ_PERMISSIONS):
            return True
        return _user_is_owner(user, table) or _user_is_steward(user)

    if normalized_action == "export":
        if not sensitive_resource:
            return True
        return _has_permission(user, SENSITIVE_EXPORT_PERMISSIONS)

    if normalized_action in {"update", "approve", "certify", "classify", "delete"}:
        if not sensitive_resource:
            return True
        if _user_is_owner(user, table) or _user_is_steward(user):
            return True
        return _has_permission(user, {"sensitive:read"}) or _has_permission(user, SENSITIVE_READ_PERMISSIONS)

    if normalized_action in SENSITIVE_ACTIONS:
        if not sensitive_resource:
            return True
        return _has_permission(user, SENSITIVE_READ_PERMISSIONS) or _user_is_owner(user, table) or _user_is_steward(user)

    return True


def sensitivity_denial_reason(
    user: User | None,
    *,
    action: str = "read",
    table: TableEntity | None = None,
    datasource: DataSource | None = None,
    column_classification: Mapping[str, Any] | None = None,
) -> str | None:
    if can_access_resource(user, action=action, table=table, datasource=datasource, column_classification=column_classification):
        return None
    normalized_action = (action or "read").strip().lower()
    if normalized_action == "export":
        return "export_sensitive_denied"
    if not _resource_domain_allowed(user, table):
        return "domain_denied"
    if not _resource_environment_allowed(user, table, datasource=datasource):
        return "environment_denied"
    if _resource_is_sensitive(table, column_classification):
        return "sensitive_permission_denied"
    return "access_denied"


def record_abac_denial(
    session,
    *,
    request,
    current_user: User | None,
    action: str,
    table: TableEntity | None = None,
    datasource: DataSource | None = None,
    column_classification: Mapping[str, Any] | None = None,
    reason: str | None = None,
) -> None:
    if session is None or request is None:
        return
    try:
        from t2c_data.services.audit import request_audit_kwargs, write_audit_log_sync
    except Exception:  # noqa: BLE001
        return
    payload = {
        "action": action,
        "reason": reason or sensitivity_denial_reason(
            current_user,
            action=action,
            table=table,
            datasource=datasource,
            column_classification=column_classification,
        ),
        "domain_name": _resource_domain_name(table),
        "environment": _resource_environment(table, datasource),
        "sensitivity_level": _resource_sensitivity_level(table, column_classification),
        "table_id": getattr(table, "id", None),
        "datasource_id": getattr(datasource, "id", None),
        "column_id": getattr(column_classification, "get", lambda *_: None)("column_id") if column_classification else None,
    }
    try:
        write_audit_log_sync(
            session,
            action="platform.abac.denied",
            entity_type="access",
            entity_id=str(getattr(table, "id", None) or getattr(datasource, "id", None) or "unknown"),
            metadata={key: value for key, value in payload.items() if value is not None},
            **request_audit_kwargs(request, current_user),
        )
        session.commit()
    except Exception:  # noqa: BLE001
        try:
            session.rollback()
        except Exception:  # noqa: BLE001
            pass
