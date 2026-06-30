from __future__ import annotations

import secrets
import ipaddress
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Literal

from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from t2c_data.core.security import hash_password, verify_password
from t2c_data.models.auth import User
from t2c_data.models.platform import PlatformApiKey
from t2c_data.schemas.external_api import ExternalApiKeyOut, ExternalApiPermissionSummaryOut


API_KEY_PREFIX = "t2c_ext"
STATUS_ACTIVE = "active"
STATUS_INACTIVE = "inactive"
STATUS_REVOKED = "revoked"
DEFAULT_KEY_ENVIRONMENT = "shared"
WRITE_EXPIRATION_MAX_DAYS = 30
DELETE_EXPIRATION_MAX_DAYS = 7

ACTION_LABELS = {
    "read": "Ler",
    "create": "Criar",
    "update": "Editar",
    "delete": "Excluir",
}

EXTERNAL_API_SCOPE_GROUPS: list[dict[str, object]] = [
    {
        "key": "catalog",
        "label": "Catálogo",
        "description": "Consulta de tabelas e metadados do catálogo.",
        "actions": [
            {
                "key": "catalog.read",
                "action": "read",
                "label": ACTION_LABELS["read"],
                "description": "Ler tabelas e detalhes do catálogo.",
                "available": True,
                "destructive": False,
                "requires_read": False,
                "methods": ["GET"],
                "endpoints": ["/external/catalog/tables", "/external/catalog/tables/{table_id}"],
            },
            {
                "key": "catalog.create",
                "action": "create",
                "label": ACTION_LABELS["create"],
                "description": "Criar recursos do catálogo via API externa.",
                "available": False,
                "destructive": False,
                "requires_read": True,
                "methods": ["POST"],
                "endpoints": [],
            },
            {
                "key": "catalog.update",
                "action": "update",
                "label": ACTION_LABELS["update"],
                "description": "Editar recursos do catálogo via API externa.",
                "available": False,
                "destructive": False,
                "requires_read": True,
                "methods": ["PATCH", "PUT"],
                "endpoints": [],
            },
            {
                "key": "catalog.delete",
                "action": "delete",
                "label": ACTION_LABELS["delete"],
                "description": "Excluir recursos do catálogo via API externa.",
                "available": False,
                "destructive": True,
                "requires_read": True,
                "methods": ["DELETE"],
                "endpoints": [],
            },
        ],
    },
    {
        "key": "explorer",
        "label": "Explorer",
        "description": "Busca e resumo canônico do Explorer.",
        "actions": [
            {
                "key": "explorer.read",
                "action": "read",
                "label": ACTION_LABELS["read"],
                "description": "Pesquisar e consultar resumos do Explorer.",
                "available": True,
                "destructive": False,
                "requires_read": False,
                "methods": ["GET"],
                "endpoints": ["/external/explorer/search", "/external/explorer/tables/{table_id}/summary"],
            },
            {
                "key": "explorer.create",
                "action": "create",
                "label": ACTION_LABELS["create"],
                "description": "Criar recursos do Explorer via API externa.",
                "available": False,
                "destructive": False,
                "requires_read": True,
                "methods": [],
                "endpoints": [],
            },
            {
                "key": "explorer.update",
                "action": "update",
                "label": ACTION_LABELS["update"],
                "description": "Editar recursos do Explorer via API externa.",
                "available": False,
                "destructive": False,
                "requires_read": True,
                "methods": [],
                "endpoints": [],
            },
            {
                "key": "explorer.delete",
                "action": "delete",
                "label": ACTION_LABELS["delete"],
                "description": "Excluir recursos do Explorer via API externa.",
                "available": False,
                "destructive": True,
                "requires_read": True,
                "methods": [],
                "endpoints": [],
            },
        ],
    },
    {
        "key": "governance",
        "label": "Governança",
        "description": "Pendências, fila de revisão e ações de governança.",
        "actions": [
            {
                "key": "governance.read",
                "action": "read",
                "label": ACTION_LABELS["read"],
                "description": "Consultar painéis e pendências de governança.",
                "available": True,
                "destructive": False,
                "requires_read": False,
                "methods": ["GET"],
                "endpoints": ["/external/governance/pending-center/summary", "/external/governance/pending-center/queue"],
            },
            {
                "key": "governance.create",
                "action": "create",
                "label": ACTION_LABELS["create"],
                "description": "Criar recursos de governança via API externa.",
                "available": False,
                "destructive": False,
                "requires_read": True,
                "methods": [],
                "endpoints": [],
            },
            {
                "key": "governance.update",
                "action": "update",
                "label": ACTION_LABELS["update"],
                "description": "Editar recursos de governança via API externa.",
                "available": False,
                "destructive": False,
                "requires_read": True,
                "methods": [],
                "endpoints": [],
            },
            {
                "key": "governance.delete",
                "action": "delete",
                "label": ACTION_LABELS["delete"],
                "description": "Excluir recursos de governança via API externa.",
                "available": False,
                "destructive": True,
                "requires_read": True,
                "methods": [],
                "endpoints": [],
            },
        ],
    },
    {
        "key": "certification",
        "label": "Certificação",
        "description": "Status e resumo de certificação.",
        "actions": [
            {
                "key": "certification.read",
                "action": "read",
                "label": ACTION_LABELS["read"],
                "description": "Consultar status e resumo de certificação.",
                "available": True,
                "destructive": False,
                "requires_read": False,
                "methods": ["GET"],
                "endpoints": ["/external/certification/tables"],
            },
            {
                "key": "certification.create",
                "action": "create",
                "label": ACTION_LABELS["create"],
                "description": "Criar recursos de certificação via API externa.",
                "available": False,
                "destructive": False,
                "requires_read": True,
                "methods": [],
                "endpoints": [],
            },
            {
                "key": "certification.update",
                "action": "update",
                "label": ACTION_LABELS["update"],
                "description": "Editar recursos de certificação via API externa.",
                "available": False,
                "destructive": False,
                "requires_read": True,
                "methods": [],
                "endpoints": [],
            },
            {
                "key": "certification.delete",
                "action": "delete",
                "label": ACTION_LABELS["delete"],
                "description": "Excluir recursos de certificação via API externa.",
                "available": False,
                "destructive": True,
                "requires_read": True,
                "methods": [],
                "endpoints": [],
            },
        ],
    },
    {
        "key": "dq",
        "label": "Data Quality",
        "description": "Regras e sinais de DQ.",
        "actions": [
            {
                "key": "dq.read",
                "action": "read",
                "label": ACTION_LABELS["read"],
                "description": "Consultar regras e resultados de DQ.",
                "available": True,
                "destructive": False,
                "requires_read": False,
                "methods": ["GET"],
                "endpoints": ["/external/dq/rules"],
            },
            {
                "key": "dq.create",
                "action": "create",
                "label": ACTION_LABELS["create"],
                "description": "Criar recursos de DQ via API externa.",
                "available": False,
                "destructive": False,
                "requires_read": True,
                "methods": [],
                "endpoints": [],
            },
            {
                "key": "dq.update",
                "action": "update",
                "label": ACTION_LABELS["update"],
                "description": "Editar recursos de DQ via API externa.",
                "available": False,
                "destructive": False,
                "requires_read": True,
                "methods": [],
                "endpoints": [],
            },
            {
                "key": "dq.delete",
                "action": "delete",
                "label": ACTION_LABELS["delete"],
                "description": "Excluir recursos de DQ via API externa.",
                "available": False,
                "destructive": True,
                "requires_read": True,
                "methods": [],
                "endpoints": [],
            },
        ],
    },
    {
        "key": "incidents",
        "label": "Incidentes",
        "description": "Incidentes operacionais e de governança.",
        "actions": [
            {
                "key": "incidents.read",
                "action": "read",
                "label": ACTION_LABELS["read"],
                "description": "Consultar incidentes e seus resumos.",
                "available": True,
                "destructive": False,
                "requires_read": False,
                "methods": ["GET"],
                "endpoints": ["/external/incidents", "/external/incidents/summary"],
            },
            {
                "key": "incidents.create",
                "action": "create",
                "label": ACTION_LABELS["create"],
                "description": "Criar incidentes via API externa.",
                "available": False,
                "destructive": False,
                "requires_read": True,
                "methods": [],
                "endpoints": [],
            },
            {
                "key": "incidents.update",
                "action": "update",
                "label": ACTION_LABELS["update"],
                "description": "Editar incidentes via API externa.",
                "available": False,
                "destructive": False,
                "requires_read": True,
                "methods": [],
                "endpoints": [],
            },
            {
                "key": "incidents.delete",
                "action": "delete",
                "label": ACTION_LABELS["delete"],
                "description": "Excluir incidentes via API externa.",
                "available": False,
                "destructive": True,
                "requires_read": True,
                "methods": [],
                "endpoints": [],
            },
        ],
    },
    {
        "key": "lineage",
        "label": "Linhagem",
        "description": "Resumo de linhagem por ativo.",
        "actions": [
            {
                "key": "lineage.read",
                "action": "read",
                "label": ACTION_LABELS["read"],
                "description": "Consultar resumo de linhagem.",
                "available": True,
                "destructive": False,
                "requires_read": False,
                "methods": ["GET"],
                "endpoints": ["/external/lineage/tables/{table_id}/summary"],
            },
            {
                "key": "lineage.create",
                "action": "create",
                "label": ACTION_LABELS["create"],
                "description": "Criar recursos de linhagem via API externa.",
                "available": False,
                "destructive": False,
                "requires_read": True,
                "methods": [],
                "endpoints": [],
            },
            {
                "key": "lineage.update",
                "action": "update",
                "label": ACTION_LABELS["update"],
                "description": "Editar recursos de linhagem via API externa.",
                "available": False,
                "destructive": False,
                "requires_read": True,
                "methods": [],
                "endpoints": [],
            },
            {
                "key": "lineage.delete",
                "action": "delete",
                "label": ACTION_LABELS["delete"],
                "description": "Excluir recursos de linhagem via API externa.",
                "available": False,
                "destructive": True,
                "requires_read": True,
                "methods": [],
                "endpoints": [],
            },
        ],
    },
    {
        "key": "platform",
        "label": "Plataforma",
        "description": "Eventos de domínio e visão operacional resumida.",
        "actions": [
            {
                "key": "platform.read",
                "action": "read",
                "label": ACTION_LABELS["read"],
                "description": "Consultar eventos e visão operacional da plataforma.",
                "available": True,
                "destructive": False,
                "requires_read": False,
                "methods": ["GET"],
                "endpoints": ["/external/platform/events", "/external/platform/events/catalog", "/external/platform/ingestion/overview"],
            },
            {
                "key": "platform.create",
                "action": "create",
                "label": ACTION_LABELS["create"],
                "description": "Criar recursos de plataforma via API externa.",
                "available": False,
                "destructive": False,
                "requires_read": True,
                "methods": [],
                "endpoints": [],
            },
            {
                "key": "platform.update",
                "action": "update",
                "label": ACTION_LABELS["update"],
                "description": "Editar recursos de plataforma via API externa.",
                "available": False,
                "destructive": False,
                "requires_read": True,
                "methods": [],
                "endpoints": [],
            },
            {
                "key": "platform.delete",
                "action": "delete",
                "label": ACTION_LABELS["delete"],
                "description": "Excluir recursos de plataforma via API externa.",
                "available": False,
                "destructive": True,
                "requires_read": True,
                "methods": [],
                "endpoints": [],
            },
        ],
    },
    {
        "key": "tags",
        "label": "Tags",
        "description": "Taxonomia, atribuições e automações de tags.",
        "actions": [
            {
                "key": "tags.read",
                "action": "read",
                "label": ACTION_LABELS["read"],
                "description": "Listar tags e taxonomia associada.",
                "available": True,
                "destructive": False,
                "requires_read": False,
                "methods": ["GET"],
                "endpoints": ["/external/tags"],
            },
            {
                "key": "tags.create",
                "action": "create",
                "label": ACTION_LABELS["create"],
                "description": "Criar tags via API externa.",
                "available": True,
                "destructive": False,
                "requires_read": True,
                "methods": ["POST"],
                "endpoints": ["/external/tags"],
            },
            {
                "key": "tags.update",
                "action": "update",
                "label": ACTION_LABELS["update"],
                "description": "Editar tags via API externa.",
                "available": True,
                "destructive": False,
                "requires_read": True,
                "methods": ["PATCH", "PUT"],
                "endpoints": ["/external/tags/{tag_id}"],
            },
            {
                "key": "tags.delete",
                "action": "delete",
                "label": ACTION_LABELS["delete"],
                "description": "Excluir tags via API externa.",
                "available": True,
                "destructive": True,
                "requires_read": True,
                "methods": ["DELETE"],
                "endpoints": ["/external/tags/{tag_id}"],
            },
        ],
    },
    {
        "key": "glossary",
        "label": "Glossário",
        "description": "Termos e categorias do glossário.",
        "actions": [
            {
                "key": "glossary.read",
                "action": "read",
                "label": ACTION_LABELS["read"],
                "description": "Listar termos e categorias do glossário.",
                "available": True,
                "destructive": False,
                "requires_read": False,
                "methods": ["GET"],
                "endpoints": ["/external/glossary/terms"],
            },
            {
                "key": "glossary.create",
                "action": "create",
                "label": ACTION_LABELS["create"],
                "description": "Criar termos do glossário via API externa.",
                "available": True,
                "destructive": False,
                "requires_read": True,
                "methods": ["POST"],
                "endpoints": ["/external/glossary/terms"],
            },
            {
                "key": "glossary.update",
                "action": "update",
                "label": ACTION_LABELS["update"],
                "description": "Editar termos do glossário via API externa.",
                "available": True,
                "destructive": False,
                "requires_read": True,
                "methods": ["PATCH", "PUT"],
                "endpoints": ["/external/glossary/terms/{term_id}"],
            },
            {
                "key": "glossary.delete",
                "action": "delete",
                "label": ACTION_LABELS["delete"],
                "description": "Excluir termos do glossário via API externa.",
                "available": True,
                "destructive": True,
                "requires_read": True,
                "methods": ["DELETE"],
                "endpoints": ["/external/glossary/terms/{term_id}"],
            },
        ],
    },
]

_EXTERNAL_API_SCOPE_LOOKUP: dict[str, dict[str, object]] = {
    str(action["key"]): action
    for group in EXTERNAL_API_SCOPE_GROUPS
    for action in group["actions"]  # type: ignore[index]
}


@dataclass(frozen=True)
class ApiKeyAuthResult:
    key: PlatformApiKey
    scopes: set[str]
    token: str


def list_external_api_scopes() -> list[dict[str, object]]:
    return EXTERNAL_API_SCOPE_GROUPS


def _supported_scope_keys() -> set[str]:
    return {key for key, action in _EXTERNAL_API_SCOPE_LOOKUP.items() if bool(action.get("available"))}


def _normalize_scope_tokens(tokens: list[str] | None) -> list[str]:
    normalized: list[str] = []
    for token in tokens or []:
        value = str(token or "").strip().lower()
        if not value:
            continue
        normalized.append(value)
    return list(dict.fromkeys(normalized))


def _normalize_environment(value: str | None) -> str:
    normalized = (value or "").strip().lower()
    return normalized or DEFAULT_KEY_ENVIRONMENT


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _normalize_allowed_ips(values: list[str] | None) -> list[str]:
    normalized: list[str] = []
    for raw in values or []:
        value = str(raw or "").strip()
        if not value:
            continue
        try:
            if "/" in value:
                network = ipaddress.ip_network(value, strict=False)
                if network.prefixlen == 0:
                    raise ValueError("broad network not allowed")
                value = str(network)
            else:
                ip = ipaddress.ip_address(value)
                if ip.is_unspecified:
                    raise ValueError("unspecified address not allowed")
                value = str(ip)
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"IP ou rede inválida: {value}.",
            ) from exc
        normalized.append(value)
    return list(dict.fromkeys(normalized))


def _expand_scope_dependencies(tokens: list[str]) -> list[str]:
    normalized: list[str] = []
    present: set[str] = set()
    for scope in list(dict.fromkeys(tokens)):
        if scope not in present:
            normalized.append(scope)
            present.add(scope)
        action = _EXTERNAL_API_SCOPE_LOOKUP.get(scope)
        if not action:
            continue
        if action.get("requires_read"):
            domain = scope.split(".", 1)[0]
            read_scope = f"{domain}.read"
            if read_scope not in present:
                normalized.append(read_scope)
                present.add(read_scope)
    return normalized


def validate_scope_keys(scopes: list[str]) -> list[str]:
    normalized = _expand_scope_dependencies(_normalize_scope_tokens(scopes))
    invalid = sorted({item for item in normalized if item not in _supported_scope_keys()})
    if invalid:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Escopos inválidos ou indisponíveis: {', '.join(invalid)}.",
        )
    return normalized


def scope_actions(scopes: list[str]) -> set[str]:
    actions: set[str] = set()
    for scope in _normalize_scope_tokens(scopes):
        parts = scope.split(".", 1)
        if len(parts) != 2:
            continue
        actions.add(parts[1])
    return actions


def _expiration_policy_for_scopes(scopes: list[str]) -> tuple[int | None, str | None]:
    actions = scope_actions(scopes)
    if "delete" in actions:
        return DELETE_EXPIRATION_MAX_DAYS, "delete"
    if actions.intersection({"create", "update"}):
        return WRITE_EXPIRATION_MAX_DAYS, "write"
    return None, None


def validate_expiration_policy(
    *,
    scopes: list[str],
    expires_at: datetime | None,
    expires_in_days: int | None,
) -> tuple[datetime | None, int | None]:
    max_days, policy_name = _expiration_policy_for_scopes(scopes)
    if max_days is None:
        return expires_at, expires_in_days

    if expires_in_days is None and expires_at is None:
        return None, max_days

    if expires_in_days is not None and expires_in_days > max_days:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Chaves com permissão de {policy_name} devem expirar em até {max_days} dias.",
        )

    if expires_at is not None:
        max_expiration = datetime.now(timezone.utc) + timedelta(days=max_days)
        if expires_at > max_expiration:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Chaves com permissão de {policy_name} devem expirar em até {max_days} dias.",
            )
    return expires_at, expires_in_days


def validate_status_value(status_value: str) -> str:
    value = (status_value or "").strip().lower() or STATUS_ACTIVE
    if value not in {STATUS_ACTIVE, STATUS_INACTIVE, STATUS_REVOKED}:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Status inválido.")
    return value


def _effective_status(key: PlatformApiKey) -> str:
    if key.status == STATUS_REVOKED:
        return STATUS_REVOKED
    expires_at = _as_utc(key.expires_at)
    if expires_at and expires_at <= datetime.now(timezone.utc):
        return "expired"
    if key.status == STATUS_INACTIVE:
        return STATUS_INACTIVE
    return STATUS_ACTIVE


def _ip_matches_rule(ip_value: str, rule: str) -> bool:
    try:
        ip = ipaddress.ip_address(ip_value)
    except ValueError:
        return False
    try:
        if "/" in rule:
            return ip in ipaddress.ip_network(rule, strict=False)
        return ip == ipaddress.ip_address(rule)
    except ValueError:
        return False


def is_api_key_ip_allowed(key: PlatformApiKey, request_ip: str | None) -> bool:
    # Use raw stored entries (do not re-run the strict validator, which raises on read).
    raw_entries = [str(value).strip() for value in (key.allowed_ips_json or []) if str(value or "").strip()]
    if not raw_entries:
        return True  # no IP restriction configured
    if not request_ip:
        return False
    # Fail closed: if an allow-list is configured, a non-matching (or unparseable) IP is denied.
    return any(_ip_matches_rule(request_ip, rule) for rule in raw_entries)


def _permission_summary(scopes: list[str]) -> ExternalApiPermissionSummaryOut:
    counts = {"read": 0, "create": 0, "update": 0, "delete": 0}
    selected_domains: dict[str, set[str]] = {}
    for scope in _normalize_scope_tokens(scopes):
        parts = scope.split(".", 1)
        if len(parts) != 2:
            continue
        domain, action = parts
        if action not in counts:
            continue
        selected_domains.setdefault(domain, set()).add(action)
    for actions in selected_domains.values():
        for action in actions:
            counts[action] += 1
    risk_level: Literal["low", "medium", "high"]
    if counts["delete"]:
        risk_level = "high"
    elif counts["create"] or counts["update"]:
        risk_level = "medium"
    else:
        risk_level = "low"
    return ExternalApiPermissionSummaryOut(
        read=counts["read"],
        create=counts["create"],
        update=counts["update"],
        delete=counts["delete"],
        total=len(selected_domains),
        risk_level=risk_level,
    )


def serialize_api_key_out(key: PlatformApiKey) -> ExternalApiKeyOut:
    scopes = list(key.scopes_json or [])
    return ExternalApiKeyOut(
        id=key.id,
        public_id=key.public_id,
        name=key.name,
        description=key.description,
        status=key.status,
        effective_status=_effective_status(key),
        scopes=scopes,
        permission_summary=_permission_summary(scopes),
        environment=_normalize_environment(getattr(key, "environment", None)),
        allowed_ips=_normalize_allowed_ips(getattr(key, "allowed_ips_json", None)),
        token_prefix=key.token_prefix,
        expires_at=key.expires_at,
        created_at=key.created_at,
        updated_at=key.updated_at,
        last_used_at=key.last_used_at,
        last_used_ip=key.last_used_ip,
        last_used_user_agent=key.last_used_user_agent,
        usage_count=int(key.usage_count or 0),
        created_by_user_id=key.created_by_user_id,
        created_by_user_email=(key.created_by_user.email if key.created_by_user else None),
        created_by_user_name=(
            (key.created_by_user.name or key.created_by_user.full_name)
            if key.created_by_user
            else None
        ),
    )


def _generate_token() -> tuple[str, str, str, str]:
    public_id = secrets.token_hex(8)
    secret = secrets.token_urlsafe(32)
    token = f"{API_KEY_PREFIX}_{public_id}.{secret}"
    token_prefix = secret[:8]
    token_hash = hash_password(secret)
    return token, public_id, token_hash, token_prefix


def _apply_expiration(
    *,
    expires_at: datetime | None,
    expires_in_days: int | None,
) -> datetime | None:
    if expires_at is not None:
        return expires_at
    if expires_in_days is not None:
        return datetime.now(timezone.utc) + timedelta(days=max(int(expires_in_days), 1))
    return None


def create_api_key(
    session: Session,
    *,
    name: str,
    description: str | None,
    scopes: list[str],
    environment: str,
    allowed_ips: list[str],
    status_value: str,
    expires_at: datetime | None,
    expires_in_days: int | None,
    created_by: User | None,
) -> tuple[PlatformApiKey, str]:
    token, public_id, token_hash, token_prefix = _generate_token()
    normalized_scopes = validate_scope_keys(scopes)
    validated_expires_at, validated_expires_in_days = validate_expiration_policy(
        scopes=normalized_scopes,
        expires_at=expires_at,
        expires_in_days=expires_in_days,
    )
    key = PlatformApiKey(
        public_id=public_id,
        name=name.strip(),
        description=description.strip() if description else None,
        status=validate_status_value(status_value),
        scopes_json=normalized_scopes,
        environment=_normalize_environment(environment),
        allowed_ips_json=_normalize_allowed_ips(allowed_ips),
        token_hash=token_hash,
        token_prefix=token_prefix,
        expires_at=_apply_expiration(expires_at=validated_expires_at, expires_in_days=validated_expires_in_days),
        created_by_user_id=created_by.id if created_by else None,
    )
    session.add(key)
    session.flush()
    return key, token


def update_api_key(
    key: PlatformApiKey,
    *,
    name: str | None,
    description: str | None,
    scopes: list[str] | None,
    environment: str | None,
    allowed_ips: list[str] | None,
    status_value: str | None,
    expires_at: datetime | None,
    expires_in_days: int | None,
) -> None:
    if name is not None:
        key.name = name.strip()
    if description is not None:
        key.description = description.strip() or None
    if scopes is not None:
        validated_scopes = validate_scope_keys(scopes)
        key.scopes_json = validated_scopes
        max_days, policy_name = _expiration_policy_for_scopes(validated_scopes)
        if max_days is None:
            if expires_at is not None or expires_in_days is not None:
                key.expires_at = _apply_expiration(expires_at=expires_at, expires_in_days=expires_in_days)
        elif expires_at is None and expires_in_days is None:
            if key.expires_at is None:
                key.expires_at = _apply_expiration(expires_at=None, expires_in_days=max_days)
            else:
                max_expiration = datetime.now(timezone.utc) + timedelta(days=max_days)
                if (_as_utc(key.expires_at) or max_expiration) > max_expiration:
                    raise HTTPException(
                        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                        detail=f"Chaves com permissão de {policy_name} devem expirar em até {max_days} dias.",
                    )
        else:
            validated_expires_at, validated_expires_in_days = validate_expiration_policy(
                scopes=validated_scopes,
                expires_at=expires_at,
                expires_in_days=expires_in_days,
            )
            key.expires_at = _apply_expiration(
                expires_at=validated_expires_at,
                expires_in_days=validated_expires_in_days,
            )
    elif expires_at is not None or expires_in_days is not None:
        validated_expires_at, validated_expires_in_days = validate_expiration_policy(
            scopes=list(key.scopes_json or []),
            expires_at=expires_at,
            expires_in_days=expires_in_days,
        )
        key.expires_at = _apply_expiration(
            expires_at=validated_expires_at,
            expires_in_days=validated_expires_in_days,
        )
    if environment is not None:
        key.environment = _normalize_environment(environment)
    if allowed_ips is not None:
        key.allowed_ips_json = _normalize_allowed_ips(allowed_ips)
    if status_value is not None:
        key.status = validate_status_value(status_value)


def rotate_api_key(key: PlatformApiKey) -> str:
    token, public_id, token_hash, token_prefix = _generate_token()
    key.public_id = public_id
    key.token_hash = token_hash
    key.token_prefix = token_prefix
    return token


def revoke_api_key(key: PlatformApiKey) -> None:
    key.status = STATUS_REVOKED


def list_api_keys(session: Session, *, offset: int = 0, limit: int = 50) -> list[PlatformApiKey]:
    return session.scalars(
        select(PlatformApiKey)
        .order_by(PlatformApiKey.created_at.desc(), PlatformApiKey.id.desc())
        .offset(max(int(offset or 0), 0))
        .limit(max(1, min(int(limit or 50), 200)))
    ).all()


def get_api_key(session: Session, key_id: int) -> PlatformApiKey | None:
    return session.get(PlatformApiKey, key_id)


_DUMMY_TOKEN_HASH: str | None = None


def _dummy_token_hash() -> str:
    global _DUMMY_TOKEN_HASH
    if _DUMMY_TOKEN_HASH is None:
        _DUMMY_TOKEN_HASH = hash_password("dummy-timing-equalizer")
    return _DUMMY_TOKEN_HASH


def resolve_api_key_from_token(token: str, session: Session) -> ApiKeyAuthResult:
    raw = (token or "").strip()
    if not raw:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="API key ausente")
    if not raw.startswith(f"{API_KEY_PREFIX}_") or "." not in raw:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="API key inválida")
    prefix, secret = raw.split(".", 1)
    public_id = prefix.replace(f"{API_KEY_PREFIX}_", "", 1)
    if not public_id or not secret:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="API key inválida")

    # Single generic 401 for every post-lookup failure (not-found/revoked/inactive/expired/
    # bad-secret) so the response does not become an oracle for public_id existence/state.
    invalid = HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="API key inválida")

    key = session.scalar(select(PlatformApiKey).where(PlatformApiKey.public_id == public_id))
    if key is None:
        # Equalize timing with the verify path so absence is not detectable via response time.
        verify_password(secret, _dummy_token_hash())
        raise invalid

    if key.status in (STATUS_REVOKED, STATUS_INACTIVE):
        raise invalid
    expires_at = _as_utc(key.expires_at)
    if expires_at and expires_at <= datetime.now(timezone.utc):
        raise invalid
    if not verify_password(secret, key.token_hash):
        raise invalid

    scopes = set(_normalize_scope_tokens(key.scopes_json))
    return ApiKeyAuthResult(key=key, scopes=scopes, token=raw)


def ensure_scopes(result: ApiKeyAuthResult, required: list[str]) -> None:
    if not required:
        return
    normalized = {token.lower() for token in required}
    if not normalized.intersection(result.scopes):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="API key sem escopo suficiente")
