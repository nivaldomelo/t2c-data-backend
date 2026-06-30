from __future__ import annotations

import re
import unicodedata
from datetime import datetime
from typing import Any

from t2c_data.models.auth import User
from t2c_data.models.catalog import ColumnEntity, TableEntity

SENSITIVITY_LEVELS = ["public", "internal", "confidential", "restricted", "personal_data"]
LEGAL_BASIS_OPTIONS = [
    "consent",
    "contract",
    "legal_obligation",
    "legitimate_interest",
    "exercise_of_rights",
    "credit_protection",
    "other",
]
ACCESS_SCOPE_OPTIONS = ["public", "authenticated", "confidential", "restricted", "personal_data"]
ACCESS_ROLE_OPTIONS = ["admin", "governance", "data_owner", "analyst", "reader"]

SENSITIVITY_LABELS = {
    "public": "Público",
    "internal": "Interno",
    "confidential": "Confidencial",
    "restricted": "Restrito",
    "personal_data": "Dado pessoal",
}

LEGAL_BASIS_LABELS = {
    "consent": "Consentimento",
    "contract": "Execução de contrato",
    "legal_obligation": "Obrigação legal/regulatória",
    "legitimate_interest": "Legítimo interesse",
    "exercise_of_rights": "Exercício regular de direitos",
    "credit_protection": "Proteção ao crédito",
    "other": "Outro",
}

ACCESS_SCOPE_LABELS = {
    "public": "Público",
    "authenticated": "Autenticados",
    "confidential": "Confidencial",
    "restricted": "Restrito",
    "personal_data": "Dado pessoal",
}

ACCESS_ROLE_LABELS = {
    "admin": "Administrador",
    "governance": "Governança",
    "data_owner": "Data Owner",
    "analyst": "Analista",
    "reader": "Leitor",
}

POSSIBLE_PERSONAL_DATA_HINTS = [
    "cpf",
    "cnpj",
    "telefone",
    "phone",
    "email",
    "mail",
    "nome",
    "name",
    "endereco",
    "endereço",
    "address",
    "rg",
    "data_nascimento",
    "nascimento",
    "birth",
    "renda",
    "salario",
    "salary",
    "income",
    "banco",
    "bank",
    "conta",
    "account",
    "agencia",
    "agency",
    "boleto",
    "parcela",
    "installment",
    "saldo_devedor",
    "credit_value",
    "valor_credito",
    "contrato",
    "proposal",
    "proposta",
    "quota",
    "cota",
]

PERSONAL_DATA_SIGNAL_RULES = [
    {
        "signal": "cpf",
        "reason": "Nome da coluna sugere identificação por CPF.",
        "confidence": "high",
        "suggested_classification": "personal_data",
    },
    {
        "signal": "cnpj",
        "reason": "Nome da coluna sugere identificação por CNPJ.",
        "confidence": "high",
        "suggested_classification": "personal_data",
    },
    {
        "signal": "email",
        "reason": "Nome da coluna sugere endereço de e-mail.",
        "confidence": "high",
        "suggested_classification": "personal_data",
    },
    {
        "signal": "mail",
        "reason": "Nome da coluna sugere endereço de e-mail.",
        "confidence": "high",
        "suggested_classification": "personal_data",
    },
    {
        "signal": "telefone",
        "reason": "Nome da coluna sugere número de telefone.",
        "confidence": "high",
        "suggested_classification": "personal_data",
    },
    {
        "signal": "phone",
        "reason": "Nome da coluna sugere número de telefone.",
        "confidence": "high",
        "suggested_classification": "personal_data",
    },
    {
        "signal": "rg",
        "reason": "Nome da coluna sugere documento de identificação.",
        "confidence": "high",
        "suggested_classification": "personal_data",
    },
    {
        "signal": "data_nascimento",
        "reason": "Nome da coluna sugere data de nascimento.",
        "confidence": "medium",
        "suggested_classification": "personal_data",
    },
    {
        "signal": "nascimento",
        "reason": "Nome da coluna sugere data de nascimento.",
        "confidence": "medium",
        "suggested_classification": "personal_data",
    },
    {
        "signal": "birth",
        "reason": "Nome da coluna sugere data de nascimento.",
        "confidence": "medium",
        "suggested_classification": "personal_data",
    },
    {
        "signal": "nome",
        "reason": "Nome da coluna sugere identificação nominal de pessoa.",
        "confidence": "medium",
        "suggested_classification": "personal_data",
    },
    {
        "signal": "name",
        "reason": "Nome da coluna sugere identificação nominal de pessoa.",
        "confidence": "medium",
        "suggested_classification": "personal_data",
    },
    {
        "signal": "endereco",
        "reason": "Nome da coluna sugere endereço residencial ou comercial.",
        "confidence": "medium",
        "suggested_classification": "personal_data",
    },
    {
        "signal": "endereço",
        "reason": "Nome da coluna sugere endereço residencial ou comercial.",
        "confidence": "medium",
        "suggested_classification": "personal_data",
    },
    {
        "signal": "address",
        "reason": "Nome da coluna sugere endereço residencial ou comercial.",
        "confidence": "medium",
        "suggested_classification": "personal_data",
    },
    {
        "signal": "renda",
        "reason": "Nome da coluna sugere valor de renda ou faturamento.",
        "confidence": "high",
        "suggested_classification": "personal_data",
    },
    {
        "signal": "salary",
        "reason": "Nome da coluna sugere valor de renda ou salário.",
        "confidence": "high",
        "suggested_classification": "personal_data",
    },
    {
        "signal": "income",
        "reason": "Nome da coluna sugere valor de renda ou salário.",
        "confidence": "high",
        "suggested_classification": "personal_data",
    },
    {
        "signal": "banco",
        "reason": "Nome da coluna sugere dado bancário.",
        "confidence": "high",
        "suggested_classification": "personal_data",
    },
    {
        "signal": "bank",
        "reason": "Nome da coluna sugere dado bancário.",
        "confidence": "high",
        "suggested_classification": "personal_data",
    },
    {
        "signal": "conta",
        "reason": "Nome da coluna sugere conta bancária ou identificador financeiro.",
        "confidence": "high",
        "suggested_classification": "personal_data",
    },
    {
        "signal": "boleto",
        "reason": "Nome da coluna sugere identificador de cobrança financeira.",
        "confidence": "high",
        "suggested_classification": "personal_data",
    },
    {
        "signal": "parcela",
        "reason": "Nome da coluna sugere parcela ou pagamento recorrente.",
        "confidence": "high",
        "suggested_classification": "personal_data",
    },
    {
        "signal": "saldo_devedor",
        "reason": "Nome da coluna sugere saldo devedor ou valor financeiro.",
        "confidence": "high",
        "suggested_classification": "personal_data",
    },
    {
        "signal": "credito",
        "reason": "Nome da coluna sugere valor de crédito ou financiamento.",
        "confidence": "high",
        "suggested_classification": "personal_data",
    },
    {
        "signal": "contrato",
        "reason": "Nome da coluna sugere contrato com valor financeiro ou vínculo pessoal.",
        "confidence": "high",
        "suggested_classification": "personal_data",
    },
    {
        "signal": "proposta",
        "reason": "Nome da coluna sugere proposta ou identificação comercial/financeira.",
        "confidence": "high",
        "suggested_classification": "personal_data",
    },
    {
        "signal": "cota",
        "reason": "Nome da coluna sugere cota ou participação financeira.",
        "confidence": "high",
        "suggested_classification": "personal_data",
    },
]


def _normalize_token(value: str | None) -> str:
    raw = (value or "").strip().lower()
    if not raw:
        return ""
    ascii_value = unicodedata.normalize("NFKD", raw).encode("ascii", "ignore").decode("ascii")
    ascii_value = re.sub(r"[^a-z0-9]+", "_", ascii_value).strip("_")
    aliases = {
        "visualizador": "reader",
        "viewer": "reader",
        "leitor": "reader",
        "reader": "reader",
        "editor": "analyst",
        "analista": "analyst",
        "analyst": "analyst",
        "governanca": "governance",
        "governance": "governance",
        "data_owner": "data_owner",
        "data_owners": "data_owner",
        "owner": "data_owner",
        "dataowner": "data_owner",
        "administrador": "admin",
        "admin": "admin",
        "autenticado": "authenticated",
        "authenticated": "authenticated",
    }
    return aliases.get(ascii_value, ascii_value)


def normalize_access_role(value: str | None) -> str:
    return _normalize_token(value)


def normalize_access_roles(values: list[str] | None) -> list[str]:
    normalized = [normalize_access_role(value) for value in (values or [])]
    return [value for value in dict.fromkeys(normalized) if value in ACCESS_ROLE_OPTIONS]


def default_access_scope_for_sensitivity(sensitivity_level: str | None) -> str:
    normalized = _normalize_token(sensitivity_level)
    mapping = {
        "public": "public",
        "internal": "authenticated",
        "confidential": "confidential",
        "restricted": "restricted",
        "personal_data": "personal_data",
    }
    return mapping.get(normalized, "authenticated")


def role_tokens_for_user(user: User | None, table: TableEntity | None = None) -> set[str]:
    tokens = {"authenticated"} if user else set()
    if not user:
        return tokens
    for role in user.roles:
        normalized = normalize_access_role(role.name)
        if normalized:
            tokens.add(normalized)
    if "admin" in tokens:
        tokens.add("governance")
    if table is not None and user.email:
        owner_email = (table.data_owner.email if table.data_owner else None) or table.owner_email
        if owner_email and owner_email.strip().lower() == user.email.strip().lower():
            tokens.add("data_owner")
    return tokens


def _can_view_table_privacy_only(
    user: User | None,
    table: TableEntity,
) -> bool:
    from t2c_data.features.platform.visibility import table_visibility_decision_from_entity

    visibility_decision = table_visibility_decision_from_entity(table, user=user)
    if not visibility_decision.visible:
        return False
    scope = _normalize_token(table.access_scope) or default_access_scope_for_sensitivity(table.sensitivity_level)
    allowed_roles = set(normalize_access_roles(table.access_roles))
    tokens = role_tokens_for_user(user, table)

    if scope == "public":
        return True
    if not user:
        return False
    if "admin" in tokens:
        return True
    if allowed_roles and tokens.intersection(allowed_roles):
        return True

    base_roles = {
        "authenticated": {"authenticated"},
        "confidential": {"governance", "data_owner", "analyst"},
        "restricted": {"governance", "data_owner", "stewardship"},
        "personal_data": {"governance", "data_owner", "stewardship"},
    }
    return bool(tokens.intersection(base_roles.get(scope, {"authenticated"})))


def can_view_table(
    user: User | None,
    table: TableEntity,
) -> bool:
    from t2c_data.features.access_control.policy import can_view_table as can_view_table_by_scope

    if not _can_view_table_privacy_only(user, table):
        return False
    return can_view_table_by_scope(user, table)


def can_edit_privacy(user: User | None, table: TableEntity | None = None) -> bool:
    if not user:
        return False
    tokens = role_tokens_for_user(user)
    if not bool(tokens.intersection({"admin", "governance", "analyst"})):
        return False
    if table is None:
        return True
    from t2c_data.features.access_control.abac import can_access_resource

    return can_access_resource(user, action="update", table=table)


def suggest_possible_personal_data(columns: list[ColumnEntity] | None) -> bool:
    return bool(suspected_personal_data_columns(columns))


def suspected_personal_data_columns(columns: list[ColumnEntity] | None) -> list[dict[str, str]]:
    if not columns:
        return []
    suspects: list[dict[str, str]] = []
    for column in columns:
        haystack = _normalize_token(column.name)
        classification = getattr(column, "classification", None)
        if classification is not None and (
            bool(getattr(classification, "is_personal_data", False))
            or bool(getattr(classification, "is_sensitive_data", False))
            or bool(getattr(classification, "is_financial_data", False))
        ):
            suspects.append(
                {
                    "column_name": column.name,
                    "data_type": column.data_type,
                    "signal": str(getattr(classification, "taxonomy_key", "classification")),
                    "reason": f"Classificação persistida como {getattr(classification, 'taxonomy_label', 'dado sensível')}.",
                    "suggested_classification": "personal_data",
                    "confidence": "high" if bool(getattr(classification, "is_sensitive_data", False)) else "medium",
                }
            )
            continue
        for rule in PERSONAL_DATA_SIGNAL_RULES:
            signal = _normalize_token(rule["signal"])
            if signal and signal in haystack:
                suspects.append(
                    {
                        "column_name": column.name,
                        "data_type": column.data_type,
                        "signal": rule["signal"],
                        "reason": rule["reason"],
                        "suggested_classification": rule["suggested_classification"],
                        "confidence": rule["confidence"],
                    }
                )
                break
    return suspects


def sensitivity_label(value: str | None) -> str:
    normalized = _normalize_token(value)
    return SENSITIVITY_LABELS.get(normalized, "Não classificado")


def legal_basis_label(value: str | None) -> str | None:
    normalized = _normalize_token(value)
    if not normalized:
        return None
    return LEGAL_BASIS_LABELS.get(normalized, value)


def access_scope_label(value: str | None, sensitivity_level: str | None = None) -> str:
    normalized = _normalize_token(value) or default_access_scope_for_sensitivity(sensitivity_level)
    return ACCESS_SCOPE_LABELS.get(normalized, "Autenticados")


def access_role_labels(values: list[str] | None) -> list[str]:
    return [ACCESS_ROLE_LABELS.get(role, role) for role in normalize_access_roles(values)]


def privacy_summary_payload(table: TableEntity) -> dict[str, Any]:
    return {
        "sensitivity_level": table.sensitivity_level,
        "sensitivity_label": sensitivity_label(table.sensitivity_level),
        "has_personal_data": table.has_personal_data,
        "has_sensitive_personal_data": table.has_sensitive_personal_data,
        "legal_basis": table.legal_basis,
        "legal_basis_label": legal_basis_label(table.legal_basis),
        "privacy_purpose": table.privacy_purpose,
        "retention_policy": table.retention_policy,
        "is_masked": table.is_masked,
        "external_sharing": table.external_sharing,
        "access_scope": table.access_scope or default_access_scope_for_sensitivity(table.sensitivity_level),
        "access_scope_label": access_scope_label(table.access_scope, table.sensitivity_level),
        "access_roles": normalize_access_roles(table.access_roles),
        "access_role_labels": access_role_labels(table.access_roles),
        "privacy_notes": table.privacy_notes,
        "privacy_reviewed_by_user_id": table.privacy_reviewed_by_user_id,
        "privacy_reviewed_by_user_name": table.privacy_reviewed_by_user_name,
        "privacy_reviewed_by_user_email": table.privacy_reviewed_by_user_email,
        "privacy_reviewed_at": table.privacy_reviewed_at,
        "possible_personal_data": suggest_possible_personal_data(getattr(table, "columns", None)),
    }


def privacy_change_snapshot(table: TableEntity) -> dict[str, Any]:
    return {
        "sensitivity_level": table.sensitivity_level,
        "has_personal_data": table.has_personal_data,
        "has_sensitive_personal_data": table.has_sensitive_personal_data,
        "legal_basis": table.legal_basis,
        "privacy_purpose": table.privacy_purpose,
        "retention_policy": table.retention_policy,
        "is_masked": table.is_masked,
        "external_sharing": table.external_sharing,
        "access_scope": table.access_scope,
        "access_roles": normalize_access_roles(table.access_roles),
        "privacy_notes": table.privacy_notes,
        "privacy_reviewed_by_user_id": table.privacy_reviewed_by_user_id,
        "privacy_reviewed_at": table.privacy_reviewed_at.isoformat() if isinstance(table.privacy_reviewed_at, datetime) else None,
    }
