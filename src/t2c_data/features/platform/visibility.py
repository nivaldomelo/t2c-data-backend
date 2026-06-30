from __future__ import annotations

from dataclasses import dataclass
from copy import deepcopy

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, object_session

from t2c_data.models.auth import User
from t2c_data.models.catalog import TableEntity
from t2c_data.models.platform import AssetVisibilityRule
from t2c_data.features.privacy_access.policy import normalize_access_role
from t2c_data.features.platform.sensitive_data import mask_payload_by_policy, redact_sensitive_metadata


@dataclass(slots=True)
class VisibilitySubject:
    table_id: int
    domain_name: str | None = None
    classification: str | None = None


@dataclass(slots=True)
class VisibilityDecision:
    visible: bool
    masked: bool = False


def _masked_copy(payload: dict[str, object], *, updates: dict[str, object]) -> dict[str, object]:
    item = deepcopy(payload)
    item.update(updates)
    return item


def _role_names(user: User | None) -> set[str]:
    if user is None:
        return set()
    return {normalize_access_role(role.name) for role in getattr(user, "roles", []) if normalize_access_role(role.name)}


def _normalize_token(value: str | None) -> str:
    return normalize_access_role(value)


def user_can_manage_visibility(user: User | None) -> bool:
    return "admin" in _role_names(user)


def _matches_rule(rule: AssetVisibilityRule, subject: VisibilitySubject) -> bool:
    scope = (rule.rule_scope or "asset").strip().lower()
    if scope == "asset":
        return rule.entity_type == "table" and rule.entity_id == subject.table_id
    if scope == "domain":
        return bool(subject.domain_name) and _normalize_token(subject.domain_name) == _normalize_token(rule.match_value)
    if scope == "classification":
        return bool(subject.classification) and _normalize_token(subject.classification) == _normalize_token(rule.match_value)
    return False


def _allows_user(rule: AssetVisibilityRule, *, user: User | None, role_names: set[str], user_id: int | None) -> bool:
    if rule.allowed_user_id is not None and rule.allowed_user_id == user_id:
        return True
    if rule.allowed_role is not None and _normalize_token(rule.allowed_role) in role_names:
        return True
    return False


def load_visibility_state_map(session: Session, subjects: list[VisibilitySubject], *, user: User | None) -> dict[int, VisibilityDecision]:
    if not subjects:
        return {}
    if "admin" in _role_names(user):
        return {subject.table_id: VisibilityDecision(visible=True, masked=False) for subject in subjects}

    table_ids = [subject.table_id for subject in subjects]
    domain_tokens = sorted({_normalize_token(subject.domain_name) for subject in subjects if subject.domain_name})
    classification_tokens = sorted({_normalize_token(subject.classification) for subject in subjects if subject.classification})

    stmt = select(AssetVisibilityRule).where(AssetVisibilityRule.is_active.is_(True))
    try:
        rules = session.scalars(stmt).all()
    except SQLAlchemyError:
        return {subject.table_id: VisibilityDecision(visible=True, masked=False) for subject in subjects}
    if not rules:
        return {subject.table_id: VisibilityDecision(visible=True, masked=False) for subject in subjects}

    role_names = _role_names(user)
    user_id = getattr(user, "id", None)
    decisions: dict[int, VisibilityDecision] = {}
    for subject in subjects:
        matched_rules = [rule for rule in rules if _matches_rule(rule, subject)]
        if not matched_rules:
            decisions[subject.table_id] = VisibilityDecision(visible=True, masked=False)
            continue
        allowed_rules = [rule for rule in matched_rules if _allows_user(rule, user=user, role_names=role_names, user_id=user_id)]
        decisions[subject.table_id] = VisibilityDecision(
            visible=bool(allowed_rules),
            masked=any(rule.mask_sensitive_fields or rule.visibility_scope == "masked" for rule in allowed_rules),
        )
    return decisions


def load_table_visibility_map(session: Session, table_ids: list[int], *, user: User | None) -> dict[int, bool]:
    if not table_ids:
        return {}
    rows = session.execute(
        select(TableEntity.id, TableEntity.sensitivity_level).where(TableEntity.id.in_(table_ids))
    ).all()
    subjects = [VisibilitySubject(table_id=int(table_id), classification=classification) for table_id, classification in rows]
    states = load_visibility_state_map(session, subjects, user=user)
    return {table_id: state.visible for table_id, state in states.items()}


def filter_visible_table_ids(session: Session, table_ids: list[int], *, user: User | None) -> list[int]:
    visibility = load_table_visibility_map(session, table_ids, user=user)
    return [table_id for table_id in table_ids if visibility.get(int(table_id), True)]


def is_table_visible(session: Session, table_id: int, *, user: User | None) -> bool:
    return load_table_visibility_map(session, [table_id], user=user).get(table_id, True)


def visibility_for_profiles(session: Session, profiles, *, user: User | None) -> dict[int, VisibilityDecision]:
    subjects = [
        VisibilitySubject(
            table_id=profile.table_id,
            domain_name=getattr(profile, "domain_name", None),
            classification=getattr(profile, "sensitivity_level", None),
        )
        for profile in profiles
    ]
    return load_visibility_state_map(session, subjects, user=user)


def visibility_for_search_records(session: Session, records, *, user: User | None) -> dict[int, VisibilityDecision]:
    grouped: dict[int, VisibilitySubject] = {}
    for record in records:
        metadata = getattr(record, "metadata", {}) or {}
        table_id = metadata.get("table_id") if isinstance(metadata.get("table_id"), int) else None
        if record.entity_type in {"table", "classification"}:
            table_id = int(record.entity_id)
        if table_id is None:
            continue
        grouped[table_id] = VisibilitySubject(
            table_id=table_id,
            domain_name=getattr(record, "domain_name", None),
            classification=metadata.get("sensitivity_level") if isinstance(metadata.get("sensitivity_level"), str) else getattr(record, "classification", None),
        )
    return load_visibility_state_map(session, list(grouped.values()), user=user)


def table_visibility_decision_from_entity(table: TableEntity, *, user: User | None) -> VisibilityDecision:
    session = object_session(table)
    if session is None:
        return VisibilityDecision(visible=True, masked=False)
    subject = VisibilitySubject(table_id=table.id, domain_name=None, classification=getattr(table, "sensitivity_level", None))
    return load_visibility_state_map(session, [subject], user=user).get(table.id, VisibilityDecision(visible=True, masked=False))


def mask_search_result_payload(payload: dict[str, object]) -> dict[str, object]:
    item = deepcopy(payload)
    metadata = redact_sensitive_metadata(dict(item.get("metadata") or {}))
    if isinstance(metadata, dict):
        metadata["classification"] = None
        metadata["masked"] = True
    item["description"] = "Parte dos metadados deste ativo foi mascarada para o seu perfil."
    item["metadata"] = metadata
    badges = [badge for badge in item.get("badges", []) if not isinstance(badge, dict) or badge.get("tone") != "warning"]
    badges.append({"label": "Metadados sensíveis mascarados", "tone": "neutral"})
    item["badges"] = badges
    return item


def mask_dashboard_asset_payload(payload: dict[str, object]) -> dict[str, object]:
    return _masked_copy(
        payload,
        updates={
            "sensitivity_level": None,
            "sensitivity_label": "Mascarado para o seu perfil",
            "owner_name": "Visibilidade parcial",
            "owner_email": None,
            "classification_defined": False,
            "masked_sensitive_fields": True,
        },
    )


def mask_table_payload(payload: dict[str, object]) -> dict[str, object]:
    masked = _masked_copy(
        payload,
        updates={
            "sensitivity_level": None,
            "has_personal_data": False,
            "has_sensitive_personal_data": False,
            "legal_basis": None,
            "privacy_purpose": None,
            "retention_policy": None,
            "access_scope": None,
            "access_roles": [],
            "privacy_notes": None,
            "privacy_reviewed_by_user_id": None,
            "privacy_reviewed_by_user_name": None,
            "privacy_reviewed_by_user_email": None,
            "privacy_reviewed_at": None,
            "owner_email": None,
            "is_masked": True,
        },
    )
    if isinstance(masked.get("data_owner"), dict):
        masked["data_owner"] = mask_payload_by_policy(masked["data_owner"], can_view_sensitive=False)
    return masked


def mask_privacy_summary_payload(payload: dict[str, object]) -> dict[str, object]:
    masked = _masked_copy(
        payload,
        updates={
            "sensitivity_level": None,
            "sensitivity_label": "Mascarado para o seu perfil",
            "legal_basis": None,
            "legal_basis_label": None,
            "privacy_purpose": None,
            "retention_policy": None,
            "is_masked": True,
            "access_scope": None,
            "access_scope_label": "Mascarado",
            "access_roles": [],
            "access_role_labels": [],
            "privacy_notes": None,
            "privacy_reviewed_by_user_id": None,
            "privacy_reviewed_by_user_name": None,
            "privacy_reviewed_by_user_email": None,
            "privacy_reviewed_at": None,
            "owner_email": None,
        },
    )
    if isinstance(masked.get("data_owner"), dict):
        masked["data_owner"] = mask_payload_by_policy(masked["data_owner"], can_view_sensitive=False)
    return masked


def mask_certification_summary_payload(payload: dict[str, object]) -> dict[str, object]:
    masked = _masked_copy(
        payload,
        updates={
            "owner": None,
            "owner_email": None,
            "data_owner_id": None,
            "data_owner": None,
            "certification_criticality": None,
            "certification_badges": [],
            "certification_notes": None,
            "owner_reviewed_by_user_id": None,
            "owner_reviewed_by_user_name": None,
            "owner_reviewed_by_user_email": None,
        },
    )
    if isinstance(masked.get("data_owner"), dict):
        masked["data_owner"] = mask_payload_by_policy(masked["data_owner"], can_view_sensitive=False)
    return masked


def mask_incident_asset_context_payload(payload: dict[str, object]) -> dict[str, object]:
    masked = _masked_copy(
        payload,
        updates={
            "owner_name": "Visibilidade parcial",
            "owner_email": None,
            "owner_defined": False,
            "data_owner_id": None,
            "sensitivity_level": None,
            "sensitivity_label": "Mascarado para o seu perfil",
            "actions": [],
        },
    )
    return masked


def mask_audit_event_payload(payload: dict[str, object]) -> dict[str, object]:
    masked = _masked_copy(
        payload,
        updates={
            "actor_name": None,
            "actor_email": None,
            "before_value": "Mascarado para o seu perfil",
            "after_value": "Mascarado para o seu perfil",
            "metadata_json": redact_sensitive_metadata(
                {
                    "masked": True,
                    "message": "Os detalhes desta alteração foram mascarados para o seu perfil.",
                }
            ),
        },
    )
    return masked
