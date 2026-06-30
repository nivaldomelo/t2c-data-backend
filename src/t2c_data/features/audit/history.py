from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from t2c_data.features.privacy_access import SENSITIVITY_LABELS
from t2c_data.features.audit.support import AuditFieldChange


def _normalize_text(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    return normalized or None


def owner_value(*, owner_id: int | None, owner_name: str | None, owner_email: str | None) -> dict[str, Any] | None:
    if owner_id is None and not (owner_name or owner_email):
        return None
    return {
        "id": owner_id,
        "label": owner_name or owner_email or "Sem owner",
        "email": owner_email,
    }


def sensitivity_value(value: str | None) -> dict[str, Any] | None:
    if not value:
        return None
    return {"value": value, "label": SENSITIVITY_LABELS.get(value, value)}


@dataclass(slots=True)
class TableHistorySnapshot:
    description_manual: str | None
    owner: dict[str, Any] | None
    lifecycle_status: str | None
    sensitivity_level: dict[str, Any] | None
    has_personal_data: bool | None
    has_sensitive_personal_data: bool | None
    legal_basis: str | None
    privacy_purpose: str | None
    retention_policy: str | None
    is_masked: bool | None
    external_sharing: bool | None
    access_scope: str | None
    access_roles: list[str] | None
    privacy_notes: str | None


def build_table_history_snapshot(table: Any) -> TableHistorySnapshot:
    access_roles = list(table.access_roles) if isinstance(getattr(table, "access_roles", None), list) else None
    return TableHistorySnapshot(
        description_manual=_normalize_text(getattr(table, "description_manual", None)),
        owner=owner_value(
            owner_id=getattr(table, "data_owner_id", None),
            owner_name=getattr(table, "owner", None),
            owner_email=getattr(table, "owner_email", None),
        ),
        lifecycle_status=getattr(table, "lifecycle_status", None),
        sensitivity_level=sensitivity_value(getattr(table, "sensitivity_level", None)),
        has_personal_data=getattr(table, "has_personal_data", None),
        has_sensitive_personal_data=getattr(table, "has_sensitive_personal_data", None),
        legal_basis=getattr(table, "legal_basis", None),
        privacy_purpose=_normalize_text(getattr(table, "privacy_purpose", None)),
        retention_policy=getattr(table, "retention_policy", None),
        is_masked=getattr(table, "is_masked", None),
        external_sharing=getattr(table, "external_sharing", None),
        access_scope=getattr(table, "access_scope", None),
        access_roles=access_roles,
        privacy_notes=_normalize_text(getattr(table, "privacy_notes", None)),
    )


def table_history_changes(before: TableHistorySnapshot, after: TableHistorySnapshot) -> list[AuditFieldChange]:
    changes: list[AuditFieldChange] = []
    if before.description_manual != after.description_manual:
        changes.append(
            AuditFieldChange(
                field_name="description",
                before=before.description_manual,
                after=after.description_manual,
                change_type="update",
            )
        )
    if before.owner != after.owner:
        change_type = "assign"
        if before.owner and not after.owner:
            change_type = "unassign"
        elif before.owner and after.owner:
            change_type = "update"
        changes.append(AuditFieldChange(field_name="owner", before=before.owner, after=after.owner, change_type=change_type))
    for field_name in (
        "lifecycle_status",
        "has_personal_data",
        "has_sensitive_personal_data",
        "legal_basis",
        "privacy_purpose",
        "retention_policy",
        "is_masked",
        "external_sharing",
        "access_scope",
        "access_roles",
        "privacy_notes",
    ):
        before_value = getattr(before, field_name)
        after_value = getattr(after, field_name)
        if before_value != after_value:
            changes.append(AuditFieldChange(field_name=field_name, before=before_value, after=after_value, change_type="update"))
    if before.sensitivity_level != after.sensitivity_level:
        changes.append(
            AuditFieldChange(
                field_name="classification",
                before=before.sensitivity_level,
                after=after.sensitivity_level,
                change_type="reclassify",
            )
        )
    return changes


def certification_changes(*, before: dict[str, Any], after: dict[str, Any]) -> list[AuditFieldChange]:
    changes: list[AuditFieldChange] = []
    for field_name in (
        "certification_criticality",
        "certification_badges",
        "certification_notes",
        "certification_submitted_at",
        "certification_review_at",
        "certification_expires_at",
    ):
        if before.get(field_name) != after.get(field_name):
            changes.append(
                AuditFieldChange(field_name=field_name, before=before.get(field_name), after=after.get(field_name), change_type="update")
            )
    if before.get("certification_status") != after.get("certification_status"):
        old_status = before.get("certification_status")
        new_status = after.get("certification_status")
        change_type = "update"
        if new_status == "certified":
            change_type = "certify"
        elif old_status == "certified" and new_status != "certified":
            change_type = "decertify"
        changes.append(
            AuditFieldChange(
                field_name="certification_status",
                before=old_status,
                after=new_status,
                change_type=change_type,
            )
        )
    return changes


__all__ = [
    "TableHistorySnapshot",
    "build_table_history_snapshot",
    "certification_changes",
    "owner_value",
    "sensitivity_value",
    "table_history_changes",
]
