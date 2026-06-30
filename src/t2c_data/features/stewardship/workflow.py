from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import HTTPException, status
from sqlalchemy import case, func, select
from sqlalchemy.orm import Session, object_session, selectinload

from t2c_data.features.audit import certification_changes
from t2c_data.features.catalog.metadata_actions import patch_table_with_audit
from t2c_data.features.catalog.taxonomy_actions import update_table_glossary_terms_with_audit
from t2c_data.features.certification.api_support import build_certification_summary_out, validate_certification_patch
from t2c_data.features.dashboard.profile_loader import load_table_profiles
from t2c_data.features.governance import mark_owner_review, mark_privacy_review
from t2c_data.features.governance.scoring import build_governance_score_for_profile
from t2c_data.features.governance.settings import get_governance_settings_snapshot
from t2c_data.features.notifications import create_user_inbox_notification, resolve_inbox_notification_recipients
from t2c_data.models.auth import Role, User
from t2c_data.models.catalog import DataOwner, Database, Schema, TableEntity
from t2c_data.models.glossary import GlossaryAssignment, GlossaryTerm
from t2c_data.models.platform import DashboardAssetReadModel
from t2c_data.models.stewardship import StewardshipRequest, StewardshipRequestEvent
from t2c_data.schemas.catalog import TableCertificationPatch, TablePatch
from t2c_data.schemas.table_metadata import TableGlossaryTermsUpdateRequest
from t2c_data.services.audit import AuditFieldChange, log_field_changes, write_audit_log_sync

REQUEST_TYPE_OPTIONS = [
    {
        "value": "table_description",
        "label": "Descrição do ativo",
        "description": "Solicita criar ou revisar a descrição principal do ativo.",
    },
    {
        "value": "owner_assignment",
        "label": "Alteração de owner",
        "description": "Solicita definir ou trocar o responsável formal do ativo.",
    },
    {
        "value": "glossary_terms",
        "label": "Vínculo de termos",
        "description": "Solicita vincular ou ajustar termos de glossário do ativo.",
    },
    {
        "value": "certification_review",
        "label": "Revisão de certificação",
        "description": "Solicita iniciar ou revalidar a certificação do ativo.",
    },
    {
        "value": "owner_review",
        "label": "Revisão periódica de owner",
        "description": "Solicita confirmar a responsabilidade atual do ativo.",
    },
    {
        "value": "privacy_review",
        "label": "Revisão periódica de privacidade",
        "description": "Solicita revalidar classificação e controles de privacidade.",
    },
]

REQUEST_TYPE_META = {item["value"]: item for item in REQUEST_TYPE_OPTIONS}
STATUS_LABELS = {
    "pending": "Pendente de aprovação",
    "approved": "Aprovada",
    "rejected": "Rejeitada",
    "cancelled": "Cancelada",
}
ORIGIN_LABELS = {
    "manual": "Manual",
    "pending_center": "Central de pendências",
    "explorer": "Explorer",
}
EVENT_LABELS = {
    "created": "Solicitação criada",
    "approved": "Solicitação aprovada",
    "rejected": "Solicitação rejeitada",
    "cancelled": "Solicitação cancelada",
}
REVIEW_REQUEST_TYPES = {"owner_review", "privacy_review"}
APPROVER_SOURCE_LABELS = {
    "manual": "Definido manualmente",
    "suggested": "Sugerido automaticamente",
    "unassigned": "Sem sugestão automática",
}
APPROVER_RULE_LABELS = {
    "domain_area_rule": "Regra por domínio e área",
    "domain_rule": "Regra por domínio",
    "area_rule": "Regra por área",
    "previous_same_type": "Mesmo aprovador do histórico recente",
    "table_owner_email": "Data Owner mapeado para usuário ativo",
    "privacy_reviewer": "Último revisor de privacidade",
    "certification_history": "Histórico de certificação do ativo",
    "least_loaded_editor": "Fila balanceada entre editores e admins",
    "manual": "Escolha manual",
    "unassigned": "Sem regra automática aplicável",
}
SLA_STATUS_LABELS = {
    "within_sla": "Dentro do SLA",
    "due_soon": "Próximo do vencimento",
    "overdue": "Fora do SLA",
}
REQUEST_SLA_DAYS = {
    "table_description": 7,
    "owner_assignment": 5,
    "glossary_terms": 7,
    "certification_review": 7,
    "owner_review": 3,
    "privacy_review": 3,
}


def _display_name(user: User | None) -> str | None:
    if user is None:
        return None
    return user.name or user.full_name or user.email


def _table_with_context(db: Session, table_id: int) -> TableEntity:
    table = db.scalar(
        select(TableEntity)
        .options(
            selectinload(TableEntity.data_owner),
            selectinload(TableEntity.certification_submitted_by_user),
            selectinload(TableEntity.certification_decided_by_user),
            selectinload(TableEntity.owner_reviewed_by_user),
            selectinload(TableEntity.privacy_reviewed_by_user),
            selectinload(TableEntity.schema).selectinload(Schema.database).selectinload(Database.datasource),
        )
        .where(TableEntity.id == table_id)
    )
    if table is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Table not found")
    return table


def _table_fqn(table: TableEntity) -> str:
    return f"{table.schema.database.datasource.name}.{table.schema.database.name}.{table.schema.name}.{table.name}"


def _table_links(table: TableEntity) -> dict[str, str]:
    table_fqn = _table_fqn(table)
    return {
        "explorer": f"/explorer?tableId={table.id}",
        "pending_center": f"/governance/pending-center?q={table_fqn}",
    }


def _normalize_comment(value: str | None) -> str | None:
    normalized = (value or "").strip()
    return normalized or None


def _active_approver_candidates(db: Session) -> list[User]:
    return db.scalars(
        select(User)
        .join(User.roles)
        .where(User.is_active.is_(True), Role.name.in_(["admin", "stewardship", "data_owner"]))
        .order_by(User.name.asc().nullslast(), User.email.asc())
    ).unique().all()


def _approver_pending_loads(db: Session) -> dict[int, int]:
    rows = db.execute(
        select(StewardshipRequest.approver_user_id, func.count(StewardshipRequest.id))
        .where(
            StewardshipRequest.status == "pending",
            StewardshipRequest.approver_user_id.is_not(None),
        )
        .group_by(StewardshipRequest.approver_user_id)
    ).all()
    return {int(user_id): int(count or 0) for user_id, count in rows if user_id is not None}


def _match_user_by_email(db: Session, email: str | None) -> User | None:
    normalized = (email or "").strip().lower()
    if not normalized:
        return None
    return db.scalar(select(User).where(User.is_active.is_(True), User.email == normalized).limit(1))


def _table_domain_name(db: Session, *, table_id: int) -> str | None:
    value = db.scalar(
        select(DashboardAssetReadModel.domain_name)
        .where(DashboardAssetReadModel.table_id == table_id)
        .limit(1)
    )
    normalized = (str(value or "")).strip()
    return normalized or None


def _match_assignment_rule(
    db: Session,
    *,
    table: TableEntity,
    request_type: str,
    settings_snapshot,
) -> tuple[User | None, str]:
    table_domain = _table_domain_name(db, table_id=table.id)
    owner_area = (table.data_owner.area or "").strip() if table.data_owner is not None else ""
    normalized_domain = (table_domain or "").strip().lower()
    normalized_area = owner_area.lower()
    candidates: list[tuple[int, int, dict[str, object]]] = []
    for rule in settings_snapshot.stewardship_assignment_rules:
        if not bool(rule.get("is_active", True)):
            continue
        rule_request_type = str(rule.get("request_type") or "any").strip().lower() or "any"
        if rule_request_type not in {"any", request_type}:
            continue
        rule_domain = (str(rule.get("domain_name") or "")).strip()
        rule_area = (str(rule.get("owner_area") or "")).strip()
        if rule_domain and rule_domain.lower() != normalized_domain:
            continue
        if rule_area and rule_area.lower() != normalized_area:
            continue
        specificity = 0
        if rule_request_type != "any":
            specificity += 4
        if rule_domain:
            specificity += 2
        if rule_area:
            specificity += 1
        candidates.append((int(rule.get("priority", 100) or 100), -specificity, rule))
    for _, _, rule in sorted(candidates, key=lambda item: (item[0], item[1], str(item[2].get("key") or ""))):
        user = db.get(User, int(rule["approver_user_id"]))
        if user is None or not user.is_active:
            continue
        if rule.get("domain_name") and rule.get("owner_area"):
            return user, "domain_area_rule"
        if rule.get("domain_name"):
            return user, "domain_rule"
        return user, "area_rule"
    return None, "unassigned"


def _suggest_approver_for_request(db: Session, *, table: TableEntity, request_type: str) -> tuple[User | None, str]:
    settings_snapshot = get_governance_settings_snapshot(db)
    matched_rule_user, matched_rule_key = _match_assignment_rule(
        db,
        table=table,
        request_type=request_type,
        settings_snapshot=settings_snapshot,
    )
    if matched_rule_user is not None:
        return matched_rule_user, matched_rule_key

    recent_same_type = db.scalar(
        select(StewardshipRequest)
        .options(selectinload(StewardshipRequest.approver_user), selectinload(StewardshipRequest.decided_by_user))
        .where(
            StewardshipRequest.table_id == table.id,
            StewardshipRequest.request_type == request_type,
            StewardshipRequest.status.in_(["pending", "approved"]),
        )
        .order_by(StewardshipRequest.updated_at.desc(), StewardshipRequest.id.desc())
        .limit(1)
    )
    if recent_same_type is not None:
        for candidate in [recent_same_type.approver_user, recent_same_type.decided_by_user]:
            if candidate is not None and candidate.is_active:
                return candidate, "previous_same_type"

    if request_type in {"table_description", "glossary_terms", "owner_review"}:
        matched = _match_user_by_email(db, table.data_owner.email if table.data_owner else None)
        if matched is not None:
            return matched, "table_owner_email"

    if request_type == "privacy_review" and table.privacy_reviewed_by_user is not None and table.privacy_reviewed_by_user.is_active:
        return table.privacy_reviewed_by_user, "privacy_reviewer"

    if request_type == "certification_review":
        for candidate in [table.certification_decided_by_user, table.certification_submitted_by_user]:
            if candidate is not None and candidate.is_active:
                return candidate, "certification_history"
        matched = _match_user_by_email(db, table.data_owner.email if table.data_owner else None)
        if matched is not None:
            return matched, "table_owner_email"

    candidates = _active_approver_candidates(db)
    if not candidates:
        return None, "unassigned"
    loads = _approver_pending_loads(db)
    candidate = sorted(
        candidates,
        key=lambda item: (loads.get(int(item.id), 0), (_display_name(item) or item.email or "").lower(), int(item.id)),
    )[0]
    return candidate, "least_loaded_editor"


def _request_sla_days(request_type: str, *, settings_snapshot=None) -> int:
    if request_type == "certification_review" and settings_snapshot is not None:
        return max(int(settings_snapshot.certification_review_sla_days), 1)
    return REQUEST_SLA_DAYS.get(request_type, 7)


def _sla_payload(*, request_type: str, created_at: datetime, settings_snapshot=None) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    created = created_at if created_at.tzinfo is not None else created_at.replace(tzinfo=timezone.utc)
    sla_days = _request_sla_days(request_type, settings_snapshot=settings_snapshot)
    due_at = created + timedelta(days=sla_days)
    remaining_days = (due_at - now).total_seconds() / 86400
    if remaining_days < 0:
        sla_status = "overdue"
    elif remaining_days <= 2:
        sla_status = "due_soon"
    else:
        sla_status = "within_sla"
    return {
        "aging_days": max((now - created).days, 0),
        "sla_days": sla_days,
        "due_at": due_at,
        "sla_status": sla_status,
        "sla_status_label": SLA_STATUS_LABELS[sla_status],
    }


def _request_context_hint(request_type: str) -> str:
    if request_type == "certification_review":
        return "Usamos a fila de certificação como trilha de aprovação e revalidação do ativo."
    if request_type in REVIEW_REQUEST_TYPES:
        return "Revisões periódicas entram com SLA curto para reduzir backlog invisível de confirmação."
    return "Solicitações de metadados seguem a fila de stewardship com um aprovador sugerido quando disponível."


def _current_glossary_terms(db: Session, *, table_id: int) -> list[GlossaryTerm]:
    return db.scalars(
        select(GlossaryTerm)
        .join(GlossaryAssignment, GlossaryAssignment.term_id == GlossaryTerm.id)
        .where(GlossaryAssignment.entity_type == "table", GlossaryAssignment.entity_id == table_id)
        .order_by(GlossaryTerm.name)
    ).all()


def _owner_payload(owner: DataOwner | None) -> dict[str, Any] | None:
    if owner is None:
        return None
    return {
        "id": int(owner.id),
        "name": owner.name,
        "email": owner.email,
        "area": owner.area,
    }


def _request_payload(db: Session, *, table: TableEntity, payload) -> tuple[dict[str, Any], dict[str, Any]]:
    request_type = payload.request_type.strip()
    if request_type not in REQUEST_TYPE_META:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Unsupported stewardship request type")

    if request_type == "table_description":
        proposed_description = (payload.description_manual or "").strip()
        current = {
            "description_manual": table.description_manual,
            "effective_description": table.description_manual or table.description_source,
        }
        proposed = {"description_manual": proposed_description}
        if current["description_manual"] == proposed_description:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Description is already up to date")
        return current, proposed

    if request_type == "owner_assignment":
        owner = db.get(DataOwner, payload.data_owner_id)
        if owner is None:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Data owner not found")
        current = {
            "data_owner_id": table.data_owner_id,
            "owner": _owner_payload(table.data_owner),
        }
        proposed = {
            "data_owner_id": int(owner.id),
            "owner": _owner_payload(owner),
        }
        if table.data_owner_id == owner.id:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Owner is already assigned to this asset")
        return current, proposed

    if request_type == "certification_review":
        summary = build_certification_summary_out(db, table)
        current = {
            "certification_status": summary.certification_status,
            "certification_status_label": summary.certification_status_label,
            "certification_review_at": summary.certification_review_at.isoformat() if summary.certification_review_at else None,
            "certification_expires_at": summary.certification_expires_at.isoformat() if summary.certification_expires_at else None,
        }
        proposed = {
            "certification_status": "in_review",
            "certification_status_label": "Em revisão",
        }
        if summary.certification_status == "in_review":
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Certification review is already in progress")
        return current, proposed

    if request_type == "owner_review":
        current = {
            "owner_reviewed_at": table.owner_reviewed_at.isoformat() if table.owner_reviewed_at else None,
            "owner_reviewed_by_user_id": table.owner_reviewed_by_user_id,
            "owner_name": table.data_owner.name if table.data_owner else None,
        }
        proposed = {
            "review_type": "owner",
            "review_action": "confirm",
        }
        return current, proposed

    if request_type == "privacy_review":
        current = {
            "privacy_reviewed_at": table.privacy_reviewed_at.isoformat() if table.privacy_reviewed_at else None,
            "privacy_reviewed_by_user_id": table.privacy_reviewed_by_user_id,
            "sensitivity_level": table.sensitivity_level,
        }
        proposed = {
            "review_type": "privacy",
            "review_action": "confirm",
        }
        return current, proposed

    current_terms = _current_glossary_terms(db, table_id=table.id)
    requested_ids = sorted({int(term_id) for term_id in (payload.term_ids or [])})
    existing_term_ids = set(db.scalars(select(GlossaryTerm.id).where(GlossaryTerm.id.in_(requested_ids))).all()) if requested_ids else set()
    missing_term_ids = sorted(set(requested_ids) - existing_term_ids)
    if missing_term_ids:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=f"Unknown term_ids: {missing_term_ids}")
    proposed_terms = db.scalars(select(GlossaryTerm).where(GlossaryTerm.id.in_(requested_ids)).order_by(GlossaryTerm.name)).all() if requested_ids else []
    current_ids = sorted(int(term.id) for term in current_terms)
    if current_ids == requested_ids:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Glossary terms are already up to date")
    return (
        {
            "term_ids": current_ids,
            "terms": [{"id": int(term.id), "label": term.name} for term in current_terms],
        },
        {
            "term_ids": requested_ids,
            "terms": [{"id": int(term.id), "label": term.name} for term in proposed_terms],
        },
    )


def _build_context(table: TableEntity) -> dict[str, Any]:
    return {
        "table_fqn": _table_fqn(table),
        "datasource_name": table.schema.database.datasource.name,
        "database_name": table.schema.database.name,
        "schema_name": table.schema.name,
        "table_name": table.name,
    }


def _add_event(
    db: Session,
    *,
    request_item: StewardshipRequest,
    event_type: str,
    actor_user_id: int | None,
    comment: str | None,
    payload_json: dict[str, Any] | None = None,
) -> StewardshipRequestEvent:
    event = StewardshipRequestEvent(
        stewardship_request_id=request_item.id,
        event_type=event_type,
        actor_user_id=actor_user_id,
        comment=comment,
        payload_json=payload_json,
    )
    db.add(event)
    db.flush()
    return event


def _audit_request(
    db: Session,
    *,
    action: str,
    request_item: StewardshipRequest,
    table: TableEntity | None,
    audit_kwargs: dict[str, Any] | None,
    metadata: dict[str, Any] | None = None,
    after: dict[str, Any] | None = None,
) -> None:
    write_audit_log_sync(
        db,
        action=action,
        entity_type="stewardship_request",
        entity_id=request_item.id,
        parent_entity_type="table" if table is not None else None,
        parent_entity_id=table.id if table is not None else None,
        source_module="stewardship",
        metadata=metadata,
        after=after,
        **(audit_kwargs or {}),
    )


def _apply_certification_review(
    db: Session,
    *,
    table: TableEntity,
    actor: User,
    request_item: StewardshipRequest,
    audit_kwargs: dict[str, Any] | None,
) -> None:
    before = {
        "certification_status": table.certification_status,
        "certification_criticality": table.certification_criticality,
        "certification_badges": table.certification_badges,
        "certification_notes": table.certification_notes,
        "certification_submitted_by_user_id": table.certification_submitted_by_user_id,
        "certification_submitted_at": table.certification_submitted_at.isoformat() if table.certification_submitted_at else None,
        "certification_decided_by_user_id": table.certification_decided_by_user_id,
        "certification_decided_at": table.certification_decided_at.isoformat() if table.certification_decided_at else None,
        "certification_review_at": table.certification_review_at.isoformat() if table.certification_review_at else None,
        "certification_expires_at": table.certification_expires_at.isoformat() if table.certification_expires_at else None,
    }
    patch = TableCertificationPatch(
        certification_status="in_review",
        certification_criticality=table.certification_criticality,
        certification_badges=list(table.certification_badges or []) or None,
        certification_notes=_normalize_comment(request_item.requester_comment) or table.certification_notes,
        certification_review_at=table.certification_review_at,
        certification_expires_at=table.certification_expires_at,
    )
    validate_certification_patch(db, table=table, payload=patch)
    now = datetime.now(timezone.utc)
    table.certification_status = "in_review"
    table.certification_notes = patch.certification_notes
    table.certification_submitted_by_user_id = actor.id
    table.certification_submitted_at = now
    table.certification_decided_by_user_id = None
    table.certification_decided_at = None
    db.flush()

    after = {
        "certification_status": table.certification_status,
        "certification_criticality": table.certification_criticality,
        "certification_badges": table.certification_badges,
        "certification_notes": table.certification_notes,
        "certification_submitted_by_user_id": table.certification_submitted_by_user_id,
        "certification_submitted_at": table.certification_submitted_at.isoformat() if table.certification_submitted_at else None,
        "certification_decided_by_user_id": table.certification_decided_by_user_id,
        "certification_decided_at": table.certification_decided_at.isoformat() if table.certification_decided_at else None,
        "certification_review_at": table.certification_review_at.isoformat() if table.certification_review_at else None,
        "certification_expires_at": table.certification_expires_at.isoformat() if table.certification_expires_at else None,
    }
    changes = certification_changes(before=before, after=after)
    if changes:
        log_field_changes(
            db,
            action="table.certification.patch",
            entity_type="table",
            entity_id=table.id,
            changes=changes,
            source_module="stewardship",
            metadata={"message": "Certification review submitted from stewardship"},
            audit_kwargs=audit_kwargs,
            actor_user_id=actor.id,
        )


def _apply_request(db: Session, *, request_item: StewardshipRequest, actor: User, audit_kwargs: dict[str, Any] | None) -> None:
    if request_item.table_id is None:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="This stewardship request is no longer linked to an active table")
    proposed = request_item.proposed_value_json or {}
    if request_item.request_type == "table_description":
        patch_table_with_audit(
            db=db,
            table_id=request_item.table_id,
            payload=TablePatch(description_manual=str(proposed.get("description_manual") or "")),
            user=actor,
            audit_kwargs=audit_kwargs,
            commit=False,
        )
        return
    if request_item.request_type == "owner_assignment":
        patch_table_with_audit(
            db=db,
            table_id=request_item.table_id,
            payload=TablePatch(data_owner_id=proposed.get("data_owner_id")),
            user=actor,
            audit_kwargs=audit_kwargs,
            commit=False,
        )
        return
    if request_item.request_type == "glossary_terms":
        update_table_glossary_terms_with_audit(
            db=db,
            table_id=request_item.table_id,
            payload=TableGlossaryTermsUpdateRequest(term_ids=list(proposed.get("term_ids") or [])),
            user=actor,
            audit_kwargs=audit_kwargs,
            commit=False,
        )
        return
    table = db.get(TableEntity, request_item.table_id)
    if table is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Table not found")
    if request_item.request_type == "owner_review":
        before = table.owner_reviewed_at.isoformat() if table.owner_reviewed_at else None
        payload_result = mark_owner_review(db, table_id=request_item.table_id, user=actor)
        log_field_changes(
            db,
            action="table.owner.review",
            entity_type="table",
            entity_id=request_item.table_id,
            changes=[AuditFieldChange(field_name="owner_reviewed_at", before=before, after=payload_result["reviewed_at"], change_type="update")],
            source_module="stewardship",
            metadata={"message": "Owner review confirmed from stewardship"},
            audit_kwargs=audit_kwargs,
            actor_user_id=actor.id,
        )
        return
    if request_item.request_type == "privacy_review":
        before = table.privacy_reviewed_at.isoformat() if table.privacy_reviewed_at else None
        payload_result = mark_privacy_review(db, table_id=request_item.table_id, user=actor)
        log_field_changes(
            db,
            action="table.privacy.review",
            entity_type="table",
            entity_id=request_item.table_id,
            changes=[AuditFieldChange(field_name="privacy_reviewed_at", before=before, after=payload_result["reviewed_at"], change_type="update")],
            source_module="stewardship",
            metadata={"message": "Privacy review confirmed from stewardship", "is_sensitive_change": True, "sensitive_category": "classification"},
            audit_kwargs=audit_kwargs,
            actor_user_id=actor.id,
        )
        return
    if request_item.request_type == "certification_review":
        _apply_certification_review(
            db,
            table=table,
            actor=actor,
            request_item=request_item,
            audit_kwargs=audit_kwargs,
        )
        return
    raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Unsupported stewardship request type")


def create_stewardship_request(
    db: Session,
    *,
    payload,
    user: User,
    audit_kwargs: dict[str, Any] | None = None,
) -> StewardshipRequest:
    table = _table_with_context(db, payload.table_id)
    duplicate = db.scalar(
        select(StewardshipRequest)
        .where(
            StewardshipRequest.table_id == payload.table_id,
            StewardshipRequest.request_type == payload.request_type.strip(),
            StewardshipRequest.status == "pending",
        )
        .limit(1)
    )
    if duplicate is not None:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="There is already a pending request of this type for this asset")

    current_value, proposed_value = _request_payload(db, table=table, payload=payload)
    suggested_approver, assignment_rule = _suggest_approver_for_request(db, table=table, request_type=payload.request_type.strip())
    if payload.approver_user_id is not None:
        approver_user_id = payload.approver_user_id
        approver_source = "manual"
        assignment_rule = "manual"
    elif suggested_approver is not None:
        approver_user_id = suggested_approver.id
        approver_source = "suggested"
    else:
        approver_user_id = None
        approver_source = "unassigned"
    request_item = StewardshipRequest(
        table_id=table.id,
        request_type=payload.request_type.strip(),
        status="pending",
        request_origin=(payload.request_origin or "manual").strip() or "manual",
        requested_by_user_id=getattr(user, "id", None),
        approver_user_id=approver_user_id,
        requester_comment=_normalize_comment(payload.requester_comment),
        current_value_json=current_value,
        proposed_value_json=proposed_value,
        context_json={
            **_build_context(table),
            "approver_source": approver_source,
            "assignment_rule": assignment_rule,
            "assignment_rule_label": APPROVER_RULE_LABELS.get(assignment_rule, assignment_rule.replace("_", " ").title()),
            "suggested_approver_user_id": suggested_approver.id if suggested_approver is not None else None,
            "suggested_approver_name": _display_name(suggested_approver),
            "suggested_approver_email": suggested_approver.email if suggested_approver is not None else None,
        },
    )
    db.add(request_item)
    db.flush()
    _add_event(
        db,
        request_item=request_item,
        event_type="created",
        actor_user_id=getattr(user, "id", None),
        comment=request_item.requester_comment,
        payload_json={"request_type": request_item.request_type, "request_origin": request_item.request_origin},
    )
    _audit_request(
        db,
        action="stewardship.request.create",
        request_item=request_item,
        table=table,
        audit_kwargs=audit_kwargs,
        metadata={"message": "Stewardship request created", "table_fqn": _table_fqn(table)},
        after={
            "request_type": request_item.request_type,
            "status": request_item.status,
            "request_origin": request_item.request_origin,
            "proposed_value_json": request_item.proposed_value_json,
        },
    )
    db.commit()
    result = get_stewardship_request(db, request_item.id)
    try:
        if result.approver_user_id is not None:
            for recipient in resolve_inbox_notification_recipients(db, user_ids=[int(result.approver_user_id)]):
                create_user_inbox_notification(
                    db,
                    user_id=recipient.id,
                    dedupe_key=f"stewardship:{result.id}:approver",
                    category="stewardship",
                    severity="high" if result.request_type in {"certification_review", "privacy_review"} else "medium",
                    source_module="stewardship",
                    source_entity_type="stewardship_request",
                    source_entity_id=result.id,
                    title=f"Nova solicitação de {REQUEST_TYPE_META.get(result.request_type, {}).get('label', result.request_type)}",
                    message=f"{_table_fqn(table)} entrou na sua fila de aprovação.",
                    href=f"/governance/stewardship?approverUserId={result.approver_user_id}",
                    context_json={"request_id": result.id, "table_id": result.table_id, "role": "approver"},
                )
        if result.requested_by_user_id is not None:
            for recipient in resolve_inbox_notification_recipients(db, user_ids=[int(result.requested_by_user_id)]):
                create_user_inbox_notification(
                    db,
                    user_id=recipient.id,
                    dedupe_key=f"stewardship:{result.id}:requester",
                    category="stewardship",
                    severity="low",
                    source_module="stewardship",
                    source_entity_type="stewardship_request",
                    source_entity_id=result.id,
                    title="Solicitação de stewardship registrada",
                    message=f"Sua solicitação para {_table_fqn(table)} entrou no workflow.",
                    href=f"/governance/stewardship?tableId={result.table_id}",
                    context_json={"request_id": result.id, "table_id": result.table_id, "role": "requester"},
                )
        db.commit()
    except Exception:
        db.rollback()
    return result


def decide_stewardship_request(
    db: Session,
    *,
    request_id: int,
    decision: str,
    actor: User,
    payload,
    audit_kwargs: dict[str, Any] | None = None,
) -> StewardshipRequest:
    request_item = db.scalar(
        select(StewardshipRequest)
        .options(
            selectinload(StewardshipRequest.table).selectinload(TableEntity.data_owner),
            selectinload(StewardshipRequest.table).selectinload(TableEntity.schema).selectinload(Schema.database).selectinload(Database.datasource),
            selectinload(StewardshipRequest.requested_by_user),
            selectinload(StewardshipRequest.approver_user),
            selectinload(StewardshipRequest.decided_by_user),
            selectinload(StewardshipRequest.events).selectinload(StewardshipRequestEvent.actor_user),
        )
        .where(StewardshipRequest.id == request_id)
    )
    if request_item is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Stewardship request not found")
    if request_item.status != "pending":
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Only pending requests can be decided")
    if request_item.approver_user_id and request_item.approver_user_id != getattr(actor, "id", None):
        actor_roles = {role.name for role in getattr(actor, "roles", [])}
        if "admin" not in actor_roles:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="This request is assigned to another approver")

    decision_comment = _normalize_comment(payload.decision_comment)
    if decision == "rejected" and not decision_comment:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Informe uma justificativa para rejeitar a solicitação.",
        )
    now = datetime.now(timezone.utc)
    if decision == "approved":
        _apply_request(db, request_item=request_item, actor=actor, audit_kwargs=audit_kwargs)
    request_item.status = decision
    request_item.decided_by_user_id = getattr(actor, "id", None)
    request_item.decision_comment = decision_comment
    request_item.decided_at = now
    if request_item.approver_user_id is None:
        request_item.approver_user_id = getattr(actor, "id", None)
    db.flush()
    _add_event(
        db,
        request_item=request_item,
        event_type=decision,
        actor_user_id=getattr(actor, "id", None),
        comment=decision_comment,
        payload_json={"status": decision},
    )
    _audit_request(
        db,
        action=f"stewardship.request.{decision}",
        request_item=request_item,
        table=request_item.table,
        audit_kwargs=audit_kwargs,
        metadata={"message": f"Stewardship request {decision}"},
        after={"status": request_item.status, "decision_comment": request_item.decision_comment},
    )
    db.commit()
    result = get_stewardship_request(db, request_item.id)
    try:
        if result.requested_by_user_id is not None:
            for recipient in resolve_inbox_notification_recipients(db, user_ids=[int(result.requested_by_user_id)]):
                create_user_inbox_notification(
                    db,
                    user_id=recipient.id,
                    dedupe_key=f"stewardship:{result.id}:decision:{decision}",
                    category="stewardship",
                    severity="critical" if decision == "rejected" else "medium",
                    source_module="stewardship",
                    source_entity_type="stewardship_request",
                    source_entity_id=result.id,
                    title=f"Solicitação {STATUS_LABELS.get(decision, decision.title())}",
                    message=f"A solicitação de {_table_fqn(request_item.table)} foi {STATUS_LABELS.get(decision, decision).lower()}.",
                    href=f"/governance/stewardship?tableId={result.table_id}",
                    context_json={"request_id": result.id, "table_id": result.table_id, "decision": decision},
                )
        db.commit()
    except Exception:
        db.rollback()
    return result


def get_stewardship_request(db: Session, request_id: int) -> StewardshipRequest:
    request_item = db.scalar(
        select(StewardshipRequest)
        .options(
            selectinload(StewardshipRequest.table).selectinload(TableEntity.data_owner),
            selectinload(StewardshipRequest.table).selectinload(TableEntity.schema).selectinload(Schema.database).selectinload(Database.datasource),
            selectinload(StewardshipRequest.requested_by_user),
            selectinload(StewardshipRequest.approver_user),
            selectinload(StewardshipRequest.decided_by_user),
            selectinload(StewardshipRequest.events).selectinload(StewardshipRequestEvent.actor_user),
        )
        .where(StewardshipRequest.id == request_id)
    )
    if request_item is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Stewardship request not found")
    return request_item


def get_stewardship_request_payload(db: Session, request_id: int) -> dict[str, Any]:
    request_item = get_stewardship_request(db, request_id)
    governance_scores = _governance_scores_for_requests(db, [request_item])
    return _request_out(request_item, governance_scores=governance_scores)


def _governance_scores_for_requests(db: Session, requests: list[StewardshipRequest]) -> dict[int, dict[str, Any]]:
    now = datetime.now(timezone.utc)
    settings_snapshot = get_governance_settings_snapshot(db)
    profiles = {profile.table_id: profile for profile in load_table_profiles(db, now)}
    result: dict[int, dict[str, Any]] = {}
    for request_item in requests:
        if request_item.table_id is None:
            continue
        profile = profiles.get(request_item.table_id)
        if profile is None:
            continue
        result[request_item.table_id] = build_governance_score_for_profile(profile, settings_snapshot=settings_snapshot)
    return result


def _request_out(request_item: StewardshipRequest, *, governance_scores: dict[int, dict[str, Any]]) -> dict[str, Any]:
    session = object_session(request_item)
    settings_snapshot = get_governance_settings_snapshot(session) if session is not None else None
    table = request_item.table
    table_name = table.name if table is not None else str((request_item.context_json or {}).get("table_name") or "Ativo removido")
    datasource_name = table.schema.database.datasource.name if table is not None else (request_item.context_json or {}).get("datasource_name")
    database_name = table.schema.database.name if table is not None else (request_item.context_json or {}).get("database_name")
    schema_name = table.schema.name if table is not None else (request_item.context_json or {}).get("schema_name")
    table_fqn = _table_fqn(table) if table is not None else str((request_item.context_json or {}).get("table_fqn") or table_name)
    owner_name = table.data_owner.name if table is not None and table.data_owner is not None else None
    data_owner_id = table.data_owner_id if table is not None else None
    context_json = request_item.context_json or {}
    suggested_approver = {
        "id": context_json.get("suggested_approver_user_id"),
        "name": context_json.get("suggested_approver_name"),
        "email": context_json.get("suggested_approver_email"),
    }
    approver_source = str(context_json.get("approver_source") or ("manual" if request_item.approver_user_id else "unassigned"))
    assignment_rule = str(context_json.get("assignment_rule") or ("manual" if approver_source == "manual" else "unassigned"))
    sla_payload = _sla_payload(request_type=request_item.request_type, created_at=request_item.created_at, settings_snapshot=settings_snapshot)
    return {
        "id": request_item.id,
        "table_id": request_item.table_id,
        "table_name": table_name,
        "table_fqn": table_fqn,
        "datasource_name": datasource_name,
        "database_name": database_name,
        "schema_name": schema_name,
        "data_owner_id": data_owner_id,
        "owner_name": owner_name,
        "request_type": request_item.request_type,
        "request_type_label": REQUEST_TYPE_META.get(request_item.request_type, {}).get("label", request_item.request_type),
        "request_type_description": REQUEST_TYPE_META.get(request_item.request_type, {}).get("description", ""),
        "status": request_item.status,
        "status_label": STATUS_LABELS.get(request_item.status, request_item.status.title()),
        "request_origin": request_item.request_origin,
        "request_origin_label": ORIGIN_LABELS.get(request_item.request_origin, request_item.request_origin.replace("_", " ").title()),
        "requester_comment": request_item.requester_comment,
        "decision_comment": request_item.decision_comment,
        "current_value_json": request_item.current_value_json,
        "proposed_value_json": request_item.proposed_value_json,
        "context_json": context_json,
        "requested_by": {
            "id": request_item.requested_by_user_id,
            "name": _display_name(request_item.requested_by_user),
            "email": request_item.requested_by_user.email if request_item.requested_by_user else None,
        },
        "approver": {
            "id": request_item.approver_user_id,
            "name": _display_name(request_item.approver_user),
            "email": request_item.approver_user.email if request_item.approver_user else None,
        },
        "suggested_approver": suggested_approver,
        "approver_source": approver_source,
        "approver_source_label": APPROVER_SOURCE_LABELS.get(approver_source, approver_source.title()),
        "assignment_rule": assignment_rule,
        "assignment_rule_label": APPROVER_RULE_LABELS.get(assignment_rule, assignment_rule.replace("_", " ").title()),
        "decided_by": {
            "id": request_item.decided_by_user_id,
            "name": _display_name(request_item.decided_by_user),
            "email": request_item.decided_by_user.email if request_item.decided_by_user else None,
        },
        "governance_score": governance_scores.get(request_item.table_id or -1),
        "aging_days": sla_payload["aging_days"],
        "sla_days": sla_payload["sla_days"],
        "due_at": sla_payload["due_at"],
        "sla_status": sla_payload["sla_status"],
        "sla_status_label": sla_payload["sla_status_label"],
        "created_at": request_item.created_at,
        "updated_at": request_item.updated_at,
        "decided_at": request_item.decided_at,
        "links": _table_links(table) if table is not None else {"explorer": "/explorer", "pending_center": "/governance/pending-center"},
        "events": [
            {
                "id": event.id,
                "event_type": event.event_type,
                "event_type_label": EVENT_LABELS.get(event.event_type, event.event_type.title()),
                "actor": {
                    "id": event.actor_user_id,
                    "name": _display_name(event.actor_user),
                    "email": event.actor_user.email if event.actor_user else None,
                },
                "comment": event.comment,
                "payload_json": event.payload_json,
                "created_at": event.created_at,
            }
            for event in request_item.events
        ],
    }


def get_stewardship_request_context(db: Session, *, table_id: int, request_type: str) -> dict[str, Any]:
    table = _table_with_context(db, table_id)
    normalized_type = request_type.strip()
    if normalized_type not in REQUEST_TYPE_META:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Unsupported stewardship request type")
    settings_snapshot = get_governance_settings_snapshot(db)
    suggested_approver, assignment_rule = _suggest_approver_for_request(db, table=table, request_type=normalized_type)
    source = "suggested" if suggested_approver is not None else "unassigned"
    sla_preview = _sla_payload(request_type=normalized_type, created_at=datetime.now(timezone.utc), settings_snapshot=settings_snapshot)
    return {
        "table_id": table.id,
        "request_type": normalized_type,
        "request_type_label": REQUEST_TYPE_META[normalized_type]["label"],
        "suggested_approver": {
            "id": suggested_approver.id if suggested_approver is not None else None,
            "name": _display_name(suggested_approver),
            "email": suggested_approver.email if suggested_approver is not None else None,
        },
        "approver_source": source,
        "approver_source_label": APPROVER_SOURCE_LABELS[source],
        "assignment_rule": assignment_rule,
        "assignment_rule_label": APPROVER_RULE_LABELS.get(assignment_rule, assignment_rule.replace("_", " ").title()),
        "sla_days": sla_preview["sla_days"],
        "due_at": sla_preview["due_at"],
        "sla_status": sla_preview["sla_status"],
        "sla_status_label": sla_preview["sla_status_label"],
        "hint": _request_context_hint(normalized_type),
    }


def get_stewardship_requests(
    db: Session,
    *,
    status_filter: str | None = None,
    request_type: str | None = None,
    table_id: int | None = None,
    approver_user_id: int | None = None,
    data_owner_id: int | None = None,
    sla_status_filter: str | None = None,
    mine_only: bool = False,
    sort: str | None = None,
    page: int = 1,
    page_size: int = 20,
    current_user: User | None = None,
) -> dict[str, Any]:
    stmt = (
        select(StewardshipRequest)
        .options(
            selectinload(StewardshipRequest.table).selectinload(TableEntity.data_owner),
            selectinload(StewardshipRequest.table).selectinload(TableEntity.schema).selectinload(Schema.database).selectinload(Database.datasource),
            selectinload(StewardshipRequest.requested_by_user),
            selectinload(StewardshipRequest.approver_user),
            selectinload(StewardshipRequest.decided_by_user),
            selectinload(StewardshipRequest.events).selectinload(StewardshipRequestEvent.actor_user),
        )
        .order_by(
            case(
                (StewardshipRequest.status == "pending", 0),
                (StewardshipRequest.status == "rejected", 1),
                (StewardshipRequest.status == "approved", 2),
                (StewardshipRequest.status == "cancelled", 3),
                else_=4,
            ),
            StewardshipRequest.updated_at.desc(),
            StewardshipRequest.id.desc(),
        )
    )
    if status_filter:
        stmt = stmt.where(StewardshipRequest.status == status_filter)
    if request_type:
        stmt = stmt.where(StewardshipRequest.request_type == request_type)
    if table_id is not None:
        stmt = stmt.where(StewardshipRequest.table_id == table_id)
    if approver_user_id is not None:
        stmt = stmt.where(StewardshipRequest.approver_user_id == approver_user_id)
    if data_owner_id is not None:
        stmt = stmt.join(StewardshipRequest.table).where(TableEntity.data_owner_id == data_owner_id)
    items = db.scalars(stmt).all()
    governance_scores = _governance_scores_for_requests(db, items)

    summary_counts = {
        "pending": 0,
        "approved": 0,
        "rejected": 0,
        "cancelled": 0,
    }
    for item in items:
        summary_counts[item.status] = summary_counts.get(item.status, 0) + 1

    owner_options = sorted(
        {
            (int(item.table.data_owner_id), item.table.data_owner.name)
            for item in items
            if item.table is not None and item.table.data_owner_id is not None and item.table.data_owner is not None
        },
        key=lambda value: value[1].lower(),
    )
    approver_options = sorted(
        {
            (int(item.approver_user_id), _display_name(item.approver_user) or item.approver_user.email)
            for item in items
            if item.approver_user_id is not None and item.approver_user is not None
        },
        key=lambda value: value[1].lower(),
    )

    # Summary, filter options and inbox stay GLOBAL (computed over the full filtered set),
    # while the items list is refined by SLA/mine, optionally sorted, and paginated.
    serialized_all = [_request_out(item, governance_scores=governance_scores) for item in items]

    refined = serialized_all
    normalized_sla = (sla_status_filter or "").strip()
    if normalized_sla:
        refined = [row for row in refined if row.get("sla_status") == normalized_sla]
    if mine_only and current_user is not None:
        uid = getattr(current_user, "id", None)
        refined = [
            row
            for row in refined
            if (row.get("approver") or {}).get("id") == uid or (row.get("requested_by") or {}).get("id") == uid
        ]
    if (sort or "").strip() == "sla":
        sla_rank = {"overdue": 0, "due_soon": 1, "within_sla": 2}
        refined = sorted(refined, key=lambda row: (sla_rank.get(row.get("sla_status"), 9), -int(row.get("aging_days") or 0)))

    filtered_total = len(refined)
    safe_page_size = max(1, min(int(page_size or 20), 100))
    total_pages = max(1, (filtered_total + safe_page_size - 1) // safe_page_size) if filtered_total > 0 else 1
    safe_page = min(max(1, int(page or 1)), total_pages)
    offset = (safe_page - 1) * safe_page_size
    page_items = refined[offset : offset + safe_page_size]

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total": filtered_total,
        "page": safe_page,
        "page_size": safe_page_size,
        "total_pages": total_pages,
        "summary": [
            {"key": key, "label": STATUS_LABELS.get(key, key.title()), "count": count}
            for key, count in summary_counts.items()
            if count > 0
        ],
        "filters": {
            "statuses": [{"value": key, "label": label} for key, label in STATUS_LABELS.items()],
            "request_types": REQUEST_TYPE_OPTIONS,
            "owners": [{"value": str(owner_id), "label": label} for owner_id, label in owner_options],
            "approvers": [{"value": str(approver_id), "label": label} for approver_id, label in approver_options],
            "sla_statuses": [{"value": key, "label": label} for key, label in SLA_STATUS_LABELS.items()],
        },
        "inbox": build_stewardship_inbox_summary(items, current_user=current_user),
        "items": page_items,
    }


def build_stewardship_inbox_summary(items: list[StewardshipRequest], *, current_user: User | None = None) -> dict[str, Any]:
    pending_items = [item for item in items if item.status == "pending"]
    pending_total = len(pending_items)
    awaiting_assignment = sum(1 for item in pending_items if item.approver_user_id is None)
    review_pending = sum(1 for item in pending_items if item.request_type in REVIEW_REQUEST_TYPES)
    certification_pending = sum(1 for item in pending_items if item.request_type == "certification_review")
    current_user_id = getattr(current_user, "id", None)
    my_approvals_pending = sum(1 for item in pending_items if current_user_id is not None and item.approver_user_id == current_user_id)
    my_owner_queue = sum(
        1
        for item in pending_items
        if current_user_id is not None
        and item.table is not None
        and item.table.data_owner is not None
        and (
            (item.table.data_owner.email and item.table.data_owner.email == getattr(current_user, "email", None))
            or (item.table.data_owner.name and item.table.data_owner.name == (_display_name(current_user) or ""))
        )
    )

    owner_counts: dict[tuple[int, str], int] = {}
    approver_counts: dict[tuple[int, str], int] = {}
    for item in pending_items:
        if item.table is not None and item.table.data_owner_id is not None and item.table.data_owner is not None:
            owner_key = (int(item.table.data_owner_id), item.table.data_owner.name)
            owner_counts[owner_key] = owner_counts.get(owner_key, 0) + 1
        if item.approver_user_id is not None and item.approver_user is not None:
            approver_label = _display_name(item.approver_user) or item.approver_user.email or f"Usuário {item.approver_user_id}"
            approver_key = (int(item.approver_user_id), approver_label)
            approver_counts[approver_key] = approver_counts.get(approver_key, 0) + 1

    by_owner = [
        {
            "key": f"owner:{owner_id}",
            "label": label,
            "count": count,
            "href": f"/governance/stewardship?dataOwnerId={owner_id}",
        }
        for (owner_id, label), count in sorted(owner_counts.items(), key=lambda item: (-item[1], item[0][1].lower()))[:6]
    ]
    by_approver = [
        {
            "key": f"approver:{approver_id}",
            "label": label,
            "count": count,
            "href": f"/governance/stewardship?approverUserId={approver_id}",
        }
        for (approver_id, label), count in sorted(approver_counts.items(), key=lambda item: (-item[1], item[0][1].lower()))[:6]
    ]

    return {
        "pending_total": pending_total,
        "awaiting_assignment": awaiting_assignment,
        "review_pending": review_pending,
        "certification_pending": certification_pending,
        "my_approvals_pending": my_approvals_pending,
        "my_owner_queue": my_owner_queue,
        "by_owner": by_owner,
        "by_approver": by_approver,
    }
