from __future__ import annotations

from fastapi import HTTPException, status
from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from t2c_data.features.audit import AuditFieldChange
from t2c_data.features.catalog.metadata_actions import ensure_table_exists, get_table_datasource_id
from t2c_data.features.tags.api_support import load_entity_tag_contexts
from t2c_data.features.tags.intelligence import manual_assign_tag, manual_unassign_tag, reprocess_table_tag_intelligence
from t2c_data.models.glossary import GlossaryAssignment, GlossaryTerm
from t2c_data.models.tag import Tag, TagAssignment
from t2c_data.services.audit import log_field_changes
from t2c_data.schemas.tag import TagOut

TABLE_ENTITY_TYPE = "table"


def get_table_tags(*, db: Session, table_id: int) -> list[TagOut]:
    ensure_table_exists(db=db, table_id=table_id)
    return load_entity_tag_contexts(db, entity_type=TABLE_ENTITY_TYPE, entity_ids=[table_id]).get(table_id, [])


def update_table_tags_with_audit(
    *,
    db: Session,
    table_id: int,
    payload,
    user,
    audit_kwargs: dict | None = None,
    commit: bool = True,
) -> list[TagOut]:
    datasource_id = get_table_datasource_id(db=db, table_id=table_id)

    requested_ids = set(payload.tag_ids)
    if requested_ids:
        existing_tag_ids = set(db.scalars(select(Tag.id).where(Tag.id.in_(requested_ids))).all())
        missing = sorted(requested_ids - existing_tag_ids)
        if missing:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Unknown tag_ids: {missing}",
            )

    existing_assignments = db.scalars(
        select(TagAssignment).where(
            TagAssignment.entity_type == TABLE_ENTITY_TYPE,
            TagAssignment.entity_id == table_id,
        )
    ).all()
    existing_ids = {assignment.tag_id for assignment in existing_assignments}

    to_add = requested_ids - existing_ids
    to_remove = existing_ids - requested_ids

    tag_names = {
        int(tag.id): {"id": int(tag.id), "label": tag.name}
        for tag in db.scalars(select(Tag).where(Tag.id.in_(sorted(requested_ids | existing_ids)))).all()
    }

    for tag_id in sorted(to_remove):
        manual_unassign_tag(
            db,
            tag_id=tag_id,
            entity_type=TABLE_ENTITY_TYPE,
            entity_id=table_id,
            datasource_id=datasource_id,
            actor_user_id=user.id,
            reason="Remoção manual de tag da tabela.",
        )

    for tag_id in sorted(to_add):
        manual_assign_tag(
            db,
            tag_id=tag_id,
            entity_type=TABLE_ENTITY_TYPE,
            entity_id=table_id,
            datasource_id=datasource_id,
            actor_user_id=user.id,
            reason="Atualização manual de tags da tabela.",
        )

    if to_add or to_remove:
        changes = [
            AuditFieldChange(field_name="tags", before=None, after=tag_names[tag_id], change_type="assign")
            for tag_id in sorted(to_add)
        ] + [
            AuditFieldChange(field_name="tags", before=tag_names[tag_id], after=None, change_type="unassign")
            for tag_id in sorted(to_remove)
        ]
        log_field_changes(
            db,
            action="table.tags.update",
            entity_type=TABLE_ENTITY_TYPE,
            entity_id=table_id,
            changes=changes,
            source_module="catalog.taxonomy",
            metadata={"message": "Table tags updated"},
            audit_kwargs=audit_kwargs,
            actor_user_id=user.id,
        )

    reprocess_table_tag_intelligence(
        db,
        table_id=table_id,
        actor_user_id=user.id,
        audit_kwargs=audit_kwargs,
        source_module="catalog.taxonomy",
        metadata={"origin": "manual_table_tags_update"},
    )

    if commit:
        db.commit()
    else:
        db.flush()
    return get_table_tags(db=db, table_id=table_id)


def get_table_glossary_terms(*, db: Session, table_id: int) -> list[GlossaryTerm]:
    ensure_table_exists(db=db, table_id=table_id)
    return db.scalars(
        select(GlossaryTerm)
        .join(GlossaryAssignment, GlossaryAssignment.term_id == GlossaryTerm.id)
        .where(
            GlossaryAssignment.entity_type == TABLE_ENTITY_TYPE,
            GlossaryAssignment.entity_id == table_id,
        )
        .order_by(GlossaryTerm.name)
    ).all()


def update_table_glossary_terms_with_audit(
    *,
    db: Session,
    table_id: int,
    payload,
    user,
    audit_kwargs: dict | None = None,
    commit: bool = True,
) -> list[GlossaryTerm]:
    datasource_id = get_table_datasource_id(db=db, table_id=table_id)

    requested_ids = set(payload.term_ids)
    if requested_ids:
        existing_term_ids = set(db.scalars(select(GlossaryTerm.id).where(GlossaryTerm.id.in_(requested_ids))).all())
        missing = sorted(requested_ids - existing_term_ids)
        if missing:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Unknown term_ids: {missing}",
            )

    existing_assignments = db.scalars(
        select(GlossaryAssignment).where(
            GlossaryAssignment.entity_type == TABLE_ENTITY_TYPE,
            GlossaryAssignment.entity_id == table_id,
        )
    ).all()
    existing_ids = {assignment.term_id for assignment in existing_assignments}

    to_add = requested_ids - existing_ids
    to_remove = existing_ids - requested_ids

    if to_remove:
        db.execute(
            delete(GlossaryAssignment).where(
                GlossaryAssignment.entity_type == TABLE_ENTITY_TYPE,
                GlossaryAssignment.entity_id == table_id,
                GlossaryAssignment.term_id.in_(to_remove),
            )
        )

    for term_id in sorted(to_add):
        db.add(
            GlossaryAssignment(
                term_id=term_id,
                datasource_id=datasource_id,
                entity_type=TABLE_ENTITY_TYPE,
                entity_id=table_id,
            )
        )

    if to_add or to_remove:
        term_names = {
            int(term.id): {"id": int(term.id), "label": term.name}
            for term in db.scalars(select(GlossaryTerm).where(GlossaryTerm.id.in_(sorted(requested_ids | existing_ids)))).all()
        }
        changes = [
            AuditFieldChange(field_name="glossary_terms", before=None, after=term_names[term_id], change_type="assign")
            for term_id in sorted(to_add)
        ] + [
            AuditFieldChange(field_name="glossary_terms", before=term_names[term_id], after=None, change_type="unassign")
            for term_id in sorted(to_remove)
        ]
        log_field_changes(
            db,
            action="table.glossary_terms.update",
            entity_type=TABLE_ENTITY_TYPE,
            entity_id=table_id,
            changes=changes,
            source_module="catalog.taxonomy",
            metadata={"message": "Table glossary terms updated"},
            audit_kwargs=audit_kwargs,
            actor_user_id=user.id,
        )

    if commit:
        db.commit()
    else:
        db.flush()
    return get_table_glossary_terms(db=db, table_id=table_id)
