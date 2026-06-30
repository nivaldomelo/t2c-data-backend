from __future__ import annotations

from sqlalchemy import and_, delete, func, or_, select
from sqlalchemy.orm import Session

from t2c_data.models.catalog import ColumnEntity, Database, Schema, TableEntity
from t2c_data.models.tag import Tag, TagAssignment, TagAssignmentOverride, TagIntelligenceEvent
from t2c_data.schemas.tag import TagOut
from t2c_data.services.audit import serialize_model
from t2c_data.services.audit import write_audit_log_sync
from t2c_data.features.tags.spreadsheet import (
    all_linked_tables,
    normalize_tag_status,
    preview_linked_tables,
    slugify_tag,
)


def resolve_datasource_id(db: Session, entity_type: str, entity_id: int) -> int | None:
    if entity_type == "database":
        return db.scalar(select(Database.datasource_id).where(Database.id == entity_id))
    if entity_type == "schema":
        return db.scalar(
            select(Database.datasource_id)
            .join(Schema, Schema.database_id == Database.id)
            .where(Schema.id == entity_id)
        )
    if entity_type == "table":
        return db.scalar(
            select(Database.datasource_id)
            .join(Schema, Schema.database_id == Database.id)
            .join(TableEntity, TableEntity.schema_id == Schema.id)
            .where(TableEntity.id == entity_id)
        )
    if entity_type == "column":
        return db.scalar(
            select(Database.datasource_id)
            .join(Schema, Schema.database_id == Database.id)
            .join(TableEntity, TableEntity.schema_id == Schema.id)
            .join(ColumnEntity, ColumnEntity.table_id == TableEntity.id)
            .where(ColumnEntity.id == entity_id)
        )
    return None



def normalize_tag_payload(data: dict) -> dict:
    from fastapi import HTTPException, status

    normalized = dict(data)
    if "slug" in normalized and normalized["slug"] is not None:
        normalized["slug"] = slugify_tag(str(normalized["slug"]))
    if "name" in normalized and normalized["name"] is not None:
        normalized["name"] = str(normalized["name"]).strip()
    for field in (
        "external_id",
        "color",
        "description",
        "group_name",
        "subgroup_name",
        "example_of_use",
        "tag_type",
        "suggested_scope",
        "synonyms",
        "notes",
    ):
        if field in normalized and normalized[field] is not None:
            value = str(normalized[field]).strip()
            normalized[field] = value or None
    if "status" in normalized and normalized["status"] is not None:
        normalized["status"] = normalize_tag_status(str(normalized["status"]))
    if not normalized.get("slug") and normalized.get("name"):
        normalized["slug"] = slugify_tag(str(normalized["name"]))
    if not normalized.get("slug"):
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Slug is required")
    if not normalized.get("name"):
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Tag name is required")
    return normalized



def usage_subquery():
    return (
        select(
            TagAssignment.tag_id.label("tag_id"),
            func.count(func.distinct(TagAssignment.entity_id)).filter(TagAssignment.entity_type == "table").label("tables_count"),
            func.count(func.distinct(TagAssignment.entity_id)).filter(TagAssignment.entity_type == "column").label("columns_count"),
        )
        .group_by(TagAssignment.tag_id)
        .subquery()
    )



def base_tag_select():
    usage_subq = usage_subquery()
    return (
        select(
            Tag.id,
            Tag.external_id,
            Tag.slug,
            Tag.name,
            Tag.color,
            Tag.description,
            Tag.group_name,
            Tag.subgroup_name,
            Tag.example_of_use,
            Tag.tag_type,
            Tag.suggested_scope,
            Tag.status,
            Tag.synonyms,
            Tag.notes,
            Tag.created_at,
            Tag.updated_at,
            func.coalesce(usage_subq.c.tables_count, 0).label("tables_count"),
            func.coalesce(usage_subq.c.columns_count, 0).label("columns_count"),
        )
        .outerjoin(usage_subq, usage_subq.c.tag_id == Tag.id)
    )



def row_to_tag_out(row: dict, linked_tables_preview: list | None = None, assignment: TagAssignment | None = None) -> TagOut:
    payload = dict(row)
    payload["linked_tables_preview"] = linked_tables_preview or []
    payload["tables_count"] = int(payload.get("tables_count") or 0)
    payload["columns_count"] = int(payload.get("columns_count") or 0)
    if assignment is not None:
        payload.update(
            {
                "confidence_score": int(assignment.confidence_score or 0),
                "inference_source": assignment.inference_source,
                "inference_reason": assignment.inference_reason,
                "evidence": assignment.evidence_json,
                "applied_automatically": bool(assignment.applied_automatically),
                "review_status": assignment.review_status,
                "rule_key": assignment.rule_key,
                "rule_label": assignment.rule_label,
                "assignment_id": assignment.id,
                "assigned_entity_type": assignment.entity_type,
                "assigned_entity_id": assignment.entity_id,
                "assigned_scope": "aggregated"
                if assignment.entity_type == "table" and assignment.inference_source == "column_tags"
                else assignment.entity_type,
                "reviewed_by_user_id": assignment.reviewed_by_user_id,
                "reviewed_at": assignment.reviewed_at,
            }
        )
    return TagOut(**payload)



def list_tags_payload(
    *,
    db: Session,
    query: str | None,
    group: str | None,
    subgroup: str | None,
    status_filter: str | None,
    tag_type: str | None,
    in_use: bool | None = None,
    without_use: bool | None = None,
) -> list[TagOut]:
    stmt = base_tag_select()
    if query:
        pattern = f"%{query.strip()}%"
        stmt = stmt.where(
            or_(
                Tag.name.ilike(pattern),
                Tag.slug.ilike(pattern),
                Tag.description.ilike(pattern),
                Tag.group_name.ilike(pattern),
                Tag.subgroup_name.ilike(pattern),
            )
        )
    if group:
        stmt = stmt.where(Tag.group_name == group)
    if subgroup:
        stmt = stmt.where(Tag.subgroup_name == subgroup)
    if status_filter:
        stmt = stmt.where(Tag.status == normalize_tag_status(status_filter))
    if tag_type:
        stmt = stmt.where(Tag.tag_type == tag_type)

    rows = db.execute(stmt.order_by(Tag.group_name.nulls_last(), Tag.subgroup_name.nulls_last(), Tag.name)).mappings().all()
    previews = preview_linked_tables(db, [int(row["id"]) for row in rows], limit_per_tag=3)
    items = [row_to_tag_out(dict(row), previews.get(int(row["id"]), [])) for row in rows]
    if in_use and without_use:
        return []
    if in_use:
        items = [item for item in items if (item.tables_count + item.columns_count) > 0]
    if without_use:
        items = [item for item in items if (item.tables_count + item.columns_count) == 0]
    return items



def get_tag_detail_payload(*, db: Session, tag_id: int):
    row = db.execute(base_tag_select().where(Tag.id == tag_id)).mappings().first()
    if not row:
        return None
    preview = preview_linked_tables(db, [tag_id], limit_per_tag=4).get(tag_id, [])
    linked_tables = all_linked_tables(db, tag_id)
    payload = row_to_tag_out(dict(row), preview).model_dump()
    payload["linked_tables"] = linked_tables
    return payload



def build_tag_out_from_model(db: Session, tag: Tag) -> TagOut:
    usage_row = db.execute(
        select(
            func.count(func.distinct(TagAssignment.entity_id)).filter(TagAssignment.entity_type == "table").label("tables_count"),
            func.count(func.distinct(TagAssignment.entity_id)).filter(TagAssignment.entity_type == "column").label("columns_count"),
        ).where(TagAssignment.tag_id == tag.id)
    ).mappings().first() or {}
    preview = preview_linked_tables(db, [tag.id]).get(tag.id, [])
    return row_to_tag_out(
        {
            **serialize_model(tag),
            "tables_count": int(usage_row.get("tables_count") or 0),
            "columns_count": int(usage_row.get("columns_count") or 0),
        },
        preview,
    )


def load_entity_tag_contexts(
    db: Session,
    *,
    entity_type: str,
    entity_ids: list[int],
) -> dict[int, list[TagOut]]:
    if not entity_ids:
        return {}
    rows = db.execute(
        select(TagAssignment, Tag)
        .join(Tag, Tag.id == TagAssignment.tag_id)
        .where(
            TagAssignment.entity_type == entity_type,
            TagAssignment.entity_id.in_(entity_ids),
        )
        .order_by(Tag.group_name.nulls_last(), Tag.subgroup_name.nulls_last(), Tag.name)
    ).all()
    grouped: dict[int, list[TagOut]] = {int(entity_id): [] for entity_id in entity_ids}
    for assignment, tag in rows:
        grouped.setdefault(int(assignment.entity_id), []).append(
            row_to_tag_out({**serialize_model(tag), "tables_count": 0, "columns_count": 0}, assignment=assignment)
        )
    return grouped



def find_existing_tag_conflict(db: Session, *, name: str, slug: str):
    return db.scalar(select(Tag).where(or_(Tag.name == name, Tag.slug == slug)))



def find_existing_assignment(db: Session, *, tag_id: int, entity_type: str, entity_id: int):
    return db.scalar(
        select(TagAssignment).where(
            and_(
                TagAssignment.tag_id == tag_id,
                TagAssignment.entity_type == entity_type,
                TagAssignment.entity_id == entity_id,
            )
        )
    )


def reset_tags(
    session: Session,
    *,
    actor_user_id: int | None = None,
    audit_kwargs: dict | None = None,
) -> tuple[int, int, int, int]:
    deleted_assignments = int(session.scalar(select(func.count(TagAssignment.id))) or 0)
    deleted_tags = int(session.scalar(select(func.count(Tag.id))) or 0)
    deleted_overrides = int(session.scalar(select(func.count(TagAssignmentOverride.id))) or 0)
    deleted_events = int(session.scalar(select(func.count(TagIntelligenceEvent.id))) or 0)
    if deleted_assignments:
        session.execute(delete(TagAssignment))
    if deleted_overrides:
        session.execute(delete(TagAssignmentOverride))
    if deleted_events:
        session.execute(delete(TagIntelligenceEvent))
    if deleted_tags:
        session.execute(delete(Tag))
    payload = dict(audit_kwargs or {})
    if actor_user_id is not None and payload.get("user_id") is None:
        payload["user_id"] = actor_user_id
    write_audit_log_sync(
        session,
        action="tags.reset_all",
        entity_type="tag",
        entity_id="all",
        source_module="tags",
        metadata={
            "deleted_tags": deleted_tags,
            "deleted_assignments": deleted_assignments,
            "deleted_overrides": deleted_overrides,
            "deleted_events": deleted_events,
            "message": "Tags reset",
        },
        **payload,
    )
    session.commit()
    return deleted_tags, deleted_assignments, deleted_overrides, deleted_events
