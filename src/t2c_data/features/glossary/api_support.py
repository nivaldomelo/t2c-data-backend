from __future__ import annotations

from sqlalchemy import and_, delete, func, or_, select
from sqlalchemy.orm import Session

from t2c_data.models.catalog import ColumnEntity, Database, Schema, TableEntity
from t2c_data.models.glossary import GlossaryAssignment, GlossaryTerm
from t2c_data.schemas.glossary import GlossaryTermOut
from t2c_data.services.audit import serialize_model
from t2c_data.services.audit import write_audit_log_sync
from t2c_data.features.glossary.spreadsheet import (
    all_linked_tables,
    normalize_glossary_status,
    normalize_priority,
    preview_linked_tables,
)
from t2c_data.features.tags.spreadsheet import slugify_tag


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



def normalize_term_payload(data: dict) -> dict:
    normalized = dict(data)
    if "slug" in normalized and normalized["slug"] is not None:
        normalized["slug"] = slugify_tag(str(normalized["slug"]))
    if "name" in normalized and normalized["name"] is not None:
        normalized["name"] = str(normalized["name"]).strip()
    for field in (
        "external_id",
        "definition",
        "description",
        "steward",
        "category",
        "subcategory",
        "example_of_use",
        "synonyms",
        "tag_labels",
        "notes",
    ):
        if field in normalized and normalized[field] is not None:
            value = str(normalized[field]).strip()
            normalized[field] = value or None
    if "status" in normalized and normalized["status"] is not None:
        normalized["status"] = normalize_glossary_status(str(normalized["status"]))
    if "suggested_priority" in normalized:
        normalized["suggested_priority"] = normalize_priority(normalized.get("suggested_priority"))
    if not normalized.get("slug") and normalized.get("name"):
        normalized["slug"] = slugify_tag(str(normalized["name"]))
    if not normalized.get("slug"):
        from fastapi import HTTPException, status

        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Slug is required")
    if not normalized.get("name"):
        from fastapi import HTTPException, status

        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Term name is required")
    if not normalized.get("definition"):
        from fastapi import HTTPException, status

        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Definition is required")
    if not normalized.get("description"):
        normalized["description"] = normalized["definition"]
    return normalized



def usage_subquery():
    return (
        select(
            GlossaryAssignment.term_id.label("term_id"),
            func.count(func.distinct(GlossaryAssignment.entity_id)).label("tables_count"),
        )
        .where(GlossaryAssignment.entity_type == "table")
        .group_by(GlossaryAssignment.term_id)
        .subquery()
    )



def base_term_select():
    usage_subq = usage_subquery()
    return (
        select(
            GlossaryTerm.id,
            GlossaryTerm.external_id,
            GlossaryTerm.slug,
            GlossaryTerm.name,
            GlossaryTerm.definition,
            GlossaryTerm.description,
            GlossaryTerm.steward,
            GlossaryTerm.category,
            GlossaryTerm.subcategory,
            GlossaryTerm.example_of_use,
            GlossaryTerm.synonyms,
            GlossaryTerm.suggested_priority,
            GlossaryTerm.status,
            GlossaryTerm.tag_labels,
            GlossaryTerm.notes,
            GlossaryTerm.created_at,
            GlossaryTerm.updated_at,
            func.coalesce(usage_subq.c.tables_count, 0).label("tables_count"),
        )
        .outerjoin(usage_subq, usage_subq.c.term_id == GlossaryTerm.id)
    )



def row_to_term_out(row: dict, linked_tables_preview: list | None = None) -> GlossaryTermOut:
    payload = dict(row)
    payload["tables_count"] = int(payload.get("tables_count") or 0)
    payload["linked_tables_preview"] = linked_tables_preview or []
    return GlossaryTermOut(**payload)



def list_terms_payload(
    *,
    db: Session,
    query: str | None,
    category: str | None,
    subcategory: str | None,
    status_filter: str | None,
    priority: str | None,
    in_use: bool | None = None,
    without_use: bool | None = None,
) -> list[GlossaryTermOut]:
    stmt = base_term_select()
    if query:
        pattern = f"%{query.strip()}%"
        stmt = stmt.where(
            or_(
                GlossaryTerm.name.ilike(pattern),
                GlossaryTerm.slug.ilike(pattern),
                GlossaryTerm.definition.ilike(pattern),
                GlossaryTerm.category.ilike(pattern),
                GlossaryTerm.subcategory.ilike(pattern),
            )
        )
    if category:
        stmt = stmt.where(GlossaryTerm.category == category)
    if subcategory:
        stmt = stmt.where(GlossaryTerm.subcategory == subcategory)
    if status_filter:
        stmt = stmt.where(GlossaryTerm.status == normalize_glossary_status(status_filter))
    if priority:
        stmt = stmt.where(GlossaryTerm.suggested_priority == normalize_priority(priority))

    rows = db.execute(
        stmt.order_by(GlossaryTerm.category.nulls_last(), GlossaryTerm.subcategory.nulls_last(), GlossaryTerm.name)
    ).mappings().all()
    if in_use is not None or without_use is not None:
        filtered_rows = []
        for row in rows:
            usage_count = int(row.get("tables_count") or 0)
            is_in_use = usage_count > 0
            if in_use is True and not is_in_use:
                continue
            if without_use is True and is_in_use:
                continue
            filtered_rows.append(row)
        rows = filtered_rows
    previews = preview_linked_tables(db, [int(row["id"]) for row in rows], limit_per_term=3)
    return [row_to_term_out(dict(row), previews.get(int(row["id"]), [])) for row in rows]


def glossary_summary_payload(
    *,
    db: Session,
    query: str | None,
    category: str | None,
    subcategory: str | None,
    status_filter: str | None,
    priority: str | None,
    in_use: bool | None = None,
    without_use: bool | None = None,
) -> dict[str, int]:
    items = list_terms_payload(
        db=db,
        query=query,
        category=category,
        subcategory=subcategory,
        status_filter=status_filter,
        priority=priority,
        in_use=in_use,
        without_use=without_use,
    )
    active = sum(1 for item in items if item.status == "active")
    in_use_count = sum(1 for item in items if int(item.tables_count or 0) > 0)
    categories = len({(item.category or "Sem categoria").strip() for item in items})
    return {
        "total": len(items),
        "active": active,
        "in_use": in_use_count,
        "categories": categories,
    }



def get_term_detail_payload(*, db: Session, term_id: int):
    row = db.execute(base_term_select().where(GlossaryTerm.id == term_id)).mappings().first()
    if not row:
        return None
    preview = preview_linked_tables(db, [term_id], limit_per_term=4).get(term_id, [])
    linked_tables = all_linked_tables(db, term_id)
    payload = row_to_term_out(dict(row), preview).model_dump()
    payload["linked_tables"] = linked_tables
    return payload



def build_term_out_from_model(db: Session, term: GlossaryTerm) -> GlossaryTermOut:
    tables_count = db.scalar(
        select(func.count(func.distinct(GlossaryAssignment.entity_id))).where(
            GlossaryAssignment.term_id == term.id,
            GlossaryAssignment.entity_type == "table",
        )
    )
    preview = preview_linked_tables(db, [term.id]).get(term.id, [])
    return row_to_term_out({**serialize_model(term), "tables_count": int(tables_count or 0)}, preview)



def find_existing_term_conflict(db: Session, *, name: str, slug: str):
    return db.scalar(select(GlossaryTerm).where(or_(GlossaryTerm.name == name, GlossaryTerm.slug == slug)))



def find_existing_assignment(db: Session, *, term_id: int, entity_type: str, entity_id: int):
    return db.scalar(
        select(GlossaryAssignment).where(
            and_(
                GlossaryAssignment.term_id == term_id,
                GlossaryAssignment.entity_type == entity_type,
                GlossaryAssignment.entity_id == entity_id,
            )
        )
    )


def reset_glossary_terms(session: Session, *, actor_user_id: int | None = None, audit_kwargs: dict | None = None) -> tuple[int, int]:
    deleted_assignments = int(session.scalar(select(func.count(GlossaryAssignment.id))) or 0)
    deleted_terms = int(session.scalar(select(func.count(GlossaryTerm.id))) or 0)
    if deleted_assignments:
        session.execute(delete(GlossaryAssignment))
    if deleted_terms:
        session.execute(delete(GlossaryTerm))
    payload = dict(audit_kwargs or {})
    if actor_user_id is not None and payload.get("user_id") is None:
        payload["user_id"] = actor_user_id
    write_audit_log_sync(
        session,
        action="glossary.reset_all",
        entity_type="glossary_term",
        entity_id="all",
        source_module="glossary",
        metadata={
            "deleted_terms": deleted_terms,
            "deleted_assignments": deleted_assignments,
            "message": "Glossary reset",
        },
        **payload,
    )
    session.commit()
    return deleted_terms, deleted_assignments
