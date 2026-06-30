from __future__ import annotations

from datetime import datetime, timezone

from fastapi import HTTPException, status
from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session, selectinload

from t2c_data.core.rbac import is_admin_role, user_role_names
from t2c_data.features.access_control.policy import user_has_data_scope_rules
from t2c_data.features.dashboard.profile_loader import load_table_profiles
from t2c_data.features.governance.scoring import build_governance_score_for_profile
from t2c_data.features.governance.settings import get_governance_settings_snapshot
from t2c_data.features.governance.trust_score import build_trust_score_for_profile
from t2c_data.features.privacy_access import can_view_table
from t2c_data.features.platform.visibility import table_visibility_decision_from_entity
from t2c_data.features.tags.api_support import load_entity_tag_contexts
from t2c_data.models.catalog import ColumnEntity, DataSource, Database, Schema, TableEntity
from t2c_data.schemas.catalog import (
    TreeDatasourceChildrenOut,
    TreeDatasourceOut,
    TreeSchemaOut,
    TreeTableColumnsOut,
    TreeTableColumnsPageOut,
    TreeTableOut,
    TreeTablePageOut,
    TableColumnSummaryOut,
    TableSearchSuggestionOut,
)


def list_tree_datasources(*, db: Session, current_user) -> list[TreeDatasourceOut]:
    datasources = db.scalars(select(DataSource).order_by(DataSource.name)).all()
    visible_datasource_ids = {
        table.schema.database.datasource_id
        for table in db.scalars(
            select(TableEntity)
            .options(
                selectinload(TableEntity.schema).selectinload(Schema.database),
                selectinload(TableEntity.data_owner),
            )
        ).all()
        if can_view_table(current_user, table)
    }
    return [
        TreeDatasourceOut(id=d.id, name=d.name, db_type=d.db_type, database=d.database)
        for d in datasources
        if d.id in visible_datasource_ids
    ]


def get_tree_datasource_children(*, db: Session, datasource_id: int, current_user) -> TreeDatasourceChildrenOut:
    datasource = db.get(DataSource, datasource_id)
    if not datasource:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Datasource not found")

    database = db.scalar(
        select(Database)
        .where(Database.datasource_id == datasource_id)
        .order_by(Database.id.desc())
        .limit(1)
    )
    if not database:
        return TreeDatasourceChildrenOut(
            datasource_id=datasource.id,
            database_id=None,
            database=datasource.database,
            schemas=[],
        )

    schemas = db.scalars(select(Schema).where(Schema.database_id == database.id).order_by(Schema.name)).all()
    visible_schema_ids = {
        table.schema_id
        for table in db.scalars(
            select(TableEntity)
            .where(TableEntity.schema_id.in_([schema.id for schema in schemas]))
            .options(selectinload(TableEntity.data_owner))
        ).all()
        if can_view_table(current_user, table)
    }
    return TreeDatasourceChildrenOut(
        datasource_id=datasource.id,
        database_id=database.id,
        database=database.name,
        schemas=[TreeSchemaOut(id=s.id, name=s.name) for s in schemas if s.id in visible_schema_ids],
    )


def list_tree_schema_tables(*, db: Session, schema_id: int, current_user) -> list[TreeTableOut]:
    schema = db.get(Schema, schema_id)
    if not schema:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Schema not found")

    tables = db.scalars(
        select(TableEntity)
        .where(TableEntity.schema_id == schema_id)
        .options(selectinload(TableEntity.data_owner))
        .order_by(TableEntity.name)
    ).all()
    visible_tables = [t for t in tables if can_view_table(current_user, t)]
    tags_by_table_id = load_entity_tag_contexts(
        db,
        entity_type="table",
        entity_ids=[t.id for t in visible_tables],
    )
    settings_snapshot = get_governance_settings_snapshot(db)
    profiles = {
        table.table_id: table
        for table in load_table_profiles(db, datetime.now(timezone.utc), table_ids=[t.id for t in visible_tables])
    }
    scores = {
        table_id: build_governance_score_for_profile(profile, settings_snapshot=settings_snapshot)
        for table_id, profile in profiles.items()
    }
    trust_scores = {
        table_id: build_trust_score_for_profile(profile, settings_snapshot=settings_snapshot)
        for table_id, profile in profiles.items()
    }
    return [
        TreeTableOut(
            id=t.id,
            name=t.name,
            kind=t.table_type if t.table_type in {"table", "view", "collection"} else "table",
            governance_score=scores.get(t.id, {}).get("score"),
            governance_label=scores.get(t.id, {}).get("label"),
            governance_tone=scores.get(t.id, {}).get("tone"),
            certification_status=profiles.get(t.id).certification_status if t.id in profiles else None,
            readiness_score=int(profiles.get(t.id).readiness_score) if t.id in profiles else None,
            trust_score=int(trust_scores.get(t.id).score) if t.id in trust_scores else None,
            trust_label=str(trust_scores.get(t.id).label) if t.id in trust_scores else None,
            trust_tone=str(trust_scores.get(t.id).tone) if t.id in trust_scores else None,
            active_dq_violation=bool(profiles.get(t.id).active_dq_violation) if t.id in profiles else False,
            owner_defined=bool(profiles.get(t.id).owner_defined) if t.id in profiles else False,
            tags=tags_by_table_id.get(t.id, []),
        )
        for t in visible_tables
    ]


def list_tree_schema_tables_page(
    *,
    db: Session,
    schema_id: int,
    page: int,
    page_size: int,
    current_user,
) -> TreeTablePageOut:
    schema = db.get(Schema, schema_id)
    if not schema:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Schema not found")

    normalized_page = max(int(page or 1), 1)
    normalized_page_size = max(min(int(page_size or 50), 200), 1)
    offset = (normalized_page - 1) * normalized_page_size

    has_grants = user_has_data_scope_rules(current_user)
    is_admin = is_admin_role(user_role_names(current_user)) if current_user is not None else False
    can_use_direct_paging = is_admin or not has_grants

    total: int | None = None
    if can_use_direct_paging:
        total = int(
            db.scalar(select(func.count(TableEntity.id)).where(TableEntity.schema_id == schema_id)) or 0
        )

    stmt = (
        select(TableEntity)
        .where(TableEntity.schema_id == schema_id)
        .options(selectinload(TableEntity.data_owner))
        .order_by(TableEntity.name)
    )
    if can_use_direct_paging:
        tables = db.scalars(stmt.offset(offset).limit(normalized_page_size + 1)).all()
    else:
        tables = db.scalars(stmt.offset(offset).limit(normalized_page_size * 2 + 1)).all()

    visible_tables = [t for t in tables if can_view_table(current_user, t)]
    has_more = len(tables) > normalized_page_size
    visible_tables = visible_tables[:normalized_page_size]

    tags_by_table_id = load_entity_tag_contexts(
        db,
        entity_type="table",
        entity_ids=[t.id for t in visible_tables],
    )
    settings_snapshot = get_governance_settings_snapshot(db)
    profiles = {
        table.table_id: table
        for table in load_table_profiles(db, datetime.now(timezone.utc), table_ids=[t.id for t in visible_tables])
    }
    scores = {
        table_id: build_governance_score_for_profile(profile, settings_snapshot=settings_snapshot)
        for table_id, profile in profiles.items()
    }
    trust_scores = {
        table_id: build_trust_score_for_profile(profile, settings_snapshot=settings_snapshot)
        for table_id, profile in profiles.items()
    }

    items = [
        TreeTableOut(
            id=t.id,
            name=t.name,
            kind=t.table_type if t.table_type in {"table", "view", "collection"} else "table",
            governance_score=scores.get(t.id, {}).get("score"),
            governance_label=scores.get(t.id, {}).get("label"),
            governance_tone=scores.get(t.id, {}).get("tone"),
            certification_status=profiles.get(t.id).certification_status if t.id in profiles else None,
            readiness_score=int(profiles.get(t.id).readiness_score) if t.id in profiles else None,
            trust_score=int(trust_scores.get(t.id).score) if t.id in trust_scores else None,
            trust_label=str(trust_scores.get(t.id).label) if t.id in trust_scores else None,
            trust_tone=str(trust_scores.get(t.id).tone) if t.id in trust_scores else None,
            active_dq_violation=bool(profiles.get(t.id).active_dq_violation) if t.id in profiles else False,
            owner_defined=bool(profiles.get(t.id).owner_defined) if t.id in profiles else False,
            tags=tags_by_table_id.get(t.id, []),
        )
        for t in visible_tables
    ]

    return TreeTablePageOut(
        page=normalized_page,
        page_size=normalized_page_size,
        total=total,
        has_more=has_more,
        items=items,
    )


def search_table_suggestions(*, db: Session, q: str, limit: int, current_user) -> list[TableSearchSuggestionOut]:
    pattern = f"%{q.strip()}%"
    rows = db.execute(
        select(TableEntity, Schema, Database, DataSource)
        .join(Schema, TableEntity.schema_id == Schema.id)
        .join(Database, Schema.database_id == Database.id)
        .join(DataSource, Database.datasource_id == DataSource.id)
        .where(
            or_(
                TableEntity.name.ilike(pattern),
                Schema.name.ilike(pattern),
                Database.name.ilike(pattern),
                DataSource.name.ilike(pattern),
                (Schema.name + "." + TableEntity.name).ilike(pattern),
                (DataSource.name + "." + Database.name + "." + Schema.name + "." + TableEntity.name).ilike(pattern),
            )
        )
        .order_by(DataSource.name.asc(), Database.name.asc(), Schema.name.asc(), TableEntity.name.asc())
        .limit(limit)
    ).all()
    suggestions: list[TableSearchSuggestionOut] = []
    for table, schema, database, datasource in rows:
        if not can_view_table(current_user, table):
            continue
        suggestions.append(
            TableSearchSuggestionOut(
                id=table.id,
                name=table.name,
                table_fqn=f"{datasource.name}.{database.name}.{schema.name}.{table.name}",
                datasource_name=datasource.name,
                database_name=database.name,
                schema_name=schema.name,
                table_type=table.table_type,
            )
        )
    return suggestions[:limit]


def list_table_columns(*, db: Session, table_id: int, current_user) -> list[TreeTableColumnsOut]:
    table = db.get(TableEntity, table_id)
    if not table:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Table not found")
    if not can_view_table(current_user, table):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Table is not visible for this profile")
    masked = table_visibility_decision_from_entity(table, user=current_user).masked

    columns = db.scalars(
        select(ColumnEntity)
        .where(ColumnEntity.table_id == table_id)
        .options(
            selectinload(ColumnEntity.data_owner),
            selectinload(ColumnEntity.owner_reviewed_by_user),
        )
        .order_by(ColumnEntity.ordinal_position)
    ).all()
    tags_by_column_id = load_entity_tag_contexts(
        db,
        entity_type="column",
        entity_ids=[column.id for column in columns],
    )
    return [
        TreeTableColumnsOut(
            id=c.id,
            table_id=c.table_id,
            data_owner_id=c.data_owner_id,
            name=c.name,
            data_type=c.data_type,
            is_nullable=c.is_nullable,
            is_primary_key=c.is_primary_key,
            ordinal_position=c.ordinal_position,
            external_id=c.external_id,
            slug=c.slug,
            udt_name=c.udt_name,
            character_maximum_length=c.character_maximum_length,
            numeric_precision=c.numeric_precision,
            numeric_scale=c.numeric_scale,
            column_default=None if masked else c.column_default,
            existing_comment=None if masked else c.existing_comment,
            description_source=None if masked else c.description_source,
            description_manual=None if masked else c.description_manual,
            dictionary_description=None if masked else c.dictionary_description,
            dictionary_comment=None if masked else c.dictionary_comment,
            data_owner=c.data_owner,
            owner_reviewed_by_user_id=c.owner_reviewed_by_user_id,
            owner_reviewed_by_user_name=c.owner_reviewed_by_user_name,
            owner_reviewed_by_user_email=c.owner_reviewed_by_user_email,
            owner_reviewed_at=c.owner_reviewed_at,
            description=None if masked else (c.description_manual or c.description_source),
            tags=tags_by_column_id.get(c.id, []),
        )
        for c in columns
    ]


def list_table_columns_page(
    *,
    db: Session,
    table_id: int,
    page: int,
    page_size: int,
    current_user,
) -> TreeTableColumnsPageOut:
    table = db.get(TableEntity, table_id)
    if not table:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Table not found")
    if not can_view_table(current_user, table):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Table is not visible for this profile")
    masked = table_visibility_decision_from_entity(table, user=current_user).masked

    normalized_page = max(int(page or 1), 1)
    normalized_page_size = max(min(int(page_size or 60), 200), 1)
    offset = (normalized_page - 1) * normalized_page_size

    total = int(
        db.scalar(select(func.count(ColumnEntity.id)).where(ColumnEntity.table_id == table_id)) or 0
    )
    columns = db.scalars(
        select(ColumnEntity)
        .where(ColumnEntity.table_id == table_id)
        .options(
            selectinload(ColumnEntity.data_owner),
            selectinload(ColumnEntity.owner_reviewed_by_user),
        )
        .order_by(ColumnEntity.ordinal_position)
        .offset(offset)
        .limit(normalized_page_size + 1)
    ).all()
    has_more = len(columns) > normalized_page_size
    columns = columns[:normalized_page_size]

    tags_by_column_id = load_entity_tag_contexts(
        db,
        entity_type="column",
        entity_ids=[column.id for column in columns],
    )
    items = [
        TreeTableColumnsOut(
            id=c.id,
            table_id=c.table_id,
            data_owner_id=c.data_owner_id,
            name=c.name,
            data_type=c.data_type,
            is_nullable=c.is_nullable,
            is_primary_key=c.is_primary_key,
            ordinal_position=c.ordinal_position,
            external_id=c.external_id,
            slug=c.slug,
            udt_name=c.udt_name,
            character_maximum_length=c.character_maximum_length,
            numeric_precision=c.numeric_precision,
            numeric_scale=c.numeric_scale,
            column_default=None if masked else c.column_default,
            existing_comment=None if masked else c.existing_comment,
            description_source=None if masked else c.description_source,
            description_manual=None if masked else c.description_manual,
            dictionary_description=None if masked else c.dictionary_description,
            dictionary_comment=None if masked else c.dictionary_comment,
            data_owner=c.data_owner,
            owner_reviewed_by_user_id=c.owner_reviewed_by_user_id,
            owner_reviewed_by_user_name=c.owner_reviewed_by_user_name,
            owner_reviewed_by_user_email=c.owner_reviewed_by_user_email,
            owner_reviewed_at=c.owner_reviewed_at,
            description=None if masked else (c.description_manual or c.description_source),
            tags=tags_by_column_id.get(c.id, []),
        )
        for c in columns
    ]
    return TreeTableColumnsPageOut(
        page=normalized_page,
        page_size=normalized_page_size,
        total=total,
        has_more=has_more,
        items=items,
    )


def get_table_columns_summary(*, db: Session, table_id: int, current_user) -> TableColumnSummaryOut:
    table = db.get(TableEntity, table_id)
    if not table:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Table not found")
    if not can_view_table(current_user, table):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Table is not visible for this profile")
    masked = table_visibility_decision_from_entity(table, user=current_user).masked

    total = int(db.scalar(select(func.count(ColumnEntity.id)).where(ColumnEntity.table_id == table_id)) or 0)
    required = int(
        db.scalar(
            select(func.count(ColumnEntity.id)).where(ColumnEntity.table_id == table_id, ColumnEntity.is_nullable.is_(False))
        )
        or 0
    )
    primary_keys = int(
        db.scalar(
            select(func.count(ColumnEntity.id)).where(ColumnEntity.table_id == table_id, ColumnEntity.is_primary_key.is_(True))
        )
        or 0
    )
    documented = int(
        db.scalar(
            select(func.count(ColumnEntity.id)).where(
                ColumnEntity.table_id == table_id,
                or_(
                    ColumnEntity.dictionary_description.is_not(None),
                    ColumnEntity.description_manual.is_not(None),
                    ColumnEntity.description_source.is_not(None),
                ),
            )
        )
        or 0
    )
    commented = int(
        db.scalar(
            select(func.count(ColumnEntity.id)).where(
                ColumnEntity.table_id == table_id,
                or_(
                    ColumnEntity.dictionary_comment.is_not(None),
                    ColumnEntity.existing_comment.is_not(None),
                ),
            )
        )
        or 0
    )
    preview_columns = db.scalars(
        select(ColumnEntity)
        .where(ColumnEntity.table_id == table_id)
        .options(
            selectinload(ColumnEntity.data_owner),
            selectinload(ColumnEntity.owner_reviewed_by_user),
        )
        .order_by(ColumnEntity.is_primary_key.desc(), ColumnEntity.ordinal_position.asc())
        .limit(6)
    ).all()

    preview = [
        TreeTableColumnsOut(
            id=c.id,
            table_id=c.table_id,
            data_owner_id=c.data_owner_id,
            name=c.name,
            data_type=c.data_type,
            is_nullable=c.is_nullable,
            is_primary_key=c.is_primary_key,
            ordinal_position=c.ordinal_position,
            external_id=c.external_id,
            slug=c.slug,
            udt_name=c.udt_name,
            character_maximum_length=c.character_maximum_length,
            numeric_precision=c.numeric_precision,
            numeric_scale=c.numeric_scale,
            column_default=None if masked else c.column_default,
            existing_comment=None if masked else c.existing_comment,
            description_source=None if masked else c.description_source,
            description_manual=None if masked else c.description_manual,
            dictionary_description=None if masked else c.dictionary_description,
            dictionary_comment=None if masked else c.dictionary_comment,
            data_owner=c.data_owner,
            owner_reviewed_by_user_id=c.owner_reviewed_by_user_id,
            owner_reviewed_by_user_name=c.owner_reviewed_by_user_name,
            owner_reviewed_by_user_email=c.owner_reviewed_by_user_email,
            owner_reviewed_at=c.owner_reviewed_at,
            description=None if masked else (c.description_manual or c.description_source),
            tags=[],
        )
        for c in preview_columns
    ]

    return TableColumnSummaryOut(
        table_id=table_id,
        total=total,
        required=required,
        nullable=max(total - required, 0),
        primary_keys=primary_keys,
        documented=0 if masked else documented,
        commented=0 if masked else commented,
        preview=preview,
    )
