from __future__ import annotations

import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from sqlalchemy import and_, asc, func, or_, select
from sqlalchemy.orm import Session, selectinload

from t2c_data.core.db import get_db
from t2c_data.core.external_auth import require_api_key_scopes, get_external_api_key
from t2c_data.core.rate_limit import enforce_external_rate_limit
from t2c_data.features.catalog.canonical_assets import compact_canonical_asset_context, load_table_canonical_context
from t2c_data.features.catalog.search_queries import search_tree
from t2c_data.features.catalog.table_detail import build_table_detail_out
from t2c_data.features.certification.api_support import (
    build_certification_summary_out,
    build_table_certification_query,
    certification_order_clause,
)
from t2c_data.features.data_quality.rule_management import list_rules_with_filters
from t2c_data.features.governance import (
    get_governance_pending_center_queue,
    get_governance_pending_center_summary,
)
from t2c_data.features.governance.settings import get_governance_settings_snapshot
from t2c_data.features.incidents.api_support import (
    build_incident_filters,
    build_incident_summary,
    incident_query,
    serialize_incident_out,
)
from t2c_data.features.incidents.query_support import filter_incidents_for_user
from t2c_data.features.lineage.table_summary import get_table_summary
from t2c_data.features.platform import list_platform_domain_events, serialize_platform_domain_event
from t2c_data.features.platform.events import PlatformEventFilters
from t2c_data.features.platform.event_catalog import list_supported_platform_events
from t2c_data.features.ingestion import IngestionIntegrationUnavailable, load_ingestion_operational_overview, operational_session
from t2c_data.features.platform.sensitive_data import can_view_sensitive_data, mask_payload_by_policy
from t2c_data.features.platform.visibility import mask_table_payload, mask_certification_summary_payload, table_visibility_decision_from_entity
from t2c_data.features.pagination import paginate_items
from t2c_data.features.privacy_access import can_view_table


logger = logging.getLogger(__name__)
from t2c_data.features.tags.api_support import build_tag_out_from_model, find_existing_tag_conflict, list_tags_payload, normalize_tag_payload
from t2c_data.features.glossary.api_support import build_term_out_from_model, find_existing_term_conflict, list_terms_payload, normalize_term_payload
from t2c_data.models.catalog import DataSource, Database, Schema, TableEntity
from t2c_data.models.incident import Incident
from t2c_data.models.glossary import GlossaryTerm
from t2c_data.models.tag import Tag
from t2c_data.models.platform import PlatformDomainEvent
from t2c_data.schemas.catalog import ExplorerSearchResultOut, TableDetailOut, TableOut, TableCertificationSummaryOut
from t2c_data.schemas.canonical_asset import CanonicalAssetOut
from t2c_data.schemas.governance import (
    GovernancePendingCenterQueueOut,
    GovernancePendingCenterSummaryOut,
)
from t2c_data.schemas.incident import IncidentOut, IncidentSummaryOut
from t2c_data.schemas.lineage import LineageAssetSummaryOut
from t2c_data.schemas.platform import PlatformDomainEventOut, PlatformDomainEventsOut, PlatformSupportedEventsOut
from t2c_data.schemas.ingestion import IngestionOperationalOverviewOut
from t2c_data.schemas.pagination import PageOut
from t2c_data.schemas.tag import TagCreate, TagOut, TagUpdate
from t2c_data.schemas.glossary import GlossaryTermCreate, GlossaryTermOut, GlossaryTermUpdate
from t2c_data.schemas.dq_rules import DQRuleOut
from t2c_data.services.audit import AuditFieldChange, log_field_changes, request_audit_kwargs, serialize_model


router = APIRouter(
    prefix="/external",
    tags=["external"],
    dependencies=[Depends(enforce_external_rate_limit)],
)


def _external_user_from_request(request: Request):
    return getattr(request.state, "external_user", None)


def _external_api_key_from_request(request: Request):
    return getattr(request.state, "current_api_key", None)


def _external_api_metadata(request: Request) -> dict[str, object]:
    api_key = _external_api_key_from_request(request)
    payload: dict[str, object] = {}
    if api_key is not None:
        payload["api_key_id"] = getattr(api_key, "id", None)
        payload["api_key_public_id"] = getattr(api_key, "public_id", None)
        payload["api_key_prefix"] = getattr(api_key, "token_prefix", None)
        payload["api_key_name"] = getattr(api_key, "name", None)
    return payload


def _tag_payload_summary(payload: dict[str, object]) -> dict[str, object]:
    keys = [
        "external_id",
        "slug",
        "name",
        "status",
        "group_name",
        "subgroup_name",
        "tag_type",
        "suggested_scope",
    ]
    return {key: payload.get(key) for key in keys if payload.get(key) is not None}


def _term_payload_summary(payload: dict[str, object]) -> dict[str, object]:
    keys = [
        "external_id",
        "slug",
        "name",
        "status",
        "category",
        "subcategory",
        "suggested_priority",
    ]
    return {key: payload.get(key) for key in keys if payload.get(key) is not None}


TAG_AUDIT_FIELDS = {
    "name",
    "description",
    "group_name",
    "subgroup_name",
    "synonyms",
    "status",
    "notes",
    "tag_type",
    "color",
    "suggested_scope",
    "example_of_use",
}

GLOSSARY_AUDIT_FIELDS = {
    "name",
    "definition",
    "description",
    "category",
    "subcategory",
    "synonyms",
    "status",
    "notes",
    "tag_labels",
    "steward",
    "suggested_priority",
    "example_of_use",
}


@router.get("/ping", response_model=dict[str, str])
def external_ping(_: object = Depends(get_external_api_key)) -> dict[str, str]:
    return {"message": "pong"}


@router.get("/catalog/tables", response_model=PageOut[TableOut])
def external_catalog_tables(
    request: Request,
    q: str | None = Query(default=None),
    schema_name: str | None = Query(default=None),
    database_name: str | None = Query(default=None),
    datasource_name: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=100),
    db: Session = Depends(get_db),
    _: object = Depends(require_api_key_scopes("catalog.read")),
) -> PageOut[TableOut]:
    user = _external_user_from_request(request)
    stmt = (
        select(TableEntity)
        .options(
            selectinload(TableEntity.schema)
            .selectinload(Schema.database)
            .selectinload(Database.datasource),
            selectinload(TableEntity.data_owner),
        )
        .order_by(asc(TableEntity.name))
    )
    if q:
        pattern = f"%{q.strip()}%"
        stmt = stmt.where(TableEntity.name.ilike(pattern))
    if schema_name:
        stmt = stmt.join(Schema, TableEntity.schema_id == Schema.id).where(
            func.lower(Schema.name) == schema_name.strip().lower()
        )
    if database_name:
        stmt = stmt.join(Schema, TableEntity.schema_id == Schema.id).join(Database, Schema.database_id == Database.id).where(
            func.lower(Database.name) == database_name.strip().lower()
        )
    if datasource_name:
        stmt = (
            stmt.join(Schema, TableEntity.schema_id == Schema.id)
            .join(Database, Schema.database_id == Database.id)
            .join(DataSource, Database.datasource_id == DataSource.id)
            .where(func.lower(DataSource.name) == datasource_name.strip().lower())
        )
    tables = db.scalars(stmt.order_by(asc(TableEntity.name))).all()
    payloads: list[TableOut] = []
    for table in tables:
        if user is not None and not can_view_table(user, table):
            continue
        item = TableOut.model_validate(table)
        decision = table_visibility_decision_from_entity(table, user=user)
        if decision.masked:
            item = TableOut(**mask_table_payload(item.model_dump()))
        if not can_view_sensitive_data(user, table=table):
            masked_payload = mask_payload_by_policy(item.model_dump(), can_view_sensitive=False)
            masked_payload["owner"] = "[masked]" if masked_payload.get("owner") is not None else None
            masked_payload["owner_email"] = "[masked]" if masked_payload.get("owner_email") is not None else None
            if isinstance(masked_payload.get("data_owner"), dict):
                masked_payload["data_owner"]["name"] = "[masked]"
                masked_payload["data_owner"]["email"] = "[masked]"
            item = TableOut(**masked_payload)
        payloads.append(item)
    return paginate_items(payloads, page=page, page_size=page_size)


@router.get("/catalog/tables/{table_id}", response_model=TableDetailOut)
def external_catalog_table_detail(
    table_id: int,
    request: Request,
    db: Session = Depends(get_db),
    _: object = Depends(require_api_key_scopes("catalog.read")),
) -> TableDetailOut:
    user = _external_user_from_request(request)
    table = db.scalar(
        select(TableEntity)
        .options(
            selectinload(TableEntity.columns),
            selectinload(TableEntity.schema)
            .selectinload(Schema.database)
            .selectinload(Database.datasource),
            selectinload(TableEntity.data_owner),
        )
        .where(TableEntity.id == table_id)
    )
    if not table:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Table not found")
    if user is not None and not can_view_table(user, table):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Table not visible")
    decision = table_visibility_decision_from_entity(table, user=user)
    return build_table_detail_out(
        db,
        table,
        masked=decision.masked,
        can_view_sensitive=can_view_sensitive_data(user, table=table),
    )


@router.get("/explorer/search", response_model=PageOut[ExplorerSearchResultOut])
def external_explorer_search(
    request: Request,
    q: str = Query(..., min_length=1),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=100),
    governance_maturity: str | None = Query(default=None),
    db: Session = Depends(get_db),
    _: object = Depends(require_api_key_scopes("explorer.read")),
) -> PageOut[ExplorerSearchResultOut]:
    user = _external_user_from_request(request)
    if user is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="External user missing")
    fetched_limit = max(page * page_size, page_size)
    results = search_tree(db=db, q=q, limit=fetched_limit, current_user=user, governance_maturity=governance_maturity)
    return paginate_items(results, page=page, page_size=page_size)


@router.get("/explorer/tables/{table_id}/summary", response_model=CanonicalAssetOut)
def external_explorer_table_summary(
    table_id: int,
    request: Request,
    db: Session = Depends(get_db),
    _: object = Depends(require_api_key_scopes("explorer.read")),
) -> CanonicalAssetOut:
    user = _external_user_from_request(request)
    return compact_canonical_asset_context(load_table_canonical_context(db, table_id, current_user=user))


@router.get("/tags", response_model=PageOut[TagOut])
def external_tags(
    query: str | None = Query(None, min_length=1),
    group: str | None = Query(None),
    subgroup: str | None = Query(None),
    status_filter: str | None = Query(None, alias="status"),
    tag_type: str | None = Query(None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=100),
    db: Session = Depends(get_db),
    _: object = Depends(require_api_key_scopes("tags.read")),
) -> PageOut[TagOut]:
    items = list_tags_payload(
        db=db,
        query=query,
        group=group,
        subgroup=subgroup,
        status_filter=status_filter,
        tag_type=tag_type,
    )
    return paginate_items(items, page=page, page_size=page_size)


@router.post("/tags", response_model=TagOut, status_code=status.HTTP_201_CREATED)
def external_create_tag(
    payload: TagCreate,
    request: Request,
    db: Session = Depends(get_db),
    _: object = Depends(require_api_key_scopes("tags.create")),
) -> TagOut:
    user = _external_user_from_request(request)
    data = normalize_tag_payload(payload.model_dump())
    existing = find_existing_tag_conflict(db, name=data["name"], slug=data["slug"])
    if existing:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Tag já existe com este nome ou slug.")
    tag = Tag(**data)
    db.add(tag)
    try:
        db.commit()
    except Exception as exc:  # noqa: BLE001
        db.rollback()
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Slug ou ID da tag já existe.") from exc
    db.refresh(tag)
    audit_kwargs = request_audit_kwargs(request, user)
    log_field_changes(
        db,
        action="external_api.tags.create",
        entity_type="tag",
        entity_id=tag.id,
        source_module="external_api",
        changes=[
            AuditFieldChange(
                field_name="tag",
                before=None,
                after={"id": tag.id, "label": tag.name},
                change_type="create",
            )
        ],
        metadata={**_tag_payload_summary(data), **_external_api_metadata(request), "message": "Tag criada via API externa"},
        audit_kwargs=audit_kwargs,
        actor_user_id=getattr(user, "id", None),
    )
    db.commit()
    return build_tag_out_from_model(db, tag)


@router.patch("/tags/{tag_id}", response_model=TagOut)
def external_update_tag(
    tag_id: int,
    payload: TagUpdate,
    request: Request,
    db: Session = Depends(get_db),
    _: object = Depends(require_api_key_scopes("tags.update")),
) -> TagOut:
    user = _external_user_from_request(request)
    tag = db.get(Tag, tag_id)
    if tag is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Tag not found")
    before = serialize_model(tag)
    updates = normalize_tag_payload({**serialize_model(tag), **payload.model_dump(exclude_unset=True)})
    for key, value in updates.items():
        setattr(tag, key, value)
    try:
        db.commit()
    except Exception as exc:  # noqa: BLE001
        db.rollback()
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Slug ou ID da tag já existe.") from exc
    db.refresh(tag)
    after = serialize_model(tag)
    audit_kwargs = request_audit_kwargs(request, user)
    log_field_changes(
        db,
        action="external_api.tags.update",
        entity_type="tag",
        entity_id=tag.id,
        source_module="external_api",
        changes=[
            AuditFieldChange(field_name=field_name, before=before.get(field_name), after=after.get(field_name))
            for field_name in sorted(TAG_AUDIT_FIELDS)
            if before.get(field_name) != after.get(field_name)
        ],
        metadata={
            **_tag_payload_summary(payload.model_dump(exclude_unset=True)),
            **_external_api_metadata(request),
            "message": "Tag atualizada via API externa",
        },
        audit_kwargs=audit_kwargs,
        actor_user_id=getattr(user, "id", None),
    )
    db.commit()
    return build_tag_out_from_model(db, tag)


@router.delete("/tags/{tag_id}", response_model=dict[str, bool])
def external_delete_tag(
    tag_id: int,
    request: Request,
    db: Session = Depends(get_db),
    _: object = Depends(require_api_key_scopes("tags.delete")),
) -> dict[str, bool]:
    user = _external_user_from_request(request)
    tag = db.get(Tag, tag_id)
    if tag is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Tag not found")
    before = serialize_model(tag)
    db.delete(tag)
    db.commit()
    audit_kwargs = request_audit_kwargs(request, user)
    log_field_changes(
        db,
        action="external_api.tags.delete",
        entity_type="tag",
        entity_id=tag_id,
        source_module="external_api",
        changes=[
            AuditFieldChange(
                field_name="tag",
                before={"id": tag_id, "label": before.get("name")},
                after=None,
                change_type="delete",
            )
        ],
        metadata={**_tag_payload_summary(before), **_external_api_metadata(request), "message": "Tag removida via API externa"},
        audit_kwargs=audit_kwargs,
        actor_user_id=getattr(user, "id", None),
    )
    db.commit()
    return {"ok": True}


@router.get("/glossary/terms", response_model=PageOut[GlossaryTermOut])
def external_glossary_terms(
    query: str | None = Query(None, min_length=1),
    category: str | None = Query(None),
    subcategory: str | None = Query(None),
    status_filter: str | None = Query(None, alias="status"),
    priority: str | None = Query(None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=100),
    db: Session = Depends(get_db),
    _: object = Depends(require_api_key_scopes("glossary.read")),
) -> PageOut[GlossaryTermOut]:
    items = list_terms_payload(
        db=db,
        query=query,
        category=category,
        subcategory=subcategory,
        status_filter=status_filter,
        priority=priority,
    )
    return paginate_items(items, page=page, page_size=page_size)


@router.post("/glossary/terms", response_model=GlossaryTermOut, status_code=status.HTTP_201_CREATED)
def external_create_term(
    payload: GlossaryTermCreate,
    request: Request,
    db: Session = Depends(get_db),
    _: object = Depends(require_api_key_scopes("glossary.create")),
) -> GlossaryTermOut:
    user = _external_user_from_request(request)
    data = normalize_term_payload(payload.model_dump())
    existing = find_existing_term_conflict(db, name=data["name"], slug=data["slug"])
    if existing:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Term already exists with this name or slug")
    term = GlossaryTerm(**data)
    db.add(term)
    try:
        db.commit()
    except Exception as exc:  # noqa: BLE001
        db.rollback()
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Slug or ID already exists.") from exc
    db.refresh(term)
    audit_kwargs = request_audit_kwargs(request, user)
    log_field_changes(
        db,
        action="external_api.glossary.create",
        entity_type="glossary_term",
        entity_id=term.id,
        source_module="external_api",
        changes=[
            AuditFieldChange(
                field_name="term",
                before=None,
                after={"id": term.id, "label": term.name},
                change_type="create",
            )
        ],
        metadata={**_term_payload_summary(data), **_external_api_metadata(request), "message": "Termo criado via API externa"},
        audit_kwargs=audit_kwargs,
        actor_user_id=getattr(user, "id", None),
    )
    db.commit()
    return build_term_out_from_model(db, term)


@router.patch("/glossary/terms/{term_id}", response_model=GlossaryTermOut)
def external_update_term(
    term_id: int,
    payload: GlossaryTermUpdate,
    request: Request,
    db: Session = Depends(get_db),
    _: object = Depends(require_api_key_scopes("glossary.update")),
) -> GlossaryTermOut:
    user = _external_user_from_request(request)
    term = db.get(GlossaryTerm, term_id)
    if term is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Term not found")
    before = serialize_model(term)
    updates = normalize_term_payload({**serialize_model(term), **payload.model_dump(exclude_unset=True)})
    for key, value in updates.items():
        setattr(term, key, value)
    try:
        db.commit()
    except Exception as exc:  # noqa: BLE001
        db.rollback()
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Slug or ID already exists.") from exc
    db.refresh(term)
    after = serialize_model(term)
    audit_kwargs = request_audit_kwargs(request, user)
    log_field_changes(
        db,
        action="external_api.glossary.update",
        entity_type="glossary_term",
        entity_id=term.id,
        source_module="external_api",
        changes=[
            AuditFieldChange(field_name=field_name, before=before.get(field_name), after=after.get(field_name))
            for field_name in sorted(GLOSSARY_AUDIT_FIELDS)
            if before.get(field_name) != after.get(field_name)
        ],
        metadata={
            **_term_payload_summary(payload.model_dump(exclude_unset=True)),
            **_external_api_metadata(request),
            "message": "Termo atualizado via API externa",
        },
        audit_kwargs=audit_kwargs,
        actor_user_id=getattr(user, "id", None),
    )
    db.commit()
    return build_term_out_from_model(db, term)


@router.delete("/glossary/terms/{term_id}", response_model=dict[str, bool])
def external_delete_term(
    term_id: int,
    request: Request,
    db: Session = Depends(get_db),
    _: object = Depends(require_api_key_scopes("glossary.delete")),
) -> dict[str, bool]:
    user = _external_user_from_request(request)
    term = db.get(GlossaryTerm, term_id)
    if term is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Term not found")
    before = serialize_model(term)
    db.delete(term)
    db.commit()
    audit_kwargs = request_audit_kwargs(request, user)
    log_field_changes(
        db,
        action="external_api.glossary.delete",
        entity_type="glossary_term",
        entity_id=term_id,
        source_module="external_api",
        changes=[
            AuditFieldChange(
                field_name="term",
                before={"id": term_id, "label": before.get("name")},
                after=None,
                change_type="delete",
            )
        ],
        metadata={**_term_payload_summary(before), **_external_api_metadata(request), "message": "Termo removido via API externa"},
        audit_kwargs=audit_kwargs,
        actor_user_id=getattr(user, "id", None),
    )
    db.commit()
    return {"ok": True}


@router.get("/certification/tables", response_model=PageOut[TableCertificationSummaryOut])
def external_certification_tables(
    request: Request,
    q: str | None = Query(default=None),
    certification_status: str | None = Query(default=None),
    certification_criticality: str | None = Query(default=None),
    owner_id: int | None = Query(default=None),
    schema_name: str | None = Query(default=None),
    database_name: str | None = Query(default=None),
    datasource_name: str | None = Query(default=None),
    sort_by: str = Query(default="updated_at"),
    sort_dir: str = Query(default="desc"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=100),
    db: Session = Depends(get_db),
    _: object = Depends(require_api_key_scopes("certification.read")),
) -> PageOut[TableCertificationSummaryOut]:
    user = _external_user_from_request(request)
    query = build_table_certification_query()
    if q and q.strip():
        term = f"%{q.strip()}%"
        query = query.where(
            or_(
                TableEntity.name.ilike(term),
                Schema.name.ilike(term),
                Database.name.ilike(term),
                DataSource.name.ilike(term),
                TableEntity.owner.ilike(term),
                TableEntity.certification_notes.ilike(term),
            )
        )
    if certification_criticality:
        query = query.where(TableEntity.certification_criticality == certification_criticality)
    if owner_id is not None:
        query = query.where(TableEntity.data_owner_id == owner_id)
    if schema_name and schema_name.strip():
        query = query.where(func.lower(Schema.name) == schema_name.strip().lower())
    if database_name and database_name.strip():
        query = query.where(func.lower(Database.name) == database_name.strip().lower())
    if datasource_name and datasource_name.strip():
        query = query.where(func.lower(DataSource.name) == datasource_name.strip().lower())
    query = query.order_by(certification_order_clause(sort_by, sort_dir), asc(Schema.name), asc(TableEntity.name))
    tables = db.scalars(query).unique().all()
    settings_snapshot = get_governance_settings_snapshot(db)
    summaries: list[TableCertificationSummaryOut] = []
    for table in tables:
        if user is not None and not can_view_table(user, table):
            continue
        summary = build_certification_summary_out(db, table, settings_snapshot=settings_snapshot)
        decision = table_visibility_decision_from_entity(table, user=user)
        if decision.masked:
            summary = TableCertificationSummaryOut(**mask_certification_summary_payload(summary.model_dump()))
        if not can_view_sensitive_data(user, table=table):
            masked_payload = mask_payload_by_policy(summary.model_dump(), can_view_sensitive=False)
            masked_payload["owner"] = "[masked]" if masked_payload.get("owner") is not None else None
            masked_payload["owner_email"] = "[masked]" if masked_payload.get("owner_email") is not None else None
            if isinstance(masked_payload.get("data_owner"), dict):
                masked_payload["data_owner"]["name"] = "[masked]"
                masked_payload["data_owner"]["email"] = "[masked]"
            summary = TableCertificationSummaryOut(**masked_payload)
        summaries.append(summary)
    if certification_status:
        summaries = [item for item in summaries if item.certification_status == certification_status]
    return paginate_items(summaries, page=page, page_size=page_size)


@router.get("/dq/rules", response_model=PageOut[DQRuleOut])
def external_dq_rules(
    request: Request,
    rule_id: int | None = Query(default=None),
    q: str | None = Query(default=None),
    table_fqn: str | None = Query(default=None),
    is_active: bool | None = Query(default=None),
    severity: str | None = Query(default=None),
    last_status: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=100),
    db: Session = Depends(get_db),
    _: object = Depends(require_api_key_scopes("dq.read")),
) -> PageOut[DQRuleOut]:
    user = _external_user_from_request(request)
    items = list_rules_with_filters(
        db=db,
        rule_id=rule_id,
        q=q,
        table_id=None,
        table_fqn=table_fqn,
        is_active=is_active,
        severity=severity,
        last_status=last_status,
        current_user=user,
    )
    return paginate_items(items, page=page, page_size=page_size)


@router.get("/incidents", response_model=PageOut[IncidentOut])
def external_incidents(
    request: Request,
    status: list[str] | None = Query(default=None),
    severity: list[str] | None = Query(default=None),
    entity_type: str | None = Query(default=None),
    owner_id: int | None = Query(default=None),
    reporter_id: int | None = Query(default=None),
    source_type: str | None = Query(default=None),
    source_ref_id: int | None = Query(default=None),
    table_id: int | None = Query(default=None),
    q: str | None = Query(default=None),
    date_from: datetime | None = Query(default=None),
    date_to: datetime | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=100),
    db: Session = Depends(get_db),
    _: object = Depends(require_api_key_scopes("incidents.read")),
) -> PageOut[IncidentOut]:
    user = _external_user_from_request(request)
    query = incident_query()
    table_fqn = None
    if table_id is not None:
        row = db.execute(
            select(Schema.name, TableEntity.name)
            .join(TableEntity, TableEntity.schema_id == Schema.id)
            .where(TableEntity.id == table_id)
        ).first()
        if not row:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Table not found")
        table_fqn = f"{row[0]}.{row[1]}"
    filters = build_incident_filters(
        status,
        severity,
        entity_type,
        owner_id,
        reporter_id,
        source_type,
        source_ref_id,
        table_fqn,
        q,
        date_from,
        date_to,
    )
    if filters:
        query = query.where(and_(*filters))
    incidents = db.scalars(query.order_by(Incident.detected_at.desc(), Incident.id.desc())).all()
    incidents, profile_map = filter_incidents_for_user(db, incidents, user=user)
    items = [serialize_incident_out(item, profile_map) for item in incidents]
    return paginate_items(items, page=page, page_size=page_size)


@router.get("/incidents/summary", response_model=IncidentSummaryOut)
def external_incidents_summary(
    request: Request,
    days: int = Query(default=30, ge=1, le=365),
    status: list[str] | None = Query(default=None),
    severity: list[str] | None = Query(default=None),
    entity_type: str | None = Query(default=None),
    owner_id: int | None = Query(default=None),
    reporter_id: int | None = Query(default=None),
    source_type: str | None = Query(default=None),
    source_ref_id: int | None = Query(default=None),
    table_id: int | None = Query(default=None),
    q: str | None = Query(default=None),
    date_from: datetime | None = Query(default=None),
    date_to: datetime | None = Query(default=None),
    db: Session = Depends(get_db),
    _: object = Depends(require_api_key_scopes("incidents.read")),
) -> IncidentSummaryOut:
    user = _external_user_from_request(request)
    table_fqn = None
    if table_id is not None:
        row = db.execute(
            select(Schema.name, TableEntity.name)
            .join(TableEntity, TableEntity.schema_id == Schema.id)
            .where(TableEntity.id == table_id)
        ).first()
        if not row:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Table not found")
        table_fqn = f"{row[0]}.{row[1]}"
    return build_incident_summary(
        db,
        days=days,
        status=status,
        severity=severity,
        entity_type=entity_type,
        owner_id=owner_id,
        reporter_id=reporter_id,
        source_type=source_type,
        source_ref_id=source_ref_id,
        table_fqn=table_fqn,
        q=q,
        date_from=date_from,
        date_to=date_to,
        current_user=user,
    )


@router.get("/governance/pending-center/summary", response_model=GovernancePendingCenterSummaryOut)
def external_governance_pending_summary(
    request: Request,
    q: str | None = Query(default=None),
    severity: str | None = Query(default=None),
    origin: str | None = Query(default=None),
    owner_id: int | None = Query(default=None, ge=1),
    owner: str | None = Query(default=None),
    datasource: str | None = Query(default=None),
    schema_name: str | None = Query(default=None),
    domain: str | None = Query(default=None),
    status_filter: str | None = Query(default=None, alias="status"),
    db: Session = Depends(get_db),
    _: object = Depends(require_api_key_scopes("governance.read")),
) -> GovernancePendingCenterSummaryOut:
    user = _external_user_from_request(request)
    return GovernancePendingCenterSummaryOut(
        **get_governance_pending_center_summary(
            db,
            q=q,
            severity=severity,
            origin=origin,
            owner_id=owner_id,
            owner=owner,
            datasource=datasource,
            schema_name=schema_name,
            domain=domain,
            status_filter=status_filter,
            current_user=user,
        )
    )


@router.get("/governance/pending-center/queue", response_model=GovernancePendingCenterQueueOut)
def external_governance_pending_queue(
    request: Request,
    q: str | None = Query(default=None),
    severity: str | None = Query(default=None),
    origin: str | None = Query(default=None),
    owner_id: int | None = Query(default=None, ge=1),
    owner: str | None = Query(default=None),
    datasource: str | None = Query(default=None),
    schema_name: str | None = Query(default=None),
    domain: str | None = Query(default=None),
    status_filter: str | None = Query(default=None, alias="status"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=10, ge=1, le=200),
    db: Session = Depends(get_db),
    _: object = Depends(require_api_key_scopes("governance.read")),
) -> GovernancePendingCenterQueueOut:
    user = _external_user_from_request(request)
    return GovernancePendingCenterQueueOut(
        **get_governance_pending_center_queue(
            db,
            q=q,
            severity=severity,
            origin=origin,
            owner_id=owner_id,
            owner=owner,
            datasource=datasource,
            schema_name=schema_name,
            domain=domain,
            status_filter=status_filter,
            page=page,
            page_size=page_size,
            current_user=user,
        )
    )


@router.get("/lineage/tables/{table_id}/summary", response_model=LineageAssetSummaryOut)
def external_lineage_table_summary(
    table_id: int,
    request: Request,
    db: Session = Depends(get_db),
    _: object = Depends(require_api_key_scopes("lineage.read")),
) -> LineageAssetSummaryOut:
    user = _external_user_from_request(request)
    return get_table_summary(db, table_id, current_user=user)


@router.get("/platform/events", response_model=PlatformDomainEventsOut)
def external_platform_events(
    days: int = Query(default=30, ge=1, le=365),
    limit: int = Query(default=100, ge=1, le=500),
    table_id: int | None = None,
    entity_type: str | None = None,
    event_key: str | None = None,
    category: str | None = None,
    severity: str | None = None,
    q: str | None = None,
    db: Session = Depends(get_db),
    _: object = Depends(require_api_key_scopes("platform.read")),
) -> PlatformDomainEventsOut:
    payload = list_platform_domain_events(
        db,
        filters=PlatformEventFilters(
            days=days,
            limit=limit,
            table_id=table_id,
            entity_type=entity_type,
            event_key=event_key,
            category=category,
            severity=severity,
            q=q,
        ),
    )
    return PlatformDomainEventsOut(**payload)


@router.get("/platform/events/{event_id}", response_model=PlatformDomainEventOut)
def external_platform_event_detail(
    event_id: int,
    db: Session = Depends(get_db),
    _: object = Depends(require_api_key_scopes("platform.read")),
) -> PlatformDomainEventOut:
    event = db.get(PlatformDomainEvent, event_id)
    if event is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Evento não encontrado.")
    return PlatformDomainEventOut(**serialize_platform_domain_event(event))


@router.get("/platform/events/catalog", response_model=PlatformSupportedEventsOut)
def external_platform_event_catalog(
    _: object = Depends(require_api_key_scopes("platform.read")),
) -> PlatformSupportedEventsOut:
    payload = list_supported_platform_events()
    return PlatformSupportedEventsOut(
        generated_at=datetime.now(timezone.utc),
        total=int(payload["total"]),
        items=payload["items"],
    )


@router.get("/platform/ingestion/overview", response_model=IngestionOperationalOverviewOut)
def external_platform_ingestion_overview(
    limit: int = Query(default=8, ge=1, le=100),
    db: Session = Depends(get_db),
    _: object = Depends(require_api_key_scopes("platform.read")),
) -> IngestionOperationalOverviewOut:
    try:
        with operational_session(db) as operational_db:
            payload = load_ingestion_operational_overview(operational_db, limit=limit)
        return IngestionOperationalOverviewOut(**payload)
    except IngestionIntegrationUnavailable as _exc:
        logger.warning("ingestion overview unavailable: %s", _exc)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Serviço de ingestão indisponível no momento.",
        ) from _exc
