from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from sqlalchemy.orm import Session

from t2c_data.core.db import get_db
from t2c_data.core.deps import require_roles
from t2c_data.features.search import (
    SearchAliasFilters,
    create_alias,
    delete_alias,
    delete_favorite_asset,
    get_alias_detail,
    get_alias_filters,
    get_critical_results,
    get_favorite_results,
    SearchFilters,
    get_popular_results,
    get_recent_asset_results,
    get_recent_searches,
    is_favorite_asset,
    list_aliases,
    search_global,
    search_suggestions,
    track_recent_query,
    track_result_click,
    upsert_favorite_asset,
    update_alias,
)
from t2c_data.features.audit import AuditFieldChange
from t2c_data.models.auth import User
from t2c_data.schemas.search import (
    SearchAliasCreateIn,
    SearchAliasFiltersOut,
    SearchAliasItemOut,
    SearchAliasListOut,
    SearchAliasUpdateIn,
    SearchCollectionResponse,
    SearchFavoriteAssetIn,
    SearchFavoriteStatusOut,
    SearchHit,
    SearchResponse,
    SearchResultsResponse,
    SearchSuggestionsResponse,
    SearchTrackClickIn,
    SearchTrackOut,
    SearchTrackQueryIn,
)
from t2c_data.services.audit import log_field_changes, request_audit_kwargs, write_audit_log_sync

router = APIRouter(prefix="/search", tags=["search"])


def _search_alias_audit_context(serialized: dict[str, object]) -> tuple[str, int, str | None, int | None]:
    entity_type = str(serialized["entity_type"])
    if entity_type == "table":
        return "table", int(serialized["table_id"]), None, None
    return "column", int(serialized["column_id"]), "table", int(serialized["table_id"])


def _filters_from_query(
    result_type: str | None,
    source: str | None,
    database: str | None,
    schema: str | None,
    domain: str | None,
    owner: str | None,
    classification: str | None,
    certification: str | None,
    incidents: str | None,
    governance_maturity: str | None,
) -> SearchFilters:
    return SearchFilters(
        result_type=result_type,
        source=source,
        database=database,
        schema=schema,
        domain=domain,
        owner=owner,
        classification=classification,
        certification=certification,
        incidents=incidents,
        governance_maturity=governance_maturity,
    )


@router.get("", response_model=SearchResponse)
def search_legacy_alias(
    q: str = Query("", description="Texto da busca global"),
    type: str | None = Query(None, alias="type"),
    source: str | None = Query(None),
    database: str | None = Query(None),
    schema: str | None = Query(None),
    domain: str | None = Query(None),
    owner: str | None = Query(None),
    classification: str | None = Query(None),
    certification: str | None = Query(None),
    incidents: str | None = Query(None),
    governance_maturity: str | None = Query(None),
    limit: int = Query(80, ge=1, le=200),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> SearchResponse:
    payload = search_global(
        db,
        q,
        filters=_filters_from_query(type, source, database, schema, domain, owner, classification, certification, incidents, governance_maturity),
        limit=limit,
        current_user=current_user,
    )
    return SearchResponse(
        query=payload["query"],
        total=payload["total"],
        hits=[
            SearchHit(
                entity_type=str(item["entity_type"]),
                entity_id=int(item["entity_id"]),
                name=str(item["title"]),
                description=(str(item["description"]) if item.get("description") else None),
            )
            for item in payload["items"]
        ],
    )


@router.get("/global", response_model=SearchResultsResponse)
def search_global_summary(
    q: str = Query("", description="Texto da busca global"),
    type: str | None = Query(None, alias="type"),
    source: str | None = Query(None),
    database: str | None = Query(None),
    schema: str | None = Query(None),
    domain: str | None = Query(None),
    owner: str | None = Query(None),
    classification: str | None = Query(None),
    certification: str | None = Query(None),
    incidents: str | None = Query(None),
    governance_maturity: str | None = Query(None),
    limit: int = Query(80, ge=1, le=200),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> SearchResultsResponse:
    payload = search_global(
        db,
        q,
        filters=_filters_from_query(type, source, database, schema, domain, owner, classification, certification, incidents, governance_maturity),
        limit=limit,
        current_user=current_user,
    )
    return SearchResultsResponse.model_validate(payload)


@router.get("/results", response_model=SearchResultsResponse)
def search_results(
    q: str = Query("", description="Texto da busca global"),
    type: str | None = Query(None, alias="type"),
    source: str | None = Query(None),
    database: str | None = Query(None),
    schema: str | None = Query(None),
    domain: str | None = Query(None),
    owner: str | None = Query(None),
    classification: str | None = Query(None),
    certification: str | None = Query(None),
    incidents: str | None = Query(None),
    governance_maturity: str | None = Query(None),
    limit: int = Query(120, ge=1, le=200),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> SearchResultsResponse:
    payload = search_global(
        db,
        q,
        filters=_filters_from_query(type, source, database, schema, domain, owner, classification, certification, incidents, governance_maturity),
        limit=limit,
        current_user=current_user,
    )
    return SearchResultsResponse.model_validate(payload)


@router.get("/suggestions", response_model=SearchSuggestionsResponse)
def search_autocomplete(
    q: str = Query("", description="Texto da busca para autocomplete"),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> SearchSuggestionsResponse:
    payload = search_suggestions(db, q, current_user=current_user)
    return SearchSuggestionsResponse.model_validate(payload)


@router.get("/recent", response_model=SearchCollectionResponse)
def search_recent_items(
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> SearchCollectionResponse:
    return SearchCollectionResponse.model_validate(get_recent_searches(db, user=current_user))


@router.get("/recent-assets", response_model=SearchCollectionResponse)
def search_recent_asset_items(
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> SearchCollectionResponse:
    return SearchCollectionResponse.model_validate(get_recent_asset_results(db, user=current_user))


@router.get("/popular", response_model=SearchCollectionResponse)
def search_popular_items(
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> SearchCollectionResponse:
    return SearchCollectionResponse.model_validate(get_popular_results(db, user=current_user))


@router.get("/critical", response_model=SearchCollectionResponse)
def search_critical_items(
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> SearchCollectionResponse:
    return SearchCollectionResponse.model_validate(get_critical_results(db, user=current_user))


@router.get("/favorites", response_model=SearchCollectionResponse)
def search_favorite_items(
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> SearchCollectionResponse:
    return SearchCollectionResponse.model_validate(get_favorite_results(db, user=current_user))


@router.get("/favorites/{entity_type}/{entity_id}", response_model=SearchFavoriteStatusOut)
def search_favorite_status(
    entity_type: str,
    entity_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> SearchFavoriteStatusOut:
    return SearchFavoriteStatusOut(
        favorite=is_favorite_asset(db, user=current_user, entity_type=entity_type.strip().lower(), entity_id=entity_id)
    )


@router.put("/favorites", response_model=SearchTrackOut)
def upsert_search_favorite(
    payload: SearchFavoriteAssetIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> SearchTrackOut:
    try:
        upsert_favorite_asset(
            db,
            user=current_user,
            entity_type=payload.entity_type,
            entity_id=payload.entity_id,
            label=payload.label,
            target_url=payload.target_url,
            category=payload.category,
            subtitle=payload.subtitle,
            context_path=payload.context_path,
            metadata=payload.metadata,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc
    db.commit()
    return SearchTrackOut(ok=True)


@router.delete("/favorites/{entity_type}/{entity_id}", response_model=SearchTrackOut)
def delete_search_favorite(
    entity_type: str,
    entity_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> SearchTrackOut:
    delete_favorite_asset(db, user=current_user, entity_type=entity_type, entity_id=entity_id)
    db.commit()
    return SearchTrackOut(ok=True)


@router.post("/track-query", response_model=SearchTrackOut)
def search_track_query(
    payload: SearchTrackQueryIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> SearchTrackOut:
    track_recent_query(db, user=current_user, query=payload.query)
    db.commit()
    return SearchTrackOut(ok=True)


@router.post("/track-click", response_model=SearchTrackOut)
def search_track_click(
    payload: SearchTrackClickIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> SearchTrackOut:
    track_result_click(
        db,
        user=current_user,
        entity_type=payload.entity_type,
        entity_id=payload.entity_id,
        query_text=payload.query,
        target_url=payload.target_url,
    )
    db.commit()
    return SearchTrackOut(ok=True)


@router.get("/alias-filters", response_model=SearchAliasFiltersOut)
def search_alias_filters(
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "viewer")),
) -> SearchAliasFiltersOut:
    return SearchAliasFiltersOut.model_validate(get_alias_filters(db))


@router.get("/aliases", response_model=SearchAliasListOut)
def search_aliases(
    q: str | None = Query(default=None),
    entity_type: str | None = Query(default=None),
    label_kind: str | None = Query(default=None),
    datasource_id: int | None = Query(default=None),
    database_id: int | None = Query(default=None),
    schema_id: int | None = Query(default=None),
    table_id: int | None = Query(default=None),
    column_id: int | None = Query(default=None),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "viewer")),
) -> SearchAliasListOut:
    payload = list_aliases(
        db,
        SearchAliasFilters(
            entity_type=entity_type,
            label_kind=label_kind,
            datasource_id=datasource_id,
            database_id=database_id,
            schema_id=schema_id,
            table_id=table_id,
            column_id=column_id,
            query=q,
            limit=limit,
            offset=offset,
        ),
    )
    return SearchAliasListOut.model_validate(payload)


@router.post("/aliases", response_model=SearchAliasItemOut, status_code=status.HTTP_201_CREATED)
def create_search_alias(
    request: Request,
    payload: SearchAliasCreateIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor")),
) -> SearchAliasItemOut:
    try:
        item = create_alias(
            db,
            entity_type=payload.entity_type,
            label_kind=payload.label_kind,
            label=payload.label,
            table_id=payload.table_id,
            column_id=payload.column_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    db.flush()
    serialized = get_alias_detail(db, entity_type=payload.entity_type, alias_id=item.id)
    audit_kwargs = request_audit_kwargs(request, current_user)
    entity_type, entity_id, parent_entity_type, parent_entity_id = _search_alias_audit_context(serialized)
    log_field_changes(
        db,
        action="search_alias.create",
        entity_type=entity_type,
        entity_id=entity_id,
        parent_entity_type=parent_entity_type,
        parent_entity_id=parent_entity_id,
        source_module="search.aliases",
        changes=[
            AuditFieldChange(
                field_name=payload.label_kind,
                before=None,
                after={"id": int(item.id), "label": str(serialized["label"])},
                change_type="create",
                metadata={"alias_id": int(item.id), "alias_entity_type": payload.entity_type},
            )
        ],
        metadata={"message": "Alias criado via manutenção de busca global"},
        audit_kwargs=audit_kwargs,
        actor_user_id=current_user.id,
    )
    db.commit()
    return SearchAliasItemOut.model_validate(serialized)


@router.put("/aliases/{entity_type}/{alias_id}", response_model=SearchAliasItemOut)
def update_search_alias(
    entity_type: str,
    alias_id: int,
    payload: SearchAliasUpdateIn,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor")),
) -> SearchAliasItemOut:
    try:
        before = get_alias_detail(db, entity_type=entity_type, alias_id=alias_id)
        update_alias(db, entity_type=entity_type, alias_id=alias_id, label_kind=payload.label_kind, label=payload.label)
        after = get_alias_detail(db, entity_type=entity_type, alias_id=alias_id)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    audit_kwargs = request_audit_kwargs(request, current_user)
    target_entity_type, target_entity_id, parent_entity_type, parent_entity_id = _search_alias_audit_context(after)
    changes = [
        AuditFieldChange(
            field_name=str(before["label_kind"]),
            before={"id": alias_id, "label": str(before["label"])},
            after={"id": alias_id, "label": str(after["label"])},
            change_type="update",
            metadata={"alias_id": alias_id, "alias_entity_type": entity_type},
        )
    ]
    if before["label_kind"] != after["label_kind"]:
        changes.append(
            AuditFieldChange(
                field_name="label_kind",
                before=str(before["label_kind"]),
                after=str(after["label_kind"]),
                change_type="update",
                metadata={"alias_id": alias_id, "alias_entity_type": entity_type},
            )
        )
    log_field_changes(
        db,
        action="search_alias.update",
        entity_type=target_entity_type,
        entity_id=target_entity_id,
        parent_entity_type=parent_entity_type,
        parent_entity_id=parent_entity_id,
        source_module="search.aliases",
        changes=changes,
        metadata={"message": "Alias atualizado via manutenção de busca global"},
        audit_kwargs=audit_kwargs,
        actor_user_id=current_user.id,
    )
    db.commit()
    return SearchAliasItemOut.model_validate(after)


@router.delete("/aliases/{entity_type}/{alias_id}", response_model=SearchTrackOut)
def delete_search_alias(
    entity_type: str,
    alias_id: int,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor")),
) -> SearchTrackOut:
    try:
        before = get_alias_detail(db, entity_type=entity_type, alias_id=alias_id)
        delete_alias(db, entity_type=entity_type, alias_id=alias_id)
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    audit_kwargs = request_audit_kwargs(request, current_user)
    target_entity_type, target_entity_id, parent_entity_type, parent_entity_id = _search_alias_audit_context(before)
    log_field_changes(
        db,
        action="search_alias.delete",
        entity_type=target_entity_type,
        entity_id=target_entity_id,
        parent_entity_type=parent_entity_type,
        parent_entity_id=parent_entity_id,
        source_module="search.aliases",
        changes=[
            AuditFieldChange(
                field_name=str(before["label_kind"]),
                before={"id": alias_id, "label": str(before["label"])},
                after=None,
                change_type="delete",
                metadata={"alias_id": alias_id, "alias_entity_type": entity_type},
            )
        ],
        metadata={"message": "Alias removido via manutenção de busca global"},
        audit_kwargs=audit_kwargs,
        actor_user_id=current_user.id,
    )
    db.commit()
    return SearchTrackOut(ok=True)
