from __future__ import annotations

import logging
from io import BytesIO
from datetime import datetime

from fastapi import APIRouter, Depends, File, HTTPException, Query, Request, UploadFile, status
from fastapi.responses import StreamingResponse
from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session, selectinload
from sqlalchemy.exc import IntegrityError

from t2c_data.core.db import get_db
from t2c_data.core.deps import require_roles
from t2c_data.features.export_security import DEFAULT_EXPORT_LIMIT, audit_export_event, enforce_export_permission, resolve_export_limit
from t2c_data.features.catalog.application import (
    get_table_glossary_terms,
    get_table_tags,
    get_tree_datasource_children,
    get_table_columns_summary,
    list_table_columns,
    list_table_columns_page,
    list_tree_datasources,
    list_tree_schema_tables,
    list_tree_schema_tables_page,
    patch_table_with_audit,
    search_table_suggestions,
    search_tree,
)
from t2c_data.features.catalog.table_detail import build_table_detail_out
from t2c_data.features.catalog.canonical_assets import compact_canonical_asset_context, load_column_canonical_context, load_table_canonical_context
from t2c_data.features.catalog.table_volume import (
    get_latest_table_volume,
    list_table_volume_history,
    measure_all_active_tables_volume,
    measure_table_volume,
)
from t2c_data.features.catalog.column_dictionary_admin import (
    ColumnDictionaryFilters,
    bulk_update_column_dictionary,
    clear_column_dictionary_item,
    export_column_dictionary_rows,
    get_column_dictionary_detail,
    get_column_dictionary_summary,
    import_column_dictionary_from_file,
    list_column_dictionary,
    preview_column_dictionary_import,
    reset_column_dictionary_curation,
    template_column_dictionary_workbook,
    update_column_dictionary_item,
)
from t2c_data.features.catalog.column_dictionary_workbook import COLUMN_DICTIONARY_HEADERS
from t2c_data.features.catalog.correlation import build_table_correlation_summary
from t2c_data.features.catalog.operational_context import load_table_operational_context
from t2c_data.features.metabase import get_table_metabase_consumption
from t2c_data.features.timeline.service import get_asset_timeline
from t2c_data.features.privacy_access import can_view_table
from t2c_data.features.platform.sensitive_data import can_view_sensitive_data, mask_payload_by_policy
from t2c_data.features.platform.visibility import mask_table_payload, table_visibility_decision_from_entity
from t2c_data.models.auth import User
from t2c_data.models.catalog import ColumnEntity, DataSource, Database, Schema, TableEntity
from t2c_data.schemas.asset_context import AssetOperationalContextOut
from t2c_data.schemas.canonical_asset import CanonicalAssetOut
from t2c_data.schemas.metabase import MetabaseConsumptionSummaryOut
from t2c_data.schemas.catalog import (
    ColumnDictionaryImportResult,
    ExplorerSearchResultOut,
    TableDetailOut,
    TableCorrelationSummaryOut,
    TableLocatorOut,
    TableOut,
    TableSearchSuggestionOut,
    TablePatch,
    TableVolumeHistoryOut,
    TableVolumeRunOut,
    TableVolumeSnapshotOut,
    TableColumnSummaryOut,
    TreeDatasourceChildrenOut,
    TreeDatasourceOut,
    TreeTableColumnsOut,
    TreeTableColumnsPageOut,
    TreeTableOut,
    TreeTablePageOut,
)
from t2c_data.schemas.column_dictionary import (
    ColumnDictionaryBulkUpdateIn,
    ColumnDictionaryBulkUpdateOut,
    ColumnDictionaryDetailOut,
    ColumnDictionaryImportPreviewOut,
    ColumnDictionaryPageOut,
    ColumnDictionaryResetOut,
    ColumnDictionarySummaryOut,
    ColumnDictionaryUpdateIn,
)
from t2c_data.schemas.glossary import GlossaryTermOut
from t2c_data.schemas.maintenance import DestructiveActionConfirmIn
from t2c_data.schemas.timeline import TimelinePageOut
from t2c_data.schemas.tag import TagOut
from t2c_data.schemas.contracts import DataContractSummaryOut
from t2c_data.services.audit import request_audit_kwargs, write_audit_log_sync
from t2c_data.features.tags.spreadsheet import TagSpreadsheetError

router = APIRouter(prefix="/catalog", tags=["catalog"])
logger = logging.getLogger(__name__)

def _serialize_table_out(
    table: TableEntity,
    *,
    masked: bool = False,
    can_view_sensitive: bool = False,
) -> TableOut:
    payload = TableOut.model_validate(table).model_dump()
    if masked:
        payload = mask_table_payload(payload)
    if not can_view_sensitive:
        payload = mask_payload_by_policy(payload, can_view_sensitive=False)
        payload["owner"] = "[masked]" if payload.get("owner") is not None else None
        payload["owner_email"] = "[masked]" if payload.get("owner_email") is not None else None
        if isinstance(payload.get("data_owner"), dict):
            payload["data_owner"]["name"] = "[masked]"
            payload["data_owner"]["email"] = "[masked]"
    return TableOut(**payload)


def _serialize_table_detail(
    table: TableEntity,
    *,
    db: Session,
    masked: bool = False,
    can_view_sensitive: bool = False,
    data_contract: DataContractSummaryOut | None = None,
) -> TableDetailOut:
    return build_table_detail_out(
        db,
        table,
        masked=masked,
        can_view_sensitive=can_view_sensitive,
        data_contract=data_contract,
    )


@router.get("/column-dictionary/template", response_model=None)
def download_column_dictionary_template(
    _: User = Depends(require_roles("admin", "editor", "viewer")),
) -> StreamingResponse:
    workbook = template_column_dictionary_workbook()
    return StreamingResponse(
        BytesIO(workbook),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": 'attachment; filename="dicionario_colunas_template.xlsx"'},
    )


@router.get("/column-dictionary/export", response_model=None)
def export_column_dictionary(
    request: Request,
    datasource_name: str | None = Query(default=None),
    q: str | None = Query(default=None),
    schema_name: str | None = Query(default=None),
    table_name: str | None = Query(default=None),
    data_type: str | None = Query(default=None),
    is_primary_key: bool | None = Query(default=None),
    is_nullable: bool | None = Query(default=None),
    has_description: bool | None = Query(default=None),
    has_comment: bool | None = Query(default=None),
    has_existing_comment: bool | None = Query(default=None),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> StreamingResponse:
    enforce_export_permission(current_user, "catalog:export")
    export_limit = resolve_export_limit(source_module="catalog", entity_type="column_dictionary")
    workbook, exported_rows, truncated = export_column_dictionary_rows(
        db,
        filters=ColumnDictionaryFilters(
            datasource_name=datasource_name,
            q=q,
            schema_name=schema_name,
            table_name=table_name,
            data_type=data_type,
            is_primary_key=is_primary_key,
            is_nullable=is_nullable,
            has_description=has_description,
            has_comment=has_comment,
            has_existing_comment=has_existing_comment,
        ),
        current_user=current_user,
        limit=export_limit,
    )
    audit_export_event(
        db,
        request=request,
        current_user=current_user,
        action="catalog.column_dictionary.export_xlsx",
        entity_type="column_dictionary",
        source_module="catalog",
        row_count=exported_rows,
        filters={
            "datasource_name": datasource_name,
            "q": q,
            "schema_name": schema_name,
            "table_name": table_name,
            "data_type": data_type,
            "is_primary_key": is_primary_key,
            "is_nullable": is_nullable,
            "has_description": has_description,
            "has_comment": has_comment,
            "has_existing_comment": has_existing_comment,
        },
        limit=export_limit,
        truncated=truncated,
        export_format="xlsx",
        permission_name="catalog:export",
    )
    return StreamingResponse(
        BytesIO(workbook),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": 'attachment; filename="dicionario_colunas_export.xlsx"'},
    )


@router.get("/column-dictionary/summary", response_model=ColumnDictionarySummaryOut)
def get_column_dictionary_summary_route(
    datasource_name: str | None = Query(default=None),
    q: str | None = Query(default=None),
    schema_name: str | None = Query(default=None),
    table_name: str | None = Query(default=None),
    data_type: str | None = Query(default=None),
    is_primary_key: bool | None = Query(default=None),
    is_nullable: bool | None = Query(default=None),
    has_description: bool | None = Query(default=None),
    has_comment: bool | None = Query(default=None),
    has_existing_comment: bool | None = Query(default=None),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> ColumnDictionarySummaryOut:
    return get_column_dictionary_summary(
        db,
        filters=ColumnDictionaryFilters(
            datasource_name=datasource_name,
            q=q,
            schema_name=schema_name,
            table_name=table_name,
            data_type=data_type,
            is_primary_key=is_primary_key,
            is_nullable=is_nullable,
            has_description=has_description,
            has_comment=has_comment,
            has_existing_comment=has_existing_comment,
        ),
        current_user=current_user,
    )


@router.get("/column-dictionary/items", response_model=ColumnDictionaryPageOut)
def list_column_dictionary_items(
    datasource_name: str | None = Query(default=None),
    q: str | None = Query(default=None),
    schema_name: str | None = Query(default=None),
    table_name: str | None = Query(default=None),
    data_type: str | None = Query(default=None),
    is_primary_key: bool | None = Query(default=None),
    is_nullable: bool | None = Query(default=None),
    has_description: bool | None = Query(default=None),
    has_comment: bool | None = Query(default=None),
    has_existing_comment: bool | None = Query(default=None),
    sort_by: str = Query(default="schema"),
    sort_dir: str = Query(default="asc"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> ColumnDictionaryPageOut:
    return list_column_dictionary(
        db,
        filters=ColumnDictionaryFilters(
            datasource_name=datasource_name,
            q=q,
            schema_name=schema_name,
            table_name=table_name,
            data_type=data_type,
            is_primary_key=is_primary_key,
            is_nullable=is_nullable,
            has_description=has_description,
            has_comment=has_comment,
            has_existing_comment=has_existing_comment,
            sort_by=sort_by,
            sort_dir=sort_dir,
        ),
        page=page,
        page_size=page_size,
        current_user=current_user,
    )


@router.get("/column-dictionary/items/{column_id}", response_model=ColumnDictionaryDetailOut)
def get_column_dictionary_item(
    column_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> ColumnDictionaryDetailOut:
    return get_column_dictionary_detail(db, column_id, current_user=current_user)


@router.put("/column-dictionary/items/{column_id}", response_model=ColumnDictionaryDetailOut)
def update_column_dictionary_item_route(
    column_id: int,
    request: Request,
    payload: ColumnDictionaryUpdateIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor")),
) -> ColumnDictionaryDetailOut:
    audit_kwargs = request_audit_kwargs(request, current_user)
    return update_column_dictionary_item(
        db,
        column_id=column_id,
        payload=payload,
        actor_user_id=current_user.id,
        audit_kwargs=audit_kwargs,
        current_user=current_user,
    )


@router.delete("/column-dictionary/items/{column_id}", response_model=ColumnDictionaryDetailOut)
def clear_column_dictionary_item_route(
    column_id: int,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor")),
) -> ColumnDictionaryDetailOut:
    audit_kwargs = request_audit_kwargs(request, current_user)
    return clear_column_dictionary_item(
        db,
        column_id=column_id,
        actor_user_id=current_user.id,
        audit_kwargs=audit_kwargs,
        current_user=current_user,
    )


@router.post("/column-dictionary/reset", response_model=ColumnDictionaryResetOut)
def reset_column_dictionary_route(
    _payload: DestructiveActionConfirmIn,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor")),
) -> ColumnDictionaryResetOut:
    audit_kwargs = request_audit_kwargs(request, current_user)
    deleted_columns = reset_column_dictionary_curation(
        db,
        actor_user_id=current_user.id,
        audit_kwargs=audit_kwargs,
        current_user=current_user,
    )
    return ColumnDictionaryResetOut(deleted_columns=deleted_columns)


@router.post("/column-dictionary/bulk-update", response_model=ColumnDictionaryBulkUpdateOut)
def bulk_update_column_dictionary_route(
    request: Request,
    payload: ColumnDictionaryBulkUpdateIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor")),
) -> ColumnDictionaryBulkUpdateOut:
    audit_kwargs = request_audit_kwargs(request, current_user)
    return bulk_update_column_dictionary(
        db,
        payload=payload,
        actor_user_id=current_user.id,
        audit_kwargs=audit_kwargs,
        current_user=current_user,
    )


_MAX_IMPORT_UPLOAD_BYTES = 25 * 1024 * 1024  # 25 MiB cap to avoid memory-exhaustion via large/zip-bomb .xlsx


@router.post("/column-dictionary/import-preview", response_model=ColumnDictionaryImportPreviewOut)
async def preview_column_dictionary_import_route(
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor")),
) -> ColumnDictionaryImportPreviewOut:
    if not file.filename.lower().endswith(".xlsx"):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Envie um arquivo .xlsx válido.")
    content = await file.read(_MAX_IMPORT_UPLOAD_BYTES + 1)
    if len(content) > _MAX_IMPORT_UPLOAD_BYTES:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail="Arquivo excede o tamanho máximo permitido (25 MB).",
        )
    if not content:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Arquivo vazio.")
    try:
        return preview_column_dictionary_import(db, content)
    except TagSpreadsheetError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("Falha ao processar o preview do dicionário.")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Falha ao processar o preview do dicionário. Consulte os logs para mais detalhes.",
        ) from exc


@router.post("/column-dictionary/import", response_model=ColumnDictionaryImportResult)
async def import_column_dictionary(
    request: Request,
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor")),
) -> ColumnDictionaryImportResult:
    if not file.filename.lower().endswith(".xlsx"):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Envie um arquivo .xlsx válido.")
    content = await file.read(_MAX_IMPORT_UPLOAD_BYTES + 1)
    if len(content) > _MAX_IMPORT_UPLOAD_BYTES:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail="Arquivo excede o tamanho máximo permitido (25 MB).",
        )
    if not content:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Arquivo vazio.")
    audit_kwargs = request_audit_kwargs(request, current_user)
    try:
        result = import_column_dictionary_from_file(
            db,
            content,
            audit_kwargs=audit_kwargs,
            actor_user_id=current_user.id,
            metadata={"filename": file.filename},
        )
    except TagSpreadsheetError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except IntegrityError as exc:
        db.rollback()
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Conflito ao importar dicionário de colunas.") from exc
    except Exception as exc:  # noqa: BLE001
        db.rollback()
        logger.exception("Falha ao importar o dicionário.")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Falha ao importar o dicionário. Consulte os logs para mais detalhes.",
        ) from exc

    write_audit_log_sync(
        db,
        action="column_dictionary.import",
        entity_type="column",
        metadata={
            "filename": file.filename,
            "processed": result.processed,
            "matched": result.matched,
            "imported": result.imported,
            "updated": result.updated,
            "ignored": result.ignored,
            "rejected": result.rejected,
            "touched_table_ids": result.touched_table_ids,
            "headers": COLUMN_DICTIONARY_HEADERS,
        },
        **audit_kwargs,
    )
    db.commit()
    return result


@router.get(
    "/tables",
    response_model=list[TableOut],
    summary="Listar tabelas do catálogo",
    description="Superfície canônica de leitura do catálogo. Use esta rota para listagem de ativos.",
)
def list_tables(
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> list[TableOut]:
    tables = db.scalars(
        select(TableEntity)
        .options(
            selectinload(TableEntity.data_owner),
            selectinload(TableEntity.certification_submitted_by_user),
            selectinload(TableEntity.certification_decided_by_user),
            selectinload(TableEntity.owner_reviewed_by_user),
            selectinload(TableEntity.privacy_reviewed_by_user),
        )
        .order_by(TableEntity.id)
        .offset(offset)
        .limit(limit)
    ).all()
    visible_tables = [table for table in tables if can_view_table(current_user, table)]
    return [
        _serialize_table_out(
            table,
            masked=table_visibility_decision_from_entity(table, user=current_user).masked,
            can_view_sensitive=can_view_sensitive_data(current_user, table=table),
        )
        for table in visible_tables
    ]


@router.get(
    "/tables/search",
    response_model=list[TableSearchSuggestionOut],
    summary="Buscar ativos do catálogo",
    description="Retorna sugestões de ativos com nome completo para autocomplete e seleção de stewardship.",
)
def search_tables(
    q: str = Query(..., min_length=1),
    limit: int = Query(10, ge=1, le=50),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> list[TableSearchSuggestionOut]:
    return search_table_suggestions(db=db, q=q, limit=limit, current_user=current_user)


@router.get(
    "/tables/{table_id}",
    response_model=TableDetailOut,
    summary="Detalhar tabela do catálogo",
    description="Superfície canônica de leitura do detalhe do ativo, incluindo colunas e metadados visíveis.",
)
def get_table(
    table_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> TableDetailOut:
    table = db.scalar(
        select(TableEntity)
        .options(
            selectinload(TableEntity.columns).selectinload(ColumnEntity.data_owner),
            selectinload(TableEntity.columns).selectinload(ColumnEntity.owner_reviewed_by_user),
            selectinload(TableEntity.data_owner),
            selectinload(TableEntity.certification_submitted_by_user),
            selectinload(TableEntity.certification_decided_by_user),
            selectinload(TableEntity.owner_reviewed_by_user),
            selectinload(TableEntity.privacy_reviewed_by_user),
        )
        .where(TableEntity.id == table_id)
    )
    if not table:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Table not found")
    if not can_view_table(current_user, table):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Table is not visible for this profile")
    from t2c_data.features.contracts.service import contract_summary

    summary_payload = DataContractSummaryOut(**contract_summary(db, table_id=table.id))
    return _serialize_table_detail(
        table,
        db=db,
        masked=table_visibility_decision_from_entity(table, user=current_user).masked,
        can_view_sensitive=can_view_sensitive_data(current_user, table=table),
        data_contract=summary_payload,
    )


@router.get(
    "/tables/{table_id}/locator",
    response_model=TableLocatorOut,
    summary="Obter localizador do ativo",
    description="Retorna os identificadores de datasource, banco e schema para navegação no Explorer.",
)
def get_table_locator(
    table_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> TableLocatorOut:
    row = db.execute(
        select(TableEntity, Schema, Database, DataSource)
        .join(Schema, TableEntity.schema_id == Schema.id)
        .join(Database, Schema.database_id == Database.id)
        .join(DataSource, Database.datasource_id == DataSource.id)
        .where(TableEntity.id == table_id)
    ).first()
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Table not found")
    table, schema, database, datasource = row
    if not can_view_table(current_user, table):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Table is not visible for this profile")
    return TableLocatorOut(
        table_id=table.id,
        datasource_id=datasource.id,
        datasource_name=datasource.name,
        database_id=database.id,
        database_name=database.name,
        schema_id=schema.id,
        schema_name=schema.name,
        table_name=table.name,
        kind=table.table_type,
        db_type=datasource.db_type,
    )


@router.get(
    "/tables/{table_id}/correlation-summary",
    response_model=TableCorrelationSummaryOut,
    summary="Obter resumo correlacionado do ativo",
    description="Superfície canônica de leitura que consolida catálogo, incidentes, Data Quality e ingestão operacional no contexto do ativo.",
)
def get_table_correlation_summary(
    table_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> TableCorrelationSummaryOut:
    return build_table_correlation_summary(db=db, table_id=table_id, current_user=current_user)


@router.get(
    "/tables/{table_id}/operational-context",
    response_model=AssetOperationalContextOut,
    summary="Obter contexto operacional do ativo",
    description="Superfície canônica de leitura do contexto operacional e de governança associado à tabela.",
)
def get_table_operational_context(
    table_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> AssetOperationalContextOut:
    row = db.execute(
        select(TableEntity, Schema, Database, DataSource)
        .join(Schema, TableEntity.schema_id == Schema.id)
        .join(Database, Schema.database_id == Database.id)
        .join(DataSource, Database.datasource_id == DataSource.id)
        .where(TableEntity.id == table_id)
    ).first()
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Table not found")
    table, schema, database, datasource = row
    if not can_view_table(current_user, table):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Table is not visible for this profile")
    payload = load_table_operational_context(
        db,
        table_id=table.id,
        datasource_id=datasource.id,
        database_id=database.id,
        schema_id=schema.id,
    )
    if payload is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Operational context not found")
    return AssetOperationalContextOut.model_validate(payload)


@router.get(
    "/tables/{table_id}/canonical-summary",
    response_model=CanonicalAssetOut,
    summary="Obter resumo canônico do ativo",
    description="Versão compacta do contexto canônico, otimizada para a primeira dobra do Explorer.",
)
def get_table_canonical_summary(
    table_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> CanonicalAssetOut:
    return compact_canonical_asset_context(load_table_canonical_context(db, table_id, current_user=current_user))


@router.get(
    "/tables/{table_id}/canonical-context",
    response_model=CanonicalAssetOut,
    summary="Obter contexto canônico do ativo",
    description="Superfície canônica de leitura que consolida identidade, owner, classificação, evidência e eventos do ativo.",
)
def get_table_canonical_context(
    table_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> CanonicalAssetOut:
    return load_table_canonical_context(db, table_id, current_user=current_user)


@router.get(
    "/tables/{table_id}/timeline",
    response_model=TimelinePageOut,
    summary="Obter timeline do ativo",
    description="Superfície canônica de leitura da timeline curada do ativo, consolidando governança e evidências operacionais.",
)
def get_table_timeline(
    table_id: int,
    column_id: int | None = Query(default=None),
    date_from: datetime | None = Query(default=None),
    date_to: datetime | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=100),
    episode_page: int = Query(default=1, ge=1),
    episode_page_size: int = Query(default=10, ge=1, le=100),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> TimelinePageOut:
    return get_asset_timeline(
        db,
        table_id=table_id,
        column_id=column_id,
        page=page,
        page_size=page_size,
        episode_page=episode_page,
        episode_page_size=episode_page_size,
        current_user=current_user,
        date_from=date_from,
        date_to=date_to,
    )


@router.get(
    "/columns/{column_id}/canonical-context",
    response_model=CanonicalAssetOut,
    summary="Obter contexto canônico da coluna",
    description="Superfície canônica de leitura da coluna, com herança do ativo pai e sinais operacionais relacionados.",
)
def get_column_canonical_context(
    column_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> CanonicalAssetOut:
    return load_column_canonical_context(db, column_id, current_user=current_user)


@router.get(
    "/tables/{table_id}/tags",
    response_model=list[TagOut],
    summary="Listar tags da tabela",
    description="Superfície canônica de leitura das tags associadas ao ativo. Para mutação, use PUT /tables/{table_id}/tags.",
)
def get_catalog_table_tags(
    table_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> list[TagOut]:
    table = db.get(TableEntity, table_id)
    if not table:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Table not found")
    if not can_view_table(current_user, table):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Table is not visible for this profile")
    return get_table_tags(db=db, table_id=table_id)


@router.get(
    "/tables/{table_id}/glossary-terms",
    response_model=list[GlossaryTermOut],
    summary="Listar termos de glossário da tabela",
    description="Superfície canônica de leitura dos termos associados ao ativo. Para mutação, use PUT /tables/{table_id}/glossary-terms.",
)
def get_catalog_table_glossary_terms(
    table_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> list[GlossaryTermOut]:
    table = db.get(TableEntity, table_id)
    if not table:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Table not found")
    if not can_view_table(current_user, table):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Table is not visible for this profile")
    return get_table_glossary_terms(db=db, table_id=table_id)


@router.patch(
    "/tables/{table_id}",
    response_model=TableDetailOut,
    deprecated=True,
    summary="Legado: atualizar tabela via /catalog",
    description="Endpoint legado de mutação. Use PATCH /tables/{table_id} como superfície canônica para alteração manual de metadados.",
)
def patch_table(
    table_id: int,
    payload: TablePatch,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_roles("admin", "editor")),
) -> TableDetailOut:
    table = patch_table_with_audit(
        db=db,
        table_id=table_id,
        payload=payload,
        user=user,
        audit_kwargs=request_audit_kwargs(request, user),
    )
    from t2c_data.features.contracts.service import contract_summary

    summary_payload = DataContractSummaryOut(**contract_summary(db, table_id=table.id))
    return _serialize_table_detail(
        table,
        db=db,
        masked=table_visibility_decision_from_entity(table, user=user).masked,
        data_contract=summary_payload,
    )


@router.get("/stats/tables-count", response_model=dict[str, int])
def table_count(
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "viewer")),
) -> dict[str, int]:
    count = db.scalar(select(func.count(TableEntity.id))) or 0
    return {"count": int(count)}


@router.get("/tree", response_model=list[TreeDatasourceOut])
def tree_datasources(
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> list[TreeDatasourceOut]:
    return list_tree_datasources(db=db, current_user=current_user)


@router.get("/tree/datasources/{datasource_id}", response_model=TreeDatasourceChildrenOut)
def tree_datasource_children(
    datasource_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> TreeDatasourceChildrenOut:
    return get_tree_datasource_children(db=db, datasource_id=datasource_id, current_user=current_user)


@router.get("/tree/schemas/{schema_id}/tables", response_model=list[TreeTableOut])
def tree_schema_tables(
    schema_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> list[TreeTableOut]:
    return list_tree_schema_tables(db=db, schema_id=schema_id, current_user=current_user)


@router.get("/tree/schemas/{schema_id}/tables/page", response_model=TreeTablePageOut)
def tree_schema_tables_page(
    schema_id: int,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> TreeTablePageOut:
    return list_tree_schema_tables_page(
        db=db,
        schema_id=schema_id,
        page=page,
        page_size=page_size,
        current_user=current_user,
    )


@router.get("/tables/{table_id}/columns", response_model=list[TreeTableColumnsOut])
def table_columns(
    table_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> list[TreeTableColumnsOut]:
    return list_table_columns(db=db, table_id=table_id, current_user=current_user)


@router.get("/tables/{table_id}/columns/page", response_model=TreeTableColumnsPageOut)
def table_columns_page(
    table_id: int,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=60, ge=1, le=200),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> TreeTableColumnsPageOut:
    return list_table_columns_page(
        db=db,
        table_id=table_id,
        page=page,
        page_size=page_size,
        current_user=current_user,
    )


@router.get("/tables/{table_id}/columns/summary", response_model=TableColumnSummaryOut)
def table_columns_summary(
    table_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> TableColumnSummaryOut:
    return get_table_columns_summary(db=db, table_id=table_id, current_user=current_user)


@router.get("/tables/{table_id}/metabase-consumption", response_model=MetabaseConsumptionSummaryOut)
def table_metabase_consumption(
    table_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> MetabaseConsumptionSummaryOut:
    return get_table_metabase_consumption(db, table_id)


@router.get("/tables/{table_id}/volume", response_model=TableVolumeSnapshotOut | None)
def table_volume_latest(
    table_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> TableVolumeSnapshotOut | None:
    return get_latest_table_volume(db=db, table_id=table_id)


@router.get("/tables/{table_id}/volume/history", response_model=TableVolumeHistoryOut)
def table_volume_history(
    table_id: int,
    limit: int = Query(default=30, ge=1, le=100),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> TableVolumeHistoryOut:
    return TableVolumeHistoryOut(items=list_table_volume_history(db=db, table_id=table_id, limit=limit))


@router.post("/tables/{table_id}/volume/measure", response_model=TableVolumeSnapshotOut | None)
def table_volume_measure(
    table_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor")),
) -> TableVolumeSnapshotOut | None:
    snapshot = measure_table_volume(db=db, table_id=table_id, measurement_context="manual")
    if snapshot is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Tabela não encontrada.")
    return snapshot


@router.post("/volume-snapshots/run", response_model=TableVolumeRunOut)
def volume_snapshots_run(
    datasource_id: int | None = Query(default=None),
    schema_id: int | None = Query(default=None),
    limit: int | None = Query(default=None, ge=1, le=500),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor")),
) -> TableVolumeRunOut:
    return measure_all_active_tables_volume(db=db, datasource_id=datasource_id, schema_id=schema_id, limit=limit)


@router.get("/tree/search", response_model=list[ExplorerSearchResultOut])
def tree_search(
    q: str = Query(..., min_length=1),
    governance_maturity: str | None = Query(default=None),
    limit: int = Query(30, ge=1, le=100),
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> list[ExplorerSearchResultOut]:
    return search_tree(db=db, q=q, governance_maturity=governance_maturity, limit=limit, current_user=current_user)
