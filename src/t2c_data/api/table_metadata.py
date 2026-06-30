from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.orm import Session

from t2c_data.core.db import get_db
from t2c_data.core.deps import require_permission, require_roles
from t2c_data.features.catalog.application import (
    get_table_glossary_terms as get_table_glossary_terms_use_case,
    get_table_tags as get_table_tags_use_case,
    patch_table_with_audit,
    update_table_glossary_terms_with_audit,
    update_table_tags_with_audit,
)
from t2c_data.features.catalog.table_detail import build_table_detail_out
from t2c_data.features.privacy_access import can_view_table
from t2c_data.features.platform.sensitive_data import can_view_sensitive_data
from t2c_data.models.auth import User
from t2c_data.models.catalog import TableEntity
from t2c_data.schemas.glossary import GlossaryTermOut
from t2c_data.schemas.catalog import TableDetailOut, TableOwnerPatch, TablePatch
from t2c_data.schemas.table_metadata import TableGlossaryTermsUpdateRequest, TableTagsUpdateRequest
from t2c_data.schemas.tag import TagOut
from t2c_data.services.audit import request_audit_kwargs

router = APIRouter(prefix="/tables", tags=["table-metadata"])


def _visible_table_or_404(db: Session, table_id: int, current_user: User) -> TableEntity:
    table = db.get(TableEntity, table_id)
    if table is None:
        raise HTTPException(status_code=404, detail="Table not found")
    if not can_view_table(current_user, table):
        raise HTTPException(status_code=403, detail="Table is not visible for this profile")
    return table


@router.patch(
    "/{table_id}",
    response_model=TableDetailOut,
    summary="Atualizar metadados manuais da tabela",
    description="Superfície canônica de mutação de metadados manuais do ativo. Leituras de catálogo ficam em /catalog.",
)
def patch_table_owner(
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
    return build_table_detail_out(
        db,
        table,
        masked=not can_view_table(user, table),
        can_view_sensitive=can_view_sensitive_data(user, table=table),
    )


@router.patch(
    "/{table_id}/owner",
    response_model=TableDetailOut,
    summary="Atualizar apenas o owner/responsável da tabela",
    description=(
        "Superfície restrita de mutação do owner/responsável do ativo. Permite que o perfil "
        "data_owner reatribua o responsável sem editar outros metadados. Requer asset.owner:write."
    ),
)
def patch_table_owner_only(
    table_id: int,
    payload: TableOwnerPatch,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_permission("asset.owner:write")),
) -> TableDetailOut:
    table = patch_table_with_audit(
        db=db,
        table_id=table_id,
        payload=payload,
        user=user,
        audit_kwargs=request_audit_kwargs(request, user),
    )
    return build_table_detail_out(
        db,
        table,
        masked=not can_view_table(user, table),
        can_view_sensitive=can_view_sensitive_data(user, table=table),
    )


@router.get(
    "/{table_id}/tags",
    response_model=list[TagOut],
    deprecated=True,
    summary="Legado: listar tags via /tables",
    description="Endpoint legado de leitura. Use GET /catalog/tables/{table_id}/tags como superfície canônica de leitura; mantenha PUT /tables/{table_id}/tags para mutação.",
)
def get_table_tags(
    table_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> list[TagOut]:
    _visible_table_or_404(db, table_id, current_user)
    return get_table_tags_use_case(db=db, table_id=table_id)


@router.put(
    "/{table_id}/tags",
    response_model=list[TagOut],
    summary="Atualizar tags da tabela",
    description="Superfície canônica de mutação das tags do ativo. Para leitura, use GET /catalog/tables/{table_id}/tags.",
)
def put_table_tags(
    table_id: int,
    payload: TableTagsUpdateRequest,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_roles("admin", "editor")),
) -> list[TagOut]:
    _visible_table_or_404(db, table_id, user)
    return update_table_tags_with_audit(
        db=db,
        table_id=table_id,
        payload=payload,
        user=user,
        audit_kwargs=request_audit_kwargs(request, user),
    )


@router.get(
    "/{table_id}/glossary-terms",
    response_model=list[GlossaryTermOut],
    deprecated=True,
    summary="Legado: listar termos via /tables",
    description="Endpoint legado de leitura. Use GET /catalog/tables/{table_id}/glossary-terms como superfície canônica de leitura; mantenha PUT /tables/{table_id}/glossary-terms para mutação.",
)
def get_table_glossary_terms(
    table_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> list[GlossaryTermOut]:
    _visible_table_or_404(db, table_id, current_user)
    return get_table_glossary_terms_use_case(db=db, table_id=table_id)


@router.put(
    "/{table_id}/glossary-terms",
    response_model=list[GlossaryTermOut],
    summary="Atualizar termos de glossário da tabela",
    description="Superfície canônica de mutação dos termos associados ao ativo. Para leitura, use GET /catalog/tables/{table_id}/glossary-terms.",
)
def put_table_glossary_terms(
    table_id: int,
    payload: TableGlossaryTermsUpdateRequest,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_roles("admin", "editor")),
) -> list[GlossaryTermOut]:
    _visible_table_or_404(db, table_id, user)
    return update_table_glossary_terms_with_audit(
        db=db,
        table_id=table_id,
        payload=payload,
        user=user,
        audit_kwargs=request_audit_kwargs(request, user),
    )
