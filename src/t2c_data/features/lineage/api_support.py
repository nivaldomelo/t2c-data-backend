from __future__ import annotations

from fastapi import HTTPException, UploadFile, status
from sqlalchemy.orm import Session

from t2c_data.features.lineage.column_actions import get_column_edge_or_404
from t2c_data.features.lineage.queries import list_relations_out
from t2c_data.features.lineage.visibility import asset_visible_to_user, relation_visible_to_user
from t2c_data.features.lineage.spreadsheet import LineageSpreadsheetError
from t2c_data.models.auth import User
from t2c_data.models.lineage import LineageAsset, LineageRelation
from t2c_data.schemas.lineage import LineageAssetOut, LineageRelationOut


def read_xlsx_upload(file: UploadFile) -> bytes:
    filename = file.filename or ""
    if not filename.lower().endswith(".xlsx"):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Envie um arquivo .xlsx válido.")
    return b""


async def read_xlsx_upload_async(file: UploadFile) -> bytes:
    filename = file.filename or ""
    if not filename.lower().endswith(".xlsx"):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Envie um arquivo .xlsx válido.")
    content = await file.read()
    if not content:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Arquivo vazio.")
    return content


def wrap_lineage_spreadsheet_error(exc: LineageSpreadsheetError) -> HTTPException:
    return HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))


def get_asset_or_404(db: Session, asset_id: int, *, current_user: User | None = None) -> LineageAsset:
    asset = db.get(LineageAsset, asset_id)
    if not asset:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Lineage asset not found")
    if current_user is not None and not asset_visible_to_user(db, current_user, asset):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Lineage asset not found")
    return asset



def get_relation_or_404(db: Session, relation_id: int, *, current_user: User | None = None) -> LineageRelation:
    relation = db.get(LineageRelation, relation_id)
    if not relation:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Lineage relation not found")
    if current_user is not None and not relation_visible_to_user(db, current_user, relation):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Lineage relation not found")
    return relation



def serialize_assets_out(items: list[LineageAsset]) -> list[LineageAssetOut]:
    return [LineageAssetOut.model_validate(asset, from_attributes=True) for asset in items]



def relation_items_for_asset(db: Session, asset_id: int, *, direction: str, current_user: User | None = None) -> list[LineageRelationOut]:
    data = list_relations_out(db, current_user=current_user).items
    if direction == "upstream":
        return [item for item in data if item.target_asset.id == asset_id and item.is_active]
    return [item for item in data if item.source_asset.id == asset_id and item.is_active]


__all__ = [
    "get_asset_or_404",
    "get_column_edge_or_404",
    "get_relation_or_404",
    "read_xlsx_upload",
    "read_xlsx_upload_async",
    "relation_items_for_asset",
    "serialize_assets_out",
    "wrap_lineage_spreadsheet_error",
]
