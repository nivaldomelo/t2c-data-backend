from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from t2c_data.core.db import get_db
from t2c_data.core.deps import require_roles
from t2c_data.features.contracts.service import (
    contract_impact_summary,
    contract_summary,
    create_contract,
    get_current_contract,
    list_contract_versions,
    validate_contract,
)
from t2c_data.models.auth import User
from t2c_data.models.contracts import DataContract
from sqlalchemy.orm import selectinload
from t2c_data.schemas.contracts import (
    DataContractIn,
    DataContractOut,
    DataContractImpactOut,
    DataContractSummaryOut,
    DataContractValidationResultOut,
    DataContractValidationOut,
)


router = APIRouter(prefix="/contracts", tags=["contracts"])


@router.get("/tables/{table_id}", response_model=DataContractSummaryOut)
def get_table_contract_summary(
    table_id: int,
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "viewer")),
) -> DataContractSummaryOut:
    return DataContractSummaryOut(**contract_summary(db, table_id=table_id))


@router.get("/tables/{table_id}/current", response_model=DataContractOut)
def get_table_current_contract(
    table_id: int,
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "viewer")),
) -> DataContractOut:
    contract = get_current_contract(db, table_id=table_id)
    if contract is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No data contract found")
    return DataContractOut.model_validate(contract)


@router.get("/tables/{table_id}/versions", response_model=list[DataContractOut])
def list_table_contract_versions(
    table_id: int,
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "viewer")),
    ) -> list[DataContractOut]:
    return [DataContractOut.model_validate(item) for item in list_contract_versions(db, table_id=table_id)]


@router.get("/tables/{table_id}/impact-summary", response_model=DataContractImpactOut)
def get_table_contract_impact_summary(
    table_id: int,
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "viewer")),
) -> DataContractImpactOut:
    return DataContractImpactOut.model_validate(contract_impact_summary(db, table_id=table_id))


@router.post("/tables/{table_id}", response_model=DataContractOut)
def create_table_contract(
    table_id: int,
    payload: DataContractIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor")),
) -> DataContractOut:
    contract = create_contract(
        db,
        table_id=table_id,
        payload=payload.model_dump(),
        created_by_user_id=current_user.id,
    )
    return DataContractOut.model_validate(contract)


@router.post("/contracts/{contract_id}/validate", response_model=DataContractValidationResultOut)
def validate_table_contract(
    contract_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor")),
) -> DataContractValidationResultOut:
    validation = validate_contract(db, contract_id=contract_id, created_by_user_id=current_user.id)
    return DataContractValidationResultOut(
        validation=DataContractValidationOut.model_validate(validation),
        summary=validation.summary_json or {},
    )


@router.get("/contracts/{contract_id}", response_model=DataContractOut)
def get_contract(
    contract_id: int,
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "viewer")),
) -> DataContractOut:
    contract = db.scalar(
        select(DataContract).options(selectinload(DataContract.columns)).where(DataContract.id == contract_id)
    )
    if contract is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Contract not found")
    return DataContractOut.model_validate(contract)
