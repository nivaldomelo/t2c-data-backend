from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime, timezone
from time import perf_counter
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session, selectinload

from t2c_data.core.json_utils import to_jsonable
from t2c_data.features.lineage.table_summary import get_table_summary
from t2c_data.models.catalog import ColumnEntity, DataSource, Database, Schema, TableEntity
from t2c_data.models.contracts import DataContract, DataContractColumn, DataContractValidation


def _now() -> datetime:
    return datetime.now(timezone.utc)


def list_contract_versions(db: Session, *, table_id: int) -> list[DataContract]:
    return db.scalars(
        select(DataContract)
        .where(DataContract.table_id == table_id)
        .options(selectinload(DataContract.columns))
        .order_by(DataContract.version.desc())
    ).all()


def get_current_contract(db: Session, *, table_id: int) -> DataContract | None:
    contract = db.scalar(
        select(DataContract)
        .where(DataContract.table_id == table_id)
        .order_by(
            (DataContract.status == "published").desc(),
            DataContract.version.desc(),
        )
        .limit(1)
    )
    if contract is None:
        return None
    return db.scalar(
        select(DataContract)
        .where(DataContract.id == contract.id)
        .options(selectinload(DataContract.columns))
    )


def contract_summary(db: Session, *, table_id: int) -> dict[str, object]:
    contract = get_current_contract(db, table_id=table_id)
    if contract is None:
        return {
            "contract_id": None,
            "version": None,
            "status": None,
            "published_at": None,
            "last_validation_status": None,
            "last_validation_at": None,
            "last_validation_issues": None,
        }
    return {
        "contract_id": contract.id,
        "version": int(contract.version),
        "status": contract.status,
        "published_at": contract.published_at,
        "last_validation_status": contract.last_validation_status,
        "last_validation_at": contract.last_validation_at,
        "last_validation_issues": contract.last_validation_issues,
    }


def create_contract(
    db: Session,
    *,
    table_id: int,
    payload: dict[str, Any],
    created_by_user_id: int | None = None,
) -> DataContract:
    current_version = db.scalar(
        select(func.max(DataContract.version)).where(DataContract.table_id == table_id)
    )
    next_version = int(current_version or 0) + 1
    status = str(payload.get("status") or "draft")
    contract = DataContract(
        table_id=table_id,
        version=next_version,
        status=status,
        description=payload.get("description"),
        notes=payload.get("notes"),
        owner_user_id=payload.get("owner_user_id"),
        steward_user_id=payload.get("steward_user_id"),
        freshness_hours=payload.get("freshness_hours"),
        min_row_count=payload.get("min_row_count"),
        max_row_count=payload.get("max_row_count"),
        compatibility_rules_json=to_jsonable(payload.get("compatibility_rules_json") or None),
        published_at=_now() if status == "published" else None,
    )
    db.add(contract)
    columns_payload = payload.get("columns") or []
    for column in columns_payload:
        contract.columns.append(
            DataContractColumn(
                column_name=str(column.get("column_name")).strip(),
                data_type=str(column.get("data_type")).strip() if column.get("data_type") else None,
                is_nullable=column.get("is_nullable"),
                is_primary_key=column.get("is_primary_key"),
                is_required=column.get("is_required"),
                ordinal_position=column.get("ordinal_position"),
                notes=column.get("notes"),
            )
        )
    db.commit()
    db.refresh(contract)
    return contract


def _normalize_column_name(value: str) -> str:
    return value.strip().lower()


def _expected_columns_map(contract: DataContract) -> dict[str, DataContractColumn]:
    return {_normalize_column_name(column.column_name): column for column in contract.columns if column.column_name}


def _actual_columns_map(columns: Iterable[ColumnEntity]) -> dict[str, ColumnEntity]:
    return {_normalize_column_name(column.name): column for column in columns if column.name}


def _type_matches(expected: str | None, actual: str | None) -> bool:
    if not expected or not actual:
        return True
    return expected.strip().lower() in actual.strip().lower()


def _contract_schema_analysis(contract: DataContract, columns: Iterable[ColumnEntity]) -> dict[str, object]:
    expected = _expected_columns_map(contract)
    actual = _actual_columns_map(columns)
    issues: list[dict[str, object]] = []
    changes: list[dict[str, object]] = []

    for name, expected_col in expected.items():
        actual_col = actual.get(name)
        if actual_col is None:
            issue = {"type": "missing_column", "column": expected_col.column_name, "detail": "Coluna esperada ausente"}
            issues.append(issue)
            changes.append({"column_name": expected_col.column_name, "kind": "schema_removal", "breaking": True, "detail": "Coluna esperada ausente"})
            continue
        if expected_col.data_type and not _type_matches(expected_col.data_type, actual_col.data_type):
            issue = {
                "type": "type_mismatch",
                "column": expected_col.column_name,
                "expected": expected_col.data_type,
                "actual": actual_col.data_type,
            }
            issues.append(issue)
            changes.append(
                {
                    "column_name": expected_col.column_name,
                    "kind": "type_change",
                    "breaking": True,
                    "detail": f"Tipo esperado {expected_col.data_type} vs atual {actual_col.data_type}",
                }
            )
        if expected_col.is_required is True and actual_col.is_nullable:
            issue = {
                "type": "required_nullable",
                "column": expected_col.column_name,
                "detail": "Coluna marcada como obrigatória está nullable",
            }
            issues.append(issue)
            changes.append(
                {
                    "column_name": expected_col.column_name,
                    "kind": "nullability_change",
                    "breaking": True,
                    "detail": "Coluna obrigatória ficou nullable",
                }
            )
        if expected_col.is_nullable is False and actual_col.is_nullable:
            issue = {
                "type": "nullable_mismatch",
                "column": expected_col.column_name,
                "expected": False,
                "actual": True,
            }
            issues.append(issue)
            changes.append(
                {
                    "column_name": expected_col.column_name,
                    "kind": "nullability_change",
                    "breaking": True,
                    "detail": "Coluna esperada não-null agora aceita nulos",
                }
            )
        if expected_col.is_primary_key is True and not actual_col.is_primary_key:
            issue = {
                "type": "primary_key_mismatch",
                "column": expected_col.column_name,
                "expected": True,
                "actual": False,
            }
            issues.append(issue)
            changes.append(
                {
                    "column_name": expected_col.column_name,
                    "kind": "primary_key_change",
                    "breaking": True,
                    "detail": "Coluna esperada como chave primária deixou de ser",
                }
            )

    for name, actual_col in actual.items():
        if name not in expected:
            changes.append(
                {
                    "column_name": actual_col.name,
                    "kind": "schema_addition",
                    "breaking": False,
                    "detail": "Coluna adicionada no schema atual",
                }
            )

    breaking_changes = sum(1 for item in changes if item.get("breaking"))
    warning_changes = sum(1 for item in changes if not item.get("breaking"))
    return {
        "expected_columns": len(expected),
        "actual_columns": len(actual),
        "issues": issues,
        "changes": changes,
        "breaking_changes_count": breaking_changes,
        "warning_changes_count": warning_changes,
        "schema_state": "breaking" if breaking_changes else "warning" if warning_changes else "compatible",
        "schema_label": "Quebra de contrato" if breaking_changes else "Atenção ao contrato" if warning_changes else "Compatível",
    }


def validate_contract(
    db: Session,
    *,
    contract_id: int,
    created_by_user_id: int | None = None,
) -> DataContractValidation:
    contract = db.scalar(
        select(DataContract)
        .options(selectinload(DataContract.columns))
        .where(DataContract.id == contract_id)
    )
    if contract is None:
        raise ValueError("Contract not found")
    table = db.get(TableEntity, contract.table_id)
    if table is None:
        raise ValueError("Table not found")
    columns = db.scalars(select(ColumnEntity).where(ColumnEntity.table_id == contract.table_id)).all()

    started = perf_counter()
    analysis = _contract_schema_analysis(contract, columns)
    summary = {
        "total_expected_columns": analysis["expected_columns"],
        "total_actual_columns": analysis["actual_columns"],
        "issues_count": len(analysis["issues"]),
        "breaking_changes_count": analysis["breaking_changes_count"],
        "warning_changes_count": analysis["warning_changes_count"],
        "schema_state": analysis["schema_state"],
        "schema_label": analysis["schema_label"],
    }
    status = "passed" if not analysis["issues"] else "failed"
    finished = _now()
    duration_ms = int((perf_counter() - started) * 1000)

    validation = DataContractValidation(
        contract_id=contract.id,
        table_id=contract.table_id,
        status=status,
        checked_at=finished,
        duration_ms=duration_ms,
        issues_json=to_jsonable(analysis["issues"]),
        summary_json=to_jsonable(summary),
        created_by_user_id=created_by_user_id,
    )
    db.add(validation)
    contract.last_validation_status = status
    contract.last_validation_at = finished
    contract.last_validation_issues = len(analysis["issues"])
    db.commit()
    db.refresh(validation)
    return validation


def contract_impact_summary(db: Session, *, table_id: int) -> dict[str, object]:
    contract = get_current_contract(db, table_id=table_id)
    table = db.get(TableEntity, table_id)
    if table is None:
        raise ValueError("Table not found")
    row = db.execute(
        select(Schema.name, TableEntity.name, DataSource.name)
        .join(Database, Schema.database_id == Database.id)
        .join(DataSource, Database.datasource_id == DataSource.id)
        .join(TableEntity, TableEntity.schema_id == Schema.id)
        .where(TableEntity.id == table_id)
    ).first()
    table_fqn = f"{row[2]}.{row[0]}.{row[1]}" if row else f"table:{table_id}"
    columns = db.scalars(select(ColumnEntity).where(ColumnEntity.table_id == table_id)).all()
    lineage_summary = get_table_summary(db, table_id)
    if contract is None:
        return {
            "table_id": table_id,
            "table_fqn": table_fqn,
            "contract_id": None,
            "contract_version": None,
            "contract_status": None,
            "contract_validation_status": None,
            "schema_state": "no_contract",
            "schema_label": "Sem contrato",
            "expected_columns": 0,
            "actual_columns": len(columns),
            "breaking_changes_count": 0,
            "warning_changes_count": 0,
            "changes": [],
            "lineage": {
                "upstream_count": int(getattr(getattr(lineage_summary, "impact", None), "upstream_count", 0) or 0),
                "downstream_count": int(getattr(getattr(lineage_summary, "impact", None), "downstream_count", 0) or 0),
                "process_count": int(getattr(getattr(lineage_summary, "impact", None), "process_count", 0) or 0),
                "dashboard_count": int(getattr(getattr(lineage_summary, "impact", None), "dashboard_count", 0) or 0),
                "direct_dependencies_count": int(getattr(getattr(lineage_summary, "impact", None), "direct_dependencies_count", 0) or 0),
                "impact_level": str(getattr(getattr(lineage_summary, "impact", None), "impact_level", "low") or "low"),
            },
            "recommendation": "Publicar um contrato antes de validar mudanças de schema.",
        }

    analysis = _contract_schema_analysis(contract, columns)
    lineage_impact = getattr(lineage_summary, "impact", None)
    recommendation = "Revalidar o contrato após a mudança" if analysis["breaking_changes_count"] else "Acompanhar o contrato e os consumidores downstream"
    if analysis["breaking_changes_count"]:
        recommendation = "Abrir incidente ou aprovar a mudança antes de seguir com o deploy"
    return {
        "table_id": table_id,
        "table_fqn": table_fqn,
        "contract_id": contract.id,
        "contract_version": int(contract.version),
        "contract_status": contract.status,
        "contract_validation_status": contract.last_validation_status,
        "schema_state": analysis["schema_state"],
        "schema_label": analysis["schema_label"],
        "expected_columns": int(analysis["expected_columns"]),
        "actual_columns": int(analysis["actual_columns"]),
        "breaking_changes_count": int(analysis["breaking_changes_count"]),
        "warning_changes_count": int(analysis["warning_changes_count"]),
        "changes": analysis["changes"],
        "lineage": {
            "upstream_count": int(getattr(lineage_impact, "upstream_count", 0) or 0),
            "downstream_count": int(getattr(lineage_impact, "downstream_count", 0) or 0),
            "process_count": int(getattr(lineage_impact, "process_count", 0) or 0),
            "dashboard_count": int(getattr(lineage_impact, "dashboard_count", 0) or 0),
            "direct_dependencies_count": int(getattr(lineage_impact, "direct_dependencies_count", 0) or 0),
            "impact_level": str(getattr(lineage_impact, "impact_level", "low") or "low"),
        },
        "recommendation": recommendation,
    }


def latest_contract_validation_map(db: Session, *, table_ids: list[int]) -> dict[int, dict[str, object]]:
    if not table_ids:
        return {}
    latest_validation_subq = (
        select(
            DataContractValidation.table_id.label("table_id"),
            func.max(DataContractValidation.checked_at).label("max_checked_at"),
        )
        .where(DataContractValidation.table_id.in_(table_ids))
        .group_by(DataContractValidation.table_id)
        .subquery()
    )
    rows = db.execute(
        select(DataContractValidation, DataContract)
        .join(
            latest_validation_subq,
            (DataContractValidation.table_id == latest_validation_subq.c.table_id)
            & (DataContractValidation.checked_at == latest_validation_subq.c.max_checked_at),
        )
        .join(DataContract, DataContract.id == DataContractValidation.contract_id)
    ).all()
    result: dict[int, dict[str, object]] = {}
    for validation, contract in rows:
        result[int(validation.table_id)] = {
            "contract_id": contract.id,
            "version": int(contract.version),
            "status": contract.status,
            "validation_status": validation.status,
            "validation_at": validation.checked_at,
            "issues": int(contract.last_validation_issues or 0),
        }
    return result
