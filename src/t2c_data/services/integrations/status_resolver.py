from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal

from t2c_data.schemas.integrations import IntegrationDimensionStatusOut, IntegrationStatusContractOut


IntegrationOverallStatus = Literal["healthy", "warning", "critical", "unknown"]
INTEGRATION_STATUS_CONTRACT_V1 = "v1"
INTEGRATION_STATUS_CONTRACT_V2 = "v2"
SUPPORTED_INTEGRATION_STATUS_CONTRACT_VERSIONS = (
    INTEGRATION_STATUS_CONTRACT_V1,
    INTEGRATION_STATUS_CONTRACT_V2,
)


@dataclass(slots=True)
class ResolvedDimensionStatus:
    status: str = "unknown"
    message: str | None = None
    checked_at: datetime | None = None
    reason_code: str | None = None
    details: dict[str, Any] = field(default_factory=dict)

    def to_out(self) -> IntegrationDimensionStatusOut:
        return IntegrationDimensionStatusOut(
            status=self.status or "unknown",
            message=self.message,
            checked_at=self.checked_at,
            reason_code=self.reason_code,
            details=self.details,
        )


def _normalize_status(value: str | None) -> str:
    normalized = (value or "").strip().lower()
    return normalized or "unknown"


def _dimension(
    *,
    status: str,
    message: str | None = None,
    checked_at: datetime | None = None,
    reason_code: str | None = None,
    details: dict[str, Any] | None = None,
) -> ResolvedDimensionStatus:
    return ResolvedDimensionStatus(
        status=_normalize_status(status),
        message=message,
        checked_at=checked_at,
        reason_code=reason_code,
        details=details or {},
    )


def _overall_status(connectivity: str, operation: str, consumption: str) -> IntegrationOverallStatus:
    normalized_connectivity = _normalize_status(connectivity)
    normalized_operation = _normalize_status(operation)
    normalized_consumption = _normalize_status(consumption)

    if normalized_connectivity in {"down", "unavailable", "misconfigured"}:
        return "critical"
    if normalized_operation in {"failed", "delayed"}:
        return "warning"
    if normalized_consumption in {"partial", "unavailable"}:
        return "warning"
    if normalized_connectivity == "unknown" or normalized_operation == "unknown" or normalized_consumption == "unknown":
        return "unknown"
    return "healthy"


def resolve_status_contract(
    *,
    source_name: str,
    connectivity: ResolvedDimensionStatus,
    operation: ResolvedDimensionStatus,
    consumption: ResolvedDimensionStatus,
    checked_at: datetime | None = None,
    contract_version: str = INTEGRATION_STATUS_CONTRACT_V1,
) -> IntegrationStatusContractOut:
    overall_status = _overall_status(connectivity.status, operation.status, consumption.status)
    overall_message = connectivity.message or operation.message or consumption.message
    return IntegrationStatusContractOut(
        contract_version=contract_version,
        source_name=source_name,
        connectivity=connectivity.to_out(),
        operation=operation.to_out(),
        consumption=consumption.to_out(),
        overall_status=overall_status,
        overall_message=overall_message,
        checked_at=checked_at or connectivity.checked_at or operation.checked_at or consumption.checked_at,
    )


def resolve_status_contract_v2(
    *,
    source_name: str,
    connectivity: ResolvedDimensionStatus,
    operation: ResolvedDimensionStatus,
    consumption: ResolvedDimensionStatus,
    checked_at: datetime | None = None,
) -> IntegrationStatusContractOut:
    return resolve_status_contract(
        source_name=source_name,
        connectivity=connectivity,
        operation=operation,
        consumption=consumption,
        checked_at=checked_at,
        contract_version=INTEGRATION_STATUS_CONTRACT_V2,
    )


def dimension_unknown(*, message: str | None = None, reason_code: str | None = None, checked_at: datetime | None = None) -> ResolvedDimensionStatus:
    return _dimension(status="unknown", message=message, reason_code=reason_code, checked_at=checked_at)


def dimension_healthy(*, message: str | None = None, reason_code: str | None = None, checked_at: datetime | None = None, details: dict[str, Any] | None = None) -> ResolvedDimensionStatus:
    return _dimension(status="healthy", message=message, reason_code=reason_code, checked_at=checked_at, details=details)


def dimension_degraded(*, message: str | None = None, reason_code: str | None = None, checked_at: datetime | None = None, details: dict[str, Any] | None = None) -> ResolvedDimensionStatus:
    return _dimension(status="degraded", message=message, reason_code=reason_code, checked_at=checked_at, details=details)


def dimension_down(*, message: str | None = None, reason_code: str | None = None, checked_at: datetime | None = None, details: dict[str, Any] | None = None) -> ResolvedDimensionStatus:
    return _dimension(status="down", message=message, reason_code=reason_code, checked_at=checked_at, details=details)


def dimension_running(*, message: str | None = None, reason_code: str | None = None, checked_at: datetime | None = None, details: dict[str, Any] | None = None) -> ResolvedDimensionStatus:
    return _dimension(status="running", message=message, reason_code=reason_code, checked_at=checked_at, details=details)


def dimension_idle(*, message: str | None = None, reason_code: str | None = None, checked_at: datetime | None = None, details: dict[str, Any] | None = None) -> ResolvedDimensionStatus:
    return _dimension(status="idle", message=message, reason_code=reason_code, checked_at=checked_at, details=details)


def dimension_delayed(*, message: str | None = None, reason_code: str | None = None, checked_at: datetime | None = None, details: dict[str, Any] | None = None) -> ResolvedDimensionStatus:
    return _dimension(status="delayed", message=message, reason_code=reason_code, checked_at=checked_at, details=details)


def dimension_failed(*, message: str | None = None, reason_code: str | None = None, checked_at: datetime | None = None, details: dict[str, Any] | None = None) -> ResolvedDimensionStatus:
    return _dimension(status="failed", message=message, reason_code=reason_code, checked_at=checked_at, details=details)


def dimension_available(*, message: str | None = None, reason_code: str | None = None, checked_at: datetime | None = None, details: dict[str, Any] | None = None) -> ResolvedDimensionStatus:
    return _dimension(status="available", message=message, reason_code=reason_code, checked_at=checked_at, details=details)


def dimension_partial(*, message: str | None = None, reason_code: str | None = None, checked_at: datetime | None = None, details: dict[str, Any] | None = None) -> ResolvedDimensionStatus:
    return _dimension(status="partial", message=message, reason_code=reason_code, checked_at=checked_at, details=details)


def dimension_unavailable(*, message: str | None = None, reason_code: str | None = None, checked_at: datetime | None = None, details: dict[str, Any] | None = None) -> ResolvedDimensionStatus:
    return _dimension(status="unavailable", message=message, reason_code=reason_code, checked_at=checked_at, details=details)
