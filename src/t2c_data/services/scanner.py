from __future__ import annotations

"""Backward-compatible scanner service wrapper.

The scanner orchestration now lives under `app.features.scanner`. This bridge
keeps older imports stable while delegating to the feature-layer entrypoint.
"""

from sqlalchemy.orm import Session

from t2c_data.features.scanner.application import run_datasource_scan
from t2c_data.features.scanner.contracts import MetadataScanGateway
from t2c_data.models.catalog import DataSource
from t2c_data.models.scan import ScanRun

__all__ = ["run_scan"]


def run_scan(
    session: Session,
    datasource: DataSource,
    started_by: int | None = None,
    scan_gateway: MetadataScanGateway | None = None,
) -> ScanRun:
    return run_datasource_scan(
        session,
        datasource=datasource,
        started_by=started_by,
        scan_gateway=scan_gateway,
    )
