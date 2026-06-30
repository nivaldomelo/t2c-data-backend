"""Compatibility bridge for PostgreSQL scanner helpers."""

from t2c_data.features.scanner.postgres import scan_postgres
from t2c_data.features.scanner.types import ScanPayload, ScannedColumn, ScannedTable

__all__ = ["ScanPayload", "ScannedColumn", "ScannedTable", "scan_postgres"]
