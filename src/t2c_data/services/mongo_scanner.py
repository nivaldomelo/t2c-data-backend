"""Compatibility bridge for MongoDB scanner helpers."""

from t2c_data.features.scanner.mongodb import scan_mongodb
from t2c_data.features.scanner.types import ScanPayload, ScannedColumn, ScannedTable

__all__ = ["ScanPayload", "ScannedColumn", "ScannedTable", "scan_mongodb"]
