from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ScannedColumn:
    name: str
    data_type: str
    is_primary_key: bool
    is_nullable: bool
    ordinal_position: int
    comment: str | None


@dataclass
class ScannedTable:
    schema_name: str
    table_name: str
    table_type: str
    comment: str | None
    columns: list[ScannedColumn]


@dataclass
class ScanPayload:
    database_name: str
    tables: list[ScannedTable]
