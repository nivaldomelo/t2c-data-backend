from datetime import datetime

from pydantic import BaseModel, Field


class ImportExportBundle(BaseModel):
    exported_at: datetime
    data: dict


class ImportResult(BaseModel):
    imported_tags: int
    imported_terms: int
    imported_lineage_assets: int = 0
    imported_lineage_relations: int = 0
    imported_lineage_processes: int = 0
    imported_lineage_edges: int = 0
    ignored_legacy_lineage_processes: int = 0
    ignored_legacy_lineage_edges: int = 0
    warnings: list[str] = Field(default_factory=list)
