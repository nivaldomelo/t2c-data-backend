from __future__ import annotations

from typing import Protocol

from t2c_data.features.lineage.openlineage_sync import rebuild_openlineage_source, rebuild_openlineage_source_for_table
from t2c_data.schemas.lineage import LineageSourceSyncOut


class LineageSyncGateway(Protocol):
    def sync_source(
        self,
        *,
        db,
        source,
        namespace: str | None = None,
        node_id: str | None = None,
        depth: int = 1,
        table_id: int | None = None,
    ) -> LineageSourceSyncOut: ...

    def sync_table(
        self,
        *,
        db,
        table_id: int,
        depth: int = 1,
    ) -> LineageSourceSyncOut: ...


class DefaultLineageSyncGateway:
    def sync_source(
        self,
        *,
        db,
        source,
        namespace: str | None = None,
        node_id: str | None = None,
        depth: int = 1,
        table_id: int | None = None,
    ) -> LineageSourceSyncOut:
        return rebuild_openlineage_source(
            db,
            source=source,
            namespace=namespace,
            node_id=node_id,
            depth=depth,
            table_id=table_id,
        )

    def sync_table(
        self,
        *,
        db,
        table_id: int,
        depth: int = 1,
    ) -> LineageSourceSyncOut:
        return rebuild_openlineage_source_for_table(db, table_id=table_id, depth=depth)
