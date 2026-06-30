from __future__ import annotations

from t2c_data.features.lineage.contracts import DefaultLineageSyncGateway, LineageSyncGateway
from t2c_data.features.lineage.source_configs import get_source_config, serialize_source_config
from t2c_data.models.auth import User
from t2c_data.services.audit import add_audit_log


def run_lineage_source_sync(
    *,
    db,
    source_id: int,
    namespace: str | None,
    node_id: str | None,
    depth: int,
    table_id: int | None,
    user: User,
    sync_gateway: LineageSyncGateway | None = None,
):
    source = get_source_config(db, source_id)
    gateway = sync_gateway or DefaultLineageSyncGateway()
    result = gateway.sync_source(
        db=db,
        source=source,
        namespace=namespace,
        node_id=node_id,
        depth=depth,
        table_id=table_id,
    )
    result.source = serialize_source_config(source, current_user=user)
    add_audit_log(
        session=db,
        actor_user_id=user.id,
        action="lineage.source.sync",
        entity_type="lineage_source",
        entity_id=source.id,
        message="Lineage source synchronized",
        changes={
            "namespace": namespace or source.default_namespace,
            "node_id": node_id,
            "table_id": table_id,
            "depth": depth,
            "datasets_synced": result.datasets_synced,
            "jobs_synced": result.jobs_synced,
            "runs_synced": result.runs_synced,
            "relations_created": result.relations_created,
            "relations_updated": result.relations_updated,
        },
    )
    db.commit()
    return result


def run_lineage_table_sync(
    *,
    db,
    table_id: int,
    depth: int,
    user: User,
    sync_gateway: LineageSyncGateway | None = None,
):
    gateway = sync_gateway or DefaultLineageSyncGateway()
    result = gateway.sync_table(db=db, table_id=table_id, depth=depth)
    result.source = serialize_source_config(get_source_config(db, result.source.id), current_user=user)
    add_audit_log(
        session=db,
        actor_user_id=user.id,
        action="lineage.table.sync",
        entity_type="table",
        entity_id=table_id,
        message="Table lineage synchronized from automatic source",
        changes={
            "table_id": table_id,
            "depth": depth,
            "datasets_synced": result.datasets_synced,
            "relations_created": result.relations_created,
            "relations_updated": result.relations_updated,
        },
    )
    db.commit()
    return result
