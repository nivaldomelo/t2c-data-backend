from __future__ import annotations

import os
from types import SimpleNamespace

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from t2c_data.core.config import settings
from t2c_data.features.access_control.policy import visible_table_ids
from t2c_data.features.lineage.api_support import get_asset_or_404
from t2c_data.features.lineage.column_edges import serialize_column_edge
from t2c_data.features.lineage.graph_summary import collect_asset_summary
from t2c_data.features.lineage.openlineage_sync import ingest_openlineage_event
from t2c_data.features.lineage.source_configs import serialize_source_config
from t2c_data.features.lineage.visibility import relation_visible_to_user
from t2c_data.models.access_control import DataAccessGrant
from t2c_data.models.catalog import DataSource, Database, Schema, TableEntity
from t2c_data.models.platform import AssetVisibilityRule
from t2c_data.models.lineage import (
    LineageAsset,
    LineageColumnEdge,
    LineageColumnEdgeVersion,
    LineageEventRaw,
    LineageJob,
    LineageRelation,
    LineageRelationVersion,
    LineageRun,
    LineageSourceConfig,
    LineageSyncCheckpoint,
)
from t2c_data.schemas.lineage import LineageEventIn


def _session_factory():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True).execution_options(
        schema_translate_map={settings.db_schema: None}
    )
    with engine.begin() as conn:
        conn.exec_driver_sql("CREATE TABLE users (id INTEGER PRIMARY KEY)")
        conn.exec_driver_sql("CREATE TABLE data_owners (id INTEGER PRIMARY KEY)")
        DataSource.__table__.create(bind=conn)
        Database.__table__.create(bind=conn)
        Schema.__table__.create(bind=conn)
        TableEntity.__table__.create(bind=conn)
        AssetVisibilityRule.__table__.create(bind=conn)
        LineageSourceConfig.__table__.create(bind=conn)
        LineageJob.__table__.create(bind=conn)
        LineageRun.__table__.create(bind=conn)
        LineageAsset.__table__.create(bind=conn)
        LineageRelation.__table__.create(bind=conn)
        LineageRelationVersion.__table__.create(bind=conn)
        LineageColumnEdge.__table__.create(bind=conn)
        LineageColumnEdgeVersion.__table__.create(bind=conn)
        LineageEventRaw.__table__.create(bind=conn)
        LineageSyncCheckpoint.__table__.create(bind=conn)
    return sessionmaker(bind=engine, future=True)


def _seed_catalog(session):
    datasource = DataSource(name="local-andromeda", db_type="postgres", host="localhost", port=5432, database="andromeda", username="tester")
    datasource.set_secret_values({"password": "secret"})
    session.add(datasource)
    session.flush()
    database = Database(datasource_id=datasource.id, name="andromeda")
    session.add(database)
    session.flush()
    bronze = Schema(database_id=database.id, name="bronze")
    demo = Schema(database_id=database.id, name="demo")
    session.add_all([bronze, demo])
    session.flush()
    source_table = TableEntity(schema_id=bronze.id, name="customers", table_type="table")
    target_table = TableEntity(schema_id=bronze.id, name="customer_metrics", table_type="table")
    blocked_table = TableEntity(schema_id=demo.id, name="tickets", table_type="table")
    session.add_all([source_table, target_table, blocked_table])
    session.flush()
    return datasource, database, bronze, demo, source_table, target_table, blocked_table


def test_openlineage_event_ingestion_persists_internal_graph():
    SessionLocal = _session_factory()

    with SessionLocal() as session:
        _datasource, _database, bronze, _demo, source_table, target_table, _blocked_table = _seed_catalog(session)
        source = LineageSourceConfig(
            name="OpenLineage",
            source_type="openlineage",
            base_url="http://openlineage.internal",
            default_namespace="local-andromeda",
        )
        session.add(source)
        session.flush()

        result = ingest_openlineage_event(
            session,
            payload=LineageEventIn(
                source_id=source.id,
                payload={
                    "eventType": "COMPLETE",
                    "eventTime": "2026-04-08T10:00:00Z",
                    "producer": "http://airflow.internal",
                    "job": {
                        "namespace": "local-andromeda",
                        "name": "bronze.customers_to_metrics",
                    },
                    "run": {"runId": "run-1"},
                    "inputs": [
                        {
                            "namespace": "local-andromeda",
                            "name": "bronze.customers",
                        }
                    ],
                    "outputs": [
                        {
                            "namespace": "local-andromeda",
                            "name": "bronze.customer_metrics",
                            "facets": {
                                "columnLineage": {
                                    "fields": {
                                        "customer_id": {
                                            "inputFields": [
                                                {
                                                    "namespace": "local-andromeda",
                                                    "name": "bronze.customers",
                                                    "field": "id",
                                                }
                                            ]
                                        }
                                    }
                                }
                            },
                        }
                    ],
                },
            ),
        )

        raw_events = session.scalars(select(LineageEventRaw)).all()
        checkpoints = session.scalars(select(LineageSyncCheckpoint)).all()
        relations = session.scalars(select(LineageRelation)).all()
        column_edges = session.scalars(select(LineageColumnEdge)).all()
        assets = session.scalars(select(LineageAsset)).all()
        serialized_column_edge = serialize_column_edge(column_edges[0], focus_asset=session.scalar(select(LineageAsset).where(LineageAsset.catalog_table_id == source_table.id)))
        summary = collect_asset_summary(session, session.scalar(select(LineageAsset).where(LineageAsset.catalog_table_id == source_table.id)))
        source_status = source.last_sync_status
        source_table_id = source_table.id
        target_table_id = target_table.id

    assert result.processed is True
    assert result.datasets_synced >= 2
    assert result.jobs_synced >= 1
    assert result.runs_synced >= 1
    assert result.relations_created >= 1
    assert result.column_edges_created >= 1
    assert len(raw_events) == 1
    assert len(checkpoints) == 1
    assert len(relations) == 1
    assert len(column_edges) == 1
    assert len(assets) == 2
    assert serialized_column_edge.relative_direction == "downstream"
    assert serialized_column_edge.local_column_name == "id"
    assert serialized_column_edge.related_column_name == "customer_id"
    assert serialized_column_edge.evidence_source == "openlineage"
    assert serialized_column_edge.evidence_label == "OpenLineage"
    assert serialized_column_edge.confidence_label == "Alta"
    assert serialized_column_edge.confidence_tier == "strong"
    assert serialized_column_edge.is_verified is False
    assert serialized_column_edge.version == 1
    assert summary.lineage_sources == ["Linhagem interna"]
    assert summary.graph_edges[0].confidence_score == 100
    assert summary.graph_edges[0].confidence_tier == "strong"
    assert summary.graph_edges[0].is_verified is False
    assert summary.graph_edges[0].version == 1
    assert raw_events[0].is_processed is True
    assert checkpoints[0].last_status == "success"
    assert source_status == "success"
    assert {asset.catalog_table_id for asset in assets} == {source_table_id, target_table_id}


def test_lineage_visibility_hides_demo_relations_for_bronze_scope():
    SessionLocal = _session_factory()

    with SessionLocal() as session:
        _datasource, _database, bronze, demo, source_table, target_table, blocked_table = _seed_catalog(session)
        bronze_asset = LineageAsset(
            catalog_table_id=source_table.id,
            asset_key=f"catalog_table:{source_table.id}",
            asset_name="bronze.customers",
            asset_type="table",
            layer="bronze",
            schema_name="bronze",
            object_name="customers",
            system_name="local-andromeda",
            asset_origin="manual",
            is_active=True,
        )
        demo_asset = LineageAsset(
            catalog_table_id=blocked_table.id,
            asset_key=f"catalog_table:{blocked_table.id}",
            asset_name="demo.tickets",
            asset_type="table",
            layer="definir",
            schema_name="demo",
            object_name="tickets",
            system_name="local-andromeda",
            asset_origin="manual",
            is_active=True,
        )
        session.add_all([bronze_asset, demo_asset])
        session.flush()
        relation = LineageRelation(
            source_asset_id=bronze_asset.id,
            target_asset_id=demo_asset.id,
            relation_type="transformation",
            discovery_method="automatic",
            confidence_score=100,
            is_active=True,
        )
        session.add(relation)
        session.flush()

        user = SimpleNamespace(
            id=99,
            email="caio@example.com",
            roles=[SimpleNamespace(name="viewer")],
            access_grants=[DataAccessGrant(id=1, user_id=99, effect="allow", schema_id=bronze.id)],
            access_groups=[],
        )

        visible_ids = visible_table_ids(user, session.scalars(select(TableEntity)).all())
        relations_visible = relation_visible_to_user(session, user, relation)
        summary = collect_asset_summary(session, bronze_asset, current_user=user)
        bronze_table_id = source_table.id
        target_table_id = target_table.id

    assert visible_ids == [bronze_table_id, target_table_id]
    assert relations_visible is False
    assert summary.downstream == []
    assert summary.impact.downstream_count == 0
