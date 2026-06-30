from __future__ import annotations

import os
from types import SimpleNamespace

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy import create_engine, select
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import sessionmaker

from t2c_data.core.config import settings
from t2c_data.features.lineage.column_actions import create_or_update_manual_column_edge_with_audit, update_manual_column_edge_with_audit
from t2c_data.features.lineage.graph_summary import collect_asset_summary
from t2c_data.features.lineage.persistence import create_relation, update_relation
from t2c_data.models.catalog import DataSource, Database, Schema, TableEntity
from t2c_data.models.lineage import (
    LineageAsset,
    LineageColumnEdge,
    LineageColumnEdgeVersion,
    LineageJob,
    LineageRelation,
    LineageRelationVersion,
    LineageSourceConfig,
)
from t2c_data.schemas.lineage import LineageColumnEdgeCreate, LineageColumnEdgeUpdate, LineageRelationAssetRefIn, LineageRelationCreate, LineageRelationUpdate


if not hasattr(SQLiteTypeCompiler, "visit_INET"):
    SQLiteTypeCompiler.visit_INET = lambda self, type_, **kw: "TEXT"  # type: ignore[attr-defined]
if not hasattr(SQLiteTypeCompiler, "visit_JSONB"):
    SQLiteTypeCompiler.visit_JSONB = lambda self, type_, **kw: "JSON"  # type: ignore[attr-defined]


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
        LineageSourceConfig.__table__.create(bind=conn)
        LineageJob.__table__.create(bind=conn)
        LineageAsset.__table__.create(bind=conn)
        LineageRelation.__table__.create(bind=conn)
        LineageRelationVersion.__table__.create(bind=conn)
        LineageColumnEdge.__table__.create(bind=conn)
        LineageColumnEdgeVersion.__table__.create(bind=conn)
    return sessionmaker(bind=engine, future=True)


def _seed_assets(session):
    source = LineageAsset(
        asset_key="catalog_table:1",
        asset_name="bronze.customers",
        asset_type="table",
        layer="bronze",
        schema_name="bronze",
        object_name="customers",
        system_name="local-andromeda",
        asset_origin="manual",
        is_active=True,
    )
    target = LineageAsset(
        asset_key="catalog_table:2",
        asset_name="silver.customers_curated",
        asset_type="table",
        layer="silver",
        schema_name="silver",
        object_name="customers_curated",
        system_name="local-andromeda",
        asset_origin="manual",
        is_active=True,
    )
    session.add_all([source, target])
    session.flush()
    return source, target


def test_relation_version_history_and_confidence_metadata():
    SessionLocal = _session_factory()

    with SessionLocal() as session:
        source, target = _seed_assets(session)
        relation = create_relation(
            session,
            LineageRelationCreate(
                source=LineageRelationAssetRefIn(asset_id=source.id),
                target=LineageRelationAssetRefIn(asset_id=target.id),
                relation_type="transformation",
                process_name="airflow_bronze_to_silver",
                process_type="airflow",
                notes="Initial curated mapping",
                discovery_method="manual",
                confidence_score=96,
            ),
            actor_user_id=1,
        )
        created_version = relation.version
        created_verified = relation.is_verified
        updated = update_relation(
            session,
            relation,
            LineageRelationUpdate(
                notes="Reviewed and adjusted",
                evidence="Reviewed by data steward",
                confidence_score=62,
                is_verified=False,
            ),
            actor_user_id=1,
        )
        summary = collect_asset_summary(session, source)
        versions = session.scalars(select(LineageRelationVersion).order_by(LineageRelationVersion.version_number.asc())).all()

    assert created_version == 1
    assert created_verified is True
    assert updated.version == 2
    assert updated.is_verified is False
    assert relation.version == 2
    assert [version.version_number for version in versions] == [1, 2]
    assert versions[0].is_verified is True
    assert versions[1].is_verified is False
    assert summary.graph_edges[0].confidence_score == 62
    assert summary.graph_edges[0].confidence_tier == "weak"
    assert summary.graph_edges[0].is_verified is False
    assert summary.graph_edges[0].version == 2


def test_column_edge_version_history_and_review_fields(monkeypatch):
    SessionLocal = _session_factory()

    with SessionLocal() as session:
        source, target = _seed_assets(session)
        user = SimpleNamespace(id=99, email="admin@example.com", roles=[SimpleNamespace(name="admin")], access_grants=[], access_groups=[])
        monkeypatch.setattr("t2c_data.features.lineage.column_actions.add_audit_log", lambda **kwargs: None)

        created = create_or_update_manual_column_edge_with_audit(
            db=session,
            user=user,
            payload=LineageColumnEdgeCreate(
                source_asset_id=source.id,
                target_asset_id=target.id,
                source_column_name="cpf",
                target_column_name="customer_cpf",
                relation_type="transformation",
                discovery_method="manual",
                confidence_score=91,
                evidence_source="manual",
                evidence="Reviewed mapping from source field",
                is_verified=True,
            ),
        )
        edge = session.scalar(select(LineageColumnEdge))
        updated = update_manual_column_edge_with_audit(
            db=session,
            edge=edge,
            user=user,
            payload=LineageColumnEdgeUpdate(
                confidence_score=68,
                evidence="Adjusted after steward review",
                is_verified=False,
            ),
        )
        versions = session.scalars(select(LineageColumnEdgeVersion).order_by(LineageColumnEdgeVersion.version_number.asc())).all()

    assert created.version == 1
    assert created.is_verified is True
    assert updated.version == 2
    assert updated.is_verified is False
    assert [version.version_number for version in versions] == [1, 2]
    assert versions[0].snapshot_json
    assert versions[1].snapshot_json
