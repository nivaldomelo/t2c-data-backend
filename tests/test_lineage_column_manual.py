from __future__ import annotations

import os
from types import SimpleNamespace

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy import create_engine, select
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import sessionmaker

from t2c_data.core.config import settings
from t2c_data.features.lineage.column_actions import (
    create_or_update_manual_column_edge_with_audit,
    deactivate_manual_column_edge_with_audit,
    get_column_edge_or_404,
    update_manual_column_edge_with_audit,
)
from t2c_data.models.access_control import DataAccessGrant
from t2c_data.models.audit import AuditLog
from t2c_data.models.catalog import DataSource, Database, Schema, TableEntity
from t2c_data.models.lineage import LineageAsset, LineageColumnEdge, LineageColumnEdgeVersion
from t2c_data.models.platform import PlatformDomainEvent
from t2c_data.schemas.lineage import LineageColumnEdgeCreate, LineageColumnEdgeUpdate


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
        AuditLog.__table__.create(bind=conn)
        PlatformDomainEvent.__table__.create(bind=conn)
        LineageAsset.__table__.create(bind=conn)
        LineageColumnEdge.__table__.create(bind=conn)
        LineageColumnEdgeVersion.__table__.create(bind=conn)
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
    session.add(bronze)
    session.flush()
    source_table = TableEntity(schema_id=bronze.id, name="customers", table_type="table")
    target_table = TableEntity(schema_id=bronze.id, name="customer_metrics", table_type="table")
    session.add_all([source_table, target_table])
    session.flush()

    source_asset = LineageAsset(
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
    target_asset = LineageAsset(
        catalog_table_id=target_table.id,
        asset_key=f"catalog_table:{target_table.id}",
        asset_name="bronze.customer_metrics",
        asset_type="table",
        layer="bronze",
        schema_name="bronze",
        object_name="customer_metrics",
        system_name="local-andromeda",
        asset_origin="manual",
        is_active=True,
    )
    session.add_all([source_asset, target_asset])
    session.flush()
    return bronze, source_table, target_table, source_asset, target_asset


def test_manual_column_lineage_create_update_deactivate():
    SessionLocal = _session_factory()

    with SessionLocal() as session:
        bronze, source_table, target_table, source_asset, target_asset = _seed_catalog(session)
        user = SimpleNamespace(
            id=42,
            email="caio@example.com",
            roles=[SimpleNamespace(name="editor")],
            access_grants=[DataAccessGrant(id=1, user_id=42, effect="allow", schema_id=bronze.id)],
            access_groups=[],
        )

        created = create_or_update_manual_column_edge_with_audit(
            db=session,
            user=user,
            payload=LineageColumnEdgeCreate(
                source_asset_id=source_asset.id,
                target_asset_id=target_asset.id,
                source_column_name="cpf",
                target_column_name="cliente_cpf",
                relation_type="transformation",
                discovery_method="manual",
                confidence_score=90,
                evidence_source="manual",
                transform_expression="normalize(cpf)",
                notes="Linha criada manualmente",
            ),
        )
        source_asset_id = source_asset.id
        target_asset_id = target_asset.id
        source_table_id = source_table.id
        target_table_id = target_table.id

        edge = session.scalar(select(LineageColumnEdge))
        updated = update_manual_column_edge_with_audit(
            db=session,
            edge=edge,
            user=user,
            payload=LineageColumnEdgeUpdate(
                target_column_name="cliente_cpf_normalizado",
                confidence_score=70,
                notes="Ajustada manualmente",
            ),
        )
        visible_edge = get_column_edge_or_404(session, edge.id, user=user)
        visible_edge_active = visible_edge.is_active
        deactivate_result = deactivate_manual_column_edge_with_audit(db=session, edge=edge, user=user)
        persisted_edge = session.scalar(select(LineageColumnEdge).where(LineageColumnEdge.id == edge.id))

    assert created.source_asset_id == source_asset_id
    assert created.target_asset_id == target_asset_id
    assert created.evidence_label == "Manual"
    assert created.confidence_label == "Alta"
    assert updated.target_column_name == "cliente_cpf_normalizado"
    assert updated.confidence_label == "Média"
    assert visible_edge_active is True
    assert deactivate_result == {"success": True}
    assert persisted_edge is not None and persisted_edge.is_active is False
    assert edge.id is not None
    assert source_table_id != target_table_id
