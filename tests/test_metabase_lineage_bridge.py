"""The Metabase -> lineage consumption bridge builds automatic edges, no manual input."""

from __future__ import annotations

import os

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy import create_engine, event, select
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.features.lineage.metabase_bridge import sync_metabase_lineage
from t2c_data.models import Base
from t2c_data.models.lineage import LineageAsset, LineageRelation
from t2c_data.models.metabase import MetabaseInstance, MetabaseObject


if not hasattr(SQLiteTypeCompiler, "visit_INET"):
    SQLiteTypeCompiler.visit_INET = lambda self, type_, **kw: "TEXT"  # type: ignore[attr-defined]
if not hasattr(SQLiteTypeCompiler, "visit_JSONB"):
    SQLiteTypeCompiler.visit_JSONB = lambda self, type_, **kw: "JSON"  # type: ignore[attr-defined]


def _build_session() -> Session:
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @event.listens_for(engine, "connect")
    def _attach_schema(dbapi_connection, _record) -> None:  # type: ignore[no-untyped-def]
        cursor = dbapi_connection.cursor()
        cursor.execute("ATTACH DATABASE ':memory:' AS t2c_data")
        cursor.execute("ATTACH DATABASE ':memory:' AS t2c_ops")
        cursor.close()

    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, class_=Session)()


def _instance(db: Session) -> MetabaseInstance:
    inst = MetabaseInstance(name="MB", base_url="http://mb.local", auth_type="session")
    inst.auth_secret = "secret"
    db.add(inst)
    db.flush()
    return inst


def test_bridge_creates_consumption_edges_from_referenced_tables() -> None:
    db = _build_session()
    inst = _instance(db)
    dash = MetabaseObject(
        instance_id=inst.id,
        external_id="10",
        object_type="dashboard",
        title="Vendas",
        archived=False,
        referenced_tables_json=[
            {"full_name": "PUBLIC.ORDERS", "name": "ORDERS", "schema": "PUBLIC", "source": "mbql", "resolved": True},
            {"full_name": "bronze.customers", "name": "customers", "schema": "bronze", "source": "sql", "resolved": True},
        ],
    )
    db.add(dash)
    db.flush()

    summary = sync_metabase_lineage(db, instance=inst, commit=True)
    assert summary["artifacts"] == 1
    assert summary["edges_created"] == 2

    relations = db.scalars(select(LineageRelation)).all()
    assert len(relations) == 2
    assert all(r.relation_type == "consumption" for r in relations)
    assert all(r.discovery_method == "automatic" for r in relations)

    artifact = db.scalar(select(LineageAsset).where(LineageAsset.asset_type == "dashboard"))
    assert artifact is not None and artifact.asset_origin == "automatic"
    # edges point table -> dashboard (dashboard consumes table)
    assert all(r.target_asset_id == artifact.id for r in relations)


def test_bridge_is_idempotent() -> None:
    db = _build_session()
    inst = _instance(db)
    db.add(
        MetabaseObject(
            instance_id=inst.id,
            external_id="20",
            object_type="question",
            title="Q",
            archived=False,
            referenced_tables_json=[{"full_name": "PUBLIC.ORDERS", "name": "ORDERS", "schema": "PUBLIC"}],
        )
    )
    db.flush()

    first = sync_metabase_lineage(db, instance=inst, commit=True)
    second = sync_metabase_lineage(db, instance=inst, commit=True)
    assert first["edges_created"] == 1
    assert second["edges_created"] == 0
    assert db.scalar(select(LineageRelation.id)) is not None
    assert len(db.scalars(select(LineageRelation)).all()) == 1
