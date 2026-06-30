from __future__ import annotations

import os

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy import create_engine, event
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.features.lineage.relation_queries import list_relations_page
from t2c_data.models import Base
from t2c_data.models.lineage import LineageAsset, LineageRelation


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
    def _attach_schema(dbapi_connection, _connection_record) -> None:  # type: ignore[no-untyped-def]
        cursor = dbapi_connection.cursor()
        cursor.execute("ATTACH DATABASE ':memory:' AS t2c_data")
        cursor.execute("ATTACH DATABASE ':memory:' AS t2c_ops")
        cursor.close()

    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, class_=Session)
    return SessionLocal()


def test_lineage_relations_paginate_results() -> None:
    db = _build_session()

    source = LineageAsset(
        asset_key="source.asset",
        asset_name="source.asset",
        asset_type="table",
        layer="bronze",
        asset_origin="manual",
    )
    target = LineageAsset(
        asset_key="target.asset",
        asset_name="target.asset",
        asset_type="table",
        layer="bronze",
        asset_origin="manual",
    )
    third = LineageAsset(
        asset_key="third.asset",
        asset_name="third.asset",
        asset_type="table",
        layer="bronze",
        asset_origin="manual",
    )
    db.add_all([source, target, third])
    db.commit()

    relation_a = LineageRelation(
        source_asset_id=source.id,
        target_asset_id=target.id,
        relation_type="upstream",
        discovery_method="manual",
    )
    relation_b = LineageRelation(
        source_asset_id=target.id,
        target_asset_id=third.id,
        relation_type="upstream",
        discovery_method="manual",
    )
    db.add_all([relation_a, relation_b])
    db.commit()

    page_1, total, has_more = list_relations_page(db, page=1, page_size=1, current_user=None)
    page_2, _, has_more_after = list_relations_page(db, page=2, page_size=1, current_user=None)

    assert total == 2
    assert len(page_1) == 1
    assert has_more is True
    assert len(page_2) == 1
    assert has_more_after is False
