from __future__ import annotations

import os

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from t2c_data.core.config import settings
from t2c_data.api.import_export import (
    _import_canonical_lineage_relations,
    _serialize_lineage_assets,
    _serialize_lineage_relations,
    _upsert_canonical_lineage_assets,
)
from t2c_data.models.lineage import LineageAsset, LineageRelation


def _session_factory():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True).execution_options(
        schema_translate_map={settings.db_schema: None}
    )
    with engine.begin() as conn:
        conn.exec_driver_sql("CREATE TABLE users (id INTEGER PRIMARY KEY)")
        conn.exec_driver_sql("CREATE TABLE data_sources (id INTEGER PRIMARY KEY)")
        conn.exec_driver_sql("CREATE TABLE tables (id INTEGER PRIMARY KEY)")
        LineageAsset.__table__.create(bind=conn)
        LineageRelation.__table__.create(bind=conn)
    return sessionmaker(bind=engine, future=True)


def test_io_bundle_serializes_canonical_lineage_shapes():
    SessionLocal = _session_factory()

    with SessionLocal() as session:
        upstream = LineageAsset(
            asset_key="table.bronze.orders_raw",
            asset_name="bronze.orders_raw",
            asset_type="table",
            layer="bronze",
        )
        downstream = LineageAsset(
            asset_key="table.silver.orders_curated",
            asset_name="silver.orders_curated",
            asset_type="table",
            layer="silver",
        )
        session.add_all([upstream, downstream])
        session.flush()
        session.add(
            LineageRelation(
                source_asset_id=upstream.id,
                target_asset_id=downstream.id,
                relation_type="transformation",
                process_name="job_orders_curated",
                process_type="airflow",
                discovery_method="manual",
                confidence_score=95,
                is_active=True,
            )
        )
        session.commit()

        assets_payload = _serialize_lineage_assets(session)
        relations_payload = _serialize_lineage_relations(session)

    assert assets_payload[0]["asset_key"]
    assert "source_asset_key" in relations_payload[0]
    assert "target_asset_key" in relations_payload[0]
    assert "process_name" in relations_payload[0]


def test_io_bundle_imports_canonical_lineage_shapes():
    SessionLocal = _session_factory()

    with SessionLocal() as session:
        warnings: list[str] = []
        imported_assets, asset_id_map = _upsert_canonical_lineage_assets(
            session,
            [
                {
                    "asset_key": "table.bronze.orders_raw",
                    "asset_name": "bronze.orders_raw",
                    "asset_type": "table",
                    "layer": "bronze",
                },
                {
                    "asset_key": "table.silver.orders_curated",
                    "asset_name": "silver.orders_curated",
                    "asset_type": "table",
                    "layer": "silver",
                },
            ],
        )
        imported_relations = _import_canonical_lineage_relations(
            session,
            [
                {
                    "source_asset_key": "table.bronze.orders_raw",
                    "target_asset_key": "table.silver.orders_curated",
                    "relation_type": "transformation",
                    "process_name": "job_orders_curated",
                    "process_type": "airflow",
                    "discovery_method": "manual",
                }
            ],
            asset_id_map=asset_id_map,
            warnings=warnings,
        )
        session.commit()

        assets = session.scalars(select(LineageAsset).order_by(LineageAsset.id)).all()
        relations = session.scalars(select(LineageRelation)).all()

    assert imported_assets == 2
    assert imported_relations == 1
    assert warnings == []
    assert [asset.asset_key for asset in assets] == [
        "table.bronze.orders_raw",
        "table.silver.orders_curated",
    ]
    assert relations[0].process_name == "job_orders_curated"
