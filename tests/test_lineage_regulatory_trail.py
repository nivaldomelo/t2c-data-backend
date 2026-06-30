from __future__ import annotations

import os

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy import create_engine, select
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import sessionmaker

from t2c_data.core.config import settings
from t2c_data.features.lineage.graph_summary import collect_asset_summary
from t2c_data.features.lineage.persistence import create_relation, update_relation
from t2c_data.features.lineage.shared import serialize_relation
from t2c_data.models.catalog import DataSource, Database, Schema, TableEntity
from t2c_data.models.lineage import LineageAsset, LineageRelation, LineageRelationVersion, LineageSourceConfig, LineageJob
from t2c_data.schemas.lineage import LineageRelationAssetRefIn, LineageRelationCreate, LineageRelationUpdate


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
        conn.exec_driver_sql("INSERT INTO users (id) VALUES (7)")
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
    return sessionmaker(bind=engine, future=True)


def _seed_catalog(session):
    datasource = DataSource(name="warehouse", db_type="postgres", host="localhost", port=5432, database="andromeda", username="tester")
    datasource.set_secret_values({"password": "secret"})
    session.add(datasource)
    session.flush()

    database = Database(datasource_id=datasource.id, name="andromeda")
    session.add(database)
    session.flush()

    source_schema = Schema(database_id=database.id, name="source")
    bronze_schema = Schema(database_id=database.id, name="bronze")
    silver_schema = Schema(database_id=database.id, name="silver")
    gold_schema = Schema(database_id=database.id, name="gold")
    dw_schema = Schema(database_id=database.id, name="dw")
    session.add_all([source_schema, bronze_schema, silver_schema, gold_schema, dw_schema])
    session.flush()

    source_table = TableEntity(schema_id=source_schema.id, name="customers_raw", table_type="table")
    bronze_table = TableEntity(schema_id=bronze_schema.id, name="customers_bronze", table_type="table")
    silver_table = TableEntity(schema_id=silver_schema.id, name="customers_silver", table_type="table")
    gold_table = TableEntity(schema_id=gold_schema.id, name="customers_gold", table_type="table")
    dw_table = TableEntity(schema_id=dw_schema.id, name="customers_dw", table_type="table")
    session.add_all([source_table, bronze_table, silver_table, gold_table, dw_table])
    session.flush()

    source_asset = LineageAsset(
        catalog_table_id=source_table.id,
        datasource_id=datasource.id,
        asset_key=f"catalog_table:{source_table.id}",
        asset_name="source.customers_raw",
        asset_type="table",
        layer="source",
        schema_name="source",
        object_name="customers_raw",
        system_name=datasource.name,
        asset_origin="automatic",
        is_active=True,
    )
    bronze_asset = LineageAsset(
        catalog_table_id=bronze_table.id,
        datasource_id=datasource.id,
        asset_key=f"catalog_table:{bronze_table.id}",
        asset_name="bronze.customers_bronze",
        asset_type="table",
        layer="bronze",
        schema_name="bronze",
        object_name="customers_bronze",
        system_name=datasource.name,
        asset_origin="automatic",
        is_active=True,
    )
    silver_asset = LineageAsset(
        catalog_table_id=silver_table.id,
        datasource_id=datasource.id,
        asset_key=f"catalog_table:{silver_table.id}",
        asset_name="silver.customers_silver",
        asset_type="table",
        layer="silver",
        schema_name="silver",
        object_name="customers_silver",
        system_name=datasource.name,
        asset_origin="automatic",
        is_active=True,
    )
    gold_asset = LineageAsset(
        catalog_table_id=gold_table.id,
        datasource_id=datasource.id,
        asset_key=f"catalog_table:{gold_table.id}",
        asset_name="gold.customers_gold",
        asset_type="table",
        layer="gold",
        schema_name="gold",
        object_name="customers_gold",
        system_name=datasource.name,
        asset_origin="automatic",
        is_active=True,
    )
    dw_asset = LineageAsset(
        catalog_table_id=dw_table.id,
        datasource_id=datasource.id,
        asset_key=f"catalog_table:{dw_table.id}",
        asset_name="dw.customers_dw",
        asset_type="table",
        layer="mart",
        schema_name="dw",
        object_name="customers_dw",
        system_name=datasource.name,
        asset_origin="automatic",
        is_active=True,
    )
    metabase_question = LineageAsset(
        asset_key="metabase.question:customers_kpi",
        asset_name="Metabase question - customers KPI",
        asset_type="question",
        layer="dashboard",
        system_name="metabase",
        asset_origin="manual",
        is_active=True,
    )
    metabase_dashboard = LineageAsset(
        asset_key="metabase.dashboard:customers_kpi",
        asset_name="Metabase dashboard - customers KPI",
        asset_type="dashboard",
        layer="dashboard",
        system_name="metabase",
        asset_origin="manual",
        is_active=True,
    )
    external_api = LineageAsset(
        asset_key="api.external:customers_summary",
        asset_name="External API - customers summary",
        asset_type="source",
        layer="source",
        system_name="external-api",
        external_type="api",
        asset_origin="manual",
        is_active=True,
    )
    dq_rule = LineageAsset(
        asset_key="dq.rule:customers_quality",
        asset_name="DQ rule - customers quality",
        asset_type="dq_rule",
        layer="source",
        system_name="data-quality",
        asset_origin="manual",
        is_active=True,
    )
    incident = LineageAsset(
        asset_key="ops.incident:customers_gold",
        asset_name="Incident - customers gold degradation",
        asset_type="incident",
        layer="source",
        system_name="ops",
        asset_origin="manual",
        is_active=True,
    )
    certification = LineageAsset(
        asset_key="governance.certification:customers_gold",
        asset_name="Certification - customers gold",
        asset_type="certification",
        layer="source",
        system_name="governance",
        asset_origin="manual",
        is_active=True,
    )
    session.add_all(
        [
            source_asset,
            bronze_asset,
            silver_asset,
            gold_asset,
            dw_asset,
            metabase_question,
            metabase_dashboard,
            external_api,
            dq_rule,
            incident,
            certification,
        ]
    )
    session.flush()
    return {
        "datasource": datasource,
        "source_asset": source_asset,
        "bronze_asset": bronze_asset,
        "silver_asset": silver_asset,
        "gold_asset": gold_asset,
        "dw_asset": dw_asset,
        "metabase_question": metabase_question,
        "metabase_dashboard": metabase_dashboard,
        "external_api": external_api,
        "dq_rule": dq_rule,
        "incident": incident,
        "certification": certification,
    }


def _ref(asset: LineageAsset) -> LineageRelationAssetRefIn:
    return LineageRelationAssetRefIn(asset_id=asset.id)


def test_regulatory_lineage_trail_reaches_upstream_origin_downstream_consumers_and_versions():
    SessionLocal = _session_factory()

    with SessionLocal() as session:
        assets = _seed_catalog(session)

        source_to_bronze = create_relation(
            session,
            LineageRelationCreate(
                source=_ref(assets["source_asset"]),
                target=_ref(assets["bronze_asset"]),
                relation_type="extracted_from",
                process_name="airflow.extract_customers",
                process_type="airflow",
                discovery_method="automatic",
                confidence_score=98,
                is_verified=True,
            ),
            actor_user_id=7,
        )
        bronze_to_silver = create_relation(
            session,
            LineageRelationCreate(
                source=_ref(assets["bronze_asset"]),
                target=_ref(assets["silver_asset"]),
                relation_type="transformed_to",
                process_name="spark.transform_customers",
                process_type="spark",
                discovery_method="automatic",
                confidence_score=95,
                is_verified=True,
            ),
            actor_user_id=7,
        )
        silver_to_gold = create_relation(
            session,
            LineageRelationCreate(
                source=_ref(assets["silver_asset"]),
                target=_ref(assets["gold_asset"]),
                relation_type="derived_from",
                process_name="airflow.publish_gold_customers",
                process_type="airflow",
                discovery_method="automatic",
                confidence_score=92,
                is_verified=True,
            ),
            actor_user_id=7,
        )
        gold_to_dw = create_relation(
            session,
            LineageRelationCreate(
                source=_ref(assets["gold_asset"]),
                target=_ref(assets["dw_asset"]),
                relation_type="loaded_to",
                process_name="airflow.load_dw_customers",
                process_type="airflow",
                discovery_method="automatic",
                confidence_score=88,
                is_verified=False,
            ),
            actor_user_id=7,
        )
        dw_to_question = create_relation(
            session,
            LineageRelationCreate(
                source=_ref(assets["dw_asset"]),
                target=_ref(assets["metabase_question"]),
                relation_type="consumed_by",
                process_name="metabase.question.customers_kpi",
                process_type="metabase",
                discovery_method="automatic",
                confidence_score=84,
                is_verified=False,
            ),
            actor_user_id=7,
        )
        question_to_dashboard = create_relation(
            session,
            LineageRelationCreate(
                source=_ref(assets["metabase_question"]),
                target=_ref(assets["metabase_dashboard"]),
                relation_type="consumed_by",
                process_name="metabase.dashboard.customers_kpi",
                process_type="metabase",
                discovery_method="automatic",
                confidence_score=82,
                is_verified=False,
            ),
            actor_user_id=7,
        )
        gold_to_api = create_relation(
            session,
            LineageRelationCreate(
                source=_ref(assets["gold_asset"]),
                target=_ref(assets["external_api"]),
                relation_type="consumed_by",
                process_name="api.customers_summary",
                process_type="api",
                discovery_method="automatic",
                confidence_score=72,
                is_verified=False,
            ),
            actor_user_id=7,
        )
        dq_to_gold = create_relation(
            session,
            LineageRelationCreate(
                source=_ref(assets["dq_rule"]),
                target=_ref(assets["gold_asset"]),
                relation_type="validates",
                process_name="dq.customers_quality",
                process_type="dq",
                discovery_method="manual",
                confidence_score=96,
                is_verified=True,
            ),
            actor_user_id=7,
        )
        incident_to_gold = create_relation(
            session,
            LineageRelationCreate(
                source=_ref(assets["incident"]),
                target=_ref(assets["gold_asset"]),
                relation_type="impacts",
                process_name="incident.customers_gold_degradation",
                process_type="ops",
                discovery_method="manual",
                confidence_score=55,
                is_verified=False,
            ),
            actor_user_id=7,
        )
        certification_to_gold = create_relation(
            session,
            LineageRelationCreate(
                source=_ref(assets["certification"]),
                target=_ref(assets["gold_asset"]),
                relation_type="validates",
                process_name="certification.customers_gold",
                process_type="governance",
                discovery_method="manual",
                confidence_score=99,
                is_verified=True,
            ),
            actor_user_id=7,
        )

        updated_gold_to_dw = update_relation(
            session,
            gold_to_dw,
            LineageRelationUpdate(
                evidence="Reviewed after steward validation",
                confidence_score=61,
                is_verified=False,
            ),
            actor_user_id=7,
        )
        updated_gold_to_dw_payload = serialize_relation(updated_gold_to_dw)
        summary = collect_asset_summary(session, assets["gold_asset"], max_depth=6)
        versions = session.scalars(
            select(LineageRelationVersion)
            .where(LineageRelationVersion.lineage_relation_id == updated_gold_to_dw.id)
            .order_by(LineageRelationVersion.version_number.asc())
        ).all()

    assert updated_gold_to_dw_payload.source_asset_id == assets["gold_asset"].id
    assert updated_gold_to_dw_payload.target_asset_id == assets["dw_asset"].id
    assert [version.version_number for version in versions] == [1, 2]
    assert summary.impact.dashboard_count == 2
    assert summary.lineage_origin == "merged"
    assert summary.graph_truncated is False

    upstream_names = {item.asset_name for item in summary.upstream}
    downstream_names = {item.asset_name for item in summary.downstream}
    relation_types = {edge.relation_type for edge in summary.graph_edges}
    confidence_tiers = {edge.confidence_tier for edge in summary.graph_edges}
    node_kinds = {node.kind for node in summary.graph_nodes}

    assert "source.customers_raw" in upstream_names
    assert "bronze.customers_bronze" in upstream_names
    assert "silver.customers_silver" in upstream_names
    assert "DQ rule - customers quality" in upstream_names
    assert "Incident - customers gold degradation" in upstream_names
    assert "Certification - customers gold" in upstream_names
    assert "dw.customers_dw" in downstream_names
    assert "Metabase question - customers KPI" in downstream_names
    assert "Metabase dashboard - customers KPI" in downstream_names
    assert "External API - customers summary" in downstream_names
    assert {"extracted_from", "transformed_to", "derived_from", "loaded_to", "consumed_by", "validates", "impacts"}.issubset(relation_types)
    assert "strong" in confidence_tiers
    assert "weak" in confidence_tiers
    assert "dashboard" in node_kinds
    assert "source" in node_kinds
