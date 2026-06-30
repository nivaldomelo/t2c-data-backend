from __future__ import annotations

import os
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import httpx
import pytest

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from fastapi import FastAPI
from fastapi import HTTPException, status
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, func, select
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.pool import StaticPool
from sqlalchemy.orm import sessionmaker

from t2c_data.api.router import api_v1_router
from t2c_data.core.db import get_db
from t2c_data.core.config import settings
from t2c_data.core.deps import get_current_user
from t2c_data.features.metabase.bootstrap import ensure_metabase_instance_from_settings
from t2c_data.features.metabase.bootstrap import snapshot_metabase_instance
from t2c_data.features.metabase.client import MetabaseClientError
from t2c_data.features.integrations import service as integrations_service
from t2c_data.features.metabase import service as metabase_service
from t2c_data.models.catalog import DataSource, Database, Schema, TableEntity
from t2c_data.models.integrations import IntegrationHealth, IntegrationHealthHistory
from t2c_data.models.lineage import LineageAsset, LineageRelation, LineageSourceConfig, LineageJob
from t2c_data.models.metabase import MetabaseInstance, MetabaseObject, MetabaseObjectLink, MetabaseSyncRun
from t2c_data.models.platform import IntegrationSyncJob
from t2c_data.features.lineage.sql_lineage import extract_sql_table_lineage


if not hasattr(SQLiteTypeCompiler, "visit_JSONB"):
    SQLiteTypeCompiler.visit_JSONB = lambda self, type_, **kw: "JSON"  # type: ignore[attr-defined]


class FakeMetabaseClient:
    def __init__(self, config) -> None:
        self.config = config

    def __enter__(self) -> "FakeMetabaseClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def authenticate(self) -> None:
        return None

    def list_collections(self):
        return [
            {
                "id": 10,
                "name": "Execução",
                "children": [],
                "updated_at": "2026-04-14T09:00:00Z",
            }
        ]

    def list_cards(self):
        return [
            {
                "id": 101,
                "name": "Orders by status",
                "collection_id": 10,
                "collection_name": "Execução",
                "database_id": 1,
                "url": "http://metabase.local/question/101",
                "dataset_query": {"native": {"query": "select * from sales.orders"}},
            }
        ]

    def list_dashboards(self):
        return [
            {
                "id": 201,
                "name": "Painel Comercial",
                "collection_id": 10,
                "collection_name": "Execução",
                "url": "http://metabase.local/dashboard/201",
                "ordered_cards": [{"card_id": 101}],
            }
        ]

    def get_card(self, card_id):
        payload = self.list_cards()[0]
        payload["id"] = int(card_id)
        return payload

    def get_dashboard(self, dashboard_id):
        payload = self.list_dashboards()[0]
        payload["id"] = int(dashboard_id)
        return payload

    def get_database_metadata(self, database_id):
        return {
            "tables": [
                {
                    "id": 501,
                    "name": "orders",
                    "schema_name": "sales",
                }
            ]
        }

    def probe_health(self):
        return {"status": "ok"}


class LineageAwareMetabaseClient(FakeMetabaseClient):
    def list_cards(self):
        return [
            {
                "id": 301,
                "name": "Customer address usage",
                "collection_id": 10,
                "collection_name": "Execução",
                "database_id": 1,
                "url": "http://metabase.local/question/301",
                "dataset_query": {"native": {"query": "select * from analytics.vw_customer_primary_address"}},
            }
        ]


def _ready_airflow_contract() -> SimpleNamespace:
    return SimpleNamespace(
        source_schema="airflow_meta",
        schema_exists=True,
        dag_runs_table_exists=True,
        dag_table_exists=True,
        task_instance_table_exists=True,
        dag_tag_table_exists=True,
        task_fail_table_exists=True,
        log_table_exists=True,
        dag_runs_view_exists=True,
        dags_view_exists=True,
        failures_view_exists=True,
        operational_view_exists=True,
        ready=True,
        missing_tables=[],
        missing_views=[],
        contract_version="v1",
    )

    def list_dashboards(self):
        return [
            {
                "id": 401,
                "name": "Customer Address Dashboard",
                "collection_id": 10,
                "collection_name": "Execução",
                "url": "http://metabase.local/dashboard/401",
                "ordered_cards": [{"card_id": 301}],
            }
        ]

    def get_database_metadata(self, database_id):
        return {
            "tables": [
                {
                    "id": 502,
                    "name": "vw_customer_primary_address",
                    "schema_name": "analytics",
                }
            ]
        }


class DirectFqnMetabaseClient(FakeMetabaseClient):
    def list_cards(self):
        return [
            {
                "id": 501,
                "name": "Customer address direct usage",
                "collection_id": 10,
                "collection_name": "Execução",
                "database_id": 1,
                "url": "http://metabase.local/question/501",
                "dataset_query": {
                    "native": {"query": 'select * from "local-andromeda".andromeda.bronze.customer_addresses'}
                },
            }
        ]

    def list_dashboards(self):
        return [
            {
                "id": 601,
                "name": "Customer Address Direct Dashboard",
                "collection_id": 10,
                "collection_name": "Execução",
                "url": "http://metabase.local/dashboard/601",
                "ordered_cards": [{"card_id": 501}],
            }
        ]

    def get_database_metadata(self, database_id):
        return {
            "tables": [
                {
                    "id": 701,
                    "name": "customer_addresses",
                    "schema_name": "bronze",
                }
            ]
        }


class NestedStagesMetabaseClient(FakeMetabaseClient):
    def list_cards(self):
        return [
            {
                "id": 701,
                "name": "Customer address nested usage",
                "collection_id": 10,
                "collection_name": "Execução",
                "database_id": 1,
                "url": "http://metabase.local/question/701",
                "dataset_query": {
                    "stages": [
                        {
                            "native": "select * from bronze.customer_addresses;",
                            "lib/type": "mbql.stage/native",
                        }
                    ],
                    "database": 1,
                    "lib/type": "mbql/query",
                },
            }
        ]

    def list_dashboards(self):
        return [
            {
                "id": 801,
                "name": "Customer Address Nested Dashboard",
                "collection_id": 10,
                "collection_name": "Execução",
                "url": "http://metabase.local/dashboard/801",
                "ordered_cards": [{"card_id": 701}],
            }
        ]

    def get_database_metadata(self, database_id):
        return {
            "tables": [
                {
                    "id": 901,
                    "name": "customer_addresses",
                    "schema_name": "bronze",
                }
            ]
        }


class DatetimePayloadMetabaseClient(FakeMetabaseClient):
    def list_cards(self):
        return [
            {
                "id": 902,
                "name": "Customer address datetime payload",
                "collection_id": 10,
                "collection_name": "Execução",
                "database_id": 1,
                "url": "http://metabase.local/question/902",
                "dataset_query": {
                    "native": {
                        "query": "select * from analytics.vw_customer_primary_address",
                        "run_at": datetime(2026, 4, 14, 10, 30, tzinfo=timezone.utc),
                    }
                },
            }
        ]

    def list_dashboards(self):
        return [
            {
                "id": 903,
                "name": "Datetime payload dashboard",
                "collection_id": 10,
                "collection_name": "Execução",
                "url": "http://metabase.local/dashboard/903",
                "ordered_cards": [{"card_id": 902}],
            }
        ]

    def get_database_metadata(self, database_id):
        return {
            "tables": [
                {
                    "id": 904,
                    "name": "vw_customer_primary_address",
                    "schema_name": "analytics",
                }
            ]
        }


class FailingAuthMetabaseClient(FakeMetabaseClient):
    def authenticate(self) -> None:
        raise MetabaseClientError("invalid credentials")


class HealthFailingMetabaseClient(FakeMetabaseClient):
    def probe_health(self):
        raise MetabaseClientError("Metabase health check failed GET /api/health against http://metabase.local: Network is unreachable")


@contextmanager
def _operational_session(session):
    yield session


def _session_factory():
    metabase_service.maybe_start_integration_job = lambda *args, **kwargs: None  # type: ignore[assignment]
    engine = create_engine(
        "sqlite+pysqlite://",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    ).execution_options(
        schema_translate_map={settings.db_schema: None}
    )
    with engine.begin() as conn:
        DataSource.__table__.create(bind=conn)
        Database.__table__.create(bind=conn)
        Schema.__table__.create(bind=conn)
        TableEntity.__table__.create(bind=conn)
        LineageSourceConfig.__table__.create(bind=conn)
        LineageJob.__table__.create(bind=conn)
        LineageAsset.__table__.create(bind=conn)
        LineageRelation.__table__.create(bind=conn)
        MetabaseInstance.__table__.create(bind=conn)
        MetabaseObject.__table__.create(bind=conn)
        MetabaseObjectLink.__table__.create(bind=conn)
        MetabaseSyncRun.__table__.create(bind=conn)
        IntegrationSyncJob.__table__.create(bind=conn)
        IntegrationHealth.__table__.create(bind=conn)
        IntegrationHealthHistory.__table__.create(bind=conn)
    return sessionmaker(bind=engine, future=True)


def _seed_catalog(session):
    datasource = DataSource(name="warehouse", db_type="postgres", host="localhost", port=5432, database="analytics", username="tester")
    datasource.set_secret_values({"password": "secret"})
    session.add(datasource)
    session.flush()

    database = Database(datasource_id=datasource.id, name="analytics")
    session.add(database)
    session.flush()

    sales_schema = Schema(database_id=database.id, name="sales")
    analytics_schema = Schema(database_id=database.id, name="analytics")
    bronze_schema = Schema(database_id=database.id, name="bronze")
    session.add_all([sales_schema, analytics_schema, bronze_schema])
    session.flush()

    table = TableEntity(schema_id=sales_schema.id, name="orders", table_type="table", certification_status="certified")
    view_table = TableEntity(
        schema_id=analytics_schema.id,
        name="vw_customer_primary_address",
        table_type="view",
        certification_status="certified",
    )
    base_table = TableEntity(
        schema_id=bronze_schema.id,
        name="customer_addresses",
        table_type="table",
        certification_status="certified",
    )
    session.add_all([table, view_table, base_table])
    session.flush()
    return datasource, database, sales_schema, table, analytics_schema, view_table, bronze_schema, base_table


def _seed_instance(session):
    instance = MetabaseInstance(
        name="Local Metabase",
        base_url="http://metabase.local",
        auth_type="none",
        timeout_seconds=5,
        sync_dashboards=True,
        sync_questions=True,
        sync_collections=True,
        enabled=True,
    )
    session.add(instance)
    session.flush()
    return instance


def _seed_local_andromeda_catalog(session):
    datasource = DataSource(name="local-andromeda", db_type="postgres", host="localhost", port=5432, database="andromeda", username="tester")
    datasource.set_secret_values({"password": "secret"})
    session.add(datasource)
    session.flush()

    database = Database(datasource_id=datasource.id, name="andromeda")
    session.add(database)
    session.flush()

    bronze_schema = Schema(database_id=database.id, name="bronze")
    session.add(bronze_schema)
    session.flush()

    table = TableEntity(
        schema_id=bronze_schema.id,
        name="customer_addresses",
        table_type="table",
        certification_status="certified",
    )
    session.add(table)
    session.flush()
    return datasource, database, bronze_schema, table


def test_metabase_sync_is_idempotent_and_links_consumers(monkeypatch):
    SessionLocal = _session_factory()
    monkeypatch.setattr(metabase_service, "MetabaseClient", FakeMetabaseClient)

    with SessionLocal() as session:
        _seed_catalog(session)
        instance = _seed_instance(session)

        first = metabase_service.run_metabase_instance_sync(session, instance.id)
        second = metabase_service.run_metabase_instance_sync(session, instance.id)
        consumption = metabase_service.get_table_metabase_consumption(session, 1)

        object_count = session.scalar(select(func.count(MetabaseObject.id)))
        link_count = session.scalar(select(func.count(MetabaseObjectLink.id)))
        sync_run_count = session.scalar(select(func.count(MetabaseSyncRun.id)))

    assert first.status == "success"
    assert second.status == "success"
    assert object_count == 3
    assert link_count == 3
    assert sync_run_count == 2
    assert consumption.available is True
    assert consumption.dashboards_count == 1
    assert consumption.questions_count == 1
    assert consumption.collections_count == 1
    assert consumption.confirmed_count == 0
    assert consumption.inferred_count == 1
    assert consumption.partial_count == 2


def test_metabase_sync_links_base_table_indirectly_through_view_lineage(monkeypatch):
    SessionLocal = _session_factory()
    monkeypatch.setattr(metabase_service, "MetabaseClient", LineageAwareMetabaseClient)

    with SessionLocal() as session:
        datasource, _, _, _, _, view_table, _, base_table = _seed_catalog(session)
        source = LineageSourceConfig(
            name="OpenLineage source",
            source_type="openlineage",
            base_url="internal://openlineage",
            enabled=True,
        )
        session.add(source)
        session.flush()
        view_asset = LineageAsset(
            lineage_source_id=source.id,
            catalog_table_id=view_table.id,
            datasource_id=datasource.id,
            asset_key=f"catalog_table:{view_table.id}",
            asset_name="analytics.vw_customer_primary_address",
            asset_type="view",
            layer="gold",
            schema_name="analytics",
            object_name="vw_customer_primary_address",
            system_name="warehouse",
            asset_origin="automatic",
            is_active=True,
        )
        base_asset = LineageAsset(
            lineage_source_id=source.id,
            catalog_table_id=base_table.id,
            datasource_id=datasource.id,
            asset_key=f"catalog_table:{base_table.id}",
            asset_name="bronze.customer_addresses",
            asset_type="table",
            layer="bronze",
            schema_name="bronze",
            object_name="customer_addresses",
            system_name="warehouse",
            asset_origin="automatic",
            is_active=True,
        )
        session.add_all([view_asset, base_asset])
        session.flush()
        session.add(
            LineageRelation(
                lineage_source_id=source.id,
                source_asset_id=base_asset.id,
                target_asset_id=view_asset.id,
                relation_type="transformation",
                process_name="metabase_view",
                process_type="sql",
                discovery_method="automatic",
                confidence_score=100,
                is_active=True,
            )
        )
        instance = _seed_instance(session)

        run = metabase_service.run_metabase_instance_sync(session, instance.id)
        base_consumption = metabase_service.get_table_metabase_consumption(session, base_table.id)
        view_consumption = metabase_service.get_table_metabase_consumption(session, view_table.id)
        link_rows = session.execute(
            select(MetabaseObjectLink).order_by(MetabaseObjectLink.id.asc())
        ).scalars().all()

    assert run.status == "success"
    assert view_consumption.dashboards_count == 0
    assert view_consumption.questions_count == 1
    assert view_consumption.match_state == "direct"
    assert base_consumption.dashboards_count == 0
    assert base_consumption.questions_count == 1
    assert base_consumption.match_state == "indirect"
    assert base_consumption.indirect_count == 1
    assert any(link.match_method == "indirect_view" for link in link_rows)
    assert any(link.confidence_reason and "vw_customer_primary_address" in link.confidence_reason for link in link_rows)
    assert base_consumption.questions[0].source_table_name == "vw_customer_primary_address"


def test_metabase_consumption_aggregates_indirect_view_links_when_base_has_no_direct_link(monkeypatch):
    SessionLocal = _session_factory()

    with SessionLocal() as session:
        datasource, _, _, _, _, view_table, _, base_table = _seed_catalog(session)
        source = LineageSourceConfig(
            name="OpenLineage source",
            source_type="openlineage",
            base_url="internal://openlineage",
            enabled=True,
        )
        session.add(source)
        session.flush()
        view_asset = LineageAsset(
            lineage_source_id=source.id,
            catalog_table_id=view_table.id,
            datasource_id=datasource.id,
            asset_key=f"catalog_table:{view_table.id}",
            asset_name="analytics.vw_customer_primary_address",
            asset_type="view",
            layer="gold",
            schema_name="analytics",
            object_name="vw_customer_primary_address",
            system_name="warehouse",
            asset_origin="automatic",
            is_active=True,
        )
        base_asset = LineageAsset(
            lineage_source_id=source.id,
            catalog_table_id=base_table.id,
            datasource_id=datasource.id,
            asset_key=f"catalog_table:{base_table.id}",
            asset_name="bronze.customer_addresses",
            asset_type="table",
            layer="bronze",
            schema_name="bronze",
            object_name="customer_addresses",
            system_name="warehouse",
            asset_origin="automatic",
            is_active=True,
        )
        session.add_all([view_asset, base_asset])
        session.flush()
        session.add(
            LineageRelation(
                lineage_source_id=source.id,
                source_asset_id=base_asset.id,
                target_asset_id=view_asset.id,
                relation_type="transformation",
                process_name="metabase_view",
                process_type="sql",
                discovery_method="automatic",
                confidence_score=100,
                is_active=True,
            )
        )
        instance = _seed_instance(session)
        question = MetabaseObject(
            instance_id=instance.id,
            external_id="301",
            object_type="question",
            title="Customer address usage",
            url="http://metabase.local/question/301",
            collection_name="Execução",
            collection_external_id="10",
            database_id=1,
            archived=False,
            last_seen_at=metabase_service._now(),
            raw_json={"id": 301},
            dataset_query_json={"native": {"query": "select * from analytics.vw_customer_primary_address"}},
            metadata_json={"id": 301},
        )
        dashboard = MetabaseObject(
            instance_id=instance.id,
            external_id="401",
            object_type="dashboard",
            title="Customer Address Dashboard",
            url="http://metabase.local/dashboard/401",
            collection_name="Execução",
            collection_external_id="10",
            archived=False,
            last_seen_at=metabase_service._now(),
            raw_json={"id": 401},
            dataset_query_json=None,
            metadata_json={"id": 401},
        )
        collection = MetabaseObject(
            instance_id=instance.id,
            external_id="10",
            object_type="collection",
            title="Execução",
            url="http://metabase.local/collection/10",
            collection_name=None,
            collection_external_id=None,
            archived=False,
            last_seen_at=metabase_service._now(),
            raw_json={"id": 10},
            dataset_query_json=None,
            metadata_json={"id": 10},
        )
        session.add_all([question, dashboard, collection])
        session.flush()
        session.add_all(
            [
                MetabaseObjectLink(
                    instance_id=instance.id,
                    metabase_object_id=question.id,
                    table_id=view_table.id,
                    column_id=None,
                    match_method="direct",
                    confidence_level="confirmed",
                    confidence_reason="Metabase source-table",
                    source_table_name=view_table.name,
                    source_schema_name=view_table.schema.name,
                    source_database_name=view_table.schema.database.name,
                    is_active=True,
                ),
                MetabaseObjectLink(
                    instance_id=instance.id,
                    metabase_object_id=dashboard.id,
                    table_id=view_table.id,
                    column_id=None,
                    match_method="dashboard_card",
                    confidence_level="partial",
                    confidence_reason="Dashboard card linked to question Customer address usage",
                    source_table_name=view_table.name,
                    source_schema_name=view_table.schema.name,
                    source_database_name=view_table.schema.database.name,
                    is_active=True,
                ),
                MetabaseObjectLink(
                    instance_id=instance.id,
                    metabase_object_id=collection.id,
                    table_id=view_table.id,
                    column_id=None,
                    match_method="collection_membership",
                    confidence_level="partial",
                    confidence_reason="Collection linked to dashboard Customer Address Dashboard",
                    source_table_name="Execução",
                    is_active=True,
                ),
            ]
        )

        base_consumption = metabase_service.get_table_metabase_consumption(session, base_table.id)

    assert base_consumption.dashboards_count == 1
    assert base_consumption.questions_count == 1
    assert base_consumption.collections_count == 1
    assert base_consumption.match_state == "indirect"
    assert base_consumption.direct_count == 0
    assert base_consumption.indirect_count == 3
    assert base_consumption.questions[0].match_method == "indirect_view"
    assert base_consumption.questions[0].confidence_reason == "Encontrado via view analytics.vw_customer_primary_address"
    assert base_consumption.questions[0].source_table_name == "vw_customer_primary_address"
    assert base_consumption.collections[0].match_method == "indirect_view"
    assert base_consumption.collections[0].confidence_reason == "Encontrado via view analytics.vw_customer_primary_address"


def test_metabase_sync_matches_direct_fqn_prefixed_table_reference(monkeypatch):
    SessionLocal = _session_factory()
    monkeypatch.setattr(metabase_service, "MetabaseClient", DirectFqnMetabaseClient)

    with SessionLocal() as session:
        _, _, _, base_table = _seed_local_andromeda_catalog(session)
        instance = _seed_instance(session)

        run = metabase_service.run_metabase_instance_sync(session, instance.id)
        consumption = metabase_service.get_table_metabase_consumption(session, base_table.id)
        link_rows = session.execute(select(MetabaseObjectLink).order_by(MetabaseObjectLink.id.asc())).scalars().all()

    assert extract_sql_table_lineage('select * from "local-andromeda".andromeda.bronze.customer_addresses') == [
        "local-andromeda.andromeda.bronze.customer_addresses"
    ]
    assert run.status == "success"
    assert consumption.dashboards_count == 1
    assert consumption.questions_count == 1
    assert consumption.collections_count == 1
    assert consumption.match_state == "direct"
    assert consumption.direct_count == 1
    assert consumption.indirect_count == 0
    assert consumption.questions[0].match_method in {"direct", "sql"}
    assert consumption.questions[0].source_table_name == "customer_addresses"
    assert any(link.table_id == base_table.id for link in link_rows)


def test_metabase_consumption_api_returns_real_links_for_direct_fqn_reference(monkeypatch):
    SessionLocal = _session_factory()
    monkeypatch.setattr(metabase_service, "MetabaseClient", DirectFqnMetabaseClient)

    with SessionLocal() as session:
        _, _, _, base_table = _seed_local_andromeda_catalog(session)
        instance = _seed_instance(session)
        run = metabase_service.run_metabase_instance_sync(session, instance.id)
        assert run.status == "success"

        app = FastAPI()
        app.include_router(api_v1_router, prefix="/api/v1")

        fake_user = SimpleNamespace(
            id=1,
            email="admin@local",
            name="Admin",
            full_name="Admin",
            is_active=True,
            roles=[SimpleNamespace(name="admin", permissions=[])],
        )

        def override_get_db():
            yield session

        app.dependency_overrides[get_db] = override_get_db
        app.dependency_overrides[get_current_user] = lambda: fake_user

        client = TestClient(app)
        response = client.get(f"/api/v1/catalog/tables/{base_table.id}/metabase-consumption")

    assert response.status_code == 200
    payload = response.json()
    assert payload["table_id"] == base_table.id
    assert payload["dashboards_count"] == 1
    assert payload["questions_count"] == 1
    assert payload["collections_count"] == 1
    assert payload["match_state"] == "direct"
    assert payload["direct_count"] == 1
    assert payload["indirect_count"] == 0
    assert payload["questions"][0]["title"] == "Customer address direct usage"
    assert payload["questions"][0]["match_method"] in {"direct", "sql"}
    assert payload["questions"][0]["source_table_name"] == "customer_addresses"


def test_metabase_sync_parses_nested_dataset_query_stages(monkeypatch):
    SessionLocal = _session_factory()
    monkeypatch.setattr(metabase_service, "MetabaseClient", NestedStagesMetabaseClient)

    with SessionLocal() as session:
        _, _, _, base_table = _seed_local_andromeda_catalog(session)
        instance = _seed_instance(session)
        run = metabase_service.run_metabase_instance_sync(session, instance.id)
        consumption = metabase_service.get_table_metabase_consumption(session, base_table.id)
        links = session.execute(select(MetabaseObjectLink).order_by(MetabaseObjectLink.id.asc())).scalars().all()

    assert run.status == "success"
    assert run.links_count >= 1
    assert any(link.table_id == base_table.id for link in links)
    assert consumption.dashboards_count == 1
    assert consumption.questions_count == 1
    assert consumption.collections_count == 1
    assert consumption.match_state == "direct"


def test_metabase_sync_serializes_datetime_inside_dataset_query_json(monkeypatch):
    SessionLocal = _session_factory()
    monkeypatch.setattr(metabase_service, "MetabaseClient", DatetimePayloadMetabaseClient)

    with SessionLocal() as session:
        _seed_catalog(session)
        instance = _seed_instance(session)

        run = metabase_service.run_metabase_instance_sync(session, instance.id)
        question = session.scalar(
            select(MetabaseObject)
            .where(MetabaseObject.instance_id == instance.id, MetabaseObject.object_type == "question")
            .order_by(MetabaseObject.id.asc())
        )

    assert run.status == "success"
    assert question is not None
    assert question.dataset_query_json is not None
    assert question.dataset_query_json["native"]["run_at"] == "2026-04-14T10:30:00+00:00"


def test_metabase_sync_skipped_when_breaker_open_serializes_summary_json(monkeypatch):
    SessionLocal = _session_factory()

    class _BreakerOpenHealth(SimpleNamespace):
        pass

    health_row = _BreakerOpenHealth(
        status="unavailable",
        status_message="breaker open",
        breaker_state="open",
        breaker_open_until_at=datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc),
        last_success_at=None,
        last_failure_at=None,
        consecutive_failures=3,
        failure_count=3,
    )

    class _ForbiddenClient(FakeMetabaseClient):
        def __init__(self, config) -> None:  # pragma: no cover - sanity guard
            raise AssertionError("Metabase client should not be instantiated when breaker is open")

    monkeypatch.setattr(metabase_service, "MetabaseClient", _ForbiddenClient)
    monkeypatch.setattr(metabase_service, "get_integration_health", lambda session, name: health_row)
    monkeypatch.setattr(metabase_service, "is_breaker_open", lambda health_row, *, current_time=None: True)

    with SessionLocal() as session:
        _seed_catalog(session)
        instance = _seed_instance(session)

        run = metabase_service.run_metabase_instance_sync(session, instance.id)
        persisted = session.scalar(
            select(MetabaseSyncRun)
            .where(MetabaseSyncRun.instance_id == instance.id)
            .order_by(MetabaseSyncRun.id.desc())
        )

    assert run.status == "failed"
    assert persisted is not None
    assert persisted.summary_json is not None
    assert persisted.summary_json["breaker_open_until_at"] == "2026-04-14T12:00:00+00:00"


def test_metabase_sync_marks_authentication_failure_without_crashing(monkeypatch):
    SessionLocal = _session_factory()
    monkeypatch.setattr(metabase_service, "MetabaseClient", FailingAuthMetabaseClient)

    with SessionLocal() as session:
        _seed_catalog(session)
        instance = _seed_instance(session)

        result = metabase_service.run_metabase_instance_sync(session, instance.id)
        refreshed = session.get(MetabaseInstance, instance.id)

    assert result.status == "failed"
    assert result.error_message == "invalid credentials"
    assert refreshed is not None
    assert refreshed.last_sync_status == "failed"
    assert refreshed.last_sync_message == "invalid credentials"


def test_metabase_startup_sync_enqueues_job_when_configured(monkeypatch):
    from t2c_data import main as app_main

    fake_instance = SimpleNamespace(id=99, base_url="http://metabase.local")
    fake_session = SimpleNamespace()
    calls: list[dict[str, object]] = []

    monkeypatch.setattr(app_main, "settings", SimpleNamespace(metabase_startup_sync_mode="enqueue"))
    monkeypatch.setattr(
        app_main,
        "enqueue_metabase_instance_sync",
        lambda session, instance_id, current_user=None, reason="manual": calls.append(
            {
                "session": session,
                "instance_id": instance_id,
                "current_user": current_user,
                "reason": reason,
            }
        ),
    )

    app_main._enqueue_metabase_startup_sync(fake_session, fake_instance)

    assert calls == [
        {
            "session": fake_session,
            "instance_id": 99,
            "current_user": None,
            "reason": "startup",
        }
    ]


def test_metabase_startup_sync_ignores_running_job_conflict(monkeypatch):
    from t2c_data import main as app_main

    fake_instance = SimpleNamespace(id=99, base_url="http://metabase.local")
    fake_session = SimpleNamespace()
    monkeypatch.setattr(
        app_main,
        "settings",
        SimpleNamespace(metabase_startup_sync_mode="enqueue"),
    )
    monkeypatch.setattr(
        app_main,
        "enqueue_metabase_instance_sync",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Já existe uma execução em andamento para este job.")
        ),
    )

    app_main._enqueue_metabase_startup_sync(fake_session, fake_instance)


def test_metabase_startup_sync_skips_when_manual_mode(monkeypatch):
    from t2c_data import main as app_main

    fake_instance = SimpleNamespace(id=99, base_url="http://metabase.local")
    fake_session = SimpleNamespace()
    calls: list[dict[str, object]] = []

    monkeypatch.setattr(app_main, "settings", SimpleNamespace(metabase_startup_sync_mode="manual"))
    monkeypatch.setattr(
        app_main,
        "enqueue_metabase_instance_sync",
        lambda session, instance_id, current_user=None, reason="manual": calls.append(
            {
                "session": session,
                "instance_id": instance_id,
                "current_user": current_user,
                "reason": reason,
            }
        ),
    )

    app_main._enqueue_metabase_startup_sync(fake_session, fake_instance)

    assert calls == []


def test_metabase_summary_failing_does_not_block_catalog_consumption_route(monkeypatch):
    SessionLocal = _session_factory()
    monkeypatch.setattr(metabase_service, "MetabaseClient", FakeMetabaseClient)

    with SessionLocal() as session:
        _seed_catalog(session)
        instance = _seed_instance(session)
        run = metabase_service.run_metabase_instance_sync(session, instance.id)
        base_table = session.scalar(select(TableEntity).where(TableEntity.name == "orders"))

        app = FastAPI()
        app.include_router(api_v1_router, prefix="/api/v1")

        fake_user = SimpleNamespace(
            id=1,
            email="admin@local",
            name="Admin",
            full_name="Admin",
            is_active=True,
            roles=[SimpleNamespace(name="admin", permissions=[])],
        )

        def override_get_db():
            yield session

        app.dependency_overrides[get_db] = override_get_db
        app.dependency_overrides[get_current_user] = lambda: fake_user

        monkeypatch.setattr(integrations_service, "MetabaseClient", HealthFailingMetabaseClient)

        client = TestClient(app)
        summary_response = client.get("/api/v1/integrations/metabase/summary")
        consumption_response = client.get(f"/api/v1/catalog/tables/{base_table.id}/metabase-consumption")

    assert run.status == "success"
    assert summary_response.status_code == 200
    assert summary_response.json()["available"] is True
    assert consumption_response.status_code == 200
    assert consumption_response.json()["available"] is True


def test_metabase_client_authenticate_wraps_connect_error(monkeypatch):
    from t2c_data.features.metabase.client import MetabaseClient, MetabaseClientConfig

    client = MetabaseClient(
        MetabaseClientConfig(
            base_url="http://metabase.local",
            auth_type="session",
            auth_username="admin@metabase.local",
            auth_secret="secret",
            timeout_seconds=3,
        )
    )

    def _raise_connect_error(*args, **kwargs):
        request = httpx.Request("POST", "http://metabase.local/api/session")
        raise httpx.ConnectError("Network is unreachable", request=request)

    monkeypatch.setattr(client._client, "post", _raise_connect_error)

    try:
        with pytest.raises(MetabaseClientError) as exc_info:
            client.authenticate()
        assert "metabase.local" in str(exc_info.value)
        assert "Network is unreachable" in str(exc_info.value)
    finally:
        client.close()


def test_metabase_health_endpoint_reports_down_when_unreachable(monkeypatch):
    SessionLocal = _session_factory()
    monkeypatch.setattr(integrations_service, "MetabaseClient", HealthFailingMetabaseClient)

    with SessionLocal() as session:
        _seed_catalog(session)
        instance = _seed_instance(session)

        app = FastAPI()
        app.include_router(api_v1_router, prefix="/api/v1")

        fake_user = SimpleNamespace(
            id=1,
            email="admin@local",
            name="Admin",
            full_name="Admin",
            is_active=True,
            roles=[SimpleNamespace(name="admin", permissions=[])],
        )

        def override_get_db():
            yield session

        app.dependency_overrides[get_db] = override_get_db
        app.dependency_overrides[get_current_user] = lambda: fake_user

        client = TestClient(app)
        response = client.get("/api/v1/integrations/metabase/health")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "DOWN"
    assert payload["configured"] is True
    assert payload["enabled"] is True
    assert payload["instance_id"] == instance.id
    assert "Network is unreachable" in payload["message"]


def test_metabase_bootstrap_from_env_creates_real_instance(monkeypatch):
    monkeypatch.setenv("METABASE_ENABLED", "true")
    monkeypatch.setenv("METABASE_INSTANCE_NAME", "Real Metabase")
    monkeypatch.setenv("METABASE_BASE_URL", "http://metabase.local")
    monkeypatch.setenv("METABASE_AUTH_TYPE", "session")
    monkeypatch.setenv("METABASE_AUTH_USERNAME", "admin@metabase.local")
    monkeypatch.setenv("METABASE_AUTH_SECRET", "metabase-secret")
    monkeypatch.setenv("METABASE_TIMEOUT_SECONDS", "12")
    monkeypatch.setenv("METABASE_SYNC_DASHBOARDS", "true")
    monkeypatch.setenv("METABASE_SYNC_QUESTIONS", "true")
    monkeypatch.setenv("METABASE_SYNC_COLLECTIONS", "true")

    SessionLocal = _session_factory()

    with SessionLocal() as session:
        _seed_catalog(session)
        instance = ensure_metabase_instance_from_settings(session)
        instances = metabase_service.list_metabase_instances(session)
        consumption = metabase_service.get_table_metabase_consumption(session, 1)

    assert instance is not None
    assert instance.name == "Real Metabase"
    assert instance.base_url == "http://metabase.local"
    assert instance.enabled is True
    assert instance.timeout_seconds == 12
    assert instances[0].name == "Real Metabase"
    assert consumption.configured is True
    assert consumption.available is True
    assert consumption.instance_name == "Real Metabase"
    assert consumption.instance_base_url == "http://metabase.local"
    assert consumption.last_sync_at is None
    assert consumption.dashboards_count == 0
    assert consumption.questions_count == 0
    assert consumption.collections_count == 0


def test_metabase_bootstrap_snapshot_materializes_session_state(monkeypatch):
    monkeypatch.setenv("METABASE_ENABLED", "true")
    monkeypatch.setenv("METABASE_BASE_URL", "http://metabase.local")
    monkeypatch.setenv("METABASE_INSTANCE_NAME", "Real Metabase")
    monkeypatch.setenv("METABASE_AUTH_TYPE", "none")
    monkeypatch.setenv("METABASE_AUTH_USERNAME", "")
    monkeypatch.setenv("METABASE_AUTH_SECRET", "")
    SessionLocal = _session_factory()

    with SessionLocal() as session:
        _seed_catalog(session)
        instance = ensure_metabase_instance_from_settings(session)
        instance_id = instance.id if instance is not None else None
        snapshot = snapshot_metabase_instance(instance)

    assert snapshot == {
        "id": instance_id,
        "name": "Real Metabase",
        "base_url": "http://metabase.local",
        "enabled": True,
        "configured": True,
        "auth_type": "none",
        "credentials_state": "not_required",
        "auth_secret_configured": False,
        "auth_username_configured": False,
        "last_sync_status": None,
        "last_sync_message": None,
        "sync_state": "never_synced",
    }


def test_metabase_bootstrap_snapshot_marks_missing_credentials(monkeypatch):
    monkeypatch.setenv("METABASE_ENABLED", "true")
    monkeypatch.setenv("METABASE_BASE_URL", "http://metabase.local")
    monkeypatch.setenv("METABASE_INSTANCE_NAME", "Real Metabase")
    monkeypatch.setenv("METABASE_AUTH_TYPE", "session")
    monkeypatch.setenv("METABASE_AUTH_USERNAME", "")
    monkeypatch.setenv("METABASE_AUTH_SECRET", "")
    SessionLocal = _session_factory()

    with SessionLocal() as session:
        _seed_catalog(session)
        instance = ensure_metabase_instance_from_settings(session)
        snapshot = snapshot_metabase_instance(instance)

    assert snapshot is not None
    assert snapshot["credentials_state"] == "missing"
    assert snapshot["auth_secret_configured"] is False
    assert snapshot["auth_username_configured"] is False
    assert snapshot["sync_state"] == "never_synced"


def test_metabase_consumption_handles_missing_instance_and_table(monkeypatch):
    monkeypatch.setenv("METABASE_ENABLED", "false")
    monkeypatch.delenv("METABASE_BASE_URL", raising=False)
    monkeypatch.delenv("METABASE_AUTH_TYPE", raising=False)
    monkeypatch.delenv("METABASE_AUTH_USERNAME", raising=False)
    monkeypatch.delenv("METABASE_AUTH_SECRET", raising=False)
    SessionLocal = _session_factory()

    with SessionLocal() as session:
        missing_table = metabase_service.get_table_metabase_consumption(session, 999)
        _seed_catalog(session)
        disabled_instance = _seed_instance(session)
        disabled_instance.enabled = False
        session.commit()
        no_instance = metabase_service.get_table_metabase_consumption(session, 1)

    assert missing_table.available is False
    assert missing_table.message == "Tabela não encontrada."
    assert no_instance.available is False
    assert no_instance.enabled is False
    assert no_instance.message == "Nenhuma instância do Metabase está configurada."


def test_integrations_metabase_summary_reports_current_counts(monkeypatch):
    SessionLocal = _session_factory()
    monkeypatch.setattr(metabase_service, "MetabaseClient", FakeMetabaseClient)
    monkeypatch.setattr(integrations_service, "MetabaseClient", FakeMetabaseClient)

    with SessionLocal() as session:
        _seed_catalog(session)
        instance = _seed_instance(session)
        run = metabase_service.run_metabase_instance_sync(session, instance.id)

        app = FastAPI()
        app.include_router(api_v1_router, prefix="/api/v1")

        fake_user = SimpleNamespace(
            id=1,
            email="admin@local",
            name="Admin",
            full_name="Admin",
            is_active=True,
            roles=[SimpleNamespace(name="admin", permissions=[])],
        )

        def override_get_db():
            yield session

        app.dependency_overrides[get_db] = override_get_db
        app.dependency_overrides[get_current_user] = lambda: fake_user

        client = TestClient(app)
        response = client.get("/api/v1/integrations/metabase/summary")

    assert run.status == "success"
    assert response.status_code == 200
    payload = response.json()
    assert payload["configured"] is True
    assert payload["enabled"] is True
    assert payload["available"] is True
    assert payload["dashboards_count"] == 1
    assert payload["questions_count"] == 1
    assert payload["collections_count"] == 1
    assert payload["direct_links_count"] == 1
    assert payload["indirect_links_count"] == 0
    assert payload["total_links_count"] == 3
    assert payload["tables_with_consumption_count"] == 1
    assert payload["recent_sync_runs"][0]["status"] == "success"
    assert payload["recent_sync_runs"][0]["instance_name"] == "Local Metabase"
    assert payload["recent_sync_runs"][0]["duration_seconds"] is not None
    assert payload["link_coverage"]["object_type"] == "all"
    assert payload["recommendations"]
    assert payload["sync_health_notes"]
    assert payload["top_tables"][0]["table_fqn"] == "warehouse.analytics.sales.orders"


def _seed_metabase_phase2_fixture(session):
    datasource, database, sales_schema, table, analytics_schema, view_table, bronze_schema, base_table = _seed_catalog(session)
    instance = _seed_instance(session)
    now = metabase_service._now()

    health_row = IntegrationHealth(
        integration_name="metabase",
        status="degraded",
        status_message="Falhas consecutivas registradas na sync do Metabase.",
        reason_code="sync_failures",
        category="operation",
        base_url=instance.base_url,
        checked_at=now,
        last_success_at=now - timedelta(hours=1),
        last_failure_at=now - timedelta(minutes=5),
        consecutive_failures=45,
        failure_count=45,
        latency_ms=320,
        error_type=None,
        error_summary=None,
        details_json={},
        breaker_state="closed",
        breaker_open_until_at=None,
    )
    session.add(health_row)

    dashboard = MetabaseObject(
        instance_id=instance.id,
        external_id="201",
        object_type="dashboard",
        title="E-commerce Insights",
        collection_external_id="10",
        collection_name="Execução",
        url="http://metabase.local/dashboard/201",
        raw_json={"ordered_cards": [{"card_id": 101}]},
        last_seen_at=now,
    )
    question_linked = MetabaseObject(
        instance_id=instance.id,
        external_id="101",
        object_type="question",
        title="Orders by status",
        collection_external_id="10",
        collection_name="Execução",
        url="http://metabase.local/question/101",
        dataset_query_json={"native": {"query": "select * from sales.orders join sales.customers on 1=1"}},
        raw_json={"database_id": 1},
        last_seen_at=now,
    )
    question_unlinked = MetabaseObject(
        instance_id=instance.id,
        external_id="301",
        object_type="question",
        title="Inventory snapshot",
        collection_external_id="10",
        collection_name="Execução",
        url="http://metabase.local/question/301",
        dataset_query_json={"native": {"query": "select * from sales.inventory"}},
        raw_json={"database_id": 1},
        last_seen_at=now,
    )
    collection = MetabaseObject(
        instance_id=instance.id,
        external_id="10",
        object_type="collection",
        title="Execução",
        collection_external_id="10",
        collection_name="Execução",
        url="http://metabase.local/collection/10",
        raw_json={"id": 10},
        last_seen_at=now,
    )
    session.add_all([dashboard, question_linked, question_unlinked, collection])
    session.flush()

    session.add_all(
        [
            MetabaseObjectLink(
                instance_id=instance.id,
                metabase_object_id=dashboard.id,
                table_id=table.id,
                confidence_level="high",
                confidence_reason="matched via ordered cards",
                match_method="direct",
                source_table_name="orders",
                source_schema_name="sales",
                source_database_name="analytics",
                is_active=True,
            ),
            MetabaseObjectLink(
                instance_id=instance.id,
                metabase_object_id=question_linked.id,
                table_id=table.id,
                confidence_level="high",
                confidence_reason="matched SQL reference",
                match_method="direct",
                source_table_name="orders",
                source_schema_name="sales",
                source_database_name="analytics",
                is_active=True,
            ),
            MetabaseObjectLink(
                instance_id=instance.id,
                metabase_object_id=question_linked.id,
                table_id=base_table.id,
                confidence_level="medium",
                confidence_reason="indirect view lineage",
                match_method="indirect_view",
                source_table_name="vw_customer_primary_address",
                source_schema_name="analytics",
                source_database_name="analytics",
                is_active=True,
            ),
            MetabaseObjectLink(
                instance_id=instance.id,
                metabase_object_id=collection.id,
                table_id=table.id,
                confidence_level="medium",
                confidence_reason="collection membership",
                match_method="collection_membership",
                source_table_name="orders",
                source_schema_name="sales",
                source_database_name="analytics",
                is_active=True,
            ),
        ]
    )

    session.add_all(
        [
            MetabaseSyncRun(
                instance_id=instance.id,
                status="success",
                started_at=now - timedelta(minutes=20),
                finished_at=now - timedelta(minutes=19),
                dashboards_count=2,
                questions_count=2,
                collections_count=1,
                links_count=4,
                unresolved_count=1,
                warnings_count=1,
                error_message=None,
                summary_json={"status": "success"},
            ),
            MetabaseSyncRun(
                instance_id=instance.id,
                status="failed",
                started_at=now - timedelta(hours=2),
                finished_at=now - timedelta(hours=2, minutes=-3),
                dashboards_count=1,
                questions_count=0,
                collections_count=0,
                links_count=0,
                unresolved_count=2,
                warnings_count=0,
                error_message="HTTP 400: Bad Request",
                summary_json={"status": "failed"},
            ),
        ]
    )
    instance.last_sync_at = now - timedelta(minutes=19)
    instance.last_sync_status = "success"
    instance.last_sync_message = "Sync concluída com alertas."
    instance.last_sync_dashboards = 2
    instance.last_sync_questions = 2
    instance.last_sync_collections = 1
    instance.last_sync_links = 4
    instance.last_sync_unresolved = 1
    instance.last_sync_warnings = 1
    session.commit()
    return instance, table, base_table, dashboard, question_linked, question_unlinked, collection


def test_integrations_metabase_phase2_sync_runs_and_artifacts_endpoints(monkeypatch):
    SessionLocal = _session_factory()

    with SessionLocal() as session:
        _seed_metabase_phase2_fixture(session)

        app = FastAPI()
        app.include_router(api_v1_router, prefix="/api/v1")

        fake_user = SimpleNamespace(
            id=1,
            email="admin@local",
            name="Admin",
            full_name="Admin",
            is_active=True,
            roles=[SimpleNamespace(name="admin", permissions=[])],
        )

        def override_get_db():
            yield session

        app.dependency_overrides[get_db] = override_get_db
        app.dependency_overrides[get_current_user] = lambda: fake_user

        client = TestClient(app)
        sync_runs_response = client.get("/api/v1/integrations/metabase/sync-runs?page_size=1&only_failures=true")
        artifacts_response = client.get("/api/v1/integrations/metabase/artifacts?page_size=2")
        partial_response = client.get("/api/v1/integrations/metabase/artifacts?linked_status=partially_linked")

    assert sync_runs_response.status_code == 200
    sync_runs_payload = sync_runs_response.json()
    assert sync_runs_payload["total"] >= 1
    assert sync_runs_payload["total_pages"] >= 1
    assert sync_runs_payload["items"][0]["status"] == "failed"
    assert sync_runs_payload["items"][0]["error_type"] == "unclassified"
    assert sync_runs_payload["items"][0]["duration_seconds"] is not None

    assert artifacts_response.status_code == 200
    artifacts_payload = artifacts_response.json()
    assert artifacts_payload["total"] >= 4
    assert artifacts_payload["total_pages"] >= 2
    assert artifacts_payload["items"][0]["linked_status"] in {"linked", "partially_linked", "unlinked", "unknown"}
    assert artifacts_payload["items"][0]["linked_tables"] or artifacts_payload["items"][0]["unresolved_references"] is not None

    assert partial_response.status_code == 200
    partial_payload = partial_response.json()
    assert any(item["linked_status"] == "partially_linked" for item in partial_payload["items"])


def test_integrations_metabase_sync_now_blocks_running_job_and_forces_stale(monkeypatch):
    SessionLocal = _session_factory()
    monkeypatch.setattr(metabase_service, "MetabaseClient", FakeMetabaseClient)
    from t2c_data.features.platform import jobs as platform_jobs

    monkeypatch.setattr(platform_jobs, "_job_table_ready", lambda _session: True)

    with SessionLocal() as session:
        instance = _seed_metabase_phase2_fixture(session)[0]
        running_job = IntegrationSyncJob(
            job_key="metabase:sync:metabase_instance:1",
            source="metabase",
            job_type="sync",
            target_type="metabase_instance",
            target_id=instance.id,
            target_name=instance.name,
            trigger_mode="manual",
            status="running",
            started_at=datetime.now(timezone.utc) - timedelta(hours=26),
        )
        session.add(running_job)
        session.commit()

        app = FastAPI()
        app.include_router(api_v1_router, prefix="/api/v1")

        fake_user = SimpleNamespace(
            id=1,
            email="admin@local",
            name="Admin",
            full_name="Admin",
            is_active=True,
            roles=[SimpleNamespace(name="admin", permissions=[])],
        )

        def override_get_db():
            yield session

        app.dependency_overrides[get_db] = override_get_db
        app.dependency_overrides[get_current_user] = lambda: fake_user

        client = TestClient(app)
        blocked_response = client.post("/api/v1/integrations/metabase/sync-now", json={"instance_id": instance.id})
        force_response = client.post("/api/v1/integrations/metabase/sync-now", json={"instance_id": instance.id, "force": True})

        assert blocked_response.status_code == 409
        blocked_payload = blocked_response.json()
        assert blocked_payload["detail"]["force_eligible"] is True
        assert "Já existe uma sincronização em andamento" in blocked_payload["detail"]["message"]

        assert force_response.status_code == 200
        force_payload = force_response.json()
        assert force_payload["status"] == "queued"
        assert session.scalar(select(func.count(MetabaseSyncRun.id))) >= 1
        session.refresh(running_job)
        assert session.scalar(select(IntegrationSyncJob).where(IntegrationSyncJob.id == running_job.id)).status == "failed"


def test_integrations_airflow_summary_endpoint_returns_structured_placeholder(monkeypatch):
    SessionLocal = _session_factory()
    now = metabase_service._now()
    summary_row = {
        "total_dags": 2,
        "active_dags": 1,
        "paused_dags": 1,
        "success_runs_24h": 3,
        "failed_runs_24h": 1,
        "task_failures_24h": 2,
        "latest_execution_at": now,
        "latest_failure_at": now,
        "latest_log_at": now,
        "updated_at": now,
    }
    recent_run_row = {
        "dag_run_pk": 11,
        "dag_id": "mysql_pg__app_mysql__customer_addresses",
        "dag_display_name": "Customer addresses",
        "is_active": True,
        "is_paused": False,
        "run_id": "scheduled__2026-04-14T22:45:00+00:00",
        "state": "success",
        "start_date": now,
        "end_date": now,
        "duration_seconds": 42,
        "run_type": "scheduled",
        "execution_date": now,
        "logical_date": now,
        "queued_at": now,
        "external_trigger": False,
        "data_interval_start": now,
        "data_interval_end": now,
        "last_scheduling_decision": now,
        "updated_at": now,
    }
    pipeline_row = {
        "dag_id": "mysql_pg__app_mysql__customer_addresses",
        "dag_display_name": "Customer addresses",
        "description": "Ingestão MySQL -> Postgres da tabela customer_addresses.",
        "is_active": True,
        "is_paused": False,
        "owner": "data-eng",
        "schedule_interval": "\"0 3 * * *\"",
        "tags": ["airflow", "ingestao"],
        "latest_run_pk": 11,
        "latest_run_id": "scheduled__2026-04-14T22:45:00+00:00",
        "latest_execution_at": now,
        "latest_state": "failed",
        "latest_duration_seconds": 42,
        "recent_runs_count_24h": 4,
        "recent_failures_count_24h": 1,
        "updated_at": now,
    }
    failure_row = {
        "dag_id": "mysql_pg__app_mysql__customer_addresses",
        "dag_display_name": "Customer addresses",
        "task_id": "executar_pipeline_mysql_pg",
        "run_id": "scheduled__2026-04-14T22:45:00+00:00",
        "map_index": -1,
        "state": "failed",
        "try_number": 2,
        "start_date": now,
        "end_date": now,
        "duration_seconds": 18,
        "operator": "PythonOperator",
        "queue": "default",
        "hostname": "airflow-worker",
        "unixname": "airflow",
        "job_id": 1,
        "queued_dttm": now,
        "updated_at": now,
        "task_display_name": "Executar pipeline",
        "next_method": None,
        "next_kwargs": None,
        "external_executor_id": None,
        "failure_at": now,
        "task_fail_count": 2,
        "last_task_fail_at": now,
        "log_event": "failed",
        "log_dttm": now,
        "log_extra": "{\"error\": \"boom\"}",
        "log_try_number": 2,
        "troubleshooting_context": "PythonOperator | default | airflow-worker | airflow | failed | {\"error\": \"boom\"}",
    }

    monkeypatch.setattr(integrations_service, "_airflow_relation_exists", lambda *args, **kwargs: True)
    monkeypatch.setattr(
        integrations_service,
        "_airflow_fetch_one",
        lambda _session, relation: summary_row if relation == integrations_service.AIRFLOW_OPERATIONAL_VIEW else None,
    )
    monkeypatch.setattr(
        integrations_service,
        "_airflow_fetch_many",
        lambda _session, relation, *, limit, order_by: (
            [recent_run_row]
            if relation == integrations_service.AIRFLOW_DAG_RUNS_VIEW
            else [failure_row]
            if relation == integrations_service.AIRFLOW_FAILURES_VIEW
            else [pipeline_row]
            if relation == integrations_service.AIRFLOW_DAGS_VIEW
            else []
        ),
    )

    with SessionLocal() as session:
        app = FastAPI()
        app.include_router(api_v1_router, prefix="/api/v1")
        monkeypatch.setattr(integrations_service, "operational_session", lambda: _operational_session(session))
        monkeypatch.setattr(
            integrations_service,
            "_validate_airflow_operational_contract_cached",
            lambda _session: _ready_airflow_contract(),
        )

        fake_user = SimpleNamespace(
            id=1,
            email="admin@local",
            name="Admin",
            full_name="Admin",
            is_active=True,
            roles=[SimpleNamespace(name="admin", permissions=[])],
        )

        def override_get_db():
            yield session

        app.dependency_overrides[get_db] = override_get_db
        app.dependency_overrides[get_current_user] = lambda: fake_user

        client = TestClient(app)
        response = client.get("/api/v1/integrations/airflow/summary")

    assert response.status_code == 200
    payload = response.json()
    assert payload["total_dags"] == 2
    assert payload["active_dags"] == 1
    assert payload["paused_dags"] == 1
    assert payload["success_runs_24h"] == 3
    assert payload["failed_runs_24h"] == 1
    assert payload["task_failures_24h"] == 2
    assert payload["integration_status"] in {"active", "degraded", "inactive"}
    assert payload["recent_runs"][0]["dag_id"] == "mysql_pg__app_mysql__customer_addresses"
    assert payload["recent_failures"][0]["task_id"] == "executar_pipeline_mysql_pg"


def test_integrations_airflow_summary_marks_connected_empty_when_no_dags(monkeypatch):
    SessionLocal = _session_factory()
    summary_row = {
        "total_dags": 0,
        "active_dags": 0,
        "paused_dags": 0,
        "success_runs_24h": 0,
        "failed_runs_24h": 0,
        "task_failures_24h": 0,
        "latest_execution_at": None,
        "latest_failure_at": None,
        "latest_log_at": None,
        "updated_at": metabase_service._now(),
    }
    monkeypatch.setattr(integrations_service, "_airflow_relation_exists", lambda *args, **kwargs: True)
    monkeypatch.setattr(
        integrations_service,
        "_airflow_fetch_one",
        lambda _session, relation: summary_row if relation == integrations_service.AIRFLOW_OPERATIONAL_VIEW else None,
    )
    monkeypatch.setattr(
        integrations_service,
        "_airflow_fetch_many",
        lambda _session, relation, *, limit, order_by: [],
    )

    with SessionLocal() as session:
        app = FastAPI()
        app.include_router(api_v1_router, prefix="/api/v1")
        monkeypatch.setattr(integrations_service, "operational_session", lambda: _operational_session(session))
        monkeypatch.setattr(
            integrations_service,
            "_validate_airflow_operational_contract_cached",
            lambda _session: _ready_airflow_contract(),
        )

        fake_user = SimpleNamespace(
            id=1,
            email="admin@local",
            name="Admin",
            full_name="Admin",
            is_active=True,
            roles=[SimpleNamespace(name="admin", permissions=[])],
        )

        def override_get_db():
            yield session

        app.dependency_overrides[get_db] = override_get_db
        app.dependency_overrides[get_current_user] = lambda: fake_user

        client = TestClient(app)
        response = client.get("/api/v1/integrations/airflow/summary")

    assert response.status_code == 200
    payload = response.json()
    assert payload["available"] is True
    assert payload["integration_status"] == "empty"
    assert payload["operational_status"] == "connected_empty"
    assert payload["message"] == "Airflow conectado, sem DAGs cadastradas."
    assert payload["total_dags"] == 0
    assert payload["recent_runs"] == []
    assert payload["recent_failures"] == []


def test_integrations_airflow_summary_marks_connected_no_runs_when_dags_without_runs(monkeypatch):
    SessionLocal = _session_factory()
    summary_row = {
        "total_dags": 3,
        "active_dags": 2,
        "paused_dags": 1,
        "success_runs_24h": 0,
        "failed_runs_24h": 0,
        "task_failures_24h": 0,
        "latest_execution_at": None,
        "latest_failure_at": None,
        "latest_log_at": None,
        "updated_at": metabase_service._now(),
    }
    monkeypatch.setattr(integrations_service, "_airflow_relation_exists", lambda *args, **kwargs: True)
    monkeypatch.setattr(
        integrations_service,
        "_airflow_fetch_one",
        lambda _session, relation: summary_row if relation == integrations_service.AIRFLOW_OPERATIONAL_VIEW else None,
    )
    monkeypatch.setattr(
        integrations_service,
        "_airflow_fetch_many",
        lambda _session, relation, *, limit, order_by: [],
    )

    with SessionLocal() as session:
        app = FastAPI()
        app.include_router(api_v1_router, prefix="/api/v1")
        monkeypatch.setattr(integrations_service, "operational_session", lambda: _operational_session(session))
        monkeypatch.setattr(
            integrations_service,
            "_validate_airflow_operational_contract_cached",
            lambda _session: _ready_airflow_contract(),
        )

        fake_user = SimpleNamespace(
            id=1,
            email="admin@local",
            name="Admin",
            full_name="Admin",
            is_active=True,
            roles=[SimpleNamespace(name="admin", permissions=[])],
        )

        def override_get_db():
            yield session

        app.dependency_overrides[get_db] = override_get_db
        app.dependency_overrides[get_current_user] = lambda: fake_user

        client = TestClient(app)
        response = client.get("/api/v1/integrations/airflow/summary")

    assert response.status_code == 200
    payload = response.json()
    assert payload["available"] is True
    assert payload["integration_status"] == "empty"
    assert payload["operational_status"] == "connected_no_runs"
    assert payload["message"] == "Existem DAGs cadastradas, mas ainda sem execuções."
    assert payload["total_dags"] == 3
    assert payload["recent_runs"] == []
    assert payload["recent_failures"] == []


def test_integrations_airflow_summary_uses_operational_view_counts(monkeypatch):
    SessionLocal = _session_factory()
    summary_row = {
        "total_dags": 17,
        "active_dags": 15,
        "paused_dags": 2,
        "success_runs_24h": 24,
        "failed_runs_24h": 69,
        "task_failures_24h": 138,
        "last_execution_at": metabase_service._now(),
        "latest_failure_at": metabase_service._now(),
        "latest_log_at": metabase_service._now(),
        "updated_at": metabase_service._now(),
    }
    monkeypatch.setattr(integrations_service, "_airflow_relation_exists", lambda *args, **kwargs: True)
    monkeypatch.setattr(
        integrations_service,
        "_airflow_fetch_one",
        lambda _session, relation: summary_row if relation == integrations_service.AIRFLOW_OPERATIONAL_VIEW else None,
    )
    monkeypatch.setattr(
        integrations_service,
        "_airflow_fetch_many",
        lambda _session, relation, *, limit, order_by: [],
    )

    with SessionLocal() as session:
        app = FastAPI()
        app.include_router(api_v1_router, prefix="/api/v1")
        monkeypatch.setattr(integrations_service, "operational_session", lambda: _operational_session(session))
        monkeypatch.setattr(
            integrations_service,
            "_validate_airflow_operational_contract_cached",
            lambda _session: _ready_airflow_contract(),
        )

        fake_user = SimpleNamespace(
            id=1,
            email="admin@local",
            name="Admin",
            full_name="Admin",
            is_active=True,
            roles=[SimpleNamespace(name="admin", permissions=[])],
        )

        def override_get_db():
            yield session

        app.dependency_overrides[get_db] = override_get_db
        app.dependency_overrides[get_current_user] = lambda: fake_user

        client = TestClient(app)
        response = client.get("/api/v1/integrations/airflow/summary")

    assert response.status_code == 200
    payload = response.json()
    assert payload["available"] is True
    assert payload["operational_status"] == "connected_active"
    assert payload["total_dags"] == 17
    assert payload["active_dags"] == 15
    assert payload["paused_dags"] == 2
    assert payload["success_runs_24h"] == 24
    assert payload["failed_runs_24h"] == 69
    assert payload["task_failures_24h"] == 138


def test_integrations_airflow_pipelines_and_failures_endpoints_return_modelled_views(monkeypatch):
    SessionLocal = _session_factory()
    now = metabase_service._now()
    pipeline_row = {
        "dag_id": "mysql_pg__app_mysql__customer_addresses",
        "dag_display_name": "Customer addresses",
        "description": "Ingestão MySQL -> Postgres da tabela customer_addresses.",
        "is_active": True,
        "is_paused": False,
        "owner": "data-eng",
        "schedule_interval": "\"0 3 * * *\"",
        "tags": ["airflow", "ingestao"],
        "latest_run_pk": 11,
        "latest_run_id": "scheduled__2026-04-14T22:45:00+00:00",
        "last_execution_at": now,
        "latest_state": "failed",
        "latest_duration_seconds": 42,
        "recent_runs_count_24h": 4,
        "recent_failures_count_24h": 1,
        "updated_at": now,
    }
    failure_row = {
        "dag_id": "mysql_pg__app_mysql__customer_addresses",
        "dag_display_name": "Customer addresses",
        "task_id": "executar_pipeline_mysql_pg",
        "run_id": "scheduled__2026-04-14T22:45:00+00:00",
        "map_index": -1,
        "state": "failed",
        "try_number": 2,
        "start_date": now,
        "end_date": now,
        "duration_seconds": 18,
        "operator": "PythonOperator",
        "queue": "default",
        "hostname": "airflow-worker",
        "unixname": "airflow",
        "job_id": 1,
        "queued_dttm": now,
        "updated_at": now,
        "task_display_name": "Executar pipeline",
        "next_method": None,
        "next_kwargs": None,
        "external_executor_id": None,
        "failure_at": now,
        "task_fail_count": 2,
        "last_task_fail_at": now,
        "log_event": "failed",
        "log_dttm": now,
        "log_extra": "{\"error\": \"boom\"}",
        "log_try_number": 2,
        "troubleshooting_context": "PythonOperator | default | airflow-worker | airflow | failed | {\"error\": \"boom\"}",
    }
    monkeypatch.setattr(integrations_service, "_airflow_relation_exists", lambda *args, **kwargs: True)
    monkeypatch.setattr(
        integrations_service,
        "_airflow_fetch_many",
        lambda _session, relation, *, limit, order_by: (
            [pipeline_row]
            if relation == integrations_service.AIRFLOW_DAGS_VIEW
            else [failure_row]
            if relation == integrations_service.AIRFLOW_FAILURES_VIEW
            else []
        ),
    )

    with SessionLocal() as session:
        app = FastAPI()
        app.include_router(api_v1_router, prefix="/api/v1")
        monkeypatch.setattr(integrations_service, "operational_session", lambda: _operational_session(session))
        monkeypatch.setattr(
            integrations_service,
            "_validate_airflow_operational_contract_cached",
            lambda _session: _ready_airflow_contract(),
        )

        fake_user = SimpleNamespace(
            id=1,
            email="admin@local",
            name="Admin",
            full_name="Admin",
            is_active=True,
            roles=[SimpleNamespace(name="admin", permissions=[])],
        )

        def override_get_db():
            yield session

        app.dependency_overrides[get_db] = override_get_db
        app.dependency_overrides[get_current_user] = lambda: fake_user

        client = TestClient(app)
        pipelines_response = client.get("/api/v1/integrations/airflow/pipelines")
        failures_response = client.get("/api/v1/integrations/airflow/failures")

    assert pipelines_response.status_code == 200
    assert failures_response.status_code == 200
    pipelines_payload = pipelines_response.json()
    failures_payload = failures_response.json()
    assert pipelines_payload["items"][0]["dag_id"] == "mysql_pg__app_mysql__customer_addresses"
    assert pipelines_payload["items"][0]["recent_failures_count_24h"] == 1
    assert failures_payload["items"][0]["task_id"] == "executar_pipeline_mysql_pg"
    assert failures_payload["items"][0]["troubleshooting_context"].startswith("PythonOperator")
