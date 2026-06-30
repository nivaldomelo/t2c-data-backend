from __future__ import annotations

import os
from datetime import datetime, timezone

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")
os.environ.setdefault("METABASE_ENABLED", "false")

from sqlalchemy import create_engine, func, select
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.features.catalog.table_detail import build_table_detail_out
from t2c_data.features.metabase import service as metabase_service
from t2c_data.features.metabase.impact import get_table_metabase_impact
from t2c_data.models import Base
from t2c_data.models.catalog import DataSource, Database, Schema, TableEntity
from t2c_data.models.metabase import MetabaseInstance, MetabaseObject, MetabaseObjectLink, MetabaseSyncRun
from t2c_data.models.metabase_impact import MetabaseAsset, MetabaseFieldDependency, MetabaseImpactSnapshot, MetabaseTableDependency


if not hasattr(SQLiteTypeCompiler, "visit_INET"):
    SQLiteTypeCompiler.visit_INET = lambda self, type_, **kw: "TEXT"  # type: ignore[attr-defined]
if not hasattr(SQLiteTypeCompiler, "visit_JSONB"):
    SQLiteTypeCompiler.visit_JSONB = lambda self, type_, **kw: "JSON"  # type: ignore[attr-defined]


class ImpactMetabaseClient:
    def __init__(self, config) -> None:
        self.config = config

    def __enter__(self) -> "ImpactMetabaseClient":
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
                "updated_at": "2026-04-16T03:00:00Z",
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


def _build_session_factory():
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    # SQLite event hooks need to be registered on the engine instance.
    from sqlalchemy import event

    @event.listens_for(engine, "connect")
    def _on_connect(dbapi_connection, _connection_record) -> None:  # type: ignore[no-untyped-def]
        cursor = dbapi_connection.cursor()
        cursor.execute("ATTACH DATABASE ':memory:' AS t2c_data")
        cursor.execute("ATTACH DATABASE ':memory:' AS t2c_ops")
        cursor.execute("ATTACH DATABASE ':memory:' AS controle")
        cursor.close()

    Base.metadata.create_all(engine)
    with engine.begin() as conn:
        conn.exec_driver_sql(
            """
            CREATE TABLE controle.metabase_assets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                instance_id INTEGER NOT NULL,
                metabase_object_id INTEGER NOT NULL,
                metabase_id TEXT NOT NULL,
                asset_type TEXT NOT NULL,
                name TEXT NOT NULL,
                collection_name TEXT,
                collection_external_id TEXT,
                url TEXT,
                archived INTEGER NOT NULL DEFAULT 0,
                source_updated_at TIMESTAMP,
                last_synced_at TIMESTAMP,
                last_verified_at TIMESTAMP,
                metadata_json JSON,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(instance_id, metabase_object_id)
            )
            """
        )
        conn.exec_driver_sql(
            """
            CREATE TABLE controle.metabase_table_dependencies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                instance_id INTEGER NOT NULL,
                table_id INTEGER NOT NULL,
                metabase_asset_id INTEGER NOT NULL,
                dependency_type TEXT NOT NULL,
                confidence_level TEXT NOT NULL DEFAULT 'medium',
                break_risk_on_drop TEXT NOT NULL DEFAULT 'medium',
                break_risk_on_change TEXT NOT NULL DEFAULT 'medium',
                details_json JSON,
                last_verified_at TIMESTAMP,
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(instance_id, table_id, metabase_asset_id)
            )
            """
        )
        conn.exec_driver_sql(
            """
            CREATE TABLE controle.metabase_field_dependencies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                instance_id INTEGER NOT NULL,
                table_id INTEGER NOT NULL,
                column_id INTEGER,
                field_name TEXT NOT NULL,
                metabase_asset_id INTEGER NOT NULL,
                dependency_type TEXT NOT NULL,
                confidence_level TEXT NOT NULL DEFAULT 'medium',
                break_risk_on_drop TEXT NOT NULL DEFAULT 'medium',
                break_risk_on_change TEXT NOT NULL DEFAULT 'medium',
                details_json JSON,
                last_verified_at TIMESTAMP,
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(instance_id, table_id, column_id, field_name, metabase_asset_id)
            )
            """
        )
        conn.exec_driver_sql(
            """
            CREATE TABLE controle.metabase_impact_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                instance_id INTEGER NOT NULL,
                table_id INTEGER NOT NULL,
                dashboard_count INTEGER NOT NULL DEFAULT 0,
                question_count INTEGER NOT NULL DEFAULT 0,
                model_count INTEGER NOT NULL DEFAULT 0,
                asset_count INTEGER NOT NULL DEFAULT 0,
                break_risk_on_drop TEXT NOT NULL DEFAULT 'none',
                break_risk_on_change TEXT NOT NULL DEFAULT 'none',
                last_verified_at TIMESTAMP,
                summary_json JSON,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.exec_driver_sql(
            """
            CREATE TABLE controle.table_row_count_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                table_id INTEGER NOT NULL,
                datasource_id INTEGER,
                schema_id INTEGER,
                connection_name TEXT,
                database_name TEXT,
                schema_name TEXT,
                table_name TEXT,
                fqn TEXT,
                row_count BIGINT,
                measurement_type TEXT,
                measurement_source TEXT,
                status TEXT NOT NULL,
                measured_at TIMESTAMP,
                duration_ms INTEGER,
                error_message TEXT,
                collection_method TEXT,
                collection_status TEXT,
                snapshot_at TIMESTAMP,
                snapshot_date DATE,
                created_at TIMESTAMP
            )
            """
        )

    return sessionmaker(bind=engine, autoflush=False, autocommit=False, class_=Session)


def _seed_catalog(session: Session) -> tuple[TableEntity, TableEntity]:
    datasource = DataSource(
        name="warehouse",
        db_type="postgres",
        host="localhost",
        port=5432,
        database="analytics",
        username="tester",
    )
    datasource.password = "secret"
    database = Database(name="analytics", datasource=datasource)
    schema = Schema(name="sales", database=database)
    table = TableEntity(name="orders", table_type="table", schema=schema, certification_status="certified")
    unused_table = TableEntity(name="customers", table_type="table", schema=schema, certification_status="certified")
    session.add_all([datasource, database, schema, table, unused_table])
    session.commit()
    return table, unused_table


def _seed_instance(session: Session) -> MetabaseInstance:
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
    session.commit()
    return instance


def test_metabase_impact_sync_persists_dependencies_and_summary(monkeypatch) -> None:
    session_factory = _build_session_factory()
    monkeypatch.setattr(metabase_service, "MetabaseClient", ImpactMetabaseClient)

    with session_factory() as session:
        table, unused_table = _seed_catalog(session)
        instance = _seed_instance(session)

        run = metabase_service.run_metabase_instance_sync(session, instance.id)

        asset_count = session.scalar(select(func.count(MetabaseAsset.id)))
        dependency_count = session.scalar(select(func.count(MetabaseTableDependency.id)))
        snapshot_count = session.scalar(select(func.count(MetabaseImpactSnapshot.id)))
        field_dependency_count = session.scalar(select(func.count(MetabaseFieldDependency.id)))
        detail = get_table_metabase_impact(session, table.id)
        unused_detail = get_table_metabase_impact(session, unused_table.id)
        table_detail = build_table_detail_out(session, table)

    assert run.status == "success"
    assert asset_count == 3
    assert dependency_count == 2
    assert snapshot_count == 1
    assert field_dependency_count == 0
    assert detail.available is True
    assert detail.dashboard_count == 1
    assert detail.question_count == 1
    assert detail.asset_count == 2
    assert detail.break_risk_on_drop == "high"
    assert detail.break_risk_on_change == "high"
    assert len(detail.dependencies) == 2
    assert any(item.asset_type == "dashboard" for item in detail.dependencies)
    assert any(item.asset_type == "question" for item in detail.dependencies)
    assert unused_detail.available is True
    assert unused_detail.asset_count == 0
    assert unused_detail.message == "Sem dependências indexadas para esta tabela."
    assert table_detail.metabase_impact is not None
    assert table_detail.metabase_impact.asset_count == 2


def test_metabase_impact_returns_unavailable_for_missing_table() -> None:
    session_factory = _build_session_factory()

    with session_factory() as session:
        detail = get_table_metabase_impact(session, 999999)

    assert detail.available is False
    assert detail.message == "Tabela não encontrada."
