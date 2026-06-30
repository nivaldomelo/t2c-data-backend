from __future__ import annotations

import os

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy import create_engine, event
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.features.access_control.policy import can_view_table
from t2c_data.features.search.analytics import (
    delete_favorite_asset,
    get_critical_results,
    get_favorite_results,
    get_popular_results,
    get_recent_asset_results,
    is_favorite_asset,
    upsert_favorite_asset,
)
from t2c_data.features.search.global_search import search_global
from t2c_data.models import Base
from t2c_data.models.access_control import DataAccessGrant
from t2c_data.models.auth import Role, User
from t2c_data.models.catalog import DataSource, Database, Schema, TableEntity
from t2c_data.models.platform import PlatformUsageEvent
from t2c_data.models.search import SearchFavoriteAsset, SearchResultClick


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


def _seed_catalog(db: Session) -> tuple[User, TableEntity, TableEntity]:
    role = Role(name="editor", description="Editor")
    user = User(email="caio@email.com.br", password_hash="hash", name="Caio Wilson", full_name="Caio Wilson", is_active=True)
    user.roles.append(role)

    datasource = DataSource(name="local-andromeda", db_type="postgres", host="localhost", port=5432, database="andromeda", username="catalog")
    datasource.password = "secret"
    database = Database(name="andromeda", datasource=datasource)
    bronze = Schema(name="bronze", database=database)
    demo = Schema(name="demo", database=database)
    bronze_table = TableEntity(name="order_items", table_type="table", schema=bronze, certification_criticality="high")
    demo_table = TableEntity(name="order_items", table_type="table", schema=demo, certification_criticality="critical")

    db.add_all([role, user, datasource, database, bronze, demo, bronze_table, demo_table])
    db.flush()
    db.add(DataAccessGrant(user=user, effect="allow", schema=bronze))

    db.add_all(
        [
            SearchResultClick(entity_type="table", entity_id=bronze_table.id, user_id=user.id, query_text="order", normalized_query="order", target_url=f"/explorer?tableId={bronze_table.id}"),
            SearchResultClick(entity_type="table", entity_id=demo_table.id, user_id=user.id, query_text="order", normalized_query="order", target_url=f"/explorer?tableId={demo_table.id}"),
            SearchResultClick(entity_type="table", entity_id=demo_table.id, user_id=user.id, query_text="order", normalized_query="order", target_url=f"/explorer?tableId={demo_table.id}"),
            PlatformUsageEvent(user_id=user.id, event_name="page_view", module_name="explorer", page_path="/explorer", entity_type="table", entity_id=bronze_table.id, target_url=f"/explorer?tableId={bronze_table.id}"),
            PlatformUsageEvent(user_id=user.id, event_name="page_view", module_name="explorer", page_path="/explorer", entity_type="table", entity_id=demo_table.id, target_url=f"/explorer?tableId={demo_table.id}"),
        ]
    )
    db.commit()
    db.refresh(user)
    db.refresh(bronze_table)
    db.refresh(demo_table)
    return user, bronze_table, demo_table


def test_search_global_and_popular_respect_schema_scope() -> None:
    db = _build_session()
    user, bronze_table, demo_table = _seed_catalog(db)

    assert can_view_table(user, bronze_table)
    assert not can_view_table(user, demo_table)

    bronze_payload = search_global(db, "bronze", current_user=user)
    assert bronze_payload["total"] > 0
    assert all("demo" not in str(item.get("context_path") or "").lower() for item in bronze_payload["items"])
    assert all("demo" not in str(item.get("title") or "").lower() for item in bronze_payload["items"])
    assert all((item.get("metadata") or {}).get("schema") == "bronze" for item in bronze_payload["items"])

    demo_payload = search_global(db, "demo", current_user=user)
    assert demo_payload["total"] == 0
    assert demo_payload["items"] == []

    popular = get_popular_results(db, user=user, limit=5)
    assert popular["enabled"] is True
    assert popular["items"]
    assert all("demo" not in str(item.get("context_path") or "").lower() for item in popular["items"])
    assert all(item["entity_id"] != demo_table.id for item in popular["items"])

    critical = get_critical_results(db, user=user, limit=5)
    assert critical["enabled"] is True
    assert [item["entity_id"] for item in critical["items"]] == [bronze_table.id]

    recent_assets = get_recent_asset_results(db, user=user, limit=5)
    assert recent_assets["enabled"] is True
    assert [item["entity_id"] for item in recent_assets["items"]] == [bronze_table.id]


def test_favorite_assets_are_personal_and_respect_visibility() -> None:
    db = _build_session()
    user, bronze_table, demo_table = _seed_catalog(db)

    upsert_favorite_asset(
        db,
        user=user,
        entity_type="table",
        entity_id=bronze_table.id,
        label="bronze.order_items",
        target_url=f"/explorer?tableId={bronze_table.id}",
        category="Tabela",
        subtitle="andromeda · bronze · order_items",
        context_path="local-andromeda > andromeda > bronze > order_items",
        metadata={"source": "test"},
    )
    db.add(
        SearchFavoriteAsset(
            user_id=user.id,
            entity_type="table",
            entity_id=demo_table.id,
            label="demo.order_items",
            target_url=f"/explorer?tableId={demo_table.id}",
        )
    )
    db.commit()

    assert is_favorite_asset(db, user=user, entity_type="table", entity_id=bronze_table.id)

    payload = get_favorite_results(db, user=user, limit=10)
    assert payload["enabled"] is True
    assert [item["entity_id"] for item in payload["items"]] == [bronze_table.id]
    assert payload["items"][0]["target_url"] == f"/explorer?tableId={bronze_table.id}"

    delete_favorite_asset(db, user=user, entity_type="table", entity_id=bronze_table.id)
    db.commit()
    assert not is_favorite_asset(db, user=user, entity_type="table", entity_id=bronze_table.id)


if __name__ == "__main__":
    test_search_global_and_popular_respect_schema_scope()
    test_favorite_assets_are_personal_and_respect_visibility()
    print("search scope tests: OK")
