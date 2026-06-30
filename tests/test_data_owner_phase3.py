from __future__ import annotations

import json
import os
import asyncio
from types import SimpleNamespace

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from starlette.requests import Request
from sqlalchemy import create_engine, event
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.api.data_owners import delete_data_owner
from t2c_data.api.governance import build_ownership_export_artifact
from t2c_data.features.governance import get_ownership_delete_impact
from t2c_data.main import app
from t2c_data.models import Base
from t2c_data.models.auth import User
from t2c_data.models.catalog import DataOwner, DataSource, Database, Schema, TableEntity


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


def _seed_owner_with_assets(db: Session) -> tuple[DataOwner, TableEntity, TableEntity]:
    owner = DataOwner(name="Nivaldo Melo", email="nivaldo@example.com", area="Dados")
    datasource = DataSource(
        name="local",
        db_type="postgres",
        host="localhost",
        port=5432,
        database="andromeda",
        username="user",
    )
    datasource.password = "secret"
    database = Database(name="andromeda", datasource=datasource)
    schema = Schema(name="bronze", database=database)
    owned_table = TableEntity(
        name="audit_logs",
        table_type="table",
        schema=schema,
        data_owner=owner,
        certification_status="not_eligible",
        certification_criticality="high",
        has_personal_data=True,
        access_scope="confidential",
    )
    unowned_table = TableEntity(
        name="events",
        table_type="table",
        schema=schema,
        certification_status="not_eligible",
    )
    db.add_all([owner, datasource, database, schema, owned_table, unowned_table])
    db.commit()
    db.refresh(owner)
    db.refresh(owned_table)
    db.refresh(unowned_table)
    return owner, owned_table, unowned_table


def test_delete_impact_and_force_delete_flow() -> None:
    db = _build_session()
    owner, owned_table, _ = _seed_owner_with_assets(db)

    impact_payload, _, _ = get_ownership_delete_impact(db, current_user=None, owner_id=owner.id)

    assert impact_payload.can_delete_without_force is False
    assert impact_payload.impact.asset_count == 1
    assert impact_payload.impact.certification_pending_assets == 1
    assert impact_payload.impact.personal_data_assets == 1
    assert impact_payload.impact.dq_unmonitored_assets == 1
    assert impact_payload.sample_assets[0].name == "audit_logs"

    current_user = SimpleNamespace(id=1, roles=[SimpleNamespace(name="admin")], email="admin@example.com")

    response = delete_data_owner(owner_id=owner.id, force=False, db=db, user=current_user)
    assert response.status_code == 409
    body = json.loads(response.body.decode("utf-8"))
    assert body["message"].startswith("Este owner possui ativos associados")
    assert body["impact"]["asset_count"] == 1
    assert db.get(DataOwner, owner.id) is not None

    response_force = delete_data_owner(owner_id=owner.id, force=True, db=db, user=current_user)
    assert response_force is None
    assert db.get(DataOwner, owner.id) is None
    refreshed_table = db.get(TableEntity, owned_table.id)
    assert refreshed_table is not None
    assert refreshed_table.data_owner_id is None
    assert refreshed_table.owner is None
    assert refreshed_table.owner_email is None


def test_delete_owner_without_assets_still_succeeds() -> None:
    db = _build_session()
    owner = DataOwner(name="Owner sem ativos", email="sem-ativos@example.com", area="Dados")
    db.add(owner)
    db.commit()
    db.refresh(owner)

    response = delete_data_owner(
        owner_id=owner.id,
        force=False,
        db=db,
        user=SimpleNamespace(id=1, roles=[SimpleNamespace(name="admin")], email="admin@example.com"),
    )

    assert response is None
    assert db.get(DataOwner, owner.id) is None


def test_export_csv_contains_owner_and_unowned_rows() -> None:
    db = _build_session()
    owner, _, unowned_table = _seed_owner_with_assets(db)
    artifact = build_ownership_export_artifact(
        db,
        current_user=SimpleNamespace(id=1, roles=[SimpleNamespace(name="admin")], email="admin@example.com"),
        query=None,
        status=None,
        area=None,
        owner_id=None,
        include_unowned=True,
        risk_level=None,
        schema_name=None,
        database_name=None,
    )
    content = artifact.payload.decode("utf-8")

    assert "row_type,owner_id,owner_name,owner_email" in content
    assert any(line.startswith("owner,") and "[masked]" in line for line in content.splitlines())
    assert "asset_name" in content
    assert "events" in content
