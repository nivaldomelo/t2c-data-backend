from __future__ import annotations

import os
from datetime import datetime, timezone

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy import create_engine, event, select
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.core.security import hash_password
from t2c_data.features.governance.change_management import (
    create_metadata_change_request,
    list_asset_slas,
    review_metadata_change_request,
    approve_metadata_change_request,
    apply_metadata_change_request,
    reject_metadata_change_request,
    upsert_asset_sla,
)
from t2c_data.models import Base
from t2c_data.models.auth import User
from t2c_data.models.catalog import ColumnEntity, DataOwner, DataSource, Database, Schema, TableEntity
from t2c_data.models.governance import AssetSla, MetadataChangeRequest


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


def _seed_asset(db: Session) -> tuple[User, DataOwner, TableEntity, ColumnEntity]:
    user = User(
        email="owner-reviewer@example.com",
        name="Owner Reviewer",
        full_name="Owner Reviewer",
        password_hash=hash_password("secret123"),
        is_active=True,
    )
    owner = DataOwner(name="Data Owner", email="owner@example.com", area="Governança", is_active=True)
    datasource = DataSource(
        name="source-a",
        db_type="postgres",
        host="localhost",
        port=5432,
        database="warehouse",
        username="catalog",
    )
    datasource.password = "secret"
    database = Database(name="warehouse", datasource=datasource)
    schema = Schema(name="bronze", database=database)
    table = TableEntity(name="customers", table_type="table", schema=schema)
    column = ColumnEntity(
        table=table,
        name="customer_id",
        data_type="integer",
        is_primary_key=True,
        is_nullable=False,
        ordinal_position=1,
    )
    db.add_all([user, owner, datasource, database, schema, table, column])
    db.commit()
    return user, owner, table, column


def test_upsert_asset_sla_and_list_asset_slas() -> None:
    db = _build_session()
    user, _owner, table, _column = _seed_asset(db)

    first = upsert_asset_sla(
        db,
        asset_type="table",
        asset_id=table.id,
        sla_kind="freshness",
        sla_hours=24,
        asset_status="active",
        source_kind="manual",
        source_ref="policy-1",
        context_json={"reason": "SLA inicial"},
        actor_user_id=user.id,
    )
    db.commit()

    second = upsert_asset_sla(
        db,
        asset_type="table",
        asset_id=table.id,
        sla_kind="freshness",
        sla_hours=48,
        asset_status="active",
        source_kind="manual",
        source_ref="policy-2",
        context_json={"reason": "SLA atualizado"},
        actor_user_id=user.id,
    )
    db.commit()

    payload = list_asset_slas(db, asset_type="table", asset_id=table.id)
    row = db.scalar(select(AssetSla))

    assert first["id"] == second["id"]
    assert payload["total"] == 1
    assert payload["items"][0]["sla_hours"] == 48
    assert payload["items"][0]["reviewed_by_user_id"] == user.id
    assert payload["asset_name"] == table.name
    assert row is not None
    assert row.source_ref == "policy-2"


def test_change_request_flow_for_table_owner_assignment() -> None:
    db = _build_session()
    user, owner, table, _column = _seed_asset(db)

    created = create_metadata_change_request(
        db,
        asset_type="table",
        asset_id=table.id,
        change_kind="owner_assignment",
        title="Definir owner da tabela",
        description="Fluxo de mudança para owner formal.",
        proposed_value_json={"data_owner_id": owner.id},
        current_value_json={"data_owner_id": None},
        actor_user_id=user.id,
    )
    db.commit()

    reviewed = review_metadata_change_request(db, request_ref=created["request_key"], comment="Revisado", actor_user_id=user.id)
    db.commit()
    approved = approve_metadata_change_request(db, request_ref=created["request_key"], comment="Aprovado", actor_user_id=user.id)
    db.commit()
    applied = apply_metadata_change_request(db, request_ref=created["request_key"], comment="Aplicado", actor_user_id=user.id)
    db.commit()

    request_row = db.scalar(select(MetadataChangeRequest).where(MetadataChangeRequest.request_key == created["request_key"]))
    refreshed_table = db.get(TableEntity, table.id)

    assert created["status"] == "draft"
    assert reviewed["status"] == "review"
    assert approved["status"] == "approved"
    assert applied["status"] == "applied"
    assert request_row is not None
    assert request_row.status == "applied"
    assert [event.event_type for event in request_row.events] == ["created", "reviewed", "approved", "applied"]
    assert refreshed_table is not None
    assert refreshed_table.data_owner_id == owner.id
    assert refreshed_table.owner == owner.name
    assert refreshed_table.owner_email == owner.email
    assert refreshed_table.owner_reviewed_by_user_id == user.id
    assert refreshed_table.owner_reviewed_at is not None


def test_change_request_flow_for_column_owner_assignment() -> None:
    db = _build_session()
    user, owner, _table, column = _seed_asset(db)

    created = create_metadata_change_request(
        db,
        asset_type="column",
        asset_id=column.id,
        change_kind="owner_assignment",
        title="Definir owner da coluna",
        description="Fluxo de mudança para owner de coluna.",
        proposed_value_json={"data_owner_id": owner.id},
        current_value_json={"data_owner_id": None},
        actor_user_id=user.id,
    )
    db.commit()
    review_metadata_change_request(db, request_ref=created["request_key"], comment="Revisado", actor_user_id=user.id)
    db.commit()
    approve_metadata_change_request(db, request_ref=created["request_key"], comment="Aprovado", actor_user_id=user.id)
    db.commit()
    applied = apply_metadata_change_request(db, request_ref=created["request_key"], comment="Aplicado", actor_user_id=user.id)
    db.commit()

    request_row = db.scalar(select(MetadataChangeRequest).where(MetadataChangeRequest.request_key == created["request_key"]))
    refreshed_column = db.get(ColumnEntity, column.id)

    assert applied["status"] == "applied"
    assert request_row is not None
    assert request_row.table_id == column.table_id
    assert request_row.column_id == column.id
    assert refreshed_column is not None
    assert refreshed_column.data_owner_id == owner.id
    assert refreshed_column.owner_reviewed_by_user_id == user.id
    assert refreshed_column.owner_reviewed_at is not None


def test_reject_change_request_updates_status_and_events() -> None:
    db = _build_session()
    user, _owner, table, _column = _seed_asset(db)

    created = create_metadata_change_request(
        db,
        asset_type="table",
        asset_id=table.id,
        change_kind="description_update",
        title="Revisar descrição",
        description="Solicitação para revisão manual.",
        proposed_value_json={"description_manual": "Nova descrição"},
        current_value_json={"description_manual": None},
        actor_user_id=user.id,
    )
    db.commit()

    rejected = reject_metadata_change_request(
        db,
        request_ref=created["request_key"],
        comment="Não aprovada nesta rodada",
        actor_user_id=user.id,
    )
    db.commit()

    request_row = db.scalar(select(MetadataChangeRequest).where(MetadataChangeRequest.request_key == created["request_key"]))

    assert rejected["status"] == "rejected"
    assert request_row is not None
    assert request_row.status == "rejected"
    assert [event.event_type for event in request_row.events] == ["created", "rejected"]
    assert request_row.apply_error is None


if __name__ == "__main__":
    test_upsert_asset_sla_and_list_asset_slas()
    test_change_request_flow_for_table_owner_assignment()
    test_change_request_flow_for_column_owner_assignment()
    test_reject_change_request_updates_status_and_events()
    print("governance change management tests: OK")
