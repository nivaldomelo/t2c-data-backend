from __future__ import annotations

import os
from datetime import datetime, timezone

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy import create_engine, event, select
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.api.privacy_access import list_privacy_table_events, register_privacy_periodic_review
from t2c_data.models import Base
from t2c_data.models.auth import Role, User
from t2c_data.models.audit import AuditLog
from t2c_data.models.catalog import ColumnEntity, DataSource, Database, Schema, TableEntity
from t2c_data.schemas.privacy_access import PrivacyPeriodicReviewIn


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


def test_privacy_periodic_review_creates_event_without_policy_change() -> None:
    db = _build_session()
    role = Role(name="admin", description="Administrator")
    user = User(email="admin@andromeda.local", password_hash="hash", name="Admin", full_name="Admin User", is_active=True)
    user.roles.append(role)

    datasource = DataSource(name="local-andromeda", db_type="postgres", host="localhost", port=5432, database="andromeda", username="catalog")
    datasource.password = "secret"
    database = Database(name="andromeda", datasource=datasource)
    schema = Schema(name="bronze", database=database)
    table = TableEntity(
        name="customers",
        table_type="table",
        schema=schema,
        owner="Owner",
        owner_email="owner@andromeda.local",
        has_personal_data=True,
        legal_basis="contract",
        access_scope="confidential",
        privacy_purpose="Atendimento",
        retention_policy="365 dias",
    )
    column = ColumnEntity(
        table=table,
        name="cpf",
        data_type="text",
        ordinal_position=1,
        is_primary_key=False,
        is_nullable=True,
    )

    db.add_all([role, user, datasource, database, schema, table, column])
    db.commit()
    db.refresh(table)
    db.refresh(user)

    detail = register_privacy_periodic_review(
        table_id=table.id,
        payload=PrivacyPeriodicReviewIn(
            notes="Revisão periódica registrada.",
            next_review_at=datetime.now(timezone.utc),
            confirmed=True,
        ),
        db=db,
        current_user=user,
    )

    assert detail.id == table.id
    assert detail.privacy.privacy_reviewed_at is not None
    assert detail.privacy.privacy_reviewed_by_user_id == user.id

    events = list_privacy_table_events(
        table_id=table.id,
        review_type=None,
        date_from=None,
        date_to=None,
        reviewer_user_id=None,
        field=None,
        risk_level=None,
        page=1,
        page_size=25,
        db=db,
        current_user=user,
    )

    assert events.total == 1
    assert events.items[0].review_type == "periodic_review"
    assert events.items[0].review_source == "manual"
    assert events.items[0].reviewer_user_id == user.id
    assert events.items[0].next_review_at is not None
    assert events.items[0].changed_fields == []
    assert events.items[0].notes == "Revisão periódica registrada."

    audit_rows = db.scalars(
        select(AuditLog)
        .where(AuditLog.action == "table.privacy.periodic_review", AuditLog.entity_id == str(table.id))
        .order_by(AuditLog.id.asc())
    ).all()
    assert audit_rows
    assert any(row.field_name == "privacy_reviewed_at" for row in audit_rows)
    assert any((row.metadata_json or {}).get("review_type") == "periodic_review" for row in audit_rows)
    assert any((row.metadata_json or {}).get("next_review_at") is not None for row in audit_rows)


def test_privacy_periodic_review_requires_justification() -> None:
    db = _build_session()
    role = Role(name="admin", description="Administrator")
    user = User(email="admin@andromeda.local", password_hash="hash", name="Admin", full_name="Admin User", is_active=True)
    user.roles.append(role)

    datasource = DataSource(name="local-andromeda", db_type="postgres", host="localhost", port=5432, database="andromeda", username="catalog")
    datasource.password = "secret"
    database = Database(name="andromeda", datasource=datasource)
    schema = Schema(name="bronze", database=database)
    table = TableEntity(
        name="customers",
        table_type="table",
        schema=schema,
        owner="Owner",
        owner_email="owner@andromeda.local",
        has_personal_data=True,
        legal_basis="contract",
        access_scope="confidential",
        privacy_purpose="Atendimento",
    )
    db.add_all([role, user, datasource, database, schema, table])
    db.commit()
    db.refresh(table)
    db.refresh(user)

    try:
        register_privacy_periodic_review(
            table_id=table.id,
            payload=PrivacyPeriodicReviewIn(
                notes="   ",
                next_review_at=datetime.now(timezone.utc),
                confirmed=True,
            ),
            db=db,
            current_user=user,
        )
        raise AssertionError("Expected periodic review to require justification")
    except Exception as exc:
        from fastapi import HTTPException

        assert isinstance(exc, HTTPException)
        assert exc.status_code == 422
        assert "justificativa" in str(exc.detail).lower()


def test_privacy_periodic_review_requires_next_review_for_personal_data() -> None:
    db = _build_session()
    role = Role(name="admin", description="Administrator")
    user = User(email="admin@andromeda.local", password_hash="hash", name="Admin", full_name="Admin User", is_active=True)
    user.roles.append(role)

    datasource = DataSource(name="local-andromeda", db_type="postgres", host="localhost", port=5432, database="andromeda", username="catalog")
    datasource.password = "secret"
    database = Database(name="andromeda", datasource=datasource)
    schema = Schema(name="bronze", database=database)
    table = TableEntity(
        name="customers",
        table_type="table",
        schema=schema,
        owner="Owner",
        owner_email="owner@andromeda.local",
        has_personal_data=True,
        legal_basis="contract",
        access_scope="confidential",
        privacy_purpose="Atendimento",
    )
    db.add_all([role, user, datasource, database, schema, table])
    db.commit()
    db.refresh(table)
    db.refresh(user)

    try:
        register_privacy_periodic_review(
            table_id=table.id,
            payload=PrivacyPeriodicReviewIn(
                notes="Revisão periódica registrada.",
                confirmed=True,
            ),
            db=db,
            current_user=user,
        )
        raise AssertionError("Expected periodic review to require next_review_at")
    except Exception as exc:
        from fastapi import HTTPException

        assert isinstance(exc, HTTPException)
        assert exc.status_code == 422
        assert "próxima revisão" in str(exc.detail).lower()


def test_privacy_periodic_review_requires_extra_approval_for_sensitive_data() -> None:
    db = _build_session()
    role = Role(name="analyst", description="Analyst")
    user = User(email="analyst@andromeda.local", password_hash="hash", name="Analyst", full_name="Analyst User", is_active=True)
    user.roles.append(role)

    datasource = DataSource(name="local-andromeda", db_type="postgres", host="localhost", port=5432, database="andromeda", username="catalog")
    datasource.password = "secret"
    database = Database(name="andromeda", datasource=datasource)
    schema = Schema(name="bronze", database=database)
    table = TableEntity(
        name="patients",
        table_type="table",
        schema=schema,
        owner="Owner",
        owner_email="owner@andromeda.local",
        has_sensitive_personal_data=True,
        legal_basis="contract",
        access_scope="confidential",
        privacy_purpose="Atendimento",
    )
    db.add_all([role, user, datasource, database, schema, table])
    db.commit()
    db.refresh(table)
    db.refresh(user)

    try:
        register_privacy_periodic_review(
            table_id=table.id,
            payload=PrivacyPeriodicReviewIn(
                notes="Revisão periódica sensível.",
                next_review_at=datetime.now(timezone.utc),
                confirmed=True,
            ),
            db=db,
            current_user=user,
        )
        raise AssertionError("Expected sensitive review to require governance/admin approval")
    except Exception as exc:
        from fastapi import HTTPException

        assert isinstance(exc, HTTPException)
        assert exc.status_code == 403
        assert "governança" in str(exc.detail).lower()


if __name__ == "__main__":
    test_privacy_periodic_review_creates_event_without_policy_change()
    print("privacy periodic review tests: OK")
