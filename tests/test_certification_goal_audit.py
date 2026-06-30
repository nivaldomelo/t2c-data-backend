from __future__ import annotations

import os

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from datetime import date

from sqlalchemy import create_engine, event
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.api import certification as certification_api
from t2c_data.core.security import hash_password
from t2c_data.models import Base
from t2c_data.models.auth import User
from t2c_data.schemas.catalog import CertificationGoalCreate, CertificationGoalUpdate


if not hasattr(SQLiteTypeCompiler, "visit_INET"):
    SQLiteTypeCompiler.visit_INET = lambda self, type_, **kw: "TEXT"  # type: ignore[attr-defined]
if not hasattr(SQLiteTypeCompiler, "visit_JSONB"):
    SQLiteTypeCompiler.visit_JSONB = lambda self, type_, **kw: "JSON"  # type: ignore[attr-defined]


def _build_session_factory():
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
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, class_=Session)


def test_certification_goal_mutations_write_audit_entries(monkeypatch) -> None:
    session_factory = _build_session_factory()
    audit_calls: list[dict[str, object]] = []

    def fake_write_audit_log_sync(_session, **kwargs):  # noqa: ANN001
        audit_calls.append(kwargs)

    monkeypatch.setattr(certification_api, "write_audit_log_sync", fake_write_audit_log_sync)

    with session_factory() as db:
        user = User(
            email="admin@andromeda.com",
            name="Admin",
            full_name="Admin User",
            password_hash=hash_password("admin123"),
            is_active=True,
        )
        db.add(user)
        db.commit()

        created = certification_api.create_certification_goal(
            CertificationGoalCreate(
                name="Meta anual",
                period_start=date(2026, 1, 1),
                period_end=date(2026, 12, 31),
                target_certified_assets=20,
                target_eligible_assets=30,
                target_reviewed_assets=15,
                target_revalidated_assets=5,
                scope_type="global",
                scope_value=None,
                owner="Governança",
                status="active",
                notes="Meta operacional",
            ),
            db=db,
            current_user=user,
        )

        updated = certification_api.patch_certification_goal(
            created.id,
            CertificationGoalUpdate(name="Meta anual revisada", notes="Atualizada"),
            db=db,
            current_user=user,
        )

        certification_api.delete_certification_goal(created.id, db=db, current_user=user)

    assert [call["action"] for call in audit_calls] == [
        "certification.goal.create",
        "certification.goal.update",
        "certification.goal.delete",
    ]
    assert audit_calls[0]["entity_type"] == "certification_goal"
    assert audit_calls[1]["before"]["name"] == "Meta anual"
    assert audit_calls[1]["after"]["name"] == "Meta anual revisada"
    assert audit_calls[2]["before"]["name"] == "Meta anual revisada"
    assert updated.name == "Meta anual revisada"


if __name__ == "__main__":
    test_certification_goal_mutations_write_audit_entries()
    print("certification goal audit tests: OK")
