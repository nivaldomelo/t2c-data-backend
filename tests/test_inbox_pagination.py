from __future__ import annotations

import os

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy import create_engine, event
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.features.notifications import create_user_inbox_notification, get_user_inbox
from t2c_data.models import Base
from t2c_data.models.auth import User


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


def test_get_user_inbox_paginates_without_loading_all_rows() -> None:
    db = _build_session()
    user = User(
        email="user@andromeda.local",
        password_hash="hash",
        name="User",
        full_name="User Test",
        is_active=True,
    )
    db.add(user)
    db.commit()
    db.refresh(user)

    for index in range(25):
        create_user_inbox_notification(
            db,
            user_id=user.id,
            dedupe_key=f"event:{index}",
            category="operations",
            severity="medium",
            source_module="ops",
            source_entity_type="job",
            source_entity_id=str(index),
            title=f"Evento {index}",
            message=f"Mensagem {index}",
            ignore_category_preferences=True,
        )
    db.commit()

    first_page = get_user_inbox(db, user=user, page=1, limit=10)
    third_page = get_user_inbox(db, user=user, page=3, limit=10)

    assert first_page["total"] == 25
    assert first_page["page"] == 1
    assert first_page["page_size"] == 10
    assert first_page["has_more"] is True
    assert len(first_page["items"]) == 10
    assert third_page["page"] == 3
    assert third_page["has_more"] is False
    assert len(third_page["items"]) == 5
