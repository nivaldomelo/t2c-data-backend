from __future__ import annotations

import os

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy import create_engine, event, select
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.features.notifications import (
    create_user_inbox_notification,
    forward_user_inbox_notification,
    search_inbox_forward_recipients,
)
from t2c_data.models import Base
from t2c_data.models.auth import Role, User
from t2c_data.models.notifications import UserInboxNotification


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


def _seed_users(db: Session) -> tuple[User, User, User]:
    admin_role = Role(name="admin", description="Admin")
    sender = User(
        email="sender@andromeda.local",
        password_hash="hash",
        name="Sender",
        full_name="Sender User",
        is_active=True,
    )
    recipient = User(
        email="recipient@andromeda.local",
        password_hash="hash",
        name="Recipient",
        full_name="Recipient User",
        is_active=True,
    )
    inactive = User(
        email="inactive@andromeda.local",
        password_hash="hash",
        name="Inactive",
        full_name="Inactive User",
        is_active=False,
    )
    sender.roles.append(admin_role)
    db.add_all([admin_role, sender, recipient, inactive])
    db.commit()
    db.refresh(sender)
    db.refresh(recipient)
    db.refresh(inactive)
    return sender, recipient, inactive


def test_search_inbox_forward_recipients_filters_active_users_and_excludes_self() -> None:
    db = _build_session()
    sender, recipient, inactive = _seed_users(db)

    results = search_inbox_forward_recipients(db, q="recip", exclude_user_id=sender.id, limit=10)
    assert [item["id"] for item in results] == [recipient.id]
    assert results[0]["display_name"] == recipient.name
    assert results[0]["email"] == recipient.email

    results_by_email = search_inbox_forward_recipients(db, q="sender@", exclude_user_id=sender.id, limit=10)
    assert results_by_email == []

    results_with_inactive = search_inbox_forward_recipients(db, q="inactive", exclude_user_id=None, limit=10)
    assert results_with_inactive == []


def test_forward_notification_creates_recipient_inbox_item_and_dedupes() -> None:
    db = _build_session()
    sender, recipient, _inactive = _seed_users(db)

    original = create_user_inbox_notification(
        db,
        user_id=sender.id,
        dedupe_key="sender:1",
        category="data_quality",
        severity="high",
        source_module="dq",
        source_entity_type="dq_rule",
        source_entity_id="101",
        title="Violação de qualidade detectada",
        message="A regra percentual de nulos foi violada.",
        href="/data-quality/rules",
        context_json={"kind": "dq_rule_violation", "rule_id": 101},
        ignore_category_preferences=True,
    )
    db.commit()

    forwarded = forward_user_inbox_notification(
        db,
        user=sender,
        notification_id=original.id,
        recipient_user_id=recipient.id,
    )
    db.commit()
    db.refresh(forwarded)

    inbox_items = db.scalars(select(UserInboxNotification)).all()
    assert len(inbox_items) == 3
    recipient_item = next(item for item in inbox_items if item.user_id == recipient.id)
    assert recipient_item.forwarded_from_notification_id == original.id
    assert recipient_item.forwarded_by_user_id == sender.id
    assert recipient_item.forwarded_at is not None
    assert recipient_item.context_json is not None
    assert recipient_item.context_json["forwarded"]["from_notification_id"] == original.id
    assert recipient_item.context_json["forwarded"]["to_user_id"] == recipient.id
    assert recipient_item.href == original.href
    admin_forwarded_items = [item for item in inbox_items if item.user_id == sender.id and item.dedupe_key.startswith("forward:")]
    assert len(admin_forwarded_items) == 1

    forwarded_again = forward_user_inbox_notification(
        db,
        user=sender,
        notification_id=original.id,
        recipient_user_id=recipient.id,
    )
    db.commit()
    assert forwarded_again.id == recipient_item.id
    inbox_items_after = db.scalars(select(UserInboxNotification)).all()
    assert len(inbox_items_after) == 3


if __name__ == "__main__":
    test_search_inbox_forward_recipients_filters_active_users_and_excludes_self()
    test_forward_notification_creates_recipient_inbox_item_and_dedupes()
    print("inbox forwarding tests: OK")
