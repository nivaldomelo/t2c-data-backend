from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy import create_engine, event, select
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.models import Base
from t2c_data.models.catalog import DataOwner, DataSource, Database, Schema, TableEntity
from t2c_data.models.governance import GovernanceNotification, GovernanceSettings
from t2c_data.features.governance.notifications import get_governance_notification_summary, refresh_governance_notifications


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


def test_governance_notification_summary_counts_active_due_and_critical() -> None:
    db = _build_session()
    datasource = DataSource(name="local-andromeda", db_type="postgres", host="localhost", port=5432, database="andromeda", username="catalog")
    datasource.password = "secret"
    database = Database(name="andromeda", datasource=datasource)
    schema = Schema(name="bronze", database=database)
    owner = DataOwner(name="Nivaldo Melo", email="owner@andromeda.local", is_active=True)
    table = TableEntity(name="audit_logs", table_type="table", schema=schema, data_owner=owner)
    db.add_all(
        [
            GovernanceSettings(
                id=1,
                governance_notifications_enabled=True,
                governance_notification_repeat_days=7,
                governance_notification_critical_repeat_hours=24,
            ),
            datasource,
            database,
            schema,
            owner,
            table,
        ]
    )
    db.commit()

    now = datetime.now(timezone.utc)
    db.add_all(
        [
            GovernanceNotification(
                dedupe_key="owner_review_due:1",
                rule_key="owner_review_due",
                channel="in_app",
                status="active",
                severity="high",
                origin="governance",
                title="Revisão de owner vencida",
                message="audit_logs precisa de revisão de owner",
                entity_type="table",
                table_id=table.id,
                data_owner_id=owner.id,
                first_detected_at=now - timedelta(days=4),
                last_detected_at=now - timedelta(hours=2),
                last_sent_at=now - timedelta(days=8),
                next_send_at=now - timedelta(hours=1),
                send_count=2,
                last_delivery_status="active",
            ),
            GovernanceNotification(
                dedupe_key="operational_governance_risk:1",
                rule_key="operational_governance_risk",
                channel="in_app",
                status="active",
                severity="critical",
                origin="operations",
                title="Falha operacional com impacto de governança",
                message="audit_logs com falha operacional recorrente",
                entity_type="table",
                table_id=table.id,
                data_owner_id=owner.id,
                first_detected_at=now - timedelta(days=1),
                last_detected_at=now - timedelta(minutes=15),
                last_sent_at=now - timedelta(hours=2),
                next_send_at=now + timedelta(hours=10),
                send_count=3,
                last_delivery_status="active",
            ),
            GovernanceNotification(
                dedupe_key="no_description:1",
                rule_key="no_description",
                channel="in_app",
                status="resolved",
                severity="medium",
                origin="metadata",
                title="Sem descrição",
                message="audit_logs sem descrição",
                entity_type="table",
                table_id=table.id,
                data_owner_id=owner.id,
                first_detected_at=now - timedelta(days=10),
                last_detected_at=now - timedelta(days=2),
                last_sent_at=now - timedelta(days=3),
                next_send_at=now - timedelta(days=1),
                resolved_at=now - timedelta(hours=4),
                send_count=1,
                last_delivery_status="resolved",
            ),
        ]
    )
    db.commit()

    summary = get_governance_notification_summary(db)

    assert summary["enabled"] is True
    assert summary["active_total"] == 2
    assert summary["due_now_total"] == 1
    assert summary["critical_total"] == 1
    assert summary["review_total"] == 1
    assert summary["operational_total"] == 1
    assert summary["top_items"][0]["severity"] == "critical"


def test_refresh_governance_notifications_creates_inactive_owner_alert() -> None:
    db = _build_session()
    datasource = DataSource(name="local-andromeda", db_type="postgres", host="localhost", port=5432, database="andromeda", username="catalog")
    datasource.password = "secret"
    database = Database(name="andromeda", datasource=datasource)
    schema = Schema(name="bronze", database=database)
    owner = DataOwner(name="Nivaldo Melo", email="owner@andromeda.local", is_active=False)
    table = TableEntity(
        name="audit_logs",
        table_type="table",
        schema=schema,
        data_owner=owner,
        owner="Nivaldo Melo",
        owner_email="owner@andromeda.local",
        description_manual="Tabela de auditoria.",
        certification_status="eligible",
        certification_criticality="medium",
    )
    db.add_all([GovernanceSettings(id=1, governance_notifications_enabled=True), datasource, database, schema, owner, table])
    db.commit()

    result = refresh_governance_notifications(db)

    assert result["candidates"] >= 1
    assert any(
        notification.rule_key == "inactive_owner_with_assets"
        for notification in db.scalars(select(GovernanceNotification)).all()
    )


if __name__ == "__main__":
    test_governance_notification_summary_counts_active_due_and_critical()
    print("governance notifications tests: OK")
