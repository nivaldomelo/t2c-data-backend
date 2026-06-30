from __future__ import annotations

import os
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy import create_engine, event
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.api.privacy_access import list_privacy_tables
from t2c_data.features.data_quality.rule_management import list_rules_with_filters_page
from t2c_data.features.platform.cockpit_ops import build_platform_cockpit_queue_page
from t2c_data.models import Base
from t2c_data.models.catalog import DataSource, Database, Schema, TableEntity
from t2c_data.schemas.dq_rules import DQRuleOut


def test_platform_cockpit_queue_page_caps_large_page_size() -> None:
    items = [
        {
            "id": f"item-{index}",
            "severity": "warning",
            "status": "degraded",
            "title": f"Item {index}",
            "updated_at": datetime.now(timezone.utc),
        }
        for index in range(150)
    ]

    with patch("t2c_data.features.platform.cockpit_ops.build_platform_cockpit_queue_items", return_value=items):
        payload = build_platform_cockpit_queue_page(object(), current_user=SimpleNamespace(id=1), page=1, page_size=999)

    assert payload["page_size"] == 100
    assert payload["total"] == 150
    assert len(payload["items"]) == 100


def test_dq_rules_page_caps_large_page_size() -> None:
    matches = [
        (SimpleNamespace(id=index, name=f"rule-{index}"), None, None, None)
        for index in range(150)
    ]

    with patch("t2c_data.features.data_quality.rule_management._collect_rule_page_rules", return_value=([item[0] for item in matches[:100]], 150, 1, 100)), patch(
        "t2c_data.features.data_quality.rule_management.latest_snapshot_support_ready",
        return_value=False,
    ), patch(
        "t2c_data.features.data_quality.rule_management.open_incidents_for_rule_ids",
        return_value={},
    ), patch(
        "t2c_data.features.data_quality.rule_management.load_rule_audit_payloads",
        return_value={},
    ), patch(
        "t2c_data.features.data_quality.rule_management.load_rule_notification_recipients",
        return_value={},
    ), patch(
        "t2c_data.features.data_quality.rule_management.map_rule_out",
        side_effect=lambda *args, **kwargs: DQRuleOut(
            id=args[1].id,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            table_fqn="scale-benchmark.bronze.orders",
            name=args[1].name,
            rule_type="column_validation",
            severity="medium",
            is_active=True,
        ),
    ):
        payload = list_rules_with_filters_page(
            db=object(),
            rule_id=None,
            q=None,
            table_id=None,
            table_fqn=None,
            is_active=True,
            severity=None,
            last_status=None,
            page=1,
            page_size=999,
            current_user=SimpleNamespace(id=1),
        )

    assert payload.page_size == 100
    assert payload.total == 150
    assert len(payload.items) == 100


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


if not hasattr(SQLiteTypeCompiler, "visit_INET"):
    SQLiteTypeCompiler.visit_INET = lambda self, type_, **kw: "TEXT"  # type: ignore[attr-defined]
if not hasattr(SQLiteTypeCompiler, "visit_JSONB"):
    SQLiteTypeCompiler.visit_JSONB = lambda self, type_, **kw: "JSON"  # type: ignore[attr-defined]


def test_privacy_tables_list_caps_large_page_size() -> None:
    db = _build_session()
    current_user = SimpleNamespace(id=1, roles=[SimpleNamespace(name="admin")], email="admin@example.com")

    datasource = DataSource(name="warehouse", db_type="postgres", host="localhost", port=5432, database="andromeda", username="catalog")
    datasource.password = "secret"
    database = Database(name="andromeda", datasource=datasource)
    schema = Schema(name="bronze", database=database)
    tables = [
        TableEntity(
            name=f"table_{index}",
            table_type="table",
            schema=schema,
            sensitivity_level="internal" if index % 2 == 0 else "confidential",
            has_personal_data=bool(index % 3 == 0),
            access_scope="confidential",
        )
        for index in range(150)
    ]
    db.add_all([datasource, database, schema, *tables])
    db.commit()

    page = list_privacy_tables(
        q=None,
        sensitivity_level=None,
        has_personal_data=None,
        access_scope=None,
        page=1,
        page_size=999,
        db=db,
        current_user=current_user,
    )

    assert page.page_size == 200
    assert page.total == 150
    assert len(page.items) == 150
