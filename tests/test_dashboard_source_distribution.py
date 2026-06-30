from __future__ import annotations

from types import SimpleNamespace

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from t2c_data.features.dashboard.source_distribution import build_source_distribution_summary
from t2c_data.models.catalog import DataSource


def _session_factory():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True).execution_options(
        schema_translate_map={"t2c_data": None}
    )
    DataSource.__table__.create(bind=engine)
    return sessionmaker(bind=engine, future=True, class_=Session)


def test_source_distribution_keeps_empty_datasource_visible() -> None:
    SessionLocal = _session_factory()
    with SessionLocal() as session:
        source_with_tables = DataSource(
            name="warehouse",
            db_type="postgres",
            host="db.local",
            port=5432,
            database="analytics",
            username="tester",
            is_active=True,
        )
        empty_source = DataSource(
            name="staging",
            db_type="postgres",
            host="db.local",
            port=5432,
            database="landing",
            username="tester",
            is_active=True,
        )
        session.add_all([source_with_tables, empty_source])
        session.commit()
        session.refresh(source_with_tables)
        session.refresh(empty_source)

        profiles = [
            SimpleNamespace(
                datasource_id=source_with_tables.id,
                schema_id=10,
                eligible_for_certification=True,
                certification_status="certified",
            ),
            SimpleNamespace(
                datasource_id=source_with_tables.id,
                schema_id=11,
                eligible_for_certification=True,
                certification_status="not_eligible",
            ),
        ]

        summary = build_source_distribution_summary(session, profiles)

        assert summary["total_sources"] == 2
        assert summary["total_schemas"] == 2
        assert summary["total_tables"] == 2
        assert summary["served_tables"] == 2
        assert summary["certified_tables"] == 1
        assert summary["pending_tables"] == 1
        assert len(summary["items"]) == 2

        empty_item = next(item for item in summary["items"] if item["datasource_name"] == "staging")
        assert empty_item["table_count"] == 0
        assert empty_item["schema_count"] == 0
        assert empty_item["served_tables"] == 0
        assert empty_item["certified_tables"] == 0
        assert empty_item["pending_tables"] == 0
        assert empty_item["status_key"] == "awaiting_inventory"
        assert empty_item["status_label"] == "Fonte monitorada, aguardando inventário"
