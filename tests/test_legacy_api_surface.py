from __future__ import annotations

import os

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from t2c_data.core.config import settings
from t2c_data.features.platform.legacy_api_surface import legacy_api_surface_summary
from t2c_data.models.governance import GovernanceSettings


def _session_factory():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True).execution_options(
        schema_translate_map={settings.db_schema: None}
    )
    with engine.begin() as conn:
        conn.exec_driver_sql("CREATE TABLE users (id INTEGER PRIMARY KEY)")
        GovernanceSettings.__table__.create(bind=conn)
        conn.exec_driver_sql(
            """
            CREATE TABLE access_log (
                id INTEGER PRIMARY KEY,
                created_at DATETIME NOT NULL,
                user_id INTEGER NULL,
                actor_name TEXT NULL,
                user_email TEXT NULL,
                ip TEXT NULL,
                user_agent TEXT NULL,
                route TEXT NOT NULL,
                method TEXT NULL,
                status_code INTEGER NULL,
                request_id TEXT NULL,
                api_version TEXT NOT NULL,
                module_name TEXT NULL,
                duration_ms INTEGER NULL,
                metadata_json TEXT NULL
            )
            """
        )
        conn.exec_driver_sql(
            """
            CREATE TABLE access_log_archive (
                id INTEGER PRIMARY KEY,
                created_at DATETIME NOT NULL,
                user_id INTEGER NULL,
                actor_name TEXT NULL,
                user_email TEXT NULL,
                ip TEXT NULL,
                user_agent TEXT NULL,
                route TEXT NOT NULL,
                method TEXT NULL,
                status_code INTEGER NULL,
                request_id TEXT NULL,
                api_version TEXT NOT NULL,
                module_name TEXT NULL,
                duration_ms INTEGER NULL,
                metadata_json TEXT NULL
            )
            """
        )
    return sessionmaker(bind=engine, future=True)


def test_legacy_api_surface_reports_removed_modules():
    SessionLocal = _session_factory()

    with SessionLocal() as session:
        session.add(
            GovernanceSettings(
                id=1,
                legacy_api_cutoff_window_days=30,
            )
        )
        session.execute(
            text(
                """
                INSERT INTO access_log (
                    id, created_at, route, api_version, module_name
                ) VALUES (
                    1, CURRENT_TIMESTAMP, '/api/datasources', 'legacy', 'datasources'
                )
                """
            )
        )
        session.commit()

        summary = legacy_api_surface_summary(session)

    items = {item["module"]: item for item in summary["items"]}
    assert summary["official_surface"] == "/api/v1"
    assert summary["recommendation"] == "Legado encerrado. Usar rota canônica /api/v1."
    assert items["datasources"]["hits_total"] == 1
    assert items["datasources"]["hits_in_window"] == 1
    assert items["datasources"]["sunset_status"] == "removed"
    assert items["datasources"]["latest_request"] is not None
    assert items["datasources"]["latest_request"]["module"] == "datasources"
    assert items["datasources"]["latest_request"]["canonical_path"] == "/api/v1/datasources"
    assert items["ready"]["canonical_prefixes"] == ["/api/v1/ready"]
    assert items["ready"]["sunset_status"] == "removed"
    assert items["ping"]["canonical_prefixes"] == ["/api/v1/ping"]
    assert items["ping"]["sunset_status"] == "removed"
    assert items["auth"]["canonical_prefixes"] == ["/api/v1/auth"]
    assert items["auth"]["sunset_status"] == "removed"
    assert items["home"]["sunset_status"] == "removed"


if __name__ == "__main__":
    test_legacy_api_surface_reports_removed_modules()
    print("legacy api surface tests: OK")
