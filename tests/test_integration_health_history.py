from __future__ import annotations

import os

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from datetime import datetime, timezone

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from t2c_data.features.integrations.health import IntegrationHealthSnapshot, upsert_integration_health
from t2c_data.models.integrations import IntegrationHealth, IntegrationHealthHistory
from t2c_data.core.config import settings


def _build_session() -> Session:
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        future=True,
    ).execution_options(schema_translate_map={settings.db_schema: None})
    with engine.begin() as conn:
        IntegrationHealth.__table__.create(bind=conn)
        IntegrationHealthHistory.__table__.create(bind=conn)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, class_=Session)
    return SessionLocal()


def test_upsert_integration_health_persists_reason_code_in_history() -> None:
    session = _build_session()
    snapshot = IntegrationHealthSnapshot(
        integration_name="airflow",
        status="degraded",
        status_message="Falha ao consultar a origem",
        category="connectivity",
        base_url="http://airflow.local",
        checked_at=datetime.now(timezone.utc),
        reason_code="source_unreachable",
        error_type="network_error",
        error_summary="Falha ao consultar a origem",
    )

    health = upsert_integration_health(session, snapshot)
    session.commit()

    history_row = session.scalar(select(IntegrationHealthHistory).where(IntegrationHealthHistory.integration_name == "airflow"))
    assert health.reason_code == "source_unreachable"
    assert history_row is not None
    assert history_row.reason_code == "source_unreachable"


if __name__ == "__main__":
    test_upsert_integration_health_persists_reason_code_in_history()
    print("integration health history tests: OK")
