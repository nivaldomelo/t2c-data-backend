from __future__ import annotations

import os
from types import SimpleNamespace
from unittest.mock import patch

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy.exc import SQLAlchemyError

from t2c_data.features.ingestion import service as ingestion_service


class _FakeSession(SimpleNamespace):
    def __init__(self) -> None:
        super().__init__()
        self.rolled_back = False

    def rollback(self) -> None:
        self.rolled_back = True


def test_ingestion_summary_rolls_back_on_sql_error() -> None:
    session = _FakeSession()
    with patch.object(ingestion_service, "_load_column_map", side_effect=SQLAlchemyError("boom")):
        payload = ingestion_service.load_table_ingestion_summary(
            session, schema_name="bronze", table_name="audit_logs"
        )

    assert session.rolled_back is True
    assert payload["linked"] is False
    assert payload["state"] == "unavailable"


if __name__ == "__main__":
    test_ingestion_summary_rolls_back_on_sql_error()
    print("ingestion transaction safety tests: OK")
