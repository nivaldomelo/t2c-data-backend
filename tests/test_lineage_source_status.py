from __future__ import annotations

import os
import unittest

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from t2c_data.core.config import settings
from t2c_data.models.lineage import LineageSourceConfig
from t2c_data.features.lineage.source_configs import list_source_statuses


class LineageSourceStatusTests(unittest.TestCase):
    def test_list_source_statuses_normalizes_legacy_source_name(self) -> None:
        engine = create_engine("sqlite+pysqlite:///:memory:", future=True).execution_options(
            schema_translate_map={settings.db_schema: None}
        )
        with engine.begin() as conn:
            LineageSourceConfig.__table__.create(bind=conn)

        SessionLocal = sessionmaker(bind=engine, future=True)
        with SessionLocal() as session:
            session.add(
                LineageSourceConfig(
                    name="Legacy lineage source",
                    source_type="openlineage",
                    base_url="internal://openlineage",
                    default_namespace="local-andromeda",
                    auth_type="none",
                    enabled=True,
                )
            )
            session.commit()

            statuses = list_source_statuses(session)

        self.assertEqual(len(statuses), 1)
        self.assertEqual(statuses[0].name, "Linhagem automática interna")
        self.assertEqual(statuses[0].source_type, "internal_openlineage")
        self.assertEqual(statuses[0].events_processed, 0)


if __name__ == "__main__":
    unittest.main()
