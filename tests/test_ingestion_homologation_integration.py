from __future__ import annotations

import os
import unittest

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from t2c_data.features.ingestion.service import (
    EXECUTION_TABLE,
    LOG_TABLE,
    SUMMARY_VIEW,
    load_table_ingestion_detail,
)


HOMOLOG_URL = os.getenv("T2C_INGESTION_HOMOLOG_DATABASE_URL")


@unittest.skipUnless(HOMOLOG_URL, "T2C_INGESTION_HOMOLOG_DATABASE_URL não configurada")
class IngestionHomologationIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.engine = create_engine(HOMOLOG_URL)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.engine.dispose()

    def test_operational_objects_exist_and_have_columns(self) -> None:
        with self.engine.connect() as conn:
            for table_name in [SUMMARY_VIEW, EXECUTION_TABLE, LOG_TABLE]:
                rows = conn.execute(
                    text(
                        """
                        select column_name
                        from information_schema.columns
                        where table_schema = 'controle' and table_name = :table_name
                        order by ordinal_position
                        """
                    ),
                    {"table_name": table_name},
                ).all()
                self.assertTrue(rows, f"{table_name} não possui colunas visíveis no schema controle")

    def test_service_can_load_one_real_table_detail(self) -> None:
        with self.engine.connect() as conn:
            sample = conn.execute(
                text(
                    f"""
                    select *
                    from controle.{SUMMARY_VIEW}
                    limit 1
                    """
                )
            ).mappings().first()

        self.assertIsNotNone(sample, "A view operacional não retornou nenhuma linha em homologação")
        sample_dict = dict(sample)
        schema_name = str(
            sample_dict.get("target_schema")
            or sample_dict.get("schema_destino")
            or sample_dict.get("dest_schema")
            or sample_dict.get("target_schema_name")
        )
        table_name = str(
            sample_dict.get("target_table")
            or sample_dict.get("tabela_destino")
            or sample_dict.get("dest_table")
            or sample_dict.get("target_table_name")
        )
        self.assertTrue(schema_name)
        self.assertTrue(table_name)

        with Session(self.engine) as session:
            payload = load_table_ingestion_detail(session, schema_name=schema_name, table_name=table_name, page=1, page_size=5)

        self.assertIn("summary", payload)
        self.assertIn("executions", payload)
        self.assertTrue(payload["summary"]["linked"])


if __name__ == "__main__":
    unittest.main()
