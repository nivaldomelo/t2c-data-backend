from __future__ import annotations

import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from sqlalchemy.exc import SQLAlchemyError

from t2c_data.core.sql_utils import safe_identifier, safe_relation
from t2c_data.features.ingestion.service import (
    IngestionIntegrationUnavailable,
    ColumnMap,
    _load_column_map,
    _load_type_label,
    _or_text_equals,
    _pipeline_history_href,
    _serialize_pipeline_row,
    _status_label,
    load_table_ingestion_summary,
    load_table_ingestion_summary_from_source,
    load_table_ingestion_detail_from_source,
    load_table_ingestion_detail,
)
from t2c_data.features.ingestion.service import load_ingestion_operational_overview_from_source
from t2c_data.features.ingestion.runtime import operational_session


class IngestionServiceTests(unittest.TestCase):
    def test_operational_views_script_uses_derived_timestamp_for_summary(self) -> None:
        script_path = Path(__file__).resolve().parents[1] / "scripts/sql/create_controle_ingestion_operational_views.sql"
        script = script_path.read_text(encoding="utf-8")

        self.assertNotIn("p.updated_at", script)
        self.assertIn("coalesce(p.ultima_execucao_fim, p.ultima_execucao_inicio, p.created_at) as updated_at", script)

    def test_status_label_maps_operational_states(self) -> None:
        self.assertEqual(_status_label("success"), "Sucesso")
        self.assertEqual(_status_label("failed"), "Falha")
        self.assertEqual(_status_label("running"), "Em execução")
        self.assertEqual(_status_label("queued"), "Pendente")
        self.assertEqual(_status_label(None), "Sem execução")

    def test_load_type_label_keeps_business_readability(self) -> None:
        self.assertEqual(_load_type_label("full_refresh"), "Full refresh")
        self.assertEqual(_load_type_label("incremental_timestamp"), "Incremental por timestamp")
        self.assertEqual(_load_type_label("incremental_ts"), "Incremental por timestamp")
        self.assertEqual(_load_type_label("window_merge"), "Window merge")
        self.assertEqual(_load_type_label("append_only"), "Append only")

    def test_or_text_equals_builds_safe_clause_and_params(self) -> None:
        clause, params = _or_text_equals("pipeline_id", ["abc", "abc", "def"], "pipeline")

        self.assertEqual(clause, "(cast(pipeline_id as text) = :pipeline_0 OR cast(pipeline_id as text) = :pipeline_1)")
        self.assertEqual(params, {"pipeline_0": "abc", "pipeline_1": "def"})

    def test_serialize_pipeline_row_uses_expected_operational_fields(self) -> None:
        row = {
          "pipeline_id": 12,
          "pipeline_name": "mysql_pg_customer",
          "dag_id": "dag_customer",
          "task_name": "extract_customers",
          "load_type": "incremental_timestamp",
          "source_connection": "mysql_operacional",
          "source_database": "crm",
          "source_table": "customers",
          "target_schema": "silver",
          "target_table": "customers",
          "status": "failed",
          "watermark_value": "2026-03-28T12:00:00",
          "watermark_column": "updated_at",
          "watermark_type": "timestamp",
          "last_error": "timeout",
          "rows_processed": 1500,
        }

        payload = _serialize_pipeline_row(row, is_primary=True)

        self.assertEqual(payload["pipeline_id"], "12")
        self.assertEqual(payload["pipeline_name"], "mysql_pg_customer")
        self.assertEqual(payload["latest_status_label"], "Falha")
        self.assertEqual(payload["load_type_label"], "Incremental por timestamp")
        self.assertEqual(payload["source_database"], "crm")
        self.assertEqual(payload["target_schema"], "silver")
        self.assertTrue(payload["is_primary"])

    def test_pipeline_history_href_points_to_dedicated_operational_route(self) -> None:
        href = _pipeline_history_href(
            {
                "dag_id": "dag_customer",
                "pipeline_id": "12",
                "target_schema": "silver",
                "target_table": "customers",
            }
        )

        self.assertEqual(href, "/ops/ingestion?dagId=dag_customer&pipelineId=12&schema=silver&table=customers")

    def test_load_table_ingestion_summary_returns_empty_state_when_unlinked(self) -> None:
        column_map = ColumnMap(
            summary_relation="vw_t2c_ingestao_operacional",
            execution_relation="vw_t2c_historico_operacional",
            log_relation="vw_t2c_log_operacional",
            summary_columns={"target_schema", "target_table"},
            execution_columns={"execution_id"},
            log_columns={"log_id"},
        )

        with patch("t2c_data.features.ingestion.service._load_column_map", return_value=column_map), patch(
            "t2c_data.features.ingestion.service._load_summary_rows",
            return_value=[],
        ):
            payload = load_table_ingestion_summary(object(), schema_name="silver", table_name="customers")

        self.assertFalse(payload["linked"])
        self.assertEqual(payload["state"], "not_linked")
        self.assertEqual(payload["message"], "Nenhum pipeline Airflow associado a esta tabela.")

    def test_load_table_ingestion_summary_returns_unavailable_state_when_structure_is_missing(self) -> None:
        with patch("t2c_data.features.ingestion.service._load_column_map", side_effect=IngestionIntegrationUnavailable("boom")):
            payload = load_table_ingestion_summary(object(), schema_name="silver", table_name="customers")

        self.assertFalse(payload["linked"])
        self.assertEqual(payload["state"], "unavailable")
        self.assertEqual(payload["pipeline_count"], 0)

    def test_load_table_ingestion_summary_from_source_returns_degraded_payload_when_connection_fails(self) -> None:
        with patch(
            "t2c_data.features.ingestion.runtime.operational_session",
            side_effect=IngestionIntegrationUnavailable("A fonte operacional externa de ingestão está indisponível."),
        ), patch("t2c_data.features.ingestion.service._record_ingestion_failure") as record_failure:
            payload = load_table_ingestion_summary_from_source(
                object(),
                schema_name="silver",
                table_name="customers",
            )

        self.assertFalse(payload["linked"])
        self.assertEqual(payload["state"], "unavailable")
        self.assertEqual(payload["pipeline_count"], 0)
        self.assertTrue(record_failure.called)

    def test_load_table_ingestion_detail_from_source_returns_degraded_payload_when_connection_fails(self) -> None:
        with patch(
            "t2c_data.features.ingestion.runtime.operational_session",
            side_effect=IngestionIntegrationUnavailable("A fonte operacional externa de ingestão está indisponível."),
        ), patch("t2c_data.features.ingestion.service._record_ingestion_failure") as record_failure:
            payload = load_table_ingestion_detail_from_source(
                object(),
                schema_name="silver",
                table_name="customers",
                page=1,
                page_size=10,
            )

        self.assertEqual(payload["summary"]["state"], "unavailable")
        self.assertEqual(payload["executions"]["state"], "unavailable")
        self.assertEqual(payload["executions"]["total"], 0)
        self.assertTrue(record_failure.called)

    def test_load_table_ingestion_detail_returns_degraded_payload_when_operational_query_breaks(self) -> None:
        with patch("t2c_data.features.ingestion.service.load_table_ingestion_summary", return_value={"linked": True, "state": "available", "message": None, "table_schema": "silver", "table_name": "customers", "pipeline_count": 1, "primary_pipeline": {}, "pipelines": []}), patch(
            "t2c_data.features.ingestion.service.list_table_ingestion_executions",
            side_effect=SQLAlchemyError("relation does not exist"),
        ):
            payload = load_table_ingestion_detail(object(), schema_name="silver", table_name="customers", page=1, page_size=10)

        self.assertEqual(payload["summary"]["state"], "unavailable")
        self.assertEqual(payload["executions"]["state"], "unavailable")
        self.assertEqual(payload["executions"]["total"], 0)

    def test_load_table_ingestion_detail_short_circuits_when_summary_is_unavailable(self) -> None:
        summary = {
            "linked": False,
            "state": "unavailable",
            "message": "A visão operacional de ingestão não está disponível neste ambiente.",
            "table_schema": "silver",
            "table_name": "customers",
            "pipeline_count": 0,
            "primary_pipeline": None,
            "pipelines": [],
        }
        with patch("t2c_data.features.ingestion.service.load_table_ingestion_summary", return_value=summary), patch(
            "t2c_data.features.ingestion.service.list_table_ingestion_executions",
            side_effect=AssertionError("should not load executions when summary is unavailable"),
        ), patch(
            "t2c_data.features.ingestion.service.list_table_ingestion_history",
            side_effect=AssertionError("should not load history when summary is unavailable"),
        ):
            payload = load_table_ingestion_detail(object(), schema_name="silver", table_name="customers", page=1, page_size=10)

        self.assertEqual(payload["summary"], summary)
        self.assertEqual(payload["executions"]["state"], "unavailable")
        self.assertEqual(payload["executions"]["total"], 0)
        self.assertEqual(payload["history"], [])

    def test_operational_overview_uses_external_source_and_falls_back_to_clear_error(self) -> None:
        with patch(
            "t2c_data.features.ingestion.runtime.operational_session",
            side_effect=IngestionIntegrationUnavailable("A fonte operacional externa de ingestão não está configurada."),
        ):
            payload = load_ingestion_operational_overview_from_source(object())

        self.assertFalse(payload["available"])
        self.assertIn("fonte operacional externa", payload["message"])

    def test_operational_source_unavailable_is_cached_between_calls(self) -> None:
        with patch("t2c_data.features.ingestion.runtime._OPERATIONAL_SOURCE_CACHE", None), patch(
            "t2c_data.features.ingestion.runtime._build_operational_url",
            return_value=None,
        ), self.assertLogs("t2c_data.features.ingestion.runtime", level="WARNING") as captured:
            with self.assertRaises(IngestionIntegrationUnavailable):
                with operational_session():
                    pass
            with self.assertRaises(IngestionIntegrationUnavailable):
                with operational_session():
                    pass

        self.assertEqual(len(captured.output), 1)

    def test_operational_connection_failure_is_cached_between_calls(self) -> None:
        with patch("t2c_data.features.ingestion.runtime._OPERATIONAL_SOURCE_CACHE", None), patch(
            "t2c_data.features.ingestion.runtime._probe_operational_url",
            side_effect=RuntimeError("Network is unreachable"),
        ), self.assertLogs("t2c_data.features.ingestion.runtime", level="WARNING") as captured:
            with self.assertRaises(IngestionIntegrationUnavailable):
                with operational_session():
                    pass
            with self.assertRaises(IngestionIntegrationUnavailable):
                with operational_session():
                    pass

        self.assertEqual(len(captured.output), 1)

    def test_missing_operational_relations_are_cached_between_calls(self) -> None:
        session = SimpleNamespace(
            get_bind=lambda: SimpleNamespace(
                engine=SimpleNamespace(
                    url=SimpleNamespace(render_as_string=lambda hide_password=False: "sqlite://cache-a"),
                )
            )
        )

        with patch("t2c_data.features.ingestion.service._COLUMN_MAP_CACHE", {}), patch(
            "t2c_data.features.ingestion.service._discover_column_map",
            side_effect=IngestionIntegrationUnavailable("missing"),
        ) as discover_mock, self.assertLogs("t2c_data.features.ingestion.service", level="WARNING") as captured:
            with self.assertRaises(IngestionIntegrationUnavailable):
                _load_column_map(session)
            with self.assertRaises(IngestionIntegrationUnavailable):
                _load_column_map(session)

        self.assertEqual(discover_mock.call_count, 1)
        self.assertEqual(len(captured.output), 1)

    def test_missing_operational_relations_do_not_leak_between_binds(self) -> None:
        session_a = SimpleNamespace(
            get_bind=lambda: SimpleNamespace(
                engine=SimpleNamespace(
                    url=SimpleNamespace(render_as_string=lambda hide_password=False: "sqlite://cache-a"),
                )
            )
        )
        session_b = SimpleNamespace(
            get_bind=lambda: SimpleNamespace(
                engine=SimpleNamespace(
                    url=SimpleNamespace(render_as_string=lambda hide_password=False: "sqlite://cache-b"),
                )
            )
        )
        column_map = ColumnMap(
            summary_relation="vw_t2c_ingestao_operacional",
            execution_relation=None,
            log_relation=None,
            summary_columns={"target_schema", "target_table"},
            execution_columns=set(),
            log_columns=set(),
        )

        with patch("t2c_data.features.ingestion.service._COLUMN_MAP_CACHE", {}), patch(
            "t2c_data.features.ingestion.service._discover_column_map",
            side_effect=[column_map, IngestionIntegrationUnavailable("missing")],
        ) as discover_mock:
            resolved = _load_column_map(session_a)
            self.assertEqual(resolved.summary_relation, "vw_t2c_ingestao_operacional")
            with self.assertRaises(IngestionIntegrationUnavailable):
                _load_column_map(session_b)

        self.assertEqual(discover_mock.call_count, 2)

    def test_safe_identifier_rejects_invalid_tokens(self) -> None:
        with self.assertRaises(ValueError):
            safe_identifier("bad-name", label="column")
        with self.assertRaises(ValueError):
            safe_identifier("select *", label="column")

    def test_safe_relation_builds_qualified_name(self) -> None:
        self.assertEqual(safe_relation("t2c_data", "tables"), "t2c_data.tables")


if __name__ == "__main__":
    unittest.main()
