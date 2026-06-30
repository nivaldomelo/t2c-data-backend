from __future__ import annotations

import os
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from t2c_data.features.scanner.execution_diagnostics import serialize_scan_run_detail


class ScanRunDiagnosticsTests(unittest.TestCase):
    def test_scan_run_detail_exposes_failure_and_discovery_context(self) -> None:
        run = SimpleNamespace(
            id=41,
            datasource_id=7,
            status="failed",
            summary={
                "execution_engine": "spark",
                "spark_master_url": "spark://spark-master:7077",
                "spark_app_id": "application_1234_0001",
                "spark_driver_id": "driver-42",
                "logs_path": "/data/spark-results/datasource-scan-run-41.log",
                "failure_stage": "connection_test",
                "error_code": "invalid_host",
                "error": "Não foi possível alcançar a fonte de dados a partir do Spark.",
                "error_detail": "java.net.UnknownHostException: host.docker.internal",
                "submitted_at": "2026-05-30T10:00:00+00:00",
                "running_at": "2026-05-30T10:00:10+00:00",
                "finished_at": "2026-05-30T10:01:40+00:00",
                "duration_seconds": 100,
                "discovery": {"schemas": 2, "tables": 14, "columns": 139},
                "row_counts": {"attempted": 14, "success": 11, "failed": 3, "skipped": 0},
                "snapshots": 153,
                "diffs": 9,
            },
            created_at=datetime(2026, 5, 30, 10, 0, tzinfo=timezone.utc),
            updated_at=datetime(2026, 5, 30, 10, 1, 40, tzinfo=timezone.utc),
        )

        detail = serialize_scan_run_detail(run)

        self.assertEqual(detail["id"], 41)
        self.assertEqual(detail["datasource_id"], 7)
        self.assertEqual(detail["execution_engine"], "spark")
        self.assertEqual(detail["failure_stage"], "connection_test")
        self.assertEqual(detail["spark_application_id"], "application_1234_0001")
        self.assertEqual(detail["spark_driver_id"], "driver-42")
        self.assertEqual(detail["spark_logs_url"], "/api/v1/scan-runs/41/logs")
        self.assertEqual(detail["discovery"], {"schemas": 2, "tables": 14, "columns": 139})
        self.assertEqual(detail["row_counts"]["failed"], 3)
        self.assertEqual(detail["diffs"], 9)
        self.assertEqual(detail["snapshots"], 153)
        self.assertEqual(detail["duration_seconds"], 100)

    def test_scan_run_detail_coerces_empty_discovery_and_extracts_log_error(self) -> None:
        run = SimpleNamespace(
            id=8,
            datasource_id=1,
            status="failed",
            summary={
                "error": "Falha ao executar o scan da fonte de dados.",
                "error_code": "scan_failed",
                "logs_path": "/tmp/nonexistent.log",
                "discovery": {"schemas": None, "tables": None, "columns": None},
                "row_counts": {"attempted": None, "success": "3", "failed": 1.0},
            },
            created_at=datetime(2026, 5, 30, 10, 0, tzinfo=timezone.utc),
            updated_at=datetime(2026, 5, 30, 10, 0, 5, tzinfo=timezone.utc),
        )

        detail = serialize_scan_run_detail(run)

        self.assertEqual(detail["discovery"], {"schemas": 0, "tables": 0, "columns": 0})
        self.assertEqual(detail["row_counts"], {"attempted": 0, "success": 3, "failed": 1})
        self.assertEqual(detail["error_message"], "Falha ao executar o scan da fonte de dados.")
        self.assertEqual(detail["error_detail"], None)


if __name__ == "__main__":
    unittest.main()
