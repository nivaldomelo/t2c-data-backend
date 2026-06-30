from __future__ import annotations

import os
import unittest

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from t2c_data.features.scanner.application import sanitize_scan_error
from t2c_data.features.scanner.execution_diagnostics import infer_scan_failure_stage


class ScannerErrorClassificationTests(unittest.TestCase):
    def test_unknown_host_is_classified_as_invalid_host(self) -> None:
        message, detail, code = sanitize_scan_error(RuntimeError("java.net.UnknownHostException: host.docker.internal"))

        self.assertEqual(code, "invalid_host")
        self.assertIn("Spark", message)
        self.assertIn("UnknownHostException", detail)

    def test_missing_spark_submit_is_classified_as_spark_unavailable(self) -> None:
        message, detail, code = sanitize_scan_error(RuntimeError("[Errno 2] No such file or directory: 'spark-submit'"))

        self.assertEqual(code, "spark_unavailable")
        self.assertIn("spark-submit", message)
        self.assertIn("spark-submit", detail)

    def test_missing_postgres_jdbc_driver_is_classified_as_driver_missing(self) -> None:
        message, detail, code = sanitize_scan_error(RuntimeError("ClassNotFoundException: org.postgresql.Driver"))

        self.assertEqual(code, "spark_jdbc_driver_missing")
        self.assertIn("driver JDBC", message)
        self.assertIn("ClassNotFoundException", detail)

    def test_failure_stage_uses_latest_scan_marker(self) -> None:
        stage = infer_scan_failure_stage(
            stdout_log="""
            [datasource-scan] stage=startup
            [datasource-scan] stage=connection_test
            [datasource-scan] stage=table_discovery
            """,
            stderr_log="",
        )

        self.assertEqual(stage, "table_discovery")


if __name__ == "__main__":
    unittest.main()
