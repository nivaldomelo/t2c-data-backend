from __future__ import annotations

import os
import unittest

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from t2c_data.core.telemetry import RuntimeMetrics


class RuntimeMetricsTests(unittest.TestCase):
    def test_request_lifecycle_updates_snapshot(self) -> None:
        metrics = RuntimeMetrics(duration_window_size=8)

        metrics.request_started(method="get")
        snapshot_during_request = metrics.snapshot()
        self.assertEqual(snapshot_during_request["in_flight_requests"], 1)
        self.assertEqual(snapshot_during_request["methods"], {"GET": 1})

        metrics.request_finished(status_code=200, duration_ms=120.5)
        metrics.request_started(method="post")
        metrics.request_finished(status_code=503, duration_ms=300.0)
        metrics.request_started(method="get")
        metrics.request_finished(status_code=404, duration_ms=50.0)

        snapshot = metrics.snapshot()
        self.assertEqual(snapshot["in_flight_requests"], 0)
        self.assertEqual(snapshot["total_requests"], 3)
        self.assertEqual(snapshot["client_error_requests"], 1)
        self.assertEqual(snapshot["server_error_requests"], 1)
        self.assertEqual(snapshot["methods"], {"GET": 2, "POST": 1})
        self.assertEqual(snapshot["status_families"], {"2xx": 1, "5xx": 1, "4xx": 1})
        self.assertAlmostEqual(snapshot["avg_duration_ms"], 156.83, places=2)
        self.assertEqual(snapshot["p95_duration_ms"], 300.0)
        self.assertGreaterEqual(snapshot["uptime_seconds"], 0.0)

    def test_export_prometheus_includes_route_and_rate_limit(self) -> None:
        metrics = RuntimeMetrics(duration_window_size=4, route_window_size=4)

        metrics.request_started(method="get")
        metrics.request_finished(status_code=200, duration_ms=25.5, method="get", route="/api/v1/catalog/tables/{table_id}")
        metrics.request_started(method="get")
        metrics.request_finished(status_code=429, duration_ms=5.0, method="get", route="/api/v1/external/catalog/tables")
        metrics.rate_limit_hit(route_group="external.catalog")
        metrics.job_finished(job="platform_read_models_refresh", duration_ms=128.2, success=True)

        exported = metrics.export_prometheus()
        self.assertIn("t2c_http_route_requests_total", exported)
        self.assertIn('route="/api/v1/catalog/tables/{table_id}"', exported)
        self.assertIn('route_group="external.catalog"', exported)
        self.assertIn('job="platform_read_models_refresh"', exported)

    def test_export_prometheus_includes_diagnostics_alerts_and_auth_metrics(self) -> None:
        metrics = RuntimeMetrics(duration_window_size=4, route_window_size=4)

        metrics.diagnostic_emitted(module="datasource_scan", severity="warning", cause="row_count_timeout")
        metrics.internal_alert_generated(module="datasource_scan", severity="warning", channel="inbox")
        metrics.export_event(module="privacy_access", outcome="denied", classification="high")
        metrics.api_auth_event(outcome="scope_denied")

        exported = metrics.export_prometheus()
        self.assertIn('t2c_operational_diagnostics_total{module="datasource_scan",severity="warning",cause="row_count_timeout"} 1', exported)
        self.assertIn('t2c_internal_alerts_total{module="datasource_scan",severity="warning",channel="inbox"} 1', exported)
        self.assertIn('t2c_exports_total{module="privacy_access",outcome="denied",classification="high"} 1', exported)
        self.assertIn('t2c_api_auth_events_total{outcome="scope_denied"} 1', exported)


if __name__ == "__main__":
    unittest.main()
