from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import patch

from t2c_data.features.catalog.canonical_assets import _pipeline_payload


class CanonicalAssetsTests(unittest.TestCase):
    def test_pipeline_payload_uses_summary_only(self) -> None:
        table = SimpleNamespace(
            name="orders",
            schema=SimpleNamespace(name="silver"),
        )
        summary = {
            "linked": True,
            "state": "available",
            "message": None,
            "table_schema": "silver",
            "table_name": "orders",
            "pipeline_count": 1,
            "primary_pipeline": {"pipeline_name": "orders_pipeline", "latest_status_label": "Sucesso"},
            "pipelines": [{"pipeline_name": "orders_pipeline"}],
        }

        with patch("t2c_data.features.catalog.canonical_assets.load_table_ingestion_summary", return_value=summary):
            payload = _pipeline_payload(SimpleNamespace(rollback=lambda: None), table)

        self.assertIsNotNone(payload)
        assert payload is not None
        self.assertTrue(payload.linked)
        self.assertEqual(payload.state, "available")
        self.assertEqual(payload.pipeline_count, 1)
        self.assertEqual(payload.primary_pipeline.pipeline_name, "orders_pipeline")
        self.assertEqual(payload.stability, None)
        self.assertEqual(payload.history, [])


if __name__ == "__main__":
    unittest.main()
