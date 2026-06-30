from __future__ import annotations

import unittest

from t2c_data.features.catalog.correlation import build_correlation_priority_payload


class CatalogCorrelationPriorityTest(unittest.TestCase):
    def test_triple_signal_priority(self) -> None:
        payload = build_correlation_priority_payload(
            table_id=12,
            asset_name="orders",
            qualified_name="sales.public.orders",
            schema_name="public",
            source_name="sales",
            has_operational_failure=True,
            has_dq_degradation=True,
            has_open_incident=True,
        )

        self.assertEqual(payload["correlation_type"], "Falha operacional + DQ degradada + incidente aberto")
        self.assertEqual(payload["priority_score"], 10)
        self.assertTrue(payload["has_operational_failure"])
        self.assertTrue(payload["has_dq_degradation"])
        self.assertTrue(payload["has_open_incident"])

    def test_partial_fallback_priority(self) -> None:
        payload = build_correlation_priority_payload(
            table_id=12,
            asset_name="orders",
            qualified_name="sales.public.orders",
            schema_name="public",
            source_name="sales",
            has_operational_failure=True,
            has_dq_degradation=False,
            has_open_incident=True,
            access_clicks=5,
        )

        self.assertEqual(payload["correlation_type"], "Falha operacional + incidente aberto")
        self.assertEqual(payload["priority_score"], 8)
        self.assertEqual(payload["summary"], "Falha operacional recente e incidente aberto aparecem ao mesmo tempo neste ativo.")

    def test_empty_signal_priority(self) -> None:
        payload = build_correlation_priority_payload(
            table_id=12,
            asset_name="orders",
            qualified_name="sales.public.orders",
            schema_name="public",
            source_name="sales",
            has_operational_failure=False,
            has_dq_degradation=False,
            has_open_incident=False,
        )

        self.assertEqual(payload["correlation_type"], "Sem correlação crítica relevante")
        self.assertEqual(payload["priority_score"], 0)


if __name__ == "__main__":
    unittest.main()
