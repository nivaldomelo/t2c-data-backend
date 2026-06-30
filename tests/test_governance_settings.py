from __future__ import annotations

import os
import unittest
import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

os.environ.setdefault("DATABASE_URL", "postgresql+psycopg://user:pass@localhost/testdb")

from t2c_data.features.governance import settings as governance_settings
from t2c_data.features.platform import analytics as platform_analytics


class GovernanceSettingsTests(unittest.TestCase):
    def test_snapshot_includes_business_audit_archive_retention(self) -> None:
        original_get_or_create = governance_settings.get_or_create_governance_settings
        try:
            governance_settings.get_or_create_governance_settings = lambda session: SimpleNamespace(
                owner_review_interval_days=90,
                privacy_review_interval_days=180,
                sensitive_privacy_review_interval_days=90,
                certification_review_interval_days=180,
                certification_review_sla_days=7,
                certification_revalidation_window_days=30,
                audit_log_retention_days=730,
                audit_log_archive_retention_days=2555,
                access_log_retention_days=30,
                access_log_archive_retention_days=365,
                platform_usage_event_retention_days=180,
                search_result_click_retention_days=180,
                governance_high_usage_click_threshold=20,
                legacy_api_cutoff_window_days=30,
                legacy_api_disabled_modules=None,
                legacy_api_force_enabled_modules=None,
                trust_score_domain_adjustments=json.dumps({"financeiro": 5}),
                trust_score_criticality_adjustments=json.dumps({"critical": -8}),
                governance_score_weights=json.dumps(
                    {
                        "owner_defined": 12,
                        "table_description_complete": 8,
                        "column_description_complete": 12,
                        "tags_applied": 8,
                        "glossary_terms": 8,
                        "dq_score": 15,
                        "certification": 10,
                        "incident_health": 10,
                        "owner_review": 6,
                        "privacy_review": 5,
                        "certification_review": 6,
                    }
                ),
            )
            snapshot = governance_settings.get_governance_settings_snapshot(session=object())
        finally:
            governance_settings.get_or_create_governance_settings = original_get_or_create

        self.assertEqual(snapshot.audit_log_archive_retention_days, 2555)
        self.assertEqual(snapshot.governance_high_usage_click_threshold, 20)
        self.assertEqual(snapshot.governance_score_weights["owner_defined"], 12)
        self.assertEqual(snapshot.trust_score_domain_adjustments["financeiro"], 5)
        self.assertEqual(snapshot.trust_score_criticality_adjustments["critical"], -8)
        self.assertEqual(sum(snapshot.governance_score_weights.values()), 100)

    def test_effective_legacy_api_disabled_modules_auto_cuts_zero_usage_modules(self) -> None:
        original_snapshot = governance_settings.get_governance_settings_snapshot
        original_usage = platform_analytics.legacy_api_usage_stats_by_module
        try:
            governance_settings.get_governance_settings_snapshot = lambda session: SimpleNamespace(
                legacy_api_cutoff_window_days=30,
                legacy_api_disabled_modules=("catalog",),
                legacy_api_force_enabled_modules=("datasources",),
            )
            cutoff = datetime.now(timezone.utc) - timedelta(days=31)
            platform_analytics.legacy_api_usage_stats_by_module = lambda session, days=30: {
                "datasources": {"hits_total": 12, "hits_in_window": 0, "last_hit_at": cutoff},
                "auth": {"hits_total": 5, "hits_in_window": 1, "last_hit_at": datetime.now(timezone.utc)},
                "home": {"hits_total": 0, "hits_in_window": 0, "last_hit_at": None},
                "scan-runs": {"hits_total": 4, "hits_in_window": 0, "last_hit_at": cutoff},
            }

            disabled = governance_settings.get_effective_legacy_api_disabled_modules(session=object())
        finally:
            governance_settings.get_governance_settings_snapshot = original_snapshot
            platform_analytics.legacy_api_usage_stats_by_module = original_usage

        self.assertIn("catalog", disabled)
        self.assertIn("scan-runs", disabled)
        self.assertNotIn("datasources", disabled)
        self.assertNotIn("home", disabled)
        self.assertNotIn("auth", disabled)


if __name__ == "__main__":
    unittest.main()
