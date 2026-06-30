from __future__ import annotations

import os
from types import SimpleNamespace

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from t2c_data.features.governance.settings import GovernanceSettingsSnapshot
from t2c_data.features.governance.trust_score import build_trust_score_for_profile, trust_score_label


def _profile(**overrides):
    base = dict(
        owner_defined=True,
        classification_defined=True,
        description_complete=True,
        dictionary_complete=True,
        tags_count=2,
        terms_count=1,
        dq_score=95.0,
        active_dq_violation=False,
        critical_open_incidents=0,
        open_incidents=0,
        recent_dq_failure_runs_30d=0,
        freshness_seconds=3600,
        search_clicks_30d=4,
        certification_criticality="medium",
        active_dq_rules_count=1,
        readiness_score=88,
    )
    base.update(overrides)
    return SimpleNamespace(**base)


def test_trust_score_reflects_operational_gaps() -> None:
    snapshot = GovernanceSettingsSnapshot(governance_high_usage_click_threshold=10)
    profile = _profile(owner_defined=False, classification_defined=False, dictionary_complete=False, tags_count=0, terms_count=0, dq_score=62.0)
    trust = build_trust_score_for_profile(profile, settings_snapshot=snapshot)

    assert trust.score < 70
    assert trust.label in {"Em atenção", "Baixa confiança"}
    assert any(item["key"] == "no_owner" for item in trust.context["penalties"])


def test_trust_score_applies_domain_and_criticality_adjustments() -> None:
    snapshot = GovernanceSettingsSnapshot(
        governance_high_usage_click_threshold=10,
        trust_score_domain_adjustments={"financeiro": 6},
        trust_score_criticality_adjustments={"critical": -4},
    )
    profile = _profile(domain_name="financeiro", certification_criticality="critical")

    trust = build_trust_score_for_profile(profile, settings_snapshot=snapshot)

    assert trust.score > 90
    assert any(item["scope"] == "domain" for item in trust.context["adjustments"])
    assert any(item["scope"] == "criticality" for item in trust.context["adjustments"])


def test_trust_score_label_breakpoints_are_stable() -> None:
    assert trust_score_label(90)[0] == "Muito confiável"
    assert trust_score_label(75)[0] == "Confiável"
    assert trust_score_label(55)[0] == "Em atenção"
    assert trust_score_label(10)[0] == "Baixa confiança"


if __name__ == "__main__":
    test_trust_score_reflects_operational_gaps()
    test_trust_score_applies_domain_and_criticality_adjustments()
    test_trust_score_label_breakpoints_are_stable()
    print("governance trust score tests: OK")
