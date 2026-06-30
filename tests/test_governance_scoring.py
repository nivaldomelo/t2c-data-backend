from __future__ import annotations

import os

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from t2c_data.features.governance.scoring import build_governance_score


def test_governance_score_full_and_partial_paths() -> None:
    strong = build_governance_score(
        owner_defined=True,
        table_description_complete=True,
        column_description_complete=True,
        tags_count=3,
        terms_count=2,
        dq_score=96.0,
        certification_status="certified",
        eligible_for_certification=True,
        open_incidents=0,
        critical_open_incidents=0,
        owner_review_current=True,
        privacy_review_current=True,
        certification_review_current=True,
    )
    assert strong["score"] == 100
    assert strong["label"] == "Forte"

    evolving = build_governance_score(
        owner_defined=True,
        table_description_complete=False,
        column_description_complete=False,
        tags_count=0,
        terms_count=0,
        dq_score=78.0,
        certification_status="not_eligible",
        eligible_for_certification=False,
        open_incidents=2,
        critical_open_incidents=0,
        owner_review_current=False,
        privacy_review_current=True,
        certification_review_current=False,
    )
    assert evolving["score"] == 29
    assert evolving["label"] == "Crítica"
    dq_factor = next(factor for factor in evolving["factors"] if factor["key"] == "dq_score")
    assert dq_factor["status"] == "partial"
    assert dq_factor["points"] == 8

    custom = build_governance_score(
        owner_defined=True,
        table_description_complete=True,
        column_description_complete=False,
        tags_count=0,
        terms_count=0,
        dq_score=78.0,
        certification_status="in_review",
        eligible_for_certification=True,
        open_incidents=1,
        critical_open_incidents=0,
        owner_review_current=True,
        privacy_review_current=False,
        certification_review_current=False,
        weights={
            "owner_defined": 5,
            "table_description_complete": 5,
            "column_description_complete": 20,
            "tags_applied": 5,
            "glossary_terms": 5,
            "dq_score": 20,
            "certification": 10,
            "incident_health": 10,
            "owner_review": 10,
            "privacy_review": 5,
            "certification_review": 5,
        },
    )
    assert custom["max_score"] == 100
    assert custom["score"] == 43
    dq_factor_custom = next(factor for factor in custom["factors"] if factor["key"] == "dq_score")
    assert dq_factor_custom["points"] == 11


if __name__ == "__main__":
    test_governance_score_full_and_partial_paths()
    print("governance scoring tests: OK")
