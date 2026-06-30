from __future__ import annotations

from datetime import datetime, timezone

from scripts.seed_large_dq_history import _build_rule_definition, _build_scale_rule_rows


def test_scale_seed_builds_structured_rules_without_sql_text() -> None:
    now = datetime(2026, 5, 26, 12, 0, tzinfo=timezone.utc)
    rows = _build_scale_rule_rows(
        table_rows=[
            (11, 21, "orders", "bronze", "warehouse"),
            (12, 21, "customers", "bronze", "warehouse"),
        ],
        existing_rule_count=0,
        target_rule_count=3,
        now=now,
    )

    assert len(rows) == 3
    assert all("sql_text" not in row for row in rows)
    assert all(row["execution_engine"] == "spark" for row in rows)
    assert all(row["rule_type"] == "nullability" for row in rows)
    assert rows[0]["rule_definition_json"] == _build_rule_definition(
        datasource_id=21,
        datasource_name="warehouse",
        schema_name="bronze",
        table_name="orders",
        table_id=11,
    )
    assert rows[0]["rule_definition_json"]["conditions"][0]["operator"] == "not_null"
    assert rows[0]["rule_definition_json"]["conditions"][0]["value_type"] == "none"
