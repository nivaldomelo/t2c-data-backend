"""Seed a large synthetic Data Quality history for scale validation.

This script is intentionally manual and should not be executed from unit tests.

Example:
    python backend/scripts/seed_large_dq_history.py --rules 5000 --rule-runs 500000 --job-runs 500000
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone

from sqlalchemy import delete, func, insert, select

from t2c_data.core.db import SessionLocal
from t2c_data.features.data_quality.latest_runs import backfill_latest_rule_runs
from t2c_data.models.catalog import DataSource, Database, Schema, TableEntity
from t2c_data.models.dq import DQJobRun, DQRule, DQRuleLatestRun, DQRuleRun


def _chunked(items: list[dict], size: int):
    for idx in range(0, len(items), size):
        yield items[idx : idx + size]


def _build_rule_definition(
    *,
    datasource_id: int,
    datasource_name: str,
    schema_name: str,
    table_name: str,
    table_id: int,
) -> dict[str, object]:
    return {
        "version": 1,
        "type": "nullability",
        "target": {
            "datasource_id": datasource_id,
            "datasource_name": datasource_name,
            "schema_name": schema_name,
            "table_name": table_name,
            "table_id": table_id,
        },
        "logic": "AND",
        "conditions": [
            {
                "column": "col_001",
                "operator": "not_null",
                "value_type": "none",
            }
        ],
    }


def _scale_table_rows(session, table_ids: list[int]) -> list[tuple[int, int, str, str, str]]:
    rows = session.execute(
        select(
            TableEntity.id,
            TableEntity.name,
            DataSource.id.label("datasource_id"),
            Schema.name.label("schema_name"),
            DataSource.name.label("datasource_name"),
        )
        .join(TableEntity.schema)
        .join(Schema.database)
        .join(Database.datasource)
        .where(TableEntity.id.in_(table_ids))
        .order_by(TableEntity.id.asc())
    ).all()
    return [
        (int(row.id), int(row.datasource_id), str(row.name), str(row.schema_name), str(row.datasource_name))
        for row in rows
    ]


def _build_scale_rule_rows(
    *,
    table_rows: list[tuple[int, int, str, str, str]],
    existing_rule_count: int,
    target_rule_count: int,
    now: datetime,
) -> list[dict[str, object]]:
    rule_rows: list[dict[str, object]] = []
    if not table_rows:
        return rule_rows
    for idx in range(existing_rule_count + 1, target_rule_count + 1):
        table_id, datasource_id, table_name, schema_name, datasource_name = table_rows[(idx - 1) % len(table_rows)]
        rule_rows.append(
            {
                "table_id": table_id,
                "execution_engine": "spark",
                "schedule_mode": "manual",
                "schedule_enabled": False,
                "table_fqn": f"{datasource_name}.{schema_name}.{table_name}",
                "name": f"scale_rule_{idx:05d}",
                "description": "Synthetic rule for scale validation",
                "rule_type": "nullability",
                "severity": "high" if idx % 10 == 0 else "medium",
                "rule_builder_version": 1,
                "rule_definition_json": _build_rule_definition(
                    datasource_id=datasource_id,
                    datasource_name=datasource_name,
                    schema_name=schema_name,
                    table_name=table_name,
                    table_id=table_id,
                ),
                "archived": False,
                "is_active": True,
                "created_at": now,
                "updated_at": now,
            }
        )
    return rule_rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed large synthetic DQ history for scale validation.")
    parser.add_argument("--rules", type=int, default=5000)
    parser.add_argument("--rule-runs", type=int, default=500000)
    parser.add_argument("--job-runs", type=int, default=500000)
    parser.add_argument("--chunk-size", type=int, default=5000)
    parser.add_argument("--skip-backfill", action="store_true")
    args = parser.parse_args()

    now = datetime.now(timezone.utc)
    session = SessionLocal()
    try:
        table_ids = session.scalars(
            select(TableEntity.id).where(TableEntity.name.like("scale_table_%")).order_by(TableEntity.id.asc()).limit(args.rules)
        ).all()
        if not table_ids:
            raise SystemExit("No scale tables found. Run seed_large_catalog.py first.")
        table_rows = _scale_table_rows(session, table_ids)

        existing_rule_count = int(
            session.scalar(select(func.count()).select_from(DQRule).where(DQRule.name.like("scale_rule_%"))) or 0
        )
        rule_rows = _build_scale_rule_rows(
            table_rows=table_rows,
            existing_rule_count=existing_rule_count,
            target_rule_count=args.rules,
            now=now,
        )
        if rule_rows:
            for chunk in _chunked(rule_rows, args.chunk_size):
                session.execute(insert(DQRule), chunk)
            session.commit()

        rules = session.execute(
            select(DQRule.id, DQRule.table_id).where(DQRule.name.like("scale_rule_%")).order_by(DQRule.id.asc()).limit(args.rules)
        ).all()
        rule_ids = [rule_id for rule_id, _table_id in rules]
        table_by_rule = {rule_id: table_id for rule_id, table_id in rules}

        existing_rule_runs = int(session.scalar(select(func.count()).select_from(DQRuleRun).where(DQRuleRun.rule_id.in_(rule_ids))) or 0)
        remaining_rule_runs = max(args.rule_runs - existing_rule_runs, 0)
        for batch_start in range(0, remaining_rule_runs, args.chunk_size):
            batch_size = min(args.chunk_size, remaining_rule_runs - batch_start)
            rule_run_rows: list[dict] = []
            for offset in range(batch_size):
                absolute_idx = existing_rule_runs + batch_start + offset + 1
                rule_id = rule_ids[(absolute_idx - 1) % len(rule_ids)]
                executed_at = now - timedelta(minutes=(absolute_idx % 100000))
                violations = absolute_idx % 13
                rule_run_rows.append(
                    {
                        "rule_id": rule_id,
                        "status": "fail" if violations else "pass",
                        "execution_engine": "spark",
                        "violations_count": violations,
                        "sample_rows_json": None,
                        "error_message": None,
                        "created_at": executed_at,
                        "updated_at": executed_at,
                    }
                )
            session.execute(insert(DQRuleRun), rule_run_rows)
            session.commit()

        existing_job_runs = int(session.scalar(select(func.count()).select_from(DQJobRun).where(DQJobRun.table_id.in_(table_ids))) or 0)
        remaining_job_runs = max(args.job_runs - existing_job_runs, 0)
        for batch_start in range(0, remaining_job_runs, args.chunk_size):
            batch_size = min(args.chunk_size, remaining_job_runs - batch_start)
            job_rows: list[dict] = []
            for offset in range(batch_size):
                absolute_idx = existing_job_runs + batch_start + offset + 1
                rule_id = rule_ids[(absolute_idx - 1) % len(rule_ids)]
                table_id = table_by_rule[rule_id]
                executed_at = now - timedelta(minutes=(absolute_idx % 100000))
                violations = absolute_idx % 13
                job_rows.append(
                    {
                        "job_type": "rules",
                        "status": "failed" if violations and absolute_idx % 29 == 0 else "success",
                        "execution_engine": "spark",
                        "table_id": table_id,
                        "table_fqn": f"table_id:{table_id}",
                        "datasource_id": None,
                        "requested_by_user_id": None,
                        "result_json": {
                            "summary": {
                                "total_rules": 1,
                                "passed_rules": 0 if violations else 1,
                                "failed_rules": 1 if violations else 0,
                            },
                            "violations_count_total": violations,
                            "requested_rule_ids": [rule_id],
                        },
                        "error_message": None,
                        "created_at": executed_at,
                        "updated_at": executed_at,
                    }
                )
            session.execute(insert(DQJobRun), job_rows)
            session.commit()

        if not args.skip_backfill:
            session.execute(delete(DQRuleLatestRun))
            session.commit()
            backfill_latest_rule_runs(session)
            session.commit()

        print(
            {
                "rules": len(rule_ids),
                "rule_runs_target": args.rule_runs,
                "job_runs_target": args.job_runs,
                "latest_backfilled": not args.skip_backfill,
                "rule_definition_mode": "structured",
            }
        )
    finally:
        session.close()


if __name__ == "__main__":
    main()
