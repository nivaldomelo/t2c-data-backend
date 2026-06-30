"""Benchmark core high-volume routes/functions against the current database.

This script is intentionally manual and should not be executed from unit tests.

Suggested flow:
    python backend/scripts/seed_large_catalog.py
    python backend/scripts/seed_large_dq_history.py
    python backend/scripts/seed_large_incidents.py
    python backend/scripts/benchmark_core_endpoints.py --repeats 5 --warmup 1
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from statistics import mean, median, quantiles
from time import perf_counter
from types import SimpleNamespace

from sqlalchemy import func, select

from t2c_data.api.audit import list_audit_history
from t2c_data.api.incidents import list_incidents
from t2c_data.api.privacy_access import export_privacy_csv, list_privacy_tables
from t2c_data.features.data_quality.profiling_executions import list_profiling_executions
from t2c_data.features.data_quality.rule_management import list_rules_with_filters_page
from t2c_data.features.governance import get_governance_review_summary, get_ownership_export_rows, get_ownership_summary
from t2c_data.features.ingestion.service import load_ingestion_operational_overview_from_source
from t2c_data.features.lineage.table_summary import get_table_summary
from t2c_data.features.platform.cockpit_ops import build_platform_cockpit_export_rows, build_platform_cockpit_queue_page
from t2c_data.features.platform.read_models import load_dashboard_profiles_with_fallback
from t2c_data.features.search.global_search import SearchFilters, search_global
from t2c_data.features.shared_cache import safe_connection_label
from t2c_data.models.auth import Role, User
from t2c_data.models.catalog import TableEntity
from t2c_data.models.dq import DQJobRun, DQRule, DQRuleRun
from t2c_data.models.incident import Incident
from t2c_data.models.audit import AuditLog
from t2c_data.models.platform import AssetRowCountSnapshot
from t2c_data.core.db import SessionLocal

def _percentile(values: list[float], percentile: int) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return round(values[0], 2)
    cuts = quantiles(values, n=100, method="inclusive")
    return round(cuts[percentile - 1], 2)


def _ensure_admin_user(session):
    role = session.scalar(select(Role).where(Role.name == "admin"))
    if role is None:
        role = Role(name="admin", description="Scale benchmark admin")
        session.add(role)
        session.flush()
    user = session.scalar(select(User).where(User.email == "scale-benchmark-admin@example.local"))
    if user is None:
        user = User(
            email="scale-benchmark-admin@example.local",
            name="scale-benchmark-admin",
            full_name="Scale Benchmark Admin",
            password_hash="not-used",
            is_active=True,
        )
        user.roles.append(role)
        session.add(user)
        session.commit()
        session.refresh(user)
    elif role not in user.roles:
        user.roles.append(role)
        session.add(user)
        session.commit()
        session.refresh(user)
    return user


def _benchmark(label: str, repeats: int, warmup: int, fn):
    for _ in range(max(warmup, 0)):
        fn()
    durations = []
    for _ in range(max(repeats, 1)):
        started = perf_counter()
        fn()
        durations.append(perf_counter() - started)
    durations_ms = [round(value * 1000, 2) for value in durations]
    return {
        "label": label,
        "runs": len(durations_ms),
        "min_ms": min(durations_ms),
        "p50_ms": round(median(durations_ms), 2),
        "p95_ms": _percentile(durations_ms, 95),
        "p99_ms": _percentile(durations_ms, 99),
        "avg_ms": round(mean(durations_ms), 2),
        "max_ms": max(durations_ms),
    }


def _first_table_id(session) -> int | None:
    return session.scalar(select(TableEntity.id).where(TableEntity.name.like("scale_table_%")).order_by(TableEntity.id.asc()).limit(1))


def _first_rule_id(session) -> int | None:
    return session.scalar(select(DQRule.id).where(DQRule.name.like("scale_rule_%")).order_by(DQRule.id.asc()).limit(1))


def _fake_request(path: str) -> SimpleNamespace:
    return SimpleNamespace(
        url=SimpleNamespace(path=path),
        method="GET",
        headers={"user-agent": "scale-benchmark/1.0"},
        state=SimpleNamespace(request_id="scale-benchmark-request"),
        client=SimpleNamespace(host="127.0.0.1"),
    )


def _dataset_counts(session) -> dict[str, int]:
    table_ids_sq = select(TableEntity.id).where(TableEntity.name.like("scale_table_%")).subquery()
    return {
        "tables": int(session.scalar(select(func.count()).select_from(table_ids_sq)) or 0),
        "columns": int(
            session.scalar(
                select(func.count())
                .select_from(TableEntity)
                .join(TableEntity.columns)
                .where(TableEntity.name.like("scale_table_%"))
            )
            or 0
        ),
        "dq_rules": int(session.scalar(select(func.count()).select_from(DQRule).where(DQRule.name.like("scale_rule_%"))) or 0),
        "dq_rule_runs": int(
            session.scalar(
                select(func.count())
                .select_from(DQRuleRun)
                .join(DQRule, DQRuleRun.rule_id == DQRule.id)
                .where(DQRule.name.like("scale_rule_%"))
            )
            or 0
        ),
        "dq_job_runs": int(session.scalar(select(func.count()).select_from(DQJobRun).where(DQJobRun.table_fqn.like("table_id:%"))) or 0),
        "incidents": int(session.scalar(select(func.count()).select_from(Incident).where(Incident.title.like("Scale incident %"))) or 0),
        "audit_events": int(session.scalar(select(func.count()).select_from(AuditLog).where(AuditLog.request_id.like("scale-request-%"))) or 0),
        "row_count_snapshots": int(
            session.scalar(
                select(func.count()).select_from(AssetRowCountSnapshot).where(AssetRowCountSnapshot.asset_fqn.like("scale-benchmark.%"))
            )
            or 0
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark core t2c_data endpoints/functions on the current database.")
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--page-size", type=int, default=50)
    parser.add_argument("--search-query", type=str, default="scale_table")
    parser.add_argument("--json", action="store_true", dest="as_json")
    parser.add_argument("--output", type=str, default=None)
    parser.add_argument("--labels", type=str, default=None, help="Comma-separated benchmark labels to run.")
    args = parser.parse_args()

    session = SessionLocal()
    try:
        admin_user = _ensure_admin_user(session)
        table_id = _first_table_id(session)
        rule_id = _first_rule_id(session)
        bench_now = datetime.now(timezone.utc)
        dataset_counts = _dataset_counts(session)

        requested_labels = {
            item.strip()
            for item in (args.labels or "").split(",")
            if item and item.strip()
        }

        benchmark_defs: list[tuple[str, callable]] = [
            (
                "dashboard.read_models",
                lambda: load_dashboard_profiles_with_fallback(session, bench_now, current_user=admin_user),
            ),
            (
                "explorer.search",
                lambda: search_global(
                    session,
                    args.search_query,
                    filters=SearchFilters(),
                    limit=args.page_size,
                    per_group=max(1, min(args.page_size // 4, 10)),
                    current_user=admin_user,
                ),
            ),
        ]

        if table_id is not None:
            benchmark_defs.extend(
                [
                    ("lineage.table.summary", lambda: get_table_summary(session, table_id, current_user=admin_user, max_relations=50)),
                    (
                        "incidents.list",
                        lambda: list_incidents(
                            status=None,
                            severity=None,
                            entity_type=None,
                            owner_id=None,
                            reporter_id=None,
                            source_type=None,
                            source_ref_id=None,
                            table_id=table_id,
                            domain_name=None,
                            owner_name=None,
                            unassigned=None,
                            sla_status=None,
                            q=None,
                            date_from=None,
                            date_to=None,
                            page=1,
                            page_size=args.page_size,
                            db=session,
                            current_user=admin_user,
                        ),
                    ),
                    (
                        "privacy.tables.list",
                        lambda: list_privacy_tables(
                            q=None,
                            sensitivity_level=None,
                            has_personal_data=None,
                            access_scope=None,
                            page=1,
                            page_size=args.page_size,
                            db=session,
                            current_user=admin_user,
                        ),
                    ),
                    ("owners.summary", lambda: get_ownership_summary(session, current_user=admin_user, page=1, page_size=args.page_size)),
                    (
                        "ownership.export.rows",
                        lambda: get_ownership_export_rows(
                            session,
                            current_user=admin_user,
                            include_unowned=True,
                        ),
                    ),
                ]
            )

        if rule_id is not None:
            benchmark_defs.extend(
                [
                    (
                        "dq.rules.page",
                        lambda: list_rules_with_filters_page(
                            db=session,
                            rule_id=None,
                            q="scale_rule",
                            table_id=None,
                            table_fqn=None,
                            is_active=True,
                            severity=None,
                            last_status=None,
                            page=1,
                            page_size=args.page_size,
                            current_user=admin_user,
                        ),
                    ),
                    (
                        "dq.profiling.executions",
                        lambda: list_profiling_executions(
                            session,
                            limit=args.page_size,
                            offset=0,
                        ),
                    ),
                ]
            )

        benchmark_defs.extend(
            [
                (
                    "ops.cockpit.queue",
                    lambda: build_platform_cockpit_queue_page(
                        session,
                        current_user=admin_user,
                        page=1,
                        page_size=args.page_size,
                    ),
                ),
                ("ops.cockpit.export.rows", lambda: build_platform_cockpit_export_rows(session, current_user=admin_user)),
                ("ingestion.overview", lambda: load_ingestion_operational_overview_from_source(session, limit=args.page_size)),
                (
                    "audit.history.list",
                    lambda: list_audit_history(
                        page=1,
                        page_size=args.page_size,
                        date_from=None,
                        date_to=None,
                        actor=None,
                        entity_type=None,
                        entity_id=None,
                        parent_entity_type=None,
                        parent_entity_id=None,
                        change_type=None,
                        field_name=None,
                        source_module=None,
                        sensitive_only=False,
                        datasource=None,
                        database=None,
                        schema=None,
                        q=None,
                        db=session,
                        _=admin_user,
                    ),
                ),
                (
                    "privacy.export.csv.filtered",
                    lambda: export_privacy_csv(
                        _fake_request("/api/v1/privacy-access/export.csv"),
                        q="scale_table",
                        sensitivity_level="personal",
                        has_personal_data=True,
                        access_scope=None,
                        db=session,
                        current_user=admin_user,
                    ),
                ),
                ("governance.review.summary", lambda: get_governance_review_summary(session, current_user=admin_user)),
            ]
        )

        selected_defs = [
            (label, fn)
            for label, fn in benchmark_defs
            if not requested_labels or label in requested_labels
        ]
        benchmarks = [
            _benchmark(label, args.repeats, args.warmup, fn)
            for label, fn in selected_defs
        ]

        payload = {
            "benchmarked_at": datetime.now(timezone.utc).isoformat(),
            "database_label": safe_connection_label(session.bind) if session.bind is not None else None,
            "page_size": args.page_size,
            "dataset_counts": dataset_counts,
            "rules_present": rule_id is not None,
            "table_id_sample": table_id,
            "results": benchmarks,
            "targets": {
                "journey_detail_ms_target": 1000,
                "list_ms_target": 2000,
                "note": "Exports remain capped and audited; scale validation should prefer async jobs when a payload exceeds the configured export limit.",
            },
        }
        if args.output:
            with open(args.output, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, indent=2, ensure_ascii=True, default=str)
        if args.as_json:
            print(json.dumps(payload, indent=2, ensure_ascii=True, default=str))
            return
        for item in payload["results"]:
            print(
                f"{item['label']}: min={item['min_ms']}ms p50={item['p50_ms']}ms "
                f"p95={item['p95_ms']}ms p99={item['p99_ms']}ms avg={item['avg_ms']}ms max={item['max_ms']}ms"
            )
        print("dataset_counts:", payload["dataset_counts"])
        print("targets:", payload["targets"])
    finally:
        session.close()


if __name__ == "__main__":
    main()
