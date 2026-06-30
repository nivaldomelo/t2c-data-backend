from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from t2c_data.features.dashboard.support import TableProfile
from t2c_data.features.dashboard.profile_loader import load_table_profiles
from t2c_data.models.contracts import DataContract
from t2c_data.models.dq import DQRule, DQRuleRun


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _tone_for_score(score: float | None) -> str:
    if score is None:
        return "neutral"
    if score >= 90:
        return "success"
    if score >= 75:
        return "accent"
    if score >= 60:
        return "warning"
    return "danger"


def _group_label(value: str | None, fallback: str) -> tuple[str, str]:
    raw = str(value or "").strip()
    if not raw:
        return "__none__", fallback
    return raw, raw


def _profile_group_value(table: TableProfile, group_kind: str) -> str:
    if group_kind == "domain":
        return str(table.domain_name or "").strip().lower()
    if group_kind == "owner":
        return str(table.owner_name or "").strip().lower()
    if group_kind == "criticality":
        return str(table.certification_criticality or "").strip().lower()
    return ""


def _risk_reasons(table: TableProfile, contract_status: str | None, contract_validation_status: str | None) -> list[str]:
    reasons: list[str] = []
    if table.dq_score is not None and table.dq_score < 70:
        reasons.append("DQ abaixo do mínimo")
    elif table.dq_score is not None and table.dq_score < 90:
        reasons.append("DQ com ressalvas")
    if table.active_dq_violation:
        reasons.append("violação ativa de DQ")
    if table.critical_open_incidents > 0:
        reasons.append("incidente crítico em aberto")
    elif table.open_incidents > 0:
        reasons.append("incidente aberto")
    if table.active_dq_rules_count <= 0:
        reasons.append("sem regra DQ ativa")
    if contract_status and contract_status not in {"published", "active", "valid", "approved"}:
        reasons.append(f"contrato {contract_status}")
    if contract_validation_status and contract_validation_status != "passed":
        reasons.append(f"validação {contract_validation_status}")
    return reasons


def _build_group_buckets(
    tables: list[TableProfile],
    *,
    key_getter,
    label_fallback: str,
    group_kind: str,
) -> list[dict[str, object]]:
    buckets: dict[str, list[TableProfile]] = defaultdict(list)
    for table in tables:
        key, label = _group_label(key_getter(table), label_fallback)
        buckets[f"{group_kind}::{key}::{label}"].append(table)

    items: list[dict[str, object]] = []
    for bucket_key, bucket_tables in buckets.items():
        _kind, key, label = bucket_key.split("::", 2)
        dq_values = [table.dq_score for table in bucket_tables if table.dq_score is not None]
        trust_values = [table.trust_score for table in bucket_tables if table.trust_score is not None]
        readiness_values = [table.readiness_score for table in bucket_tables]
        tables_with_rules = [table for table in bucket_tables if table.active_dq_rules_count > 0]
        tables_without_rules = [table for table in bucket_tables if table.active_dq_rules_count <= 0]
        critical_without_rules = [
            table
            for table in bucket_tables
            if table.active_dq_rules_count <= 0 and (table.certification_criticality or "").strip().lower() in {"high", "critical"}
        ]
        items.append(
            {
                "key": key,
                "label": label,
                "scope_kind": group_kind,
                "scope_value": key,
                "count": len(bucket_tables),
                "avg_dq_score": round(sum(dq_values) / len(dq_values), 1) if dq_values else None,
                "avg_trust_score": round(sum(trust_values) / len(trust_values), 1) if trust_values else None,
                "avg_readiness_score": round(sum(readiness_values) / len(readiness_values), 1) if readiness_values else None,
                "rules_coverage_pct": round((len(tables_with_rules) / len(bucket_tables)) * 100.0, 1) if bucket_tables else None,
                "contract_coverage_pct": None,
                "open_incidents": sum(table.open_incidents for table in bucket_tables),
                "critical_incidents": sum(table.critical_open_incidents for table in bucket_tables),
                "tables_without_rules": len(tables_without_rules),
                "critical_tables_without_rules": len(critical_without_rules),
                "contract_breaking": 0,
                "contract_warning": 0,
                "tone": _tone_for_score(round(sum(dq_values) / len(dq_values), 1) if dq_values else None),
            }
    )
    items.sort(key=lambda item: (item["avg_dq_score"] is None, item["avg_dq_score"] or 0.0, item["count"]), reverse=False)
    return items


def _latest_contract_rows(db: Session) -> list[tuple[int, str | None, str | None, int | None]]:
    ranked = (
        select(
            DataContract.table_id.label("table_id"),
            DataContract.status.label("status"),
            DataContract.last_validation_status.label("last_validation_status"),
            DataContract.last_validation_issues.label("last_validation_issues"),
            func.row_number()
            .over(partition_by=DataContract.table_id, order_by=(DataContract.version.desc(), DataContract.id.desc()))
            .label("rn"),
        ).subquery()
    )
    rows = db.execute(
        select(
            ranked.c.table_id,
            ranked.c.status,
            ranked.c.last_validation_status,
            ranked.c.last_validation_issues,
        ).where(ranked.c.rn == 1)
    ).all()
    return [
        (
            int(row.table_id),
            str(row.status) if row.status is not None else None,
            str(row.last_validation_status) if row.last_validation_status is not None else None,
            int(row.last_validation_issues) if row.last_validation_issues is not None else None,
        )
        for row in rows
    ]


def _latest_failing_rules(db: Session) -> list[dict[str, object]]:
    ranked = (
        select(
            DQRuleRun.rule_id.label("rule_id"),
            DQRuleRun.status.label("status"),
            DQRuleRun.violations_count.label("violations_count"),
            DQRuleRun.created_at.label("run_at"),
            func.row_number()
            .over(partition_by=DQRuleRun.rule_id, order_by=(DQRuleRun.created_at.desc(), DQRuleRun.id.desc()))
            .label("rn"),
        ).subquery()
    )
    rows = db.execute(
        select(
            DQRule.id,
            DQRule.name,
            DQRule.table_fqn,
            DQRule.severity,
            ranked.c.status,
            ranked.c.violations_count,
            ranked.c.run_at,
        )
        .join(ranked, ranked.c.rule_id == DQRule.id)
        .where(ranked.c.rn == 1, DQRule.is_active.is_(True), ranked.c.status == "fail")
        .order_by(ranked.c.run_at.desc())
        .limit(8)
    ).all()
    items: list[dict[str, object]] = []
    for row in rows:
        severity = str(row.severity or "medium").lower()
        tone = "danger" if severity in {"critical", "high"} else "warning" if severity == "medium" else "accent"
        items.append(
            {
                "key": str(row.id),
                "name": str(row.name),
                "table_fqn": str(row.table_fqn),
                "severity": severity,
                "status": str(row.status or "fail"),
                "violations_count": int(row.violations_count or 0),
                "last_run_at": row.run_at,
                "open_incident_id": None,
                "tone": tone,
            }
        )
    return items


def build_dq_platform_scorecard_summary(
    db: Session,
    *,
    current_user=None,
    scope_domain: str | None = None,
    scope_owner: str | None = None,
    scope_criticality: str | None = None,
) -> dict[str, object]:
    now = _now()
    profiles = load_table_profiles(db, now, current_user=current_user)
    if scope_domain:
        profiles = [profile for profile in profiles if (profile.domain_name or "").strip().lower() == scope_domain.strip().lower()]
    if scope_owner:
        profiles = [profile for profile in profiles if (profile.owner_name or "").strip().lower() == scope_owner.strip().lower()]
    if scope_criticality:
        profiles = [
            profile
            for profile in profiles
            if (profile.certification_criticality or "").strip().lower() == scope_criticality.strip().lower()
        ]

    table_count = len(profiles)
    dq_values = [profile.dq_score for profile in profiles if profile.dq_score is not None]
    trust_values = [profile.trust_score for profile in profiles if profile.trust_score is not None]
    readiness_values = [profile.readiness_score for profile in profiles]
    documentation_values = [profile.documentation_score for profile in profiles]
    active_rules_tables = [profile for profile in profiles if profile.active_dq_rules_count > 0]
    tables_without_rules = [profile for profile in profiles if profile.active_dq_rules_count <= 0]
    critical_tables_without_rules = [
        profile
        for profile in profiles
        if profile.active_dq_rules_count <= 0 and (profile.certification_criticality or "").strip().lower() in {"high", "critical"}
    ]
    sensitive_tables_without_rules = [
        profile
        for profile in profiles
        if profile.active_dq_rules_count <= 0 and (profile.has_sensitive_personal_data or profile.has_personal_data)
    ]
    high_risk_tables = [
        profile
        for profile in profiles
        if (
            (profile.dq_score is not None and profile.dq_score < 70)
            or profile.critical_open_incidents > 0
            or profile.active_dq_violation
            or profile.recent_dq_failure_runs_30d > 0
        )
    ]

    latest_contract_rows = _latest_contract_rows(db)
    latest_contract_map = {table_id: {"status": status, "last_validation_status": validation_status, "last_validation_issues": issues} for table_id, status, validation_status, issues in latest_contract_rows}
    contracts_total = len(latest_contract_rows)
    published_contracts = sum(1 for _table_id, status, _validation_status, _issues in latest_contract_rows if (status or "").lower() == "published")
    contracts_with_validation = sum(1 for _table_id, _status, validation_status, _issues in latest_contract_rows if validation_status is not None)
    failed_contract_validations = sum(1 for _table_id, _status, validation_status, _issues in latest_contract_rows if (validation_status or "").lower() == "failed")
    breaking_contracts = failed_contract_validations
    warning_contracts = sum(
        1
        for _table_id, status, validation_status, _issues in latest_contract_rows
        if (status or "").lower() in {"draft", "in_review"} or validation_status is None
    )
    active_rules_total = int(
        db.scalar(select(func.count(DQRule.id)).where(DQRule.is_active.is_(True))) or 0
    )
    failing_rules = _latest_failing_rules(db)

    rule_coverage_pct = round((len(active_rules_tables) / table_count) * 100.0, 1) if table_count else 0.0
    contract_coverage_pct = round((contracts_total / table_count) * 100.0, 1) if table_count else 0.0
    totals = {
        "tables": table_count,
        "with_metrics": len(dq_values),
        "avg_dq_score": round(sum(dq_values) / len(dq_values), 1) if dq_values else None,
        "avg_trust_score": round(sum(trust_values) / len(trust_values), 1) if trust_values else None,
        "avg_readiness_score": round(sum(readiness_values) / len(readiness_values), 1) if readiness_values else None,
        "avg_documentation_score": round(sum(documentation_values) / len(documentation_values), 1) if documentation_values else None,
        "active_rules": active_rules_total,
        "tables_with_rules": len(active_rules_tables),
        "tables_without_rules": len(tables_without_rules),
        "critical_tables_without_rules": len(critical_tables_without_rules),
        "sensitive_tables_without_rules": len(sensitive_tables_without_rules),
        "contracts_total": contracts_total,
        "contracts_with_validation": contracts_with_validation,
        "failed_contract_validations": failed_contract_validations,
        "contract_coverage_pct": contract_coverage_pct,
        "breaking_contracts": breaking_contracts,
        "warning_contracts": warning_contracts,
        "high_risk_tables": len(high_risk_tables),
    }

    by_domain = _build_group_buckets(
        profiles,
        key_getter=lambda profile: profile.domain_name,
        label_fallback="Sem domínio",
        group_kind="domain",
    )
    by_owner = _build_group_buckets(
        profiles,
        key_getter=lambda profile: profile.owner_name,
        label_fallback="Sem owner",
        group_kind="owner",
    )
    by_criticality = _build_group_buckets(
        profiles,
        key_getter=lambda profile: profile.certification_criticality,
        label_fallback="Sem criticidade",
        group_kind="criticality",
    )

    for bucket in by_domain + by_owner + by_criticality:
        group_kind = str(bucket.get("scope_kind") or "")
        scope_value = str(bucket.get("scope_value") or "").strip().lower()
        matching_profiles = [
            profile
            for profile in profiles
            if _profile_group_value(profile, group_kind) == scope_value
        ]
        bucket["contract_coverage_pct"] = round(
            (sum(1 for profile in matching_profiles if profile.table_id in latest_contract_map) / len(matching_profiles)) * 100.0,
            1,
        ) if matching_profiles else None
        bucket["contract_breaking"] = sum(
            1
            for profile in matching_profiles
            if profile.table_id in latest_contract_map and (latest_contract_map[profile.table_id]["last_validation_status"] or "").lower() == "failed"
        )
        bucket["contract_warning"] = sum(
            1
            for profile in matching_profiles
            if profile.table_id in latest_contract_map
            and (
                (latest_contract_map[profile.table_id]["status"] or "").lower() in {"draft", "in_review"}
                or latest_contract_map[profile.table_id]["last_validation_status"] is None
            )
        )

    top_risks = sorted(
        profiles,
        key=lambda profile: (
            profile.dq_score is None,
            profile.dq_score or 0.0,
            -(profile.critical_open_incidents * 3 + profile.open_incidents + profile.active_dq_violation_count),
            profile.table_fqn,
        ),
    )[:8]
    top_risk_items: list[dict[str, object]] = []
    for profile in top_risks:
        contract_row = latest_contract_map.get(profile.table_id)
        top_risk_items.append(
            {
                "table_id": profile.table_id,
                "table_fqn": profile.table_fqn,
                "table_name": profile.table_name,
                "domain_name": profile.domain_name,
                "owner_name": profile.owner_name,
                "dq_score": round(profile.dq_score, 1) if profile.dq_score is not None else None,
                "trust_score": profile.trust_score,
                "readiness_score": profile.readiness_score,
                "documentation_score": profile.documentation_score,
                "certification_status": profile.certification_status,
                "criticality": profile.certification_criticality,
                "active_rules": profile.active_dq_rules_count,
                "open_incidents": profile.open_incidents,
                "critical_open_incidents": profile.critical_open_incidents,
                "contract_status": contract_row["status"] if contract_row else None,
                "contract_validation_status": contract_row["last_validation_status"] if contract_row else None,
                "contract_issues": contract_row["last_validation_issues"] if contract_row else None,
                "rule_coverage_pct": 100.0 if profile.active_dq_rules_count > 0 else 0.0,
                "trust_label": profile.trust_label,
                "trust_tone": profile.trust_tone,
                "reasons": _risk_reasons(profile, contract_row["status"] if contract_row else None, contract_row["last_validation_status"] if contract_row else None),
            }
        )

    return {
        "generated_at": now,
        "scope_domain": scope_domain,
        "scope_owner": scope_owner,
        "scope_criticality": scope_criticality,
        "totals": totals,
        "by_domain": by_domain,
        "by_owner": by_owner,
        "by_criticality": by_criticality,
        "failing_rules": failing_rules,
        "top_risks": top_risk_items,
    }
