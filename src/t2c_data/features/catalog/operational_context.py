from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from t2c_data.features.certification.api_support import resolve_certification_status_for_profile
from t2c_data.features.dashboard.executive_scoring import compute_priority_score, compute_profile_priority_score, recommended_actions, risk_label, risk_tone
from t2c_data.features.dashboard.profile_loader import load_table_profiles
from t2c_data.features.dashboard.support import TableProfile
from t2c_data.features.governance.settings import GovernanceSettingsSnapshot, get_governance_settings_snapshot
from t2c_data.features.governance.rules import (
    certification_next_review_at,
    certification_review_due,
    owner_review_due,
    owner_review_next_at,
    privacy_review_due,
    privacy_review_next_at,
    review_due_label,
)
from t2c_data.models.lineage import LineageAsset, LineageRelation
from t2c_data.models.metabase_impact import MetabaseImpactSnapshot
from t2c_data.models.search import SearchResultClick
from t2c_data.features.privacy_access.policy import sensitivity_label

_CERTIFICATION_STATUS_LABELS = {
    "not_assessed": "Não avaliada",
    "not_eligible": "Não elegível",
    "eligible": "Elegível",
    "in_review": "Em revisão",
    "certified": "Certificada",
    "rejected": "Recusada",
    "expired": "Vencida",
    "revalidation_pending": "Pendente de revalidação",
}


def build_asset_links(
    *,
    table_id: int,
    datasource_id: int,
    database_id: int | None,
    schema_id: int,
    data_owner_id: int | None,
    column_id: int | None = None,
) -> dict[str, str]:
    explorer = f"/explorer?tableId={table_id}"
    if column_id is not None:
        explorer = f"{explorer}&tab=columns&columnId={column_id}"
    asset_type = "column" if column_id is not None else "table"
    asset_id = column_id if column_id is not None else table_id
    return {
        "explorer": explorer,
        "change_management": f"/governance/change-management?assetType={asset_type}&assetId={asset_id}",
        "metabase_consumption": f"/explorer?tableId={table_id}&tab=consumption",
        "lineage": f"/explorer?tableId={table_id}&tab=lineage",
        "data_quality": f"/data-quality?tableId={table_id}",
        "incidents": f"/incidents/tickets?tableId={table_id}",
        "audit": f"/audit?entity_type=table&entity_id={table_id}",
        "certification": f"/certification?tableId={table_id}",
        "owners": f"/data-owners{f'?ownerId={data_owner_id}' if data_owner_id else ''}",
        "privacy": f"/privacy-access?tableId={table_id}",
        "datasource": f"/explorer?datasourceId={datasource_id}",
        "database": (
            f"/explorer?datasourceId={datasource_id}&databaseId={database_id}"
            if database_id is not None
            else f"/explorer?datasourceId={datasource_id}"
        ),
        "schema": (
            f"/explorer?datasourceId={datasource_id}&databaseId={database_id}&schemaId={schema_id}"
            if database_id is not None
            else f"/explorer?datasourceId={datasource_id}&schemaId={schema_id}"
        ),
    }


def build_contextual_actions(
    table: TableProfile,
    links: dict[str, str],
    settings_snapshot: GovernanceSettingsSnapshot | None = None,
) -> list[dict[str, str]]:
    actions: list[dict[str, str]] = []
    settings_snapshot = settings_snapshot or GovernanceSettingsSnapshot()
    high_usage_threshold = max(int(getattr(settings_snapshot, "governance_high_usage_click_threshold", 20) or 20), 1)
    high_usage = int(getattr(table, "search_clicks_30d", 0) or 0) >= high_usage_threshold
    owner_due = owner_review_due(table, settings_snapshot=settings_snapshot)
    privacy_due = privacy_review_due(table, settings_snapshot=settings_snapshot)
    certification_due = certification_review_due(table, settings_snapshot=settings_snapshot)

    if table.open_incidents > 0:
        actions.append(
            {
                "key": "open_incidents",
                "label": "Tratar incidente aberto",
                "description": "Existe incidente em aberto para este ativo. Vá direto para a fila já filtrada.",
                "href": links["incidents"],
                "category": "incident",
                "tone": "danger",
            }
        )
    elif table.dq_score is not None and table.dq_score < 70:
        actions.append(
            {
                "key": "create_incident",
                "label": "Abrir incidente operacional",
                "description": "A qualidade atual indica risco real de consumo. Registre e acompanhe o tratamento.",
                "href": f"{links['incidents']}&create=1",
                "category": "incident",
                "tone": "warning",
            }
        )

    if table.dq_score is None or table.dq_score < 90:
        actions.append(
            {
                "key": "review_dq",
                "label": "Revisar Data Quality",
                "description": "Abra o recorte de DQ do ativo para investigar score, histórico e regras.",
                "href": links["data_quality"],
                "category": "dq",
                "tone": "accent",
            }
        )
    if (table.certification_criticality or "").strip().lower() in {"high", "critical"} and int(
        getattr(table, "active_dq_rules_count", 0) or 0
    ) <= 0:
        actions.append(
            {
                "key": "create_dq_baseline",
                "label": "Criar DQ mínima",
                "description": "O ativo é crítico e ainda não possui regra DQ mínima ativa para sustentação contínua.",
                "href": links["data_quality"],
                "category": "dq",
                "tone": "danger",
            }
        )

    if not table.owner_defined:
        actions.append(
            {
                "key": "define_owner",
                "label": "Definir owner",
                "description": "O ativo ainda não tem responsável claro. Direcione a ownership antes de avançar.",
                "href": links["owners"],
                "category": "governance",
                "tone": "warning",
            }
        )
    elif owner_due:
        actions.append(
            {
                "key": "review_owner",
                "label": "Revisar owner",
                "description": "A confirmação de ownership está vencida e precisa ser revalidada.",
                "href": links["owners"],
                "category": "governance",
                "tone": "warning",
            }
        )

    if not table.classification_defined and high_usage:
        actions.append(
            {
                "key": "review_classification_high_usage",
                "label": "Classificar ativo de alto uso",
                "description": "O ativo tem uso elevado e ainda não possui classificação definida.",
                "href": links["privacy"],
                "category": "privacy",
                "tone": "warning",
            }
        )
    elif table.sensitivity_level and not table.classification_defined:
        actions.append(
            {
                "key": "review_classification",
                "label": "Revisar classificação",
                "description": "Há sensibilidade registrada sem contexto de governança suficiente. Revise a classificação.",
                "href": links["explorer"],
                "category": "privacy",
                "tone": "warning",
            }
        )

    if not table.description_complete or not table.dictionary_complete:
        actions.append(
            {
                "key": "complete_dictionary",
                "label": "Completar metadados",
                "description": "Atualize descrição e dicionário para reduzir ambiguidade de uso e suporte.",
                "href": links["explorer"],
                "category": "metadata",
                "tone": "neutral",
            }
        )

    effective_status = resolve_certification_status_for_profile(table)
    if effective_status == "revalidation_pending" or certification_due:
        actions.append(
            {
                "key": "revalidate_certification",
                "label": "Revalidar certificação",
                "description": "A certificação exige revisão ou revalidação para continuar vigente.",
                "href": links["certification"],
                "category": "certification",
                "tone": "warning",
            }
        )
    elif table.eligible_for_certification and effective_status != "certified":
        actions.append(
            {
                "key": "review_certification",
                "label": "Revisar certificação",
                "description": "O ativo já reúne sinais para avaliação. Leve a decisão de certificação adiante.",
                "href": links["certification"],
                "category": "certification",
                "tone": "accent",
            }
        )

    if privacy_due:
        actions.append(
            {
                "key": "review_privacy",
                "label": "Revisar privacidade",
                "description": "A classificação e os controles de privacidade precisam de confirmação periódica.",
                "href": links["privacy"],
                "category": "privacy",
                "tone": "warning",
            }
        )

    actions.append(
        {
            "key": "view_history",
            "label": "Ver histórico",
            "description": "Consulte a trilha de mudanças para entender contexto, autoria e decisões anteriores.",
            "href": links["audit"],
            "category": "audit",
            "tone": "neutral",
        }
    )
    return actions[:6]


def incident_origin_payload(source_type: str | None, evidence_json: dict | None) -> dict[str, object]:
    source = (source_type or "").strip().lower()
    evidence = evidence_json or {}
    if source == "dq_rule":
        return {
            "kind": "dq_rule",
            "label": "Data Quality · regra",
            "mode": evidence.get("origin_mode") or "automatic",
            "dq_rule_id": evidence.get("dq_rule_id"),
            "dq_rule_run_id": evidence.get("dq_rule_run_id"),
            "dq_run_id": evidence.get("dq_run_id"),
        }
    if source == "dq_profile":
        return {
            "kind": "dq_profile",
            "label": "Data Quality · profiling",
            "mode": evidence.get("origin_mode") or "automatic",
            "dq_rule_id": None,
            "dq_rule_run_id": None,
            "dq_run_id": evidence.get("dq_run_id"),
        }
    if source == "platform_ops":
        return {
            "kind": "platform_ops",
            "label": "Operação · pipeline",
            "mode": evidence.get("origin_mode") or "manual",
            "dq_rule_id": evidence.get("dq_rule_id"),
            "dq_rule_run_id": evidence.get("dq_rule_run_id"),
            "dq_run_id": evidence.get("dq_run_id"),
        }
    if source:
        return {
            "kind": source,
            "label": source.replace("_", " ").title(),
            "mode": evidence.get("origin_mode") or "manual",
            "dq_rule_id": evidence.get("dq_rule_id"),
            "dq_rule_run_id": evidence.get("dq_rule_run_id"),
            "dq_run_id": evidence.get("dq_run_id"),
        }
    return {
        "kind": "manual",
        "label": "Registro manual",
        "mode": evidence.get("origin_mode") or "manual",
        "dq_rule_id": evidence.get("dq_rule_id"),
        "dq_rule_run_id": evidence.get("dq_rule_run_id"),
        "dq_run_id": evidence.get("dq_run_id"),
    }


def incident_impact_payload(table: TableProfile | None, *, source_type: str | None = None) -> dict[str, str]:
    if table is None:
        return {
            "summary": "Incidente sem contexto adicional do ativo.",
            "operational": "Revise o item manualmente para entender o escopo do impacto.",
            "governance": "Não foi possível derivar owner, criticidade ou pendências do ativo.",
        }

    if (source_type or "").startswith("dq_"):
        summary = "Falha de Data Quality com impacto potencial no consumo e na confiança do ativo."
    elif table.critical_open_incidents > 0:
        summary = "Ativo já opera sob criticidade elevada e requer resposta coordenada."
    else:
        summary = "Incidente associado a ativo governado com necessidade de tratamento contextual."

    operational_bits = []
    if table.dq_score is not None and table.dq_score < 70:
        operational_bits.append(f"DQ em {round(table.dq_score, 1)} pts")
    if table.open_incidents > 1:
        operational_bits.append(f"{table.open_incidents} incidentes simultâneos")
    if table.critical_open_incidents > 0:
        operational_bits.append("há criticidade aberta")
    operational = ", ".join(operational_bits) or "Sem sinais operacionais adicionais além do incidente atual."

    governance_bits = []
    if not table.owner_defined:
        governance_bits.append("owner não definido")
    if not table.dictionary_complete:
        governance_bits.append("dicionário incompleto")
    if table.eligible_for_certification and resolve_certification_status_for_profile(table) != "certified":
        governance_bits.append("certificação pendente")
    if table.sensitivity_level:
        governance_bits.append(f"sensibilidade {sensitivity_label(table.sensitivity_level).lower()}")
    governance = ", ".join(governance_bits) or "Governança mínima atendida para este ativo."

    return {
        "summary": summary,
        "operational": operational,
        "governance": governance,
    }


def _metabase_dashboard_count(session: Session, *, table_id: int) -> int:
    snapshot = session.scalar(
        select(MetabaseImpactSnapshot)
        .where(MetabaseImpactSnapshot.table_id == table_id)
        .order_by(
            MetabaseImpactSnapshot.last_verified_at.desc().nullslast(),
            MetabaseImpactSnapshot.created_at.desc(),
            MetabaseImpactSnapshot.id.desc(),
        )
    )
    return int(snapshot.dashboard_count or 0) if snapshot is not None else 0


def _lineage_impact(session: Session, *, table_id: int) -> dict[str, int]:
    asset = session.scalar(
        select(LineageAsset).where(
            LineageAsset.catalog_table_id == table_id,
            LineageAsset.is_active.is_(True),
        )
    )
    if asset is None:
        return {"upstream": 0, "downstream": 0}
    upstream = int(
        session.scalar(
            select(func.count(LineageRelation.id)).where(
                LineageRelation.target_asset_id == asset.id,
                LineageRelation.is_active.is_(True),
            )
        )
        or 0
    )
    downstream = int(
        session.scalar(
            select(func.count(LineageRelation.id)).where(
                LineageRelation.source_asset_id == asset.id,
                LineageRelation.is_active.is_(True),
            )
        )
        or 0
    )
    return {"upstream": upstream, "downstream": downstream}


def _search_user_count(session: Session, *, table_id: int) -> int:
    since = datetime.now(timezone.utc) - timedelta(days=30)
    return int(
        session.scalar(
            select(func.count(func.distinct(SearchResultClick.user_id))).where(
                SearchResultClick.created_at >= since,
                SearchResultClick.entity_type == "table",
                SearchResultClick.entity_id == table_id,
                SearchResultClick.user_id.is_not(None),
            )
        )
        or 0
    )


def table_operational_context_payload(
    table: TableProfile,
    *,
    datasource_id: int,
    database_id: int | None,
    schema_id: int,
    column_id: int | None = None,
    settings_snapshot: GovernanceSettingsSnapshot | None = None,
    metabase_dashboards: int = 0,
    impacted_users: int | None = None,
    lineage_upstream: int = 0,
    lineage_downstream: int = 0,
) -> dict[str, object]:
    now = datetime.now(timezone.utc)
    recent_incident_count = table.open_incidents
    recent_occurrences = table.open_incidents
    risk_score, factors = compute_priority_score(
        table,
        recent_incident_count=recent_incident_count,
        recent_occurrences=recent_occurrences,
    )
    score = compute_profile_priority_score(
        table,
        risk_score,
        dashboards=metabase_dashboards,
        users=impacted_users,
        upstream=lineage_upstream,
        downstream=lineage_downstream,
    )
    certification_status = resolve_certification_status_for_profile(table)
    settings_snapshot = settings_snapshot or GovernanceSettingsSnapshot()
    owner_due = owner_review_due(table, settings_snapshot=settings_snapshot)
    privacy_due = privacy_review_due(table, settings_snapshot=settings_snapshot)
    certification_due = certification_review_due(table, settings_snapshot=settings_snapshot)
    links = build_asset_links(
        table_id=table.table_id,
        datasource_id=datasource_id,
        database_id=database_id,
        schema_id=schema_id,
        data_owner_id=table.data_owner_id,
        column_id=column_id,
    )
    return {
        "table_id": table.table_id,
        "table_name": table.table_name,
        "table_fqn": table.table_fqn,
        "datasource_id": datasource_id,
        "datasource_name": table.datasource_name,
        "database_id": database_id,
        "database_name": table.database_name,
        "schema_id": schema_id,
        "schema_name": table.schema_name,
        "owner_name": table.owner_name or "Não definido",
        "owner_defined": table.owner_defined,
        "data_owner_id": table.data_owner_id,
        "criticality_score": score,
        "criticality_label": risk_label(score),
        "criticality_tone": risk_tone(score),
        "dq_score": round(table.dq_score, 1) if table.dq_score is not None else None,
        "dq_status_label": "Não avaliado" if table.dq_score is None else f"{round(table.dq_score, 1)} pts",
        "certification_status": certification_status,
        "certification_status_label": _CERTIFICATION_STATUS_LABELS.get(certification_status, certification_status),
        "dictionary_complete": table.dictionary_complete,
        "description_complete": table.description_complete,
        "tags_count": table.tags_count,
        "terms_count": table.terms_count,
        "open_incidents": table.open_incidents,
        "critical_open_incidents": table.critical_open_incidents,
        "eligible_for_certification": table.eligible_for_certification,
        "sensitivity_level": table.sensitivity_level,
        "sensitivity_label": sensitivity_label(table.sensitivity_level),
        "owner_review_due": owner_due,
        "owner_review_next_at": owner_review_next_at(table, settings_snapshot=settings_snapshot),
        "privacy_review_due": privacy_due,
        "privacy_review_next_at": privacy_review_next_at(table, settings_snapshot=settings_snapshot),
        "certification_review_due": certification_due,
        "certification_next_review_at": certification_next_review_at(table),
        "review_due_label": review_due_label(
            owner_due=owner_due,
            privacy_due=privacy_due,
            certification_due=certification_due,
        ),
        "last_review_at": table.last_review_at or None,
        "last_updated_at": table.last_updated_at or None,
        "last_sync_at": table.last_sync_at or None,
        "recommended_actions": recommended_actions(table, recent_incident_count=recent_incident_count),
        "actions": build_contextual_actions(table, links, settings_snapshot=settings_snapshot),
        "links": links,
        "generated_at": now.isoformat(),
        "score_factors": [
            {
                "key": factor.key,
                "label": factor.label,
                "points": factor.points,
                "applied": factor.applied,
                "detail": factor.detail,
            }
            for factor in factors
        ],
    }


def load_table_operational_context(
    session: Session,
    *,
    table_id: int,
    datasource_id: int,
    database_id: int | None,
    schema_id: int,
    column_id: int | None = None,
) -> dict[str, object] | None:
    now = datetime.now(timezone.utc)
    settings_snapshot = get_governance_settings_snapshot(session)
    tables = load_table_profiles(session, now, table_ids=[table_id])
    table = next(iter(tables), None)
    if table is None:
        return None
    metabase_dashboards = _metabase_dashboard_count(session, table_id=table.table_id)
    lineage_impact = _lineage_impact(session, table_id=table.table_id)
    impacted_users = _search_user_count(session, table_id=table.table_id)
    return table_operational_context_payload(
        table,
        datasource_id=datasource_id,
        database_id=database_id,
        schema_id=schema_id,
        column_id=column_id,
        settings_snapshot=settings_snapshot,
        metabase_dashboards=metabase_dashboards,
        impacted_users=impacted_users,
        lineage_upstream=lineage_impact["upstream"],
        lineage_downstream=lineage_impact["downstream"],
    )


__all__ = [
    "build_asset_links",
    "build_contextual_actions",
    "incident_impact_payload",
    "incident_origin_payload",
    "load_table_operational_context",
    "table_operational_context_payload",
]
