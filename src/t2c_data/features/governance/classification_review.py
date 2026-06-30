from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Iterable

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from t2c_data.features.catalog.operational_context import build_asset_links, build_contextual_actions
from t2c_data.features.certification.api_support import certification_status_label
from t2c_data.features.dashboard.profile_loader import load_table_profiles
from t2c_data.features.governance.scoring import build_governance_score_for_profile
from t2c_data.features.governance.recommendations import refresh_governance_recommendations
from t2c_data.features.governance.settings import get_governance_settings_snapshot
from t2c_data.features.governance.trust_score import build_trust_score_for_profile
from t2c_data.features.privacy_access.policy import sensitivity_label
from t2c_data.features.tags.api_support import load_entity_tag_contexts
from t2c_data.features.tags.intelligence import load_pending_tag_intelligence_events
from t2c_data.models.catalog import ColumnEntity
from t2c_data.models.glossary import GlossaryAssignment, GlossaryTerm
from t2c_data.models.tag import TagIntelligenceEvent
from t2c_data.schemas.governance import (
    ClassificationReviewFilterOptionOut,
    ClassificationReviewFiltersOut,
    ClassificationReviewItemOut,
    ClassificationReviewOut,
    ClassificationReviewSignalOut,
    ClassificationReviewSummaryOut,
    ClassificationReviewTermPreviewOut,
)
from t2c_data.services.audit import write_audit_log_sync

TABLE_ENTITY_TYPE = "table"
COLUMN_ENTITY_TYPE = "column"
TABLE_TAG_INHERITANCE_LABELS = {
    "PII": "Contém PII",
    "Sensível": "Contém Dados Sensíveis",
    "Coluna Crítica": "Possui Coluna Crítica",
    "Campo Obrigatório": "Requer Governança",
    "Campo Auditável": "Requer Governança",
}
OPEN_REVIEW_STATUSES = {"pending_review", "suggested", "gap", "conflict"}


def _normalized(value: str | None) -> str:
    return (value or "").strip().lower()


def _filter_options(values: Iterable[str], *, labels: dict[str, str] | None = None) -> list[ClassificationReviewFilterOptionOut]:
    normalized = [value for value in dict.fromkeys(v for v in values if v)]
    return [
        ClassificationReviewFilterOptionOut(
            value=value,
            label=(labels or {}).get(value, value.replace("_", " ").title()),
        )
        for value in sorted(normalized, key=str.lower)
    ]


def _term_preview(term: GlossaryTerm) -> ClassificationReviewTermPreviewOut:
    return ClassificationReviewTermPreviewOut(
        id=int(term.id),
        name=term.name,
        definition=term.definition,
        steward=term.steward,
    )


def _load_table_terms_map(session: Session, table_ids: list[int]) -> dict[int, list[ClassificationReviewTermPreviewOut]]:
    if not table_ids:
        return {}
    rows = session.execute(
        select(GlossaryAssignment.entity_id, GlossaryTerm)
        .join(GlossaryTerm, GlossaryTerm.id == GlossaryAssignment.term_id)
        .where(GlossaryAssignment.entity_type == TABLE_ENTITY_TYPE, GlossaryAssignment.entity_id.in_(table_ids))
        .order_by(GlossaryAssignment.entity_id, GlossaryTerm.name)
    ).all()
    grouped: dict[int, list[ClassificationReviewTermPreviewOut]] = defaultdict(list)
    for entity_id, term in rows:
        grouped[int(entity_id)].append(_term_preview(term))
    return grouped


def _profile_signals(profile) -> list[ClassificationReviewSignalOut]:
    signals: list[ClassificationReviewSignalOut] = []
    signals.append(
        ClassificationReviewSignalOut(
            key="owner",
            label="Owner",
            value=profile.owner_name or "Não definido",
            tone="warning" if not profile.owner_defined else "success",
            detail="O ativo precisa de owner claro para consolidar governança." if not profile.owner_defined else "Owner já definido.",
        )
    )
    signals.append(
        ClassificationReviewSignalOut(
            key="dictionary",
            label="Dicionário",
            value="Completo" if profile.dictionary_complete else "Pendente",
            tone="success" if profile.dictionary_complete else "warning",
            detail="A cobertura do dicionário está completa." if profile.dictionary_complete else "Ainda há colunas sem documentação suficiente.",
        )
    )
    signals.append(
        ClassificationReviewSignalOut(
            key="tags",
            label="Tags",
            value=str(profile.tags_count),
            tone="success" if profile.tags_count > 0 else "warning",
            detail=f"{profile.tags_count} tag(s) já aplicadas." if profile.tags_count > 0 else "Nenhuma tag aplicada ainda.",
        )
    )
    signals.append(
        ClassificationReviewSignalOut(
            key="terms",
            label="Termos",
            value=str(profile.terms_count),
            tone="success" if profile.terms_count > 0 else "warning",
            detail=f"{profile.terms_count} termo(s) vinculados." if profile.terms_count > 0 else "Nenhum termo vinculado.",
        )
    )
    signals.append(
        ClassificationReviewSignalOut(
            key="sensitivity",
            label="Sensibilidade",
            value=sensitivity_label(profile.sensitivity_level),
            tone="warning"
            if profile.sensitivity_level in {"confidential", "restricted", "personal_data"}
            else "neutral",
            detail="O ativo tem contexto sensível e pede revisão de classificação."
            if profile.sensitivity_level in {"confidential", "restricted", "personal_data"}
            else "Sem contexto sensível relevante.",
        )
    )
    classification_label = "Classificada" if profile.classification_defined else "Não classificada"
    signals.append(
        ClassificationReviewSignalOut(
            key="classification",
            label="Classificação",
            value=classification_label,
            tone="accent" if profile.classification_defined else "warning",
            detail="Há sinais de classificação/criticidade já consolidados no ativo."
            if profile.classification_defined
            else "Ainda não há classificação consolidada.",
        )
    )
    if profile.active_dq_violation:
        signals.append(
            ClassificationReviewSignalOut(
                key="dq",
                label="DQ ativa",
                value=", ".join(profile.active_dq_rule_names[:3]) if profile.active_dq_rule_names else "Violação ativa",
                tone="danger",
                detail="Há violação ativa de Data Quality no ativo.",
            )
        )
    if profile.critical_open_incidents > 0:
        signals.append(
            ClassificationReviewSignalOut(
                key="incidents",
                label="Incidentes críticos",
                value=str(profile.critical_open_incidents),
                tone="danger",
                detail="Incidentes críticos abertos afetam a confiança operacional.",
            )
        )
    return signals


def _event_inheritance_label(tag_name: str | None) -> str | None:
    if not tag_name:
        return None
    return TABLE_TAG_INHERITANCE_LABELS.get(tag_name)


def _dedupe_table_ids(table_ids: list[int]) -> list[int]:
    return list(dict.fromkeys(int(table_id) for table_id in table_ids if table_id is not None))


def _is_open_review_item(item: dict[str, object]) -> bool:
    return str(item.get("review_status") or "") in OPEN_REVIEW_STATUSES and item.get("kind") in {
        "suggestion",
        "gap",
        "conflict",
    }


def _build_suggestion_item(
    event: dict[str, object],
    *,
    profile,
    table_tags: list,
    column_tags: list,
    table_terms: list[ClassificationReviewTermPreviewOut],
    governance_score: dict[str, object],
    trust_score: dict[str, object],
) -> dict[str, object] | None:
    table_id = int(event["table_id"]) if event.get("table_id") is not None else None
    datasource_id = int(event["datasource_id"]) if event.get("datasource_id") is not None else None
    schema_name = str(event.get("schema_name") or "")
    table_name = str(event.get("table_name") or "")
    if table_id is None or not schema_name or not table_name or profile is None:
        return None

    column_id = int(event["column_id"]) if event.get("column_id") is not None else None
    column_name = str(event.get("column_name") or "") or None
    entity_level = "column" if column_id is not None else "table"
    entity_type = entity_level
    entity_tags = column_tags if entity_level == "column" else table_tags
    current_tags = entity_tags or table_tags
    suggested_tag_name = str(event.get("tag_name") or "")
    suggested_tag_slug = str(event.get("tag_slug") or "")
    confidence_score = int(event.get("confidence_score") or 0)
    score_value = int(governance_score["score"])
    risk_score = min(
        100,
        max(0, (100 - confidence_score) + max(0, 100 - score_value) // 2),
    )
    if profile.active_dq_violation:
        risk_score = min(100, risk_score + 10)
    if not profile.owner_defined:
        risk_score = min(100, risk_score + 8)
    if not profile.classification_defined:
        risk_score = min(100, risk_score + 6)
    if profile.tags_count <= 0:
        risk_score = min(100, risk_score + 4)

    signals = _profile_signals(profile)
    inheritance_label = _event_inheritance_label(suggested_tag_name)
    if inheritance_label and entity_level == "column":
        table_tag_names = {tag.name for tag in table_tags}
        signals.append(
            ClassificationReviewSignalOut(
                key="table_inheritance",
                label="Herança para tabela",
                value=inheritance_label,
                tone="accent",
                detail=(
                    f"A coluna sugere derivação automática para a tabela como \"{inheritance_label}\"."
                    if inheritance_label not in table_tag_names
                    else f"A tabela já possui a derivação \"{inheritance_label}\"."
                ),
            )
        )

    return {
        "key": f"suggestion:{event['id']}",
        "kind": "suggestion",
        "entity_level": entity_level,
        "entity_type": entity_type,
        "table_id": table_id,
        "table_name": table_name,
        "table_fqn": str(event.get("table_fqn") or table_name),
        "column_id": column_id,
        "column_name": column_name,
        "datasource_id": datasource_id or profile.datasource_id,
        "datasource_name": str(event.get("datasource_name") or profile.datasource_name),
        "database_name": str(event.get("database_name") or profile.database_name),
        "schema_name": schema_name,
        "domain_name": profile.domain_name,
        "owner_name": profile.owner_name,
        "certification_status": profile.certification_status,
        "certification_status_label": certification_status_label(profile.certification_status),
        "sensitivity_level": profile.sensitivity_level,
        "sensitivity_label": sensitivity_label(profile.sensitivity_level),
        "owner_defined": profile.owner_defined,
        "description_complete": profile.description_complete,
        "dictionary_complete": profile.dictionary_complete,
        "classification_defined": profile.classification_defined,
        "total_columns": profile.total_columns,
        "classified_columns": int(getattr(profile, "classified_columns", 0) or 0),
        "personal_classified_columns": int(getattr(profile, "personal_classified_columns", 0) or 0),
        "sensitive_classified_columns": int(getattr(profile, "sensitive_classified_columns", 0) or 0),
        "financial_classified_columns": int(getattr(profile, "financial_classified_columns", 0) or 0),
        "operational_classified_columns": int(getattr(profile, "operational_classified_columns", 0) or 0),
        "classification_coverage_pct": float(getattr(profile, "classification_coverage_pct", 0.0) or 0.0),
        "column_classification_reviewed_at": getattr(profile, "column_classification_reviewed_at", None),
        "tags_count": profile.tags_count,
        "terms_count": profile.terms_count,
        "readiness_score": profile.readiness_score,
        "governance_score": score_value,
        "governance_label": governance_score["label"],
        "governance_tone": governance_score["tone"],
        "trust_score": int(trust_score["score"]),
        "trust_label": str(trust_score["label"]),
        "trust_tone": str(trust_score["tone"]),
        "dq_score": profile.dq_score,
        "has_personal_data": profile.has_personal_data,
        "has_sensitive_personal_data": profile.has_sensitive_personal_data,
        "active_dq_violation": profile.active_dq_violation,
        "active_dq_rule_names": list(profile.active_dq_rule_names or []),
        "critical_open_incidents": profile.critical_open_incidents,
        "suggestion_tag_id": int(event.get("tag_id") or 0),
        "suggestion_tag_name": suggested_tag_name,
        "suggestion_tag_slug": suggested_tag_slug,
        "confidence_score": confidence_score,
        "inference_source": str(event.get("inference_source") or ""),
        "inference_reason": str(event.get("inference_reason") or ""),
        "applied_automatically": bool(event.get("applied_automatically")),
        "review_status": str(event.get("review_status") or "suggested"),
        "current_tags": current_tags,
        "table_tags": table_tags,
        "column_tags": column_tags,
        "current_terms": table_terms,
        "signals": signals,
        "recommended_actions": [
            "Aplicar sugestão",
            "Bloquear sugestão",
            "Abrir no Explorer",
        ],
        "links": build_asset_links(
            table_id=profile.table_id,
            datasource_id=profile.datasource_id,
            database_id=profile.database_id,
            schema_id=profile.schema_id,
            data_owner_id=profile.data_owner_id,
            column_id=column_id,
        ),
        "created_at": event.get("created_at") or profile.last_updated_at or datetime.now(timezone.utc),
        "updated_at": event.get("updated_at") or profile.last_updated_at or datetime.now(timezone.utc),
        "reviewed_at": event.get("reviewed_at"),
        "risk_score": risk_score,
    }


def _build_gap_item(
    profile,
    *,
    table_tags: list,
    table_terms: list[ClassificationReviewTermPreviewOut],
    governance_score: dict[str, object],
    trust_score: dict[str, object],
) -> dict[str, object] | None:
    if profile.classification_defined:
        return None
    if not (
        profile.sensitivity_level
        or profile.has_personal_data
        or profile.has_sensitive_personal_data
        or profile.tags_count <= 0
        or profile.terms_count <= 0
        or profile.active_dq_violation
        or profile.critical_open_incidents > 0
    ):
        return None

    score_value = int(governance_score["score"])
    risk_score = min(
        100,
        max(0, 100 - score_value + (10 if profile.active_dq_violation else 0) + (8 if not profile.owner_defined else 0)),
    )
    if profile.has_sensitive_personal_data:
        risk_score = min(100, risk_score + 8)
    elif profile.has_personal_data:
        risk_score = min(100, risk_score + 5)

    signals = _profile_signals(profile)
    if profile.tags_count <= 0:
        signals.append(
            ClassificationReviewSignalOut(
                key="tags_gap",
                label="Tags estratégicas",
                value="Sem tags",
                tone="warning",
                detail="A tabela ainda não possui tags suficientes para orientar a governança.",
            )
        )
    if profile.terms_count <= 0:
        signals.append(
            ClassificationReviewSignalOut(
                key="terms_gap",
                label="Termos",
                value="Sem termos",
                tone="warning",
                detail="Ainda não há termos de glossário associados ao ativo.",
            )
        )

    review_status = "conflict" if (profile.active_dq_violation or profile.critical_open_incidents > 0) else "gap"
    kind = "conflict" if review_status == "conflict" else "gap"
    return {
        "key": f"gap:{profile.table_id}",
        "kind": kind,
        "entity_level": "table",
        "entity_type": "table",
        "table_id": profile.table_id,
        "table_name": profile.table_name,
        "table_fqn": profile.table_fqn,
        "column_id": None,
        "column_name": None,
        "datasource_id": profile.datasource_id,
        "datasource_name": profile.datasource_name,
        "database_name": profile.database_name,
        "schema_name": profile.schema_name,
        "domain_name": profile.domain_name,
        "owner_name": profile.owner_name,
        "certification_status": profile.certification_status,
        "certification_status_label": certification_status_label(profile.certification_status),
        "sensitivity_level": profile.sensitivity_level,
        "sensitivity_label": sensitivity_label(profile.sensitivity_level),
        "owner_defined": profile.owner_defined,
        "description_complete": profile.description_complete,
        "dictionary_complete": profile.dictionary_complete,
        "classification_defined": profile.classification_defined,
        "total_columns": profile.total_columns,
        "classified_columns": int(getattr(profile, "classified_columns", 0) or 0),
        "personal_classified_columns": int(getattr(profile, "personal_classified_columns", 0) or 0),
        "sensitive_classified_columns": int(getattr(profile, "sensitive_classified_columns", 0) or 0),
        "financial_classified_columns": int(getattr(profile, "financial_classified_columns", 0) or 0),
        "operational_classified_columns": int(getattr(profile, "operational_classified_columns", 0) or 0),
        "classification_coverage_pct": float(getattr(profile, "classification_coverage_pct", 0.0) or 0.0),
        "column_classification_reviewed_at": getattr(profile, "column_classification_reviewed_at", None),
        "tags_count": profile.tags_count,
        "terms_count": profile.terms_count,
        "readiness_score": profile.readiness_score,
        "governance_score": score_value,
        "governance_label": governance_score["label"],
        "governance_tone": governance_score["tone"],
        "trust_score": int(trust_score["score"]),
        "trust_label": str(trust_score["label"]),
        "trust_tone": str(trust_score["tone"]),
        "dq_score": profile.dq_score,
        "has_personal_data": profile.has_personal_data,
        "has_sensitive_personal_data": profile.has_sensitive_personal_data,
        "active_dq_violation": profile.active_dq_violation,
        "active_dq_rule_names": list(profile.active_dq_rule_names or []),
        "critical_open_incidents": profile.critical_open_incidents,
        "suggestion_tag_id": None,
        "suggestion_tag_name": None,
        "suggestion_tag_slug": None,
        "confidence_score": None,
        "inference_source": "governance_gap",
        "inference_reason": "O ativo apresenta lacunas de classificação ou contexto governado.",
        "applied_automatically": None,
        "review_status": review_status,
        "current_tags": table_tags,
        "table_tags": table_tags,
        "column_tags": [],
        "current_terms": table_terms,
        "signals": signals,
        "recommended_actions": [
            action["label"]
            for action in build_contextual_actions(
                profile,
                build_asset_links(
                    table_id=profile.table_id,
                    datasource_id=profile.datasource_id,
                    database_id=profile.database_id,
                    schema_id=profile.schema_id,
                    data_owner_id=profile.data_owner_id,
                ),
            )
        ],
        "links": build_asset_links(
            table_id=profile.table_id,
            datasource_id=profile.datasource_id,
            database_id=profile.database_id,
            schema_id=profile.schema_id,
            data_owner_id=profile.data_owner_id,
        ),
        "created_at": profile.last_updated_at or datetime.now(timezone.utc),
        "updated_at": profile.last_updated_at or datetime.now(timezone.utc),
        "reviewed_at": None,
        "risk_score": risk_score,
    }


def _build_summary(items: list[dict[str, object]], *, reviewed_recently: int) -> ClassificationReviewSummaryOut:
    pending_reviews = sum(1 for item in items if _is_open_review_item(item))
    high_confidence_reviews = sum(
        1
        for item in items
        if item["kind"] == "suggestion"
        and int(item.get("confidence_score") or 0) >= 90
        and str(item.get("review_status") or "") in {"pending_review", "suggested"}
    )
    probable_pii = sum(
        1
        for item in items
        if bool(item.get("has_personal_data"))
        or str(item.get("suggestion_tag_name") or "").strip().lower() in {"pii", "contém pii", "contém pii"}
    )
    probable_sensitive = sum(
        1
        for item in items
        if bool(item.get("has_sensitive_personal_data"))
        or str(item.get("suggestion_tag_name") or "").strip().lower() in {"sensível", "contém dados sensíveis"}
    )
    conflicts = sum(1 for item in items if item["kind"] == "conflict")
    critical_columns = sum(
        1
        for item in items
        if item.get("entity_level") == "column"
        and str(item.get("suggestion_tag_name") or "").strip().lower() in {"coluna crítica", "coluna critica"}
    )
    inheritance_pending = sum(
        1
        for item in items
        if item.get("kind") == "suggestion"
        and item.get("entity_level") == "column"
        and str(item.get("suggestion_tag_name") or "").strip() in {"PII", "Sensível", "Coluna Crítica"}
        and not any(tag.name == TABLE_TAG_INHERITANCE_LABELS.get(str(item.get("suggestion_tag_name") or "")) for tag in item.get("table_tags") or [])
    )
    trust_at_risk = sum(1 for item in items if int(item.get("trust_score") or 0) < 60)
    return ClassificationReviewSummaryOut(
        pending_reviews=pending_reviews,
        high_confidence_reviews=high_confidence_reviews,
        trust_at_risk=trust_at_risk,
        probable_pii=probable_pii,
        probable_sensitive=probable_sensitive,
        conflicts=conflicts,
        critical_columns=critical_columns,
        inheritance_pending=inheritance_pending,
        reviewed_recently=reviewed_recently,
    )


def get_governance_classification_review(
    session: Session,
    *,
    current_user=None,
    q: str | None = None,
    kind: str | None = None,
    entity_level: str | None = None,
    review_status: str | None = None,
    source: str | None = None,
    datasource: str | None = None,
    schema_name: str | None = None,
    domain: str | None = None,
    owner: str | None = None,
    tag: str | None = None,
    min_confidence: int | None = None,
    max_confidence: int | None = None,
    contains_pii: bool | None = None,
    contains_sensitive: bool | None = None,
    contains_critical: bool | None = None,
    sort_by: str = "risk_desc",
    page: int = 1,
    page_size: int = 25,
) -> dict[str, object]:
    now = datetime.now(timezone.utc)
    settings_snapshot = get_governance_settings_snapshot(session)
    profiles = load_table_profiles(session, now, current_user=current_user)
    profile_map = {profile.table_id: profile for profile in profiles}
    trust_map = {
        profile.table_id: build_trust_score_for_profile(profile, settings_snapshot=settings_snapshot)
        for profile in profiles
    }
    table_ids = [profile.table_id for profile in profiles]

    table_tags_map = load_entity_tag_contexts(session, entity_type=TABLE_ENTITY_TYPE, entity_ids=table_ids) if table_ids else {}
    term_map = _load_table_terms_map(session, table_ids)

    events = load_pending_tag_intelligence_events(
        session,
        limit=500,
        sort_by="certainty_desc" if sort_by == "confidence_desc" else "risk_desc",
    )

    column_ids = [int(event["column_id"]) for event in events if event.get("column_id") is not None]
    column_tables = {
        int(column.id): int(column.table_id)
        for column in session.scalars(select(ColumnEntity).where(ColumnEntity.id.in_(column_ids))).all()
    } if column_ids else {}
    column_tags_map = load_entity_tag_contexts(session, entity_type=COLUMN_ENTITY_TYPE, entity_ids=column_ids) if column_ids else {}

    items: list[dict[str, object]] = []
    for event in events:
        table_id = int(event.get("table_id") or 0)
        profile = profile_map.get(table_id)
        if profile is None:
            continue
        table_tags = table_tags_map.get(table_id, [])
        table_terms = term_map.get(table_id, [])
        column_id = int(event["column_id"]) if event.get("column_id") is not None else None
        if column_id is not None and column_id not in column_tables:
            continue
        column_tags = column_tags_map.get(column_id, []) if column_id is not None else []
        governance_score = build_governance_score_for_profile(profile, settings_snapshot=settings_snapshot)
        item = _build_suggestion_item(
            event,
            profile=profile,
            table_tags=table_tags,
            column_tags=column_tags,
            table_terms=table_terms,
            governance_score=governance_score,
            trust_score={
                "score": trust_map[profile.table_id].score,
                "label": trust_map[profile.table_id].label,
                "tone": trust_map[profile.table_id].tone,
            },
        )
        if item is not None:
            items.append(item)

    for profile in profiles:
        table_tags = table_tags_map.get(profile.table_id, [])
        table_terms = term_map.get(profile.table_id, [])
        governance_score = build_governance_score_for_profile(profile, settings_snapshot=settings_snapshot)
        gap_item = _build_gap_item(
            profile,
            table_tags=table_tags,
            table_terms=table_terms,
            governance_score=governance_score,
            trust_score={
                "score": trust_map[profile.table_id].score,
                "label": trust_map[profile.table_id].label,
                "tone": trust_map[profile.table_id].tone,
            },
        )
        if gap_item is not None:
            items.append(gap_item)

    normalized_q = _normalized(q)
    normalized_kind = _normalized(kind)
    normalized_level = _normalized(entity_level)
    normalized_review_status = _normalized(review_status)
    normalized_source = _normalized(source)
    normalized_datasource = _normalized(datasource)
    normalized_schema = _normalized(schema_name)
    normalized_domain = _normalized(domain)
    normalized_owner = _normalized(owner)
    normalized_tag = _normalized(tag)

    def matches(item: dict[str, object]) -> bool:
        if normalized_q:
            haystack = " ".join(
                str(value)
                for value in [
                    item.get("table_fqn"),
                    item.get("table_name"),
                    item.get("column_name"),
                    item.get("owner_name"),
                    item.get("suggestion_tag_name"),
                    item.get("suggestion_tag_slug"),
                    item.get("inference_reason"),
                    item.get("datasource_name"),
                    item.get("schema_name"),
                    item.get("domain_name"),
                    item.get("sensitivity_label"),
                ]
                if value
            ).lower()
            if normalized_q not in haystack:
                return False
        if normalized_kind and item.get("kind") != normalized_kind:
            return False
        if normalized_level and item.get("entity_level") != normalized_level:
            return False
        if normalized_review_status and _normalized(str(item.get("review_status") or "")) != normalized_review_status:
            return False
        if normalized_source and _normalized(str(item.get("inference_source") or "")) != normalized_source:
            return False
        if normalized_datasource and _normalized(str(item.get("datasource_name") or "")) != normalized_datasource:
            return False
        if normalized_schema and _normalized(str(item.get("schema_name") or "")) != normalized_schema:
            return False
        if normalized_domain and _normalized(str(item.get("domain_name") or "")) != normalized_domain:
            return False
        if normalized_owner and _normalized(str(item.get("owner_name") or "")) != normalized_owner:
            return False
        if normalized_tag:
            tag_haystack = " ".join(
                [
                    str(item.get("suggestion_tag_name") or ""),
                    str(item.get("suggestion_tag_slug") or ""),
                    " ".join(tag.name for tag in item.get("current_tags") or []),
                ]
            ).lower()
            if normalized_tag not in tag_haystack:
                return False
        if min_confidence is not None and int(item.get("confidence_score") or 0) < min_confidence:
            return False
        if max_confidence is not None and int(item.get("confidence_score") or 0) > max_confidence:
            return False
        if contains_pii is True and not (
            bool(item.get("has_personal_data"))
            or str(item.get("suggestion_tag_name") or "").strip().lower() in {"pii", "contém pii", "contém ppi"}
        ):
            return False
        if contains_sensitive is True and not (
            bool(item.get("has_sensitive_personal_data"))
            or str(item.get("suggestion_tag_name") or "").strip().lower() in {"sensível", "contém dados sensíveis"}
        ):
            return False
        if contains_critical is True and not (
            bool(item.get("critical_open_incidents"))
            or str(item.get("suggestion_tag_name") or "").strip().lower() == "coluna crítica"
        ):
            return False
        return True

    filtered_items = [item for item in items if matches(item)]

    reviewed_recently = int(
        session.scalar(
            select(func.count(TagIntelligenceEvent.id)).where(
                TagIntelligenceEvent.reviewed_at.is_not(None),
                TagIntelligenceEvent.reviewed_at >= now - timedelta(days=7),
                TagIntelligenceEvent.review_status.in_(["manual_applied", "blocked"]),
            )
        )
        or 0
    )

    def sort_key(item: dict[str, object]) -> tuple[int, int, int, str]:
        risk = int(item.get("risk_score") or 0)
        confidence = int(item.get("confidence_score") or 0)
        created_at = item.get("created_at")
        timestamp = int(created_at.timestamp()) if isinstance(created_at, datetime) else 0
        if sort_by == "confidence_desc":
            return (-confidence, -risk, -timestamp, str(item.get("table_fqn") or ""))
        if sort_by == "newest":
            return (-timestamp, -risk, -confidence, str(item.get("table_fqn") or ""))
        if sort_by == "oldest":
            return (timestamp, -risk, -confidence, str(item.get("table_fqn") or ""))
        return (-risk, -confidence, -timestamp, str(item.get("table_fqn") or ""))

    filtered_items.sort(key=sort_key)

    total = len(filtered_items)
    start = max(page - 1, 0) * page_size
    end = start + page_size
    page_items = filtered_items[start:end]

    result_items = [ClassificationReviewItemOut(**item) for item in page_items]

    filter_sources = sorted(
        {str(item.get("inference_source") or "") for item in items if item.get("inference_source")},
    )
    filter_tags = sorted(
        {
            str(item.get("suggestion_tag_name") or "")
            for item in items
            if item.get("suggestion_tag_name")
        }
    )
    filter_datasources = sorted({str(item.get("datasource_name") or "") for item in items if item.get("datasource_name")})
    filter_databases = sorted({str(item.get("database_name") or "") for item in items if item.get("database_name")})
    filter_schemas = sorted({str(item.get("schema_name") or "") for item in items if item.get("schema_name")})
    filter_domains = sorted({str(item.get("domain_name") or "") for item in items if item.get("domain_name")})
    filter_owners = sorted({str(item.get("owner_name") or "") for item in items if item.get("owner_name")})
    filter_review_statuses = sorted({str(item.get("review_status") or "") for item in items if item.get("review_status")})

    summary = _build_summary(items, reviewed_recently=reviewed_recently)

    return {
        "generated_at": now,
        "total": total,
        "page": page,
        "page_size": page_size,
        "filters": ClassificationReviewFiltersOut(
            kinds=_filter_options(["suggestion", "gap", "conflict"], labels={"suggestion": "Sugestão", "gap": "Lacuna", "conflict": "Conflito"}),
            entity_levels=_filter_options(["table", "column"], labels={"table": "Tabela", "column": "Coluna"}),
            review_statuses=_filter_options(filter_review_statuses, labels={
                "pending_review": "Pendente",
                "suggested": "Pendente",
                "manual_applied": "Aplicada manualmente",
                "blocked": "Bloqueada",
                "gap": "Lacuna",
                "conflict": "Conflito",
            }),
            sources=_filter_options(filter_sources, labels={
                "openlineage": "OpenLineage",
                "heuristic": "Heurística",
                "sql": "SQL",
                "governance_gap": "Lacuna de governança",
            }),
            datasources=_filter_options(filter_datasources),
            databases=_filter_options(filter_databases),
            schemas=_filter_options(filter_schemas),
            domains=_filter_options(filter_domains),
            owners=_filter_options(filter_owners),
            tags=_filter_options(filter_tags),
        ),
        "summary": summary,
        "items": result_items,
    }


def promote_governance_classification_review_tables(
    session: Session,
    *,
    table_ids: list[int],
    current_user=None,
    request_audit: dict[str, object] | None = None,
) -> dict[str, object]:
    normalized_table_ids = _dedupe_table_ids(table_ids)
    if not normalized_table_ids:
        return {
            "generated_at": datetime.now(timezone.utc),
            "requested_table_ids": [],
            "promoted_count": 0,
            "refresh_created": 0,
            "refresh_updated": 0,
            "refresh_reopened": 0,
            "refresh_resolved": 0,
            "refresh_purged": 0,
            "retention_days": 90,
        }

    refresh_result = refresh_governance_recommendations(session, current_user=current_user, table_ids=normalized_table_ids)
    write_audit_log_sync(
        session,
        action="governance.classification_review.batch_promote",
        user_id=getattr(current_user, "id", None),
        actor_name=getattr(current_user, "name", None),
        user_email=getattr(current_user, "email", None),
        entity_type="governance_classification_review",
        entity_id=",".join(str(table_id) for table_id in normalized_table_ids),
        before={"requested_table_ids": normalized_table_ids},
        after={"refresh": refresh_result},
        metadata=request_audit,
    )
    return {
        "generated_at": refresh_result["generated_at"],
        "requested_table_ids": normalized_table_ids,
        "promoted_count": len(normalized_table_ids),
        "refresh_created": int(refresh_result.get("created", 0)),
        "refresh_updated": int(refresh_result.get("updated", 0)),
        "refresh_reopened": int(refresh_result.get("reopened", 0)),
        "refresh_resolved": int(refresh_result.get("resolved", 0)),
        "refresh_purged": int(refresh_result.get("purged", 0)),
        "retention_days": int(refresh_result.get("retention_days", 90)),
    }


__all__ = ["get_governance_classification_review", "promote_governance_classification_review_tables"]
