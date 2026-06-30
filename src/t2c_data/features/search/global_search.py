from __future__ import annotations

import re
import unicodedata
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from time import perf_counter
from typing import Iterable

from sqlalchemy import desc, func, select
from sqlalchemy.orm import Session

from t2c_data.models.catalog import ColumnEntity, DataOwner, DataSource, Database, Schema, TableEntity
from t2c_data.features.dashboard.profile_loader import load_table_profiles
from t2c_data.features.governance.scoring import build_governance_score_for_profile
from t2c_data.features.governance.settings import get_governance_settings_snapshot
from t2c_data.features.access_control.policy import can_view_datasource, can_view_schema, can_view_table
from t2c_data.features.tags.api_support import load_entity_tag_contexts
from t2c_data.models.glossary import GlossaryTerm
from t2c_data.models.incident import Incident
from t2c_data.models.search import ColumnSearchAlias, SearchResultClick, TableSearchAlias
from t2c_data.models.tag import Tag, TagAssignment


CATEGORY_LABELS = {
    "table": "Tabelas",
    "column": "Colunas",
    "glossary_term": "Termos de glossário",
    "tag": "Tags",
    "owner": "Owners",
    "classification": "Classificações",
    "datasource": "Fontes",
    "database": "Bancos",
    "schema": "Schemas",
}

CATEGORY_ORDER = {
    "table": 0,
    "column": 1,
    "glossary_term": 2,
    "tag": 3,
    "owner": 4,
    "classification": 5,
    "datasource": 6,
    "database": 7,
    "schema": 8,
}

CLASSIFICATION_LABELS = {
    "public": "Público",
    "internal": "Interno",
    "confidential": "Confidencial",
    "restricted": "Restrito",
    "personal_data": "Dado pessoal",
}

MATCH_REASON_LABELS = {
    "exact_name": "Encontrado no nome exato",
    "name": "Encontrado no nome",
    "alias": "Encontrado em alias",
    "synonym": "Encontrado em sinônimo",
    "description": "Encontrado na descrição",
    "context": "Encontrado no contexto relacionado",
}


@dataclass
class SearchRecord:
    entity_type: str
    entity_id: int
    title: str
    subtitle: str | None
    description: str | None
    context_path: str | None
    target_url: str
    searchable_name: list[str] = field(default_factory=list)
    searchable_aliases: list[str] = field(default_factory=list)
    searchable_synonyms: list[str] = field(default_factory=list)
    searchable_descriptions: list[str] = field(default_factory=list)
    searchable_context: list[str] = field(default_factory=list)
    source_name: str | None = None
    database_name: str | None = None
    schema_name: str | None = None
    owner_name: str | None = None
    domain_name: str | None = None
    classification: str | None = None
    governance_score: int | None = None
    governance_label: str | None = None
    governance_tone: str | None = None
    certified: bool = False
    open_incidents: int = 0
    popularity_count: int = 0
    metadata: dict[str, str | int | bool | None] = field(default_factory=dict)


@dataclass
class SearchFilters:
    result_type: str | None = None
    source: str | None = None
    database: str | None = None
    schema: str | None = None
    domain: str | None = None
    owner: str | None = None
    classification: str | None = None
    certification: str | None = None
    incidents: str | None = None
    governance_maturity: str | None = None


def _split_synonyms(value: str | None) -> list[str]:
    if not value:
        return []
    parts = re.split(r"[;,\n|]", value)
    return [part.strip() for part in parts if part.strip()]


def normalize_search_text(value: str | None) -> str:
    raw = (value or "").strip().lower()
    if not raw:
        return ""
    normalized = unicodedata.normalize("NFKD", raw).encode("ascii", "ignore").decode("ascii")
    normalized = normalized.replace("_", " ").replace("-", " ").replace("/", " ")
    normalized = re.sub(r"[^a-z0-9\s]", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def _token_variants(tokens: list[str]) -> set[str]:
    variants: set[str] = set()
    for token in tokens:
        if not token:
            continue
        variants.add(token)
        if token.endswith("s") and len(token) > 3:
            variants.add(token[:-1])
        if token.endswith("es") and len(token) > 4:
            variants.add(token[:-2])
    return variants


def _query_tokens(query: str) -> tuple[str, list[str], set[str]]:
    normalized = normalize_search_text(query)
    tokens = [token for token in normalized.split(" ") if token]
    return normalized, tokens, _token_variants(tokens)


def _contains_all_tokens(text: str, query_tokens: set[str]) -> bool:
    if not text:
        return False
    haystack_tokens = set(text.split(" "))
    return query_tokens.issubset(haystack_tokens) or all(token in text for token in query_tokens)


def _best_match(query: str, query_tokens: set[str], values: Iterable[str], *, exact: int, prefix: int, contains: int, default_reason: str) -> tuple[int, str | None]:
    best_score = 0
    best_reason: str | None = None
    for raw_value in values:
        value = normalize_search_text(raw_value)
        if not value:
            continue
        if value == query:
            score = exact
            reason = "exact_name" if default_reason == "name" else default_reason
        elif value.startswith(query):
            score = prefix
            reason = default_reason
        elif query in value or _contains_all_tokens(value, query_tokens):
            score = contains
            reason = default_reason
        else:
            continue
        if score > best_score:
            best_score = score
            best_reason = reason
    return best_score, best_reason


def _score_record(record: SearchRecord, query: str, query_tokens: set[str]) -> tuple[int, str | None]:
    candidates = [
        _best_match(query, query_tokens, record.searchable_name, exact=100, prefix=92, contains=82, default_reason="name"),
        _best_match(query, query_tokens, record.searchable_aliases, exact=92, prefix=86, contains=78, default_reason="alias"),
        _best_match(query, query_tokens, record.searchable_synonyms, exact=92, prefix=84, contains=76, default_reason="synonym"),
        _best_match(query, query_tokens, record.searchable_descriptions, exact=58, prefix=56, contains=52, default_reason="description"),
        _best_match(query, query_tokens, record.searchable_context, exact=46, prefix=44, contains=38, default_reason="context"),
    ]
    score, reason = max(candidates, key=lambda item: item[0])
    if score <= 0 or reason is None:
        return 0, None
    if record.certified:
        score += 6
    if record.owner_name:
        score += 4
    if record.entity_type in {"table", "column"} and record.open_incidents > 0:
        score += 3
    if record.popularity_count > 0:
        score += min(8, 1 + (record.popularity_count // 3))
    score += max(0, 4 - CATEGORY_ORDER.get(record.entity_type, 9))
    return score, reason


def _badge(label: str, tone: str = "neutral") -> dict[str, str]:
    return {"label": label, "tone": tone}


def _tag_preview(tags) -> list[dict[str, object]]:
    preview: list[dict[str, object]] = []
    for tag in list(tags)[:3]:
        preview.append(
            {
                "id": tag.id,
                "name": tag.name,
                "color": tag.color,
                "confidence_score": tag.confidence_score,
                "inference_source": tag.inference_source,
                "inference_reason": tag.inference_reason,
                "applied_automatically": tag.applied_automatically,
                "review_status": tag.review_status,
                "rule_label": tag.rule_label,
            }
        )
    return preview


def _serialize(record: SearchRecord, score: int, reason: str) -> dict[str, object]:
    badges: list[dict[str, str]] = []
    if record.certified:
        badges.append(_badge("Certificado", "success"))
    if record.owner_name:
        badges.append(_badge("Owner definido", "accent"))
    if record.classification:
        badges.append(_badge(record.classification, "warning"))
    certification_status = (record.metadata or {}).get("certification_status")
    if certification_status == "certified":
        badges.append(_badge("Certificada", "success"))
    elif certification_status == "eligible":
        badges.append(_badge("Elegível", "accent"))
    elif certification_status == "revalidation_pending":
        badges.append(_badge("Pendente de revalidação", "warning"))
    readiness_score = (record.metadata or {}).get("readiness_score")
    if isinstance(readiness_score, int):
        badges.append(_badge(f"Prontidão {readiness_score}%", "accent" if readiness_score >= 50 else "neutral"))
    if (record.metadata or {}).get("active_dq_violation"):
        badges.append(_badge("DQ ativa", "warning"))
    if record.open_incidents > 0:
        badges.append(_badge(f"{record.open_incidents} incidente(s)", "warning"))
    if (record.metadata or {}).get("masked_sensitive_fields"):
        badges.append(_badge("Metadados mascarados", "neutral"))

    return {
        "entity_type": record.entity_type,
        "entity_id": record.entity_id,
        "category": CATEGORY_LABELS.get(record.entity_type, record.entity_type),
        "title": record.title,
        "subtitle": record.subtitle,
        "description": record.description,
        "context_path": record.context_path,
        "match_reason": MATCH_REASON_LABELS.get(reason, MATCH_REASON_LABELS["context"]),
        "relevance_score": score,
        "target_url": record.target_url,
        "badges": badges,
        "metadata": {
            "source": record.source_name,
            "database": record.database_name,
            "schema": record.schema_name,
            "owner": record.owner_name,
            "domain": record.domain_name,
            "classification": record.classification,
            "governance_score": record.governance_score,
            "governance_label": record.governance_label,
            "governance_tone": record.governance_tone,
            "popularity_count": record.popularity_count,
            "tags": record.metadata.get("tags", []),
            **record.metadata,
        },
    }


def _load_popularity_counts(session: Session) -> dict[tuple[str, int], int]:
    try:
        rows = session.execute(
            select(
                SearchResultClick.entity_type,
                SearchResultClick.entity_id,
                func.count(SearchResultClick.id).label("click_count"),
            )
            .group_by(SearchResultClick.entity_type, SearchResultClick.entity_id)
            .order_by(desc("click_count"))
        ).all()
    except Exception:  # noqa: BLE001
        return {}
    return {
        (str(row.entity_type), int(row.entity_id)): int(row.click_count or 0)
        for row in rows
        if row.entity_type and row.entity_id is not None
    }


def _load_tables(session: Session, *, table_ids: list[int] | None = None) -> list[SearchRecord]:
    try:
        alias_rows = session.execute(
            select(TableSearchAlias.table_id, TableSearchAlias.label_kind, TableSearchAlias.label)
        ).all()
    except Exception:  # noqa: BLE001
        alias_rows = []
    table_aliases: dict[int, dict[str, list[str]]] = defaultdict(lambda: {"friendly_name": [], "alias": [], "synonym": []})
    for table_id, label_kind, label in alias_rows:
        if table_id is None or not label:
            continue
        bucket = table_aliases[int(table_id)]
        bucket.setdefault(str(label_kind), []).append(str(label))

    incident_counts = {
        str(table_fqn): int(open_incidents or 0)
        for table_fqn, open_incidents in session.execute(
            select(Incident.table_fqn, func.count(Incident.id))
            .where(Incident.entity_type == "table", Incident.status.in_(["open", "investigating"]))
            .group_by(Incident.table_fqn)
        ).all()
        if table_fqn
    }
    stmt = (
        select(
            TableEntity.id,
            TableEntity.name,
            TableEntity.description_manual,
            TableEntity.description_source,
            TableEntity.certification_status,
            TableEntity.sensitivity_level,
            TableEntity.owner,
            DataOwner.name,
            Schema.id,
            Schema.name,
            Database.id,
            Database.name,
            DataSource.id,
            DataSource.name,
        )
        .join(Schema, TableEntity.schema_id == Schema.id)
        .join(Database, Schema.database_id == Database.id)
        .join(DataSource, Database.datasource_id == DataSource.id)
        .outerjoin(DataOwner, TableEntity.data_owner_id == DataOwner.id)
    )
    if table_ids:
        stmt = stmt.where(TableEntity.id.in_(table_ids))
    rows = session.execute(stmt).all()
    tags_by_table_id = load_entity_tag_contexts(
        session,
        entity_type="table",
        entity_ids=[int(row[0]) for row in rows],
    ) if rows else {}
    return [
        SearchRecord(
            entity_type="table",
            entity_id=int(row[0]),
            title=(table_aliases[int(row[0])].get("friendly_name") or [row[1]])[0],
            subtitle=f"{row[1]} · {row[13]} · {row[11]}.{row[9]}",
            description=row[2] or row[3],
            context_path=f"{row[13]} > {row[11]} > {row[9]} > {row[1]}",
            target_url=f"/explorer?tableId={int(row[0])}",
            searchable_name=[row[1], *(table_aliases[int(row[0])].get("friendly_name") or [])],
            searchable_aliases=table_aliases[int(row[0])].get("alias") or [],
            searchable_synonyms=table_aliases[int(row[0])].get("synonym") or [],
            searchable_descriptions=[row[2] or "", row[3] or ""],
            searchable_context=[row[9], row[11], row[13], row[4] or "", row[5] or "", row[6] or "", row[7] or ""],
            source_name=row[13],
            database_name=row[11],
            schema_name=row[9],
            owner_name=row[7] or row[6],
            domain_name=None,
            classification=CLASSIFICATION_LABELS.get(row[5], None),
            certified=row[4] == "certified",
            open_incidents=incident_counts.get(f"{row[9]}.{row[1]}", 0),
            metadata={
                "datasource_id": int(row[12]) if row[12] is not None else None,
                "database_id": int(row[10]) if row[10] is not None else None,
                "schema_id": int(row[8]) if row[8] is not None else None,
                "table_name": row[1],
                "table_fqn": f"{row[9]}.{row[1]}",
                "incidents_target_url": f"/incidents/tickets?q={row[9]}.{row[1]}&entity_type=table",
                "dq_target_url": f"/data-quality?tableId={int(row[0])}",
                "alias_count": len(table_aliases[int(row[0])].get("alias") or []) + len(table_aliases[int(row[0])].get("synonym") or []),
                "tags": _tag_preview(tags_by_table_id.get(int(row[0]), [])),
            },
        )
        for row in rows
    ]


def _load_columns(session: Session, *, table_ids: list[int] | None = None) -> list[SearchRecord]:
    try:
        alias_rows = session.execute(
            select(ColumnSearchAlias.column_id, ColumnSearchAlias.label_kind, ColumnSearchAlias.label)
        ).all()
    except Exception:  # noqa: BLE001
        alias_rows = []
    column_aliases: dict[int, dict[str, list[str]]] = defaultdict(lambda: {"friendly_name": [], "alias": [], "synonym": []})
    for column_id, label_kind, label in alias_rows:
        if column_id is None or not label:
            continue
        bucket = column_aliases[int(column_id)]
        bucket.setdefault(str(label_kind), []).append(str(label))

    stmt = (
        select(
            ColumnEntity.id,
            ColumnEntity.name,
            ColumnEntity.data_type,
            ColumnEntity.description_manual,
            ColumnEntity.description_source,
            ColumnEntity.dictionary_description,
            TableEntity.id,
            TableEntity.name,
            TableEntity.certification_status,
            TableEntity.sensitivity_level,
            TableEntity.owner,
            DataOwner.name,
            Schema.id,
            Schema.name,
            Database.id,
            Database.name,
            DataSource.id,
            DataSource.name,
        )
        .join(TableEntity, ColumnEntity.table_id == TableEntity.id)
        .join(Schema, TableEntity.schema_id == Schema.id)
        .join(Database, Schema.database_id == Database.id)
        .join(DataSource, Database.datasource_id == DataSource.id)
        .outerjoin(DataOwner, TableEntity.data_owner_id == DataOwner.id)
    )
    if table_ids:
        stmt = stmt.where(ColumnEntity.table_id.in_(table_ids))
    rows = session.execute(stmt).all()
    tags_by_column_id = load_entity_tag_contexts(
        session,
        entity_type="column",
        entity_ids=[int(row[0]) for row in rows],
    ) if rows else {}
    return [
        SearchRecord(
            entity_type="column",
            entity_id=int(row[0]),
            title=(column_aliases[int(row[0])].get("friendly_name") or [row[1]])[0],
            subtitle=f"{row[7]} · {row[17]}.{row[15]}.{row[13]}",
            description=row[5] or row[3] or row[4] or row[2],
            context_path=f"{row[17]} > {row[15]} > {row[13]} > {row[7]} > {row[1]}",
            target_url=f"/explorer?tableId={int(row[6])}&tab=columns&columnId={int(row[0])}",
            searchable_name=[row[1], *(column_aliases[int(row[0])].get("friendly_name") or [])],
            searchable_aliases=column_aliases[int(row[0])].get("alias") or [],
            searchable_synonyms=column_aliases[int(row[0])].get("synonym") or [],
            searchable_descriptions=[row[5] or "", row[3] or "", row[4] or "", row[2] or ""],
            searchable_context=[row[7], row[13], row[15], row[17], row[9] or "", row[10] or "", row[11] or ""],
            source_name=row[17],
            database_name=row[15],
            schema_name=row[13],
            owner_name=row[11] or row[10],
            classification=CLASSIFICATION_LABELS.get(row[9], None),
            certified=row[8] == "certified",
            metadata={
                "datasource_id": int(row[16]) if row[16] is not None else None,
                "database_id": int(row[14]) if row[14] is not None else None,
                "schema_id": int(row[12]) if row[12] is not None else None,
                "table_id": int(row[6]),
                "table_name": row[7],
                "table_fqn": f"{row[13]}.{row[7]}",
                "data_type": row[2],
                "incidents_target_url": f"/incidents/tickets?q={row[13]}.{row[7]}&entity_type=table",
                "dq_target_url": f"/data-quality?tableId={int(row[6])}",
                "alias_count": len(column_aliases[int(row[0])].get("alias") or []) + len(column_aliases[int(row[0])].get("synonym") or []),
                "tags": _tag_preview(tags_by_column_id.get(int(row[0]), [])),
            },
        )
        for row in rows
    ]


def _load_glossary_terms(session: Session) -> list[SearchRecord]:
    rows = session.execute(
        select(
            GlossaryTerm.id,
            GlossaryTerm.name,
            GlossaryTerm.definition,
            GlossaryTerm.description,
            GlossaryTerm.category,
            GlossaryTerm.subcategory,
            GlossaryTerm.synonyms,
            GlossaryTerm.status,
        )
    ).all()
    return [
        SearchRecord(
            entity_type="glossary_term",
            entity_id=int(row[0]),
            title=row[1],
            subtitle=row[4] or "Glossário",
            description=row[2] or row[3],
            context_path=f"Glossário > {row[4] or 'Sem categoria'}",
            target_url=f"/glossary?termId={int(row[0])}",
            searchable_name=[row[1]],
            searchable_synonyms=_split_synonyms(row[6]),
            searchable_descriptions=[row[2] or "", row[3] or ""],
            searchable_context=[row[4] or "", row[5] or "", row[7] or ""],
            metadata={"category": row[4], "status": row[7]},
        )
        for row in rows
    ]


def _load_tags(session: Session) -> list[SearchRecord]:
    assignment_counts = {
        int(tag_id): int(count or 0)
        for tag_id, count in session.execute(
            select(TagAssignment.tag_id, func.count(TagAssignment.id)).group_by(TagAssignment.tag_id)
        ).all()
    }
    rows = session.execute(
        select(
            Tag.id,
            Tag.name,
            Tag.description,
            Tag.group_name,
            Tag.subgroup_name,
            Tag.tag_type,
            Tag.synonyms,
            Tag.status,
        )
    ).all()
    return [
        SearchRecord(
            entity_type="tag",
            entity_id=int(row[0]),
            title=row[1],
            subtitle=row[3] or "Tag",
            description=row[2],
            context_path=f"Tags > {row[3] or 'Sem grupo'} > {row[4] or 'Sem subgrupo'}",
            target_url=f"/tags?tagId={int(row[0])}",
            searchable_name=[row[1]],
            searchable_synonyms=_split_synonyms(row[6]),
            searchable_descriptions=[row[2] or ""],
            searchable_context=[row[3] or "", row[4] or "", row[5] or "", row[7] or ""],
            metadata={"group_name": row[3], "tag_type": row[5], "assignments": assignment_counts.get(int(row[0]), 0)},
        )
        for row in rows
    ]


def _load_owners(session: Session) -> list[SearchRecord]:
    table_counts = {
        int(owner_id): int(count or 0)
        for owner_id, count in session.execute(
            select(TableEntity.data_owner_id, func.count(TableEntity.id))
            .where(TableEntity.data_owner_id.is_not(None))
            .group_by(TableEntity.data_owner_id)
        ).all()
        if owner_id is not None
    }
    rows = session.execute(select(DataOwner.id, DataOwner.name, DataOwner.email, DataOwner.area, DataOwner.description)).all()
    return [
        SearchRecord(
            entity_type="owner",
            entity_id=int(row[0]),
            title=row[1],
            subtitle=row[2],
            description=row[4] or row[3],
            context_path=f"Owners > {row[3] or 'Sem área'}",
            target_url=f"/data-owners?ownerId={int(row[0])}",
            searchable_name=[row[1], row[2]],
            searchable_descriptions=[row[4] or ""],
            searchable_context=[row[3] or ""],
            owner_name=row[1],
            metadata={"area": row[3], "assets_count": table_counts.get(int(row[0]), 0)},
        )
        for row in rows
    ]


def _load_sources(session: Session) -> list[SearchRecord]:
    datasource_rows = session.execute(select(DataSource.id, DataSource.name, DataSource.db_type, DataSource.host, DataSource.database)).all()
    database_rows = session.execute(
        select(Database.id, Database.name, Database.description_manual, Database.description_source, DataSource.id, DataSource.name)
        .join(DataSource, Database.datasource_id == DataSource.id)
    ).all()
    schema_rows = session.execute(
        select(Schema.id, Schema.name, Schema.description_manual, Schema.description_source, Database.id, Database.name, DataSource.id, DataSource.name)
        .join(Database, Schema.database_id == Database.id)
        .join(DataSource, Database.datasource_id == DataSource.id)
    ).all()

    records: list[SearchRecord] = []
    records.extend(
        SearchRecord(
            entity_type="datasource",
            entity_id=int(row[0]),
            title=row[1],
            subtitle=row[2].upper() if row[2] else "Fonte",
            description=f"Host {row[3]} · banco padrão {row[4]}",
            context_path=f"Fontes > {row[1]}",
            target_url=f"/explorer?datasourceId={int(row[0])}",
            searchable_name=[row[1]],
            searchable_aliases=[row[2] or ""],
            searchable_context=[row[3] or "", row[4] or ""],
            source_name=row[1],
            metadata={"db_type": row[2], "datasource_id": int(row[0])},
        )
        for row in datasource_rows
    )
    records.extend(
        SearchRecord(
            entity_type="database",
            entity_id=int(row[0]),
            title=row[1],
            subtitle=row[5],
            description=row[2] or row[3],
            context_path=f"{row[5]} > {row[1]}",
            target_url=f"/explorer?datasourceId={int(row[4])}&databaseId={int(row[0])}",
            searchable_name=[row[1]],
            searchable_descriptions=[row[2] or "", row[3] or ""],
            searchable_context=[row[5]],
            source_name=row[5],
            database_name=row[1],
            metadata={"datasource_id": int(row[4]), "database_id": int(row[0])},
        )
        for row in database_rows
    )
    records.extend(
        SearchRecord(
            entity_type="schema",
            entity_id=int(row[0]),
            title=row[1],
            subtitle=f"{row[7]} · {row[5]}",
            description=row[2] or row[3],
            context_path=f"{row[7]} > {row[5]} > {row[1]}",
            target_url=f"/explorer?datasourceId={int(row[6])}&databaseId={int(row[4])}&schemaId={int(row[0])}",
            searchable_name=[row[1]],
            searchable_descriptions=[row[2] or "", row[3] or ""],
            searchable_context=[row[5], row[7]],
            source_name=row[7],
            database_name=row[5],
            schema_name=row[1],
            metadata={"datasource_id": int(row[6]), "database_id": int(row[4]), "schema_id": int(row[0])},
        )
        for row in schema_rows
    )
    return records


def _load_classifications(session: Session, *, table_ids: list[int] | None = None) -> list[SearchRecord]:
    stmt = (
        select(
            TableEntity.id,
            TableEntity.name,
            TableEntity.sensitivity_level,
            TableEntity.certification_status,
            Schema.id,
            Schema.name,
            Database.id,
            Database.name,
            DataSource.id,
            DataSource.name,
        )
        .join(Schema, TableEntity.schema_id == Schema.id)
        .join(Database, Schema.database_id == Database.id)
        .join(DataSource, Database.datasource_id == DataSource.id)
        .where(TableEntity.sensitivity_level.is_not(None))
    )
    if table_ids:
        stmt = stmt.where(TableEntity.id.in_(table_ids))
    rows = session.execute(stmt).all()
    return [
        SearchRecord(
            entity_type="classification",
            entity_id=int(row[0]),
            title=CLASSIFICATION_LABELS.get(row[2], row[2] or "Não classificado"),
            subtitle=row[1],
            description=f"Classificação aplicada ao ativo {row[1]}",
            context_path=f"{row[9]} > {row[7]} > {row[5]} > {row[1]}",
            target_url=f"/explorer?tableId={int(row[0])}",
            searchable_name=[CLASSIFICATION_LABELS.get(row[2], row[2] or "")],
            searchable_aliases=[row[2] or ""],
            searchable_context=[row[1], row[5], row[7], row[9]],
            source_name=row[9],
            database_name=row[7],
            schema_name=row[5],
            classification=CLASSIFICATION_LABELS.get(row[2], row[2] or ""),
            certified=row[3] == "certified",
            metadata={
                "datasource_id": int(row[8]) if row[8] is not None else None,
                "database_id": int(row[6]) if row[6] is not None else None,
                "schema_id": int(row[4]) if row[4] is not None else None,
                "table_name": row[1],
                "table_fqn": f"{row[5]}.{row[1]}",
                "incidents_target_url": f"/incidents/tickets?q={row[5]}.{row[1]}&entity_type=table",
                "dq_target_url": f"/data-quality?tableId={int(row[0])}",
            },
        )
        for row in rows
    ]


def load_search_records_live(
    session: Session,
    *,
    table_ids: list[int] | None = None,
    include_global_entities: bool = True,
) -> list[SearchRecord]:
    records: list[SearchRecord] = []
    records.extend(_load_tables(session, table_ids=table_ids))
    records.extend(_load_columns(session, table_ids=table_ids))
    if include_global_entities:
        records.extend(_load_glossary_terms(session))
        records.extend(_load_tags(session))
        records.extend(_load_owners(session))
        records.extend(_load_sources(session))
    records.extend(_load_classifications(session, table_ids=table_ids))
    popularity_counts = _load_popularity_counts(session)
    for record in records:
        record.popularity_count = popularity_counts.get((record.entity_type, record.entity_id), 0)
    related_table_ids = {
        record.entity_id
        for record in records
        if record.entity_type == "classification"
    }
    related_table_ids.update(
        int(record.metadata.get("table_id"))
        for record in records
        if isinstance(record.metadata.get("table_id"), int)
    )
    if related_table_ids:
        settings_snapshot = get_governance_settings_snapshot(session)
        table_profiles = {
            profile.table_id: profile
            for profile in load_table_profiles(session, datetime.now(timezone.utc), table_ids=sorted(related_table_ids))
        }
        table_scores = {
            table_id: build_governance_score_for_profile(profile, settings_snapshot=settings_snapshot)
            for table_id, profile in table_profiles.items()
        }
        for record in records:
            table_id = _record_table_id(record)
            score_payload = table_scores.get(table_id) if table_id is not None else None
            profile = table_profiles.get(table_id) if table_id is not None else None
            if not score_payload:
                continue
            record.governance_score = int(score_payload["score"])
            record.governance_label = str(score_payload["label"])
            record.governance_tone = str(score_payload["tone"])
            if profile is not None:
                record.certified = profile.certification_status == "certified" or record.certified
                record.classification = record.classification or profile.sensitivity_level
                metadata = dict(record.metadata or {})
                metadata.update(
                    {
                        "certification_status": profile.certification_status,
                        "readiness_score": int(profile.readiness_score),
                        "active_dq_violation": bool(profile.active_dq_violation),
                        "owner_defined": bool(profile.owner_defined),
                        "description_complete": bool(profile.description_complete),
                        "dictionary_complete": bool(profile.dictionary_complete),
                        "has_personal_data": bool(profile.has_personal_data),
                        "has_sensitive_personal_data": bool(profile.has_sensitive_personal_data),
                    }
                )
                record.metadata = metadata
    return records


def _record_table_id(record: SearchRecord) -> int | None:
    if record.entity_type in {"table", "classification"}:
        return int(record.entity_id)
    table_id = (record.metadata or {}).get("table_id")
    return int(table_id) if isinstance(table_id, int) else None


def _load_records(session: Session) -> list[SearchRecord]:
    from t2c_data.features.platform.read_models import load_search_records_from_read_model

    records = load_search_records_from_read_model(session)
    return records or load_search_records_live(session)


def _apply_visibility(session: Session, records: list[SearchRecord], user) -> list[SearchRecord]:
    from t2c_data.features.platform.visibility import visibility_for_search_records

    mask_decisions = visibility_for_search_records(session, records, user=user)
    table_ids = sorted({table_id for record in records if (table_id := _record_table_id(record)) is not None})
    table_map = {
        table.id: table
        for table in session.scalars(select(TableEntity).where(TableEntity.id.in_(table_ids))).all()
    } if table_ids else {}
    if user is not None and any(record.entity_type in {"datasource", "database", "schema"} for record in records):
        datasource_ids = sorted({
            int(metadata_id)
            for record in records
            for metadata_id in [((record.metadata or {}).get("datasource_id"))]
            if isinstance(metadata_id, int)
        })
        database_ids = sorted({
            int(metadata_id)
            for record in records
            for metadata_id in [((record.metadata or {}).get("database_id"))]
            if isinstance(metadata_id, int)
        })
        schema_ids = sorted({
            int(metadata_id)
            for record in records
            for metadata_id in [((record.metadata or {}).get("schema_id"))]
            if isinstance(metadata_id, int)
        })
        datasource_map = {
            datasource.id: datasource
            for datasource in session.scalars(select(DataSource).where(DataSource.id.in_(datasource_ids))).all()
        } if datasource_ids else {}
        database_map = {
            database.id: database
            for database in session.scalars(select(Database).where(Database.id.in_(database_ids))).all()
        } if database_ids else {}
        schema_map = {
            schema.id: schema
            for schema in session.scalars(select(Schema).where(Schema.id.in_(schema_ids))).all()
        } if schema_ids else {}
        tables_by_datasource: dict[int, list[TableEntity]] = defaultdict(list)
        tables_by_database: dict[int, list[TableEntity]] = defaultdict(list)
        tables_by_schema: dict[int, list[TableEntity]] = defaultdict(list)
        schemas_by_datasource: dict[int, list[Schema]] = defaultdict(list)
        schemas_by_database: dict[int, list[Schema]] = defaultdict(list)
        for table in table_map.values():
            schema = getattr(table, "schema", None)
            database = getattr(schema, "database", None) if schema else None
            datasource = getattr(database, "datasource", None) if database else None
            if schema is not None:
                tables_by_schema[int(schema.id)].append(table)
                if database is not None:
                    schemas_by_database[int(database.id)].append(schema)
            if database is not None:
                tables_by_database[int(database.id)].append(table)
                if datasource is not None:
                    tables_by_datasource[int(datasource.id)].append(table)
                    schemas_by_datasource[int(datasource.id)].append(schema)
        visible_datasource_ids: set[int] = set()
        visible_database_ids: set[int] = set()
        visible_schema_ids: set[int] = set()
        for datasource_id, datasource in datasource_map.items():
            if can_view_datasource(
                user,
                datasource,
                schemas=schemas_by_datasource.get(datasource_id),
                tables=tables_by_datasource.get(datasource_id),
            ):
                visible_datasource_ids.add(datasource_id)
        for database_id, database in database_map.items():
            datasource = getattr(database, "datasource", None)
            if datasource is not None and can_view_datasource(
                user,
                datasource,
                schemas=schemas_by_database.get(database_id),
                tables=tables_by_database.get(database_id),
            ):
                visible_database_ids.add(database_id)
        for schema_id, schema in schema_map.items():
            if can_view_schema(user, schema, tables=tables_by_schema.get(schema_id)):
                visible_schema_ids.add(schema_id)

    filtered: list[SearchRecord] = []
    for record in records:
        table_id = _record_table_id(record)
        metadata = dict(record.metadata or {})
        if table_id is not None:
            table = table_map.get(table_id)
            if table is None or not can_view_table(user, table):
                continue
            decision = mask_decisions.get(table_id)
            if decision and decision.masked:
                cloned = SearchRecord(**{**record.__dict__})
                cloned.classification = None
                cloned_metadata = dict(cloned.metadata or {})
                cloned_metadata["masked_sensitive_fields"] = True
                cloned.metadata = cloned_metadata
                filtered.append(cloned)
                continue
            filtered.append(record)
            continue
        if record.entity_type == "datasource":
            datasource_id = metadata.get("datasource_id")
            if isinstance(datasource_id, int) and datasource_id in visible_datasource_ids:
                filtered.append(record)
            continue
        if record.entity_type == "database":
            database_id = metadata.get("database_id")
            if isinstance(database_id, int) and database_id in visible_database_ids:
                filtered.append(record)
            continue
        if record.entity_type == "schema":
            schema_id = metadata.get("schema_id")
            if isinstance(schema_id, int) and schema_id in visible_schema_ids:
                filtered.append(record)
            continue
        filtered.append(record)
    return filtered


def _matches_filters(record: SearchRecord, filters: SearchFilters) -> bool:
    if filters.result_type and record.entity_type != filters.result_type:
        return False
    if filters.source and record.source_name != filters.source:
        return False
    if filters.database and record.database_name != filters.database:
        return False
    if filters.schema and record.schema_name != filters.schema:
        return False
    if filters.domain and (record.domain_name or "Sem dados suficientes") != filters.domain:
        return False
    if filters.owner and (record.owner_name or "Sem owner") != filters.owner:
        return False
    if filters.classification and (record.classification or "Não classificado") != filters.classification:
        return False
    if filters.governance_maturity and (record.governance_label or "") != filters.governance_maturity:
        return False
    if filters.certification == "certified" and not record.certified:
        return False
    if filters.certification == "not_certified" and record.certified:
        return False
    if filters.incidents == "with_open" and record.open_incidents <= 0:
        return False
    if filters.incidents == "without_open" and record.open_incidents > 0:
        return False
    return True


def _available_filters(records: list[dict[str, object]]) -> dict[str, list[dict[str, str]]]:
    def options(values: list[str]) -> list[dict[str, str]]:
        return [{"value": value, "label": value} for value in sorted(dict.fromkeys(value for value in values if value))]

    return {
        "types": options([str(item["entity_type"]) for item in records]),
        "sources": options([str((item.get("metadata") or {}).get("source") or "") for item in records]),
        "databases": options([str((item.get("metadata") or {}).get("database") or "") for item in records]),
        "schemas": options([str((item.get("metadata") or {}).get("schema") or "") for item in records]),
        "domains": options([str((item.get("metadata") or {}).get("domain") or "") for item in records]),
        "owners": options([str((item.get("metadata") or {}).get("owner") or "") for item in records]),
        "classifications": options([str((item.get("metadata") or {}).get("classification") or "") for item in records]),
        "certification": [{"value": "certified", "label": "Certificado"}, {"value": "not_certified", "label": "Não certificado"}],
        "incidents": [{"value": "with_open", "label": "Com incidentes"}, {"value": "without_open", "label": "Sem incidentes"}],
        "governance_maturity": options([str((item.get("metadata") or {}).get("governance_label") or "") for item in records]),
    }


def _group_results(items: list[dict[str, object]], *, per_group: int | None = None) -> list[dict[str, object]]:
    grouped: defaultdict[str, list[dict[str, object]]] = defaultdict(list)
    for item in items:
        grouped[str(item["entity_type"])].append(item)
    groups = []
    for entity_type, group_items in sorted(grouped.items(), key=lambda item: CATEGORY_ORDER.get(item[0], 99)):
        groups.append(
            {
                "key": entity_type,
                "label": CATEGORY_LABELS.get(entity_type, entity_type),
                "total": len(group_items),
                "items": group_items[:per_group] if per_group is not None else group_items,
            }
        )
    return groups


def search_global(session: Session, q: str, *, filters: SearchFilters | None = None, limit: int = 80, per_group: int | None = None, current_user=None) -> dict[str, object]:
    started = perf_counter()
    normalized_query, tokens, variants = _query_tokens(q)
    if len(normalized_query) < 2:
        return {
            "query": q,
            "total": 0,
            "groups": [],
            "items": [],
            "available_filters": _available_filters([]),
            "applied_filters": vars(filters or SearchFilters()),
            "took_ms": 0,
            "min_query_length": 2,
        }

    active_filters = filters or SearchFilters()
    records = _apply_visibility(session, _load_records(session), current_user)
    scored_results: list[dict[str, object]] = []
    for record in records:
        if not _matches_filters(record, active_filters):
            continue
        score, reason = _score_record(record, normalized_query, variants)
        if score <= 0 or reason is None:
            continue
        scored_results.append(_serialize(record, score, reason))

    scored_results.sort(
        key=lambda item: (
            -int(item["relevance_score"]),
            -int((item.get("metadata") or {}).get("governance_score") or 0),
            CATEGORY_ORDER.get(str(item["entity_type"]), 99),
            str(item["title"]).lower(),
        )
    )
    items = scored_results[:limit]
    took_ms = int((perf_counter() - started) * 1000)
    return {
        "query": q,
        "total": len(scored_results),
        "groups": _group_results(items, per_group=per_group),
        "items": items,
        "available_filters": _available_filters(scored_results),
        "applied_filters": vars(active_filters),
        "took_ms": took_ms,
        "min_query_length": 2,
    }


def search_suggestions(session: Session, q: str, *, current_user=None) -> dict[str, object]:
    payload = search_global(session, q, limit=28, per_group=4, current_user=current_user)
    return {
        "query": payload["query"],
        "groups": payload["groups"],
        "took_ms": payload["took_ms"],
        "min_query_length": payload["min_query_length"],
    }


def search_recent() -> dict[str, object]:
    return {"enabled": False, "items": []}


def search_popular() -> dict[str, object]:
    return {"enabled": False, "items": []}
