from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import re
import unicodedata
from typing import Iterable

from sqlalchemy import and_, delete, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, selectinload

from t2c_data.features.audit import AuditFieldChange
from t2c_data.models.catalog import ColumnEntity, Database, Schema, TableEntity
from t2c_data.models.tag import Tag, TagAssignment, TagAssignmentOverride, TagIntelligenceEvent, TagAutomationRule
from t2c_data.services.audit import log_field_changes, write_audit_log_sync
from t2c_data.features.tags.spreadsheet import slugify_tag


AUTO_TAG_DEFINITIONS: list[dict[str, str]] = [
    {"slug": "nome", "name": "Nome", "scope": "column", "color": "#0284c7"},
    {"slug": "email", "name": "Email", "scope": "column", "color": "#0ea5e9"},
    {"slug": "telefone", "name": "Telefone", "scope": "column", "color": "#22c55e"},
    {"slug": "cpf", "name": "CPF", "scope": "column", "color": "#dc2626"},
    {"slug": "cnpj", "name": "CNPJ", "scope": "column", "color": "#7c3aed"},
    {"slug": "data-nascimento", "name": "Data de Nascimento", "scope": "column", "color": "#a855f7"},
    {"slug": "pii", "name": "PII", "scope": "column", "color": "#dc2626"},
    {"slug": "sensivel", "name": "Sensível", "scope": "column", "color": "#a855f7"},
    {"slug": "documento", "name": "Documento", "scope": "column", "color": "#2563eb"},
    {"slug": "contato", "name": "Contato", "scope": "column", "color": "#0ea5e9"},
    {"slug": "endereco", "name": "Endereço", "scope": "column", "color": "#14b8a6"},
    {"slug": "financeiro", "name": "Financeiro", "scope": "column", "color": "#f59e0b"},
    {"slug": "boleto", "name": "Boleto", "scope": "column", "color": "#f97316"},
    {"slug": "parcela", "name": "Parcela", "scope": "column", "color": "#ea580c"},
    {"slug": "saldo-devedor", "name": "Saldo devedor", "scope": "column", "color": "#b45309"},
    {"slug": "valor-credito", "name": "Valor de crédito", "scope": "column", "color": "#d97706"},
    {"slug": "contrato", "name": "Contrato", "scope": "column", "color": "#92400e"},
    {"slug": "proposta", "name": "Proposta", "scope": "column", "color": "#a16207"},
    {"slug": "cota", "name": "Cota", "scope": "column", "color": "#b91c1c"},
    {"slug": "identificador-financeiro", "name": "Identificador financeiro", "scope": "column", "color": "#7c2d12"},
    {"slug": "dado-pessoal", "name": "Dado pessoal", "scope": "column", "color": "#be123c"},
    {"slug": "dado-sensivel", "name": "Dado sensível", "scope": "column", "color": "#7e22ce"},
    {"slug": "dado-operacional", "name": "Dado operacional", "scope": "column", "color": "#475569"},
    {"slug": "identificador-unico", "name": "Identificador Único", "scope": "column", "color": "#334155"},
    {"slug": "identificador-tecnico", "name": "Identificador Técnico", "scope": "column", "color": "#1f2937"},
    {"slug": "pk", "name": "PK", "scope": "column", "color": "#0f766e"},
    {"slug": "temporal", "name": "Temporal", "scope": "column", "color": "#0891b2"},
    {"slug": "json", "name": "JSON", "scope": "column", "color": "#7c3aed"},
    {"slug": "coluna-critica", "name": "Coluna Crítica", "scope": "column", "color": "#f97316"},
    {"slug": "campo-obrigatorio", "name": "Campo Obrigatório", "scope": "column", "color": "#0284c7"},
    {"slug": "campo-auditavel", "name": "Campo Auditável", "scope": "column", "color": "#0f766e"},
    {"slug": "contem-pii", "name": "Contém PII", "scope": "table", "color": "#b91c1c"},
    {"slug": "contem-dados-sensiveis", "name": "Contém Dados Sensíveis", "scope": "table", "color": "#9333ea"},
    {"slug": "possui-coluna-critica", "name": "Possui Coluna Crítica", "scope": "table", "color": "#ea580c"},
    {"slug": "requer-governanca", "name": "Requer Governança", "scope": "table", "color": "#1d4ed8"},
    {"slug": "alta-prioridade-dq", "name": "Alta Prioridade DQ", "scope": "table", "color": "#b45309"},
]


AUDITABLE_KEYWORDS = ("created at", "updated at", "created by", "updated by", "inserted at", "audit", "history", "log", "event")
AUDITABLE_DATA_TYPES = ("timestamp", "datetime", "date", "timestamptz")
JSON_DATA_TYPES = ("json", "jsonb")

DEFAULT_AUTOMATION_RULES = [
    {
        "slug": "nome",
        "name": "Nome",
        "keywords": [
            "nome",
            "name",
            "full_name",
            "customer_name",
            "user_name",
            "nome_completo",
            "owner_name",
            "data_owner_name",
        ],
    },
    {
        "slug": "email",
        "name": "Email",
        "keywords": [
            "email",
            "e-mail",
            "email_address",
            "user_email",
            "customer_email",
            "owner_email",
            "data_owner_email",
            "contact_email",
        ],
    },
    {
        "slug": "telefone",
        "name": "Telefone",
        "keywords": [
            "telefone",
            "phone",
            "phone_number",
            "celular",
            "mobile",
            "mobile_phone",
            "whatsapp",
        ],
    },
    {
        "slug": "endereco",
        "name": "Endereço",
        "keywords": [
            "endereco",
            "address",
            "logradouro",
            "rua",
            "bairro",
            "cidade",
            "estado",
            "uf",
            "cep",
            "zip",
            "postal_code",
        ],
    },
    {
        "slug": "cpf",
        "name": "CPF",
        "keywords": ["cpf"],
    },
    {
        "slug": "cnpj",
        "name": "CNPJ",
        "keywords": ["cnpj"],
    },
    {
        "slug": "documento",
        "name": "Documento",
        "keywords": ["documento", "doc"],
    },
    {
        "slug": "data-nascimento",
        "name": "Data de Nascimento",
        "keywords": ["data_nascimento", "data nascimento", "birth_date", "dob"],
    },
    {
        "slug": "renda",
        "name": "Renda",
        "keywords": ["renda", "salario", "salário", "income", "earnings", "proventos"],
    },
    {
        "slug": "dados_bancarios",
        "name": "Dados bancários",
        "keywords": ["bank", "banco", "conta", "account", "agencia", "agência", "agency", "iban", "pix", "bank_account", "account_number"],
    },
    {
        "slug": "boleto",
        "name": "Boleto",
        "keywords": ["boleto", "linha_digitavel", "linha digitavel", "billing", "invoice", "cobranca", "charge"],
    },
    {
        "slug": "parcela",
        "name": "Parcela",
        "keywords": ["parcela", "installment", "instalment", "quota_parcela", "installment_number"],
    },
    {
        "slug": "saldo-devedor",
        "name": "Saldo devedor",
        "keywords": ["saldo_devedor", "saldo devedor", "debt_balance", "outstanding_balance", "remaining_balance", "balance_due"],
    },
    {
        "slug": "valor-credito",
        "name": "Valor de crédito",
        "keywords": ["valor_credito", "valor de credito", "valor de crédito", "credit_value", "loan_amount", "financed_value", "credit_amount"],
    },
    {
        "slug": "contrato",
        "name": "Contrato",
        "keywords": ["contrato", "contract", "agreement", "contract_number", "contract_id"],
    },
    {
        "slug": "proposta",
        "name": "Proposta",
        "keywords": ["proposta", "proposal", "application", "proposal_id", "proposal_number"],
    },
    {
        "slug": "cota",
        "name": "Cota",
        "keywords": ["cota", "quota", "share", "share_number", "quota_number", "quota_id"],
    },
    {
        "slug": "identificador-financeiro",
        "name": "Identificador Financeiro",
        "keywords": [
            "financial_id",
            "financial_identifier",
            "payment_id",
            "transaction_id",
            "charge_id",
            "invoice_id",
            "billing_id",
            "contract_id",
            "proposal_id",
            "quota_id",
        ],
    },
    {
        "slug": "dado-pessoal",
        "name": "Dado pessoal",
        "keywords": ["dado_pessoal", "dado pessoal", "personal_data", "personal", "person", "cliente", "customer", "client", "titular"],
    },
    {
        "slug": "dado-sensivel",
        "name": "Dado sensível",
        "keywords": ["dado_sensivel", "dado sensível", "sensivel", "sensível", "confidential", "restricted", "private"],
    },
    {
        "slug": "dado-operacional",
        "name": "Dado operacional",
        "keywords": ["status", "state", "stage", "event", "source", "origin", "payload", "metadata", "flag", "indicator", "created_at", "updated_at", "processed_at", "executed_at", "reference", "sequence"],
    },
]


AUTO_APPLY_THRESHOLD = 90


@dataclass(frozen=True)
class TagIntelligenceSignal:
    entity_type: str
    entity_id: int
    tag_slug: str
    tag_name: str
    tag_id: int | None
    confidence_score: int
    inference_source: str
    inference_reason: str
    rule_key: str
    rule_label: str
    evidence: dict[str, object]
    applied_automatically: bool
    review_status: str


@dataclass
class TagIntelligenceSummary:
    table_id: int
    current_columns: int
    column_tags_applied: int = 0
    table_tags_applied: int = 0
    suggestions_created: int = 0
    assignments_updated: int = 0
    assignments_removed: int = 0
    blocked_assignments: int = 0
    manual_assignments_preserved: int = 0


def _normalize_text(value: str | None) -> str:
    raw = (value or "").strip().lower()
    if not raw:
        return ""
    normalized = unicodedata.normalize("NFKD", raw).encode("ascii", "ignore").decode("ascii")
    normalized = normalized.replace("_", " ").replace("-", " ").replace("/", " ")
    normalized = re.sub(r"[^a-z0-9\s]", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def _contains_any(text: str, keywords: Iterable[str]) -> list[str]:
    matches: list[str] = []
    for keyword in keywords:
        normalized = _normalize_text(keyword)
        if normalized and normalized in text:
            matches.append(keyword)
    return matches


 


def _table_text(table: TableEntity) -> str:
    schema = table.schema
    database = schema.database if schema else None
    parts = [
        table.name,
        table.description_source,
        table.description_manual,
        schema.name if schema else None,
        schema.description_source if schema else None,
        schema.description_manual if schema else None,
        database.name if database else None,
        database.description_source if database else None,
        database.description_manual if database else None,
    ]
    return " ".join(_normalize_text(part) for part in parts if part)


def _column_text(column: ColumnEntity, table: TableEntity) -> dict[str, str]:
    return {
        "name": _normalize_text(column.name),
        "description": _normalize_text(
            " ".join(
                part
                for part in (
                    column.dictionary_description,
                    column.dictionary_comment,
                    column.description_manual,
                    column.description_source,
                    column.existing_comment,
                )
                if part
            )
        ),
        "context": _normalize_text(
            " ".join(
                part
                for part in (
                    table.name,
                    table.description_manual,
                    table.description_source,
                    table.schema.name if table.schema else None,
                    table.schema.database.name if table.schema and table.schema.database else None,
                )
                if part
            )
        ),
        "data_type": _normalize_text(column.data_type),
    }


def _ensure_tag(
    session: Session,
    *,
    slug: str,
    name: str,
    color: str,
    scope: str,
) -> Tag:
    tag = session.scalar(select(Tag).where(Tag.slug == slug))
    if tag is None:
        tag = session.scalar(select(Tag).where(Tag.name == name))
    if tag is None:
        tag = Tag(
            slug=slugify_tag(slug) if not slug else slug,
            name=name,
            color=color,
            tag_type="classificacao_inteligente",
            suggested_scope=scope,
            status="active",
            group_name="Classificação inteligente",
            subgroup_name=scope.capitalize(),
        )
        session.add(tag)
        session.flush()
    return tag


def ensure_core_intelligence_tags(session: Session) -> dict[str, Tag]:
    tags: dict[str, Tag] = {}
    for definition in AUTO_TAG_DEFINITIONS:
        tags[definition["slug"]] = _ensure_tag(
            session,
            slug=definition["slug"],
            name=definition["name"],
            color=definition["color"],
            scope=definition["scope"],
        )
    return tags


def ensure_default_automation_rules(session: Session, tag_map: dict[str, Tag]) -> list[TagAutomationRule]:
    existing_rules = session.scalars(select(TagAutomationRule)).all()
    existing_by_tag = {rule.tag_id for rule in existing_rules}
    created: list[TagAutomationRule] = []
    for definition in DEFAULT_AUTOMATION_RULES:
        tag = tag_map.get(definition["slug"])
        if tag is None or tag.id in existing_by_tag:
            continue
        rule = TagAutomationRule(
            tag_id=tag.id,
            name=f"Regra automática: {definition['name']}",
            scope="column",
            status="active",
            action="apply",
            category="sensivel",
            priority=10,
            match_fields=["name", "description", "comment"],
            keywords=definition["keywords"],
            aliases=[],
            regex_pattern=None,
            min_confidence=90,
            notes="Regra padrão para dados sensíveis previsíveis.",
        )
        session.add(rule)
        created.append(rule)
    if created:
        session.flush()
    return created


def load_active_automation_rules(session: Session) -> list[TagAutomationRule]:
    return session.scalars(
        select(TagAutomationRule)
        .options(selectinload(TagAutomationRule.tag))
        .where(TagAutomationRule.status == "active")
    ).all()


def infer_column_signals(
    *,
    table: TableEntity,
    column: ColumnEntity,
    rules: list[TagAutomationRule],
) -> list[TagIntelligenceSignal]:
    text = _column_text(column, table)
    signals: list[TagIntelligenceSignal] = []

    for rule in sorted(rules, key=lambda item: (item.priority, item.id)):
        if rule.scope != "column" or rule.status != "active":
            continue
        match_fields = [field.strip().lower() for field in (rule.match_fields or []) if field]
        if not match_fields:
            match_fields = ["name", "description", "comment"]
        keywords = [value for value in (rule.keywords or []) if value]
        aliases = [value for value in (rule.aliases or []) if value]
        search_terms = list(dict.fromkeys(keywords + aliases))
        matched_sources: list[str] = []
        matched_terms: list[str] = []
        score = 0
        if "name" in match_fields:
            matches = _contains_any(text["name"], search_terms)
            if matches:
                matched_sources.append("column_name")
                matched_terms.extend(matches)
                score = max(score, 96)
        if "description" in match_fields or "comment" in match_fields:
            matches = _contains_any(text["description"], search_terms)
            if matches:
                matched_sources.append("description")
                matched_terms.extend(matches)
                score = max(score, 92)
        if rule.regex_pattern:
            try:
                if re.search(rule.regex_pattern, text["name"]):
                    matched_sources.append("regex")
                    matched_terms.append(rule.regex_pattern)
                    score = max(score, 97)
            except re.error:
                continue
        if not matched_sources:
            continue
        if score < int(rule.min_confidence or AUTO_APPLY_THRESHOLD):
            continue
        evidence = {
            "column_name": column.name,
            "data_type": column.data_type,
            "matched_terms": sorted(set(matched_terms)),
            "matched_sources": sorted(set(matched_sources)),
            "rule_id": rule.id,
            "rule_name": rule.name,
        }
        applied = rule.action == "apply"
        review_status = "auto_applied" if applied else "suggested"
        signals.append(
            TagIntelligenceSignal(
                entity_type="column",
                entity_id=column.id,
                tag_slug=rule.tag.slug if rule.tag else "",
                tag_name=rule.tag.name if rule.tag else rule.name,
                tag_id=None,
                confidence_score=score,
                inference_source="+".join(sorted(set(matched_sources))),
                inference_reason=f"{rule.name}: {', '.join(sorted(set(matched_terms)))}.",
                rule_key=f"rule:{rule.id}",
                rule_label=rule.name,
                evidence=evidence,
                applied_automatically=applied,
                review_status=review_status,
            )
        )
    if column.is_primary_key:
        signals.append(
            TagIntelligenceSignal(
                entity_type="column",
                entity_id=column.id,
                tag_slug="pk",
                tag_name="PK",
                tag_id=None,
                confidence_score=98,
                inference_source="schema",
                inference_reason="A coluna é chave primária no schema.",
                rule_key="column_primary_key",
                rule_label="Chave primária",
                evidence={
                    "column_name": column.name,
                    "data_type": column.data_type,
                    "is_primary_key": column.is_primary_key,
                },
                applied_automatically=True,
                review_status="auto_applied",
            )
        )
        signals.append(
            TagIntelligenceSignal(
                entity_type="column",
                entity_id=column.id,
                tag_slug="identificador-tecnico",
                tag_name="Identificador Técnico",
                tag_id=None,
                confidence_score=95,
                inference_source="schema",
                inference_reason="A coluna é chave primária no schema.",
                rule_key="column_identifier_technical",
                rule_label="Identificador técnico",
                evidence={
                    "column_name": column.name,
                    "data_type": column.data_type,
                    "is_primary_key": column.is_primary_key,
                },
                applied_automatically=True,
                review_status="auto_applied",
            )
        )
    if _normalize_text(column.data_type) in AUDITABLE_DATA_TYPES:
        signals.append(
            TagIntelligenceSignal(
                entity_type="column",
                entity_id=column.id,
                tag_slug="temporal",
                tag_name="Temporal",
                tag_id=None,
                confidence_score=92,
                inference_source="data_type",
                inference_reason="Tipo de dado temporal.",
                rule_key="column_temporal",
                rule_label="Campo temporal",
                evidence={
                    "column_name": column.name,
                    "data_type": column.data_type,
                },
                applied_automatically=True,
                review_status="auto_applied",
            )
        )
    if any(keyword in _normalize_text(column.data_type) for keyword in JSON_DATA_TYPES):
        signals.append(
            TagIntelligenceSignal(
                entity_type="column",
                entity_id=column.id,
                tag_slug="json",
                tag_name="JSON",
                tag_id=None,
                confidence_score=92,
                inference_source="data_type",
                inference_reason="Tipo de dado JSON.",
                rule_key="column_json",
                rule_label="Campo JSON",
                evidence={
                    "column_name": column.name,
                    "data_type": column.data_type,
                },
                applied_automatically=True,
                review_status="auto_applied",
            )
        )
    if not column.is_nullable or column.is_primary_key:
        signals.append(
            TagIntelligenceSignal(
                entity_type="column",
                entity_id=column.id,
                tag_slug="campo-obrigatorio",
                tag_name="Campo Obrigatório",
                tag_id=None,
                confidence_score=98 if not column.is_nullable else 96,
                inference_source="schema",
                inference_reason="A coluna é obrigatória no schema.",
                rule_key="column_required",
                rule_label="Campo obrigatório",
                evidence={
                    "column_name": column.name,
                    "data_type": column.data_type,
                    "is_nullable": column.is_nullable,
                    "is_primary_key": column.is_primary_key,
                },
                applied_automatically=True,
                review_status="auto_applied",
            )
        )
    audit_name_matches = _contains_any(text["name"], AUDITABLE_KEYWORDS)
    audit_desc_matches = _contains_any(text["description"], AUDITABLE_KEYWORDS)
    if audit_name_matches or audit_desc_matches or _normalize_text(column.data_type) in AUDITABLE_DATA_TYPES:
        signals.append(
            TagIntelligenceSignal(
                entity_type="column",
                entity_id=column.id,
                tag_slug="campo-auditavel",
                tag_name="Campo Auditável",
                tag_id=None,
                confidence_score=95 if audit_name_matches or audit_desc_matches else 90,
                inference_source="column_name+dictionary" if audit_name_matches and audit_desc_matches else ("column_name" if audit_name_matches else "data_type"),
                inference_reason="A coluna parece registrar trilha de auditoria, tempo ou histórico.",
                rule_key="column_auditable",
                rule_label="Campo auditável",
                evidence={
                    "column_name": column.name,
                    "data_type": column.data_type,
                    "matched_terms": sorted(set(audit_name_matches + audit_desc_matches)),
                    "is_nullable": column.is_nullable,
                    "is_primary_key": column.is_primary_key,
                },
                applied_automatically=True,
                review_status="auto_applied",
            )
        )
    return signals


def infer_table_signals(
    *,
    table: TableEntity,
    column_signals: list[TagIntelligenceSignal],
) -> list[TagIntelligenceSignal]:
    signals: list[TagIntelligenceSignal] = []
    table_text = _table_text(table)
    pii_columns = [signal for signal in column_signals if signal.tag_slug == "pii" and signal.applied_automatically]
    sensitive_columns = [signal for signal in column_signals if signal.tag_slug == "sensivel" and signal.applied_automatically]
    critical_columns = [signal for signal in column_signals if signal.tag_slug == "coluna-critica" and signal.applied_automatically]

    def add_table_signal(
        tag_slug: str,
        tag_name: str,
        rule_key: str,
        rule_label: str,
        *,
        evidence: dict[str, object],
        score: int,
    ) -> None:
        signals.append(
            TagIntelligenceSignal(
                entity_type="table",
                entity_id=table.id,
                tag_slug=tag_slug,
                tag_name=tag_name,
                tag_id=None,
                confidence_score=score,
                inference_source="column_tags" if evidence.get("source_columns") else "table_context",
                inference_reason=f"{rule_label}: {', '.join(evidence.get('matched_terms', [])) or 'contexto da tabela'}.",
                rule_key=rule_key,
                rule_label=rule_label,
                evidence=evidence,
                applied_automatically=True,
                review_status="auto_applied",
            )
        )

    if pii_columns:
        add_table_signal(
            "contem-pii",
            "Contém PII",
            "table_contains_pii",
            "A tabela contém PII",
            evidence={
                "source_columns": [
                    {"column_id": signal.entity_id, "column_name": signal.evidence.get("column_name"), "reason": signal.inference_reason}
                    for signal in pii_columns
                ],
                "matched_terms": sorted({term for signal in pii_columns for term in signal.evidence.get("matched_terms", [])}),
            },
            score=min(100, 88 + min(8, len(pii_columns) * 2)),
        )
    if sensitive_columns:
        add_table_signal(
            "contem-dados-sensiveis",
            "Contém Dados Sensíveis",
            "table_contains_sensitive",
            "A tabela contém dados sensíveis",
            evidence={
                "source_columns": [
                    {"column_id": signal.entity_id, "column_name": signal.evidence.get("column_name"), "reason": signal.inference_reason}
                    for signal in sensitive_columns
                ],
                "matched_terms": sorted({term for signal in sensitive_columns for term in signal.evidence.get("matched_terms", [])}),
            },
            score=min(100, 90 + min(8, len(sensitive_columns) * 2)),
        )
    if critical_columns:
        add_table_signal(
            "possui-coluna-critica",
            "Possui Coluna Crítica",
            "table_has_critical_column",
            "A tabela possui coluna crítica",
            evidence={
                "source_columns": [
                    {"column_id": signal.entity_id, "column_name": signal.evidence.get("column_name"), "reason": signal.inference_reason}
                    for signal in critical_columns
                ],
                "matched_terms": sorted({term for signal in critical_columns for term in signal.evidence.get("matched_terms", [])}),
            },
            score=min(100, 86 + min(10, len(critical_columns) * 2)),
        )

    if (
        pii_columns
        or sensitive_columns
        or critical_columns
        or table.has_personal_data
        or table.has_sensitive_personal_data
        or (table.sensitivity_level or "").lower() in {"confidential", "restricted"}
        or "lgpd" in table_text
        or "privacidade" in table_text
    ):
        add_table_signal(
            "requer-governanca",
            "Requer Governança",
            "table_requires_governance",
            "A tabela requer governança reforçada",
            evidence={
                "source_columns": [
                    {"column_id": signal.entity_id, "column_name": signal.evidence.get("column_name"), "reason": signal.inference_reason}
                    for signal in pii_columns + sensitive_columns + critical_columns
                ],
                "matched_terms": sorted(
                    {
                        term
                        for signal in pii_columns + sensitive_columns + critical_columns
                        for term in signal.evidence.get("matched_terms", [])
                    }
                ),
                "table_flags": {
                    "has_personal_data": table.has_personal_data,
                    "has_sensitive_personal_data": table.has_sensitive_personal_data,
                    "sensitivity_level": table.sensitivity_level,
                },
            },
            score=84,
        )
        add_table_signal(
            "alta-prioridade-dq",
            "Alta Prioridade DQ",
            "table_high_dq_priority",
            "A tabela deve entrar em prioridade de DQ",
            evidence={
                "source_columns": [
                    {"column_id": signal.entity_id, "column_name": signal.evidence.get("column_name"), "reason": signal.inference_reason}
                    for signal in pii_columns + sensitive_columns + critical_columns
                ],
                "matched_terms": sorted(
                    {
                        term
                        for signal in pii_columns + sensitive_columns + critical_columns
                        for term in signal.evidence.get("matched_terms", [])
                    }
                ),
            },
            score=82,
        )

    return signals


def _assignment_identity(assignment: TagAssignment) -> tuple[str, int, int]:
    return assignment.entity_type, int(assignment.entity_id), int(assignment.tag_id)


def _override_exists(session: Session, *, tag_id: int, entity_type: str, entity_id: int) -> bool:
    return bool(
        session.scalar(
            select(TagAssignmentOverride.id).where(
                TagAssignmentOverride.tag_id == tag_id,
                TagAssignmentOverride.entity_type == entity_type,
                TagAssignmentOverride.entity_id == entity_id,
                TagAssignmentOverride.state == "blocked",
            )
        )
    )


def _existing_assignment_map(session: Session, *, entity_type: str, entity_ids: list[int]) -> dict[tuple[str, int, int], TagAssignment]:
    if not entity_ids:
        return {}
    rows = session.scalars(
        select(TagAssignment).where(
            TagAssignment.entity_type == entity_type,
            TagAssignment.entity_id.in_(entity_ids),
        )
    ).all()
    return {_assignment_identity(row): row for row in rows}


def _record_event(
    session: Session,
    *,
    signal: TagIntelligenceSignal,
    datasource_id: int | None,
    actor_user_id: int | None,
    event_status: str,
    metadata: dict[str, object] | None = None,
) -> None:
    session.add(
        TagIntelligenceEvent(
            tag_id=signal.tag_id or 0,
            datasource_id=datasource_id,
            entity_type=signal.entity_type,
            entity_id=signal.entity_id,
            rule_key=signal.rule_key,
            rule_label=signal.rule_label,
            inference_source=signal.inference_source,
            inference_reason=signal.inference_reason,
            confidence_score=signal.confidence_score,
            applied_automatically=signal.applied_automatically,
            review_status=event_status,
            evidence=metadata or signal.evidence,
            created_by_user_id=actor_user_id,
        )
    )


def _attach_tag_id(signal: TagIntelligenceSignal, *, tag_id: int) -> TagIntelligenceSignal:
    return TagIntelligenceSignal(
        entity_type=signal.entity_type,
        entity_id=signal.entity_id,
        tag_slug=signal.tag_slug,
        tag_name=signal.tag_name,
        tag_id=tag_id,
        confidence_score=signal.confidence_score,
        inference_source=signal.inference_source,
        inference_reason=signal.inference_reason,
        rule_key=signal.rule_key,
        rule_label=signal.rule_label,
        evidence={**signal.evidence, "tag_id": tag_id},
        applied_automatically=signal.applied_automatically,
        review_status=signal.review_status,
    )


def _upsert_assignment_from_signal(
    session: Session,
    *,
    signal: TagIntelligenceSignal,
    tag: Tag,
    datasource_id: int | None,
    actor_user_id: int | None,
    manual: bool = False,
) -> tuple[TagAssignment, bool]:
    assignment = session.scalar(
        select(TagAssignment).where(
            TagAssignment.tag_id == tag.id,
            TagAssignment.entity_type == signal.entity_type,
            TagAssignment.entity_id == signal.entity_id,
        )
    )
    if assignment is None:
        assignment = TagAssignment(
            tag_id=tag.id,
            datasource_id=datasource_id,
            entity_type=signal.entity_type,
            entity_id=signal.entity_id,
            confidence_score=signal.confidence_score if not manual else 100,
            inference_source=signal.inference_source if not manual else "manual",
            inference_reason=signal.inference_reason if not manual else None,
            evidence_json=signal.evidence if not manual else signal.evidence,
            applied_automatically=not manual,
            review_status="manual_applied" if manual else "auto_applied",
            rule_key=signal.rule_key if not manual else None,
            rule_label=signal.rule_label if not manual else None,
            reviewed_by_user_id=actor_user_id if manual else None,
            reviewed_at=datetime.now(timezone.utc) if manual else None,
        )
        session.add(assignment)
        session.flush()
        return assignment, True

    if manual:
        assignment.datasource_id = datasource_id or assignment.datasource_id
        assignment.confidence_score = 100
        assignment.inference_source = "manual"
        assignment.inference_reason = None
        assignment.evidence_json = signal.evidence
        assignment.applied_automatically = False
        assignment.review_status = "manual_applied"
        assignment.rule_key = None
        assignment.rule_label = None
        assignment.reviewed_by_user_id = actor_user_id
        assignment.reviewed_at = datetime.now(timezone.utc)
        session.flush()
        return assignment, False

    if not assignment.applied_automatically:
        return assignment, False

    assignment.datasource_id = datasource_id or assignment.datasource_id
    assignment.confidence_score = signal.confidence_score
    assignment.inference_source = signal.inference_source
    assignment.inference_reason = signal.inference_reason
    assignment.evidence_json = signal.evidence
    assignment.applied_automatically = True
    assignment.review_status = "auto_applied"
    assignment.rule_key = signal.rule_key
    assignment.rule_label = signal.rule_label
    assignment.reviewed_by_user_id = actor_user_id
    assignment.reviewed_at = None
    session.flush()
    return assignment, False


def _delete_auto_assignment_if_present(
    session: Session,
    *,
    tag_id: int,
    entity_type: str,
    entity_id: int,
) -> bool:
    assignment = session.scalar(
        select(TagAssignment).where(
            TagAssignment.tag_id == tag_id,
            TagAssignment.entity_type == entity_type,
            TagAssignment.entity_id == entity_id,
        )
    )
    if assignment is None or not assignment.applied_automatically:
        return False
    session.delete(assignment)
    return True


def _clear_override(session: Session, *, tag_id: int, entity_type: str, entity_id: int) -> None:
    session.execute(
        delete(TagAssignmentOverride).where(
            TagAssignmentOverride.tag_id == tag_id,
            TagAssignmentOverride.entity_type == entity_type,
            TagAssignmentOverride.entity_id == entity_id,
        )
    )


def _set_override_block(
    session: Session,
    *,
    tag_id: int,
    entity_type: str,
    entity_id: int,
    datasource_id: int | None,
    actor_user_id: int | None,
    reason: str | None = None,
) -> None:
    override = session.scalar(
        select(TagAssignmentOverride).where(
            TagAssignmentOverride.tag_id == tag_id,
            TagAssignmentOverride.entity_type == entity_type,
            TagAssignmentOverride.entity_id == entity_id,
        )
    )
    if override is None:
        override = TagAssignmentOverride(
            tag_id=tag_id,
            datasource_id=datasource_id,
            entity_type=entity_type,
            entity_id=entity_id,
            state="blocked",
            reason=reason,
            created_by_user_id=actor_user_id,
        )
        session.add(override)
        return
    override.datasource_id = datasource_id or override.datasource_id
    override.state = "blocked"
    override.reason = reason
    override.created_by_user_id = actor_user_id or override.created_by_user_id


def reprocess_table_tag_intelligence(
    session: Session,
    *,
    table_id: int,
    actor_user_id: int | None = None,
    audit_kwargs: dict | None = None,
    source_module: str = "tags.intelligence",
    metadata: dict | None = None,
) -> dict[str, int]:
    table = session.scalar(
        select(TableEntity)
        .options(
            selectinload(TableEntity.columns),
            selectinload(TableEntity.schema).selectinload(Schema.database).selectinload(Database.datasource),
        )
        .where(TableEntity.id == table_id)
    )
    if table is None:
        return {
            "table_id": table_id,
            "current_columns": 0,
            "column_tags_applied": 0,
            "table_tags_applied": 0,
            "suggestions_created": 0,
            "assignments_updated": 0,
            "assignments_removed": 0,
            "blocked_assignments": 0,
            "manual_assignments_preserved": 0,
        }

    tag_map = ensure_core_intelligence_tags(session)
    ensure_default_automation_rules(session, tag_map)
    rules = load_active_automation_rules(session)
    datasource_id = table.schema.database.datasource_id if table.schema and table.schema.database else None
    columns = list(table.columns or [])
    current_columns = len(columns)

    column_signals: list[TagIntelligenceSignal] = []
    column_high_confidence: dict[int, list[TagIntelligenceSignal]] = {}
    column_suggestions: list[TagIntelligenceSignal] = []
    for column in columns:
        signals = infer_column_signals(table=table, column=column, rules=rules)
        for signal in signals:
            signal = _attach_tag_id(signal, tag_id=tag_map[signal.tag_slug].id)
            column_signals.append(signal)
            if signal.applied_automatically:
                column_high_confidence.setdefault(column.id, []).append(signal)
            else:
                column_suggestions.append(signal)

    table_signals = infer_table_signals(table=table, column_signals=column_signals)
    table_high_confidence = [signal for signal in table_signals if signal.applied_automatically]

    summary = TagIntelligenceSummary(table_id=table.id, current_columns=current_columns)
    column_entity_ids = [column.id for column in columns]
    current_column_assignments = _existing_assignment_map(session, entity_type="column", entity_ids=column_entity_ids)
    current_table_assignments = _existing_assignment_map(session, entity_type="table", entity_ids=[table.id])

    desired_auto_assignments: dict[tuple[str, int, int], TagIntelligenceSignal] = {}
    for signal in column_signals:
        if signal.applied_automatically:
            desired_auto_assignments[(signal.entity_type, signal.entity_id, tag_map[signal.tag_slug].id)] = signal
    for signal in table_high_confidence:
        desired_auto_assignments[(signal.entity_type, signal.entity_id, tag_map[signal.tag_slug].id)] = signal

    # Remove stale automatic assignments first so they can be replaced cleanly.
    for key, assignment in list({**current_column_assignments, **current_table_assignments}.items()):
        if not assignment.applied_automatically:
            continue
        desired_signal = desired_auto_assignments.get(key)
        if desired_signal is not None:
            assignment.confidence_score = desired_signal.confidence_score
            assignment.inference_source = desired_signal.inference_source
            assignment.inference_reason = desired_signal.inference_reason
            assignment.evidence_json = desired_signal.evidence
            assignment.review_status = "auto_applied"
            assignment.rule_key = desired_signal.rule_key
            assignment.rule_label = desired_signal.rule_label
            assignment.reviewed_by_user_id = actor_user_id
            assignment.reviewed_at = None
            summary.assignments_updated += 1
            continue
        if _override_exists(session, tag_id=assignment.tag_id, entity_type=assignment.entity_type, entity_id=assignment.entity_id):
            _record_event(
                session,
                signal=_attach_tag_id(
                    TagIntelligenceSignal(
                        entity_type=assignment.entity_type,
                        entity_id=assignment.entity_id,
                        tag_slug="",
                        tag_name="",
                        tag_id=None,
                        confidence_score=assignment.confidence_score,
                        inference_source=assignment.inference_source or "automatic",
                        inference_reason=assignment.inference_reason or "Removido por bloqueio manual.",
                        rule_key=assignment.rule_key or "manual_block",
                        rule_label=assignment.rule_label or "Bloqueio manual",
                        evidence={"assignment_id": assignment.id},
                        applied_automatically=False,
                        review_status="blocked",
                    ),
                    tag_id=assignment.tag_id,
                ),
                datasource_id=datasource_id,
                actor_user_id=actor_user_id,
                event_status="blocked",
                metadata={"assignment_id": assignment.id, "table_id": table.id},
            )
            session.delete(assignment)
            summary.blocked_assignments += 1
            continue
        session.delete(assignment)
        summary.assignments_removed += 1
        _record_event(
            session,
            signal=_attach_tag_id(
                TagIntelligenceSignal(
                    entity_type=assignment.entity_type,
                    entity_id=assignment.entity_id,
                    tag_slug="",
                    tag_name="",
                    tag_id=None,
                    confidence_score=assignment.confidence_score,
                    inference_source=assignment.inference_source or "automatic",
                    inference_reason=assignment.inference_reason or "A regra deixou de corresponder.",
                    rule_key=assignment.rule_key or "auto_removed",
                    rule_label=assignment.rule_label or "Regra automática removida",
                    evidence={"assignment_id": assignment.id},
                    applied_automatically=False,
                    review_status="removed",
                ),
                tag_id=assignment.tag_id,
            ),
            datasource_id=datasource_id,
            actor_user_id=actor_user_id,
            event_status="removed",
            metadata={"assignment_id": assignment.id, "table_id": table.id},
        )

    # Apply high-confidence column and table assignments.
    for signal in column_signals + table_high_confidence:
        tag = tag_map[signal.tag_slug]
        key = (signal.entity_type, signal.entity_id, tag.id)
        if _override_exists(session, tag_id=tag.id, entity_type=signal.entity_type, entity_id=signal.entity_id):
            summary.blocked_assignments += 1
            _record_event(
                session,
                signal=signal,
                datasource_id=datasource_id,
                actor_user_id=actor_user_id,
                event_status="blocked",
                metadata={**signal.evidence, "tag_id": tag.id, "table_id": table.id},
            )
            continue
        current_assignment = (current_column_assignments | current_table_assignments).get(key)
        if current_assignment and not current_assignment.applied_automatically:
            summary.manual_assignments_preserved += 1
            continue
        assignment, created = _upsert_assignment_from_signal(
            session,
            signal=signal,
            tag=tag,
            datasource_id=datasource_id,
            actor_user_id=actor_user_id,
            manual=False,
        )
        summary.assignments_updated += int(created or signal.applied_automatically)
        if signal.entity_type == "column":
            summary.column_tags_applied += 1
        else:
            summary.table_tags_applied += 1
        _record_event(
            session,
            signal=_attach_tag_id(signal, tag_id=tag.id),
            datasource_id=datasource_id,
            actor_user_id=actor_user_id,
            event_status="auto_applied",
            metadata={**signal.evidence, "assignment_id": assignment.id, "table_id": table.id},
        )

    # Suggestions that did not reach automatic threshold.
    for signal in column_suggestions:
        tag = tag_map[signal.tag_slug]
        if _override_exists(session, tag_id=tag.id, entity_type=signal.entity_type, entity_id=signal.entity_id):
            summary.blocked_assignments += 1
            _record_event(
                session,
                signal=_attach_tag_id(signal, tag_id=tag.id),
                datasource_id=datasource_id,
                actor_user_id=actor_user_id,
                event_status="blocked",
                metadata={**signal.evidence, "table_id": table.id},
            )
            continue
        current_assignment = current_column_assignments.get((signal.entity_type, signal.entity_id, tag.id))
        if current_assignment and not current_assignment.applied_automatically:
            summary.manual_assignments_preserved += 1
            continue
        summary.suggestions_created += 1
        _record_event(
            session,
            signal=_attach_tag_id(signal, tag_id=tag.id),
            datasource_id=datasource_id,
            actor_user_id=actor_user_id,
            event_status="suggested",
            metadata={**signal.evidence, "table_id": table.id},
        )

    payload = asdict(summary)
    payload.update(metadata or {})
    write_audit_log_sync(
        session,
        action="tag.intelligence.reprocess",
        entity_type="table",
        entity_id=table.id,
        source_module=source_module,
        metadata=payload,
        **(audit_kwargs or {}),
    )
    return payload


def _manual_assignment_signal(
    *,
    entity_type: str,
    entity_id: int,
    tag_id: int,
    tag_name: str,
    datasource_id: int | None,
    reason: str | None,
    user_id: int | None,
) -> TagIntelligenceSignal:
    return TagIntelligenceSignal(
        entity_type=entity_type,
        entity_id=entity_id,
        tag_slug=slugify_tag(tag_name),
        tag_name=tag_name,
        tag_id=tag_id,
        confidence_score=100,
        inference_source="manual",
        inference_reason=reason or "Atribuição manual",
        rule_key="manual_assignment",
        rule_label="Atribuição manual",
        evidence={"tag_id": tag_id, "datasource_id": datasource_id},
        applied_automatically=False,
        review_status="manual_applied",
    )


def manual_assign_tag(
    session: Session,
    *,
    tag_id: int,
    entity_type: str,
    entity_id: int,
    datasource_id: int | None,
    actor_user_id: int | None = None,
    reason: str | None = None,
) -> TagAssignment:
    tag = session.get(Tag, tag_id)
    if tag is None:
        raise ValueError("Tag not found")
    _clear_override(session, tag_id=tag.id, entity_type=entity_type, entity_id=entity_id)
    signal = _manual_assignment_signal(
        entity_type=entity_type,
        entity_id=entity_id,
        tag_id=tag.id,
        tag_name=tag.name,
        datasource_id=datasource_id,
        reason=reason,
        user_id=actor_user_id,
    )
    assignment, _created = _upsert_assignment_from_signal(
        session,
        signal=_attach_tag_id(signal, tag_id=tag.id),
        tag=tag,
        datasource_id=datasource_id,
        actor_user_id=actor_user_id,
        manual=True,
    )
    _record_event(
        session,
        signal=_attach_tag_id(signal, tag_id=tag.id),
        datasource_id=datasource_id,
        actor_user_id=actor_user_id,
        event_status="manual_applied",
        metadata={"assignment_id": assignment.id},
    )
    return assignment


def manual_unassign_tag(
    session: Session,
    *,
    tag_id: int,
    entity_type: str,
    entity_id: int,
    datasource_id: int | None,
    actor_user_id: int | None = None,
    reason: str | None = None,
) -> None:
    tag = session.get(Tag, tag_id)
    assignment = session.scalar(
        select(TagAssignment).where(
            TagAssignment.tag_id == tag_id,
            TagAssignment.entity_type == entity_type,
            TagAssignment.entity_id == entity_id,
        )
    )
    if assignment is not None:
        session.delete(assignment)
    _set_override_block(
        session,
        tag_id=tag_id,
        entity_type=entity_type,
        entity_id=entity_id,
        datasource_id=datasource_id,
        actor_user_id=actor_user_id,
        reason=reason,
    )
    if tag is not None:
        signal = _manual_assignment_signal(
            entity_type=entity_type,
            entity_id=entity_id,
            tag_id=tag.id,
            tag_name=tag.name,
            datasource_id=datasource_id,
            reason=reason,
            user_id=actor_user_id,
        )
        _record_event(
            session,
            signal=_attach_tag_id(signal, tag_id=tag.id),
            datasource_id=datasource_id,
            actor_user_id=actor_user_id,
            event_status="manual_unassigned",
            metadata={"reason": reason, "assignment_id": getattr(assignment, "id", None)},
        )


def purge_tag_intelligence_for_entity_ids(
    session: Session,
    *,
    entity_type: str,
    entity_ids: list[int],
    delete_history: bool = False,
) -> None:
    if not entity_ids:
        return
    session.execute(
        delete(TagAssignment).where(
            TagAssignment.entity_type == entity_type,
            TagAssignment.entity_id.in_(entity_ids),
        )
    )
    session.execute(
        delete(TagAssignmentOverride).where(
            TagAssignmentOverride.entity_type == entity_type,
            TagAssignmentOverride.entity_id.in_(entity_ids),
        )
    )
    if delete_history:
        session.execute(
            delete(TagIntelligenceEvent).where(
                TagIntelligenceEvent.entity_type == entity_type,
                TagIntelligenceEvent.entity_id.in_(entity_ids),
            )
        )


def load_pending_tag_intelligence_events(
    session: Session,
    *,
    limit: int | None = 100,
    entity_type: str | None = None,
    table_id: int | None = None,
    column_id: int | None = None,
    table_query: str | None = None,
    column_query: str | None = None,
    tag_slug: str | None = None,
    inference_source: str | None = None,
    review_status: str | None = None,
    risk_band: str | None = None,
    min_confidence: int | None = None,
    max_confidence: int | None = None,
    sort_by: str = "risk_desc",
) -> list[dict[str, object]]:
    events = session.scalars(
        select(TagIntelligenceEvent)
        .where(TagIntelligenceEvent.review_status.in_(["pending_review", "suggested"]))
        .order_by(TagIntelligenceEvent.created_at.desc())
    ).all()
    if not events:
        return []

    tag_ids = sorted({int(event.tag_id) for event in events})
    tags = {tag.id: tag for tag in session.scalars(select(Tag).where(Tag.id.in_(tag_ids))).all()}
    column_ids = [int(event.entity_id) for event in events if event.entity_type == "column"]
    table_ids = [int(event.entity_id) for event in events if event.entity_type == "table"]
    columns = {
        column.id: column
        for column in session.scalars(select(ColumnEntity).where(ColumnEntity.id.in_(column_ids))).all()
    } if column_ids else {}
    tables = {
        table.id: table
        for table in session.scalars(
            select(TableEntity)
            .options(selectinload(TableEntity.schema).selectinload(Schema.database).selectinload(Database.datasource))
            .where(TableEntity.id.in_(table_ids))
        ).all()
    } if table_ids else {}

    payloads: list[dict[str, object]] = []
    for event in events:
        tag = tags.get(int(event.tag_id))
        table = tables.get(int(event.entity_id)) if event.entity_type == "table" else None
        column = columns.get(int(event.entity_id)) if event.entity_type == "column" else None
        if tag is None:
            continue
        datasource = getattr(getattr(getattr(table, "schema", None), "database", None), "datasource", None) if table else None
        if column is not None:
            table = getattr(column, "table", None) or table
            if table is not None and getattr(table, "schema", None) is None:
                table = session.scalar(
                    select(TableEntity)
                    .options(selectinload(TableEntity.schema).selectinload(Schema.database).selectinload(Database.datasource))
                    .where(TableEntity.id == column.table_id)
                ) or table
            datasource = getattr(getattr(getattr(table, "schema", None), "database", None), "datasource", None) if table else datasource
        table_schema = getattr(getattr(table, "schema", None), "name", None) if table else None
        table_database = getattr(getattr(getattr(table, "schema", None), "database", None), "name", None) if table else None
        table_datasource = getattr(getattr(getattr(getattr(table, "schema", None), "database", None), "datasource", None), "name", None) if table else None
        explorer_url: str | None = None
        if table is not None:
            if column is not None:
                explorer_url = f"/explorer?tableId={table.id}&tab=columns&columnId={column.id}"
            elif event.entity_type == "table":
                explorer_url = f"/explorer?tableId={table.id}&tab=tags"
            else:
                explorer_url = f"/explorer?tableId={table.id}"
        payloads.append(
            {
                "id": int(event.id),
                "tag_id": int(event.tag_id),
                "tag_name": tag.name,
                "tag_slug": tag.slug,
                "entity_type": event.entity_type,
                "entity_id": int(event.entity_id),
                "datasource_id": int(event.datasource_id) if event.datasource_id is not None else getattr(datasource, "id", None),
                "datasource_name": table_datasource,
                "database_name": table_database,
                "schema_name": table_schema,
                "table_id": table.id if table else None,
                "table_name": table.name if table else None,
                "column_id": column.id if column else None,
                "column_name": column.name if column else None,
                "table_fqn": f"{table_schema}.{table.name}" if table and table_schema else table.name if table else None,
                "rule_key": event.rule_key,
                "rule_label": event.rule_label,
                "inference_source": event.inference_source,
                "inference_reason": event.inference_reason,
                "confidence_score": int(event.confidence_score or 0),
                "applied_automatically": bool(event.applied_automatically),
                "review_status": event.review_status,
                "evidence": event.evidence,
                "explorer_url": explorer_url,
                "created_by_user_id": event.created_by_user_id,
                "reviewed_by_user_id": event.reviewed_by_user_id,
                "reviewed_at": event.reviewed_at,
                "created_at": event.created_at,
                "updated_at": event.updated_at,
            }
        )

    normalized_entity_type = (entity_type or "").strip().lower()
    normalized_table_query = _normalize_text(table_query)
    normalized_column_query = _normalize_text(column_query)

    filtered_payloads: list[dict[str, object]] = []
    normalized_tag_slug = _normalize_text(tag_slug)
    normalized_inference_source = _normalize_text(inference_source)
    normalized_review_status = _normalize_text(review_status)
    normalized_risk_band = _normalize_text(risk_band)
    for item in payloads:
        if normalized_entity_type and item.get("entity_type") != normalized_entity_type:
            continue
        if table_id is not None and item.get("table_id") != table_id:
            continue
        if column_id is not None and item.get("column_id") != column_id:
            continue
        if normalized_tag_slug and normalized_tag_slug not in _normalize_text(str(item.get("tag_slug") or "")):
            continue
        if normalized_inference_source and normalized_inference_source not in _normalize_text(str(item.get("inference_source") or "")):
            continue
        if normalized_review_status and normalized_review_status != _normalize_text(str(item.get("review_status") or "")):
            continue
        confidence = int(item.get("confidence_score") or 0)
        if min_confidence is not None and confidence < min_confidence:
            continue
        if max_confidence is not None and confidence > max_confidence:
            continue
        if normalized_risk_band:
            if confidence >= 80:
                item_band = "low"
            elif confidence >= 60:
                item_band = "medium"
            else:
                item_band = "high"
            if normalized_risk_band != item_band:
                continue
        if normalized_table_query:
            table_name = _normalize_text(str(item.get("table_name") or ""))
            table_fqn = _normalize_text(str(item.get("table_fqn") or ""))
            if normalized_table_query not in table_name and normalized_table_query not in table_fqn:
                continue
        if normalized_column_query:
            column_name = _normalize_text(str(item.get("column_name") or ""))
            if normalized_column_query not in column_name:
                continue
        filtered_payloads.append(item)

    def sort_key(item: dict[str, object]):
        confidence = float(int(item.get("confidence_score") or 0))
        created_at = item.get("created_at")
        timestamp = created_at.timestamp() if isinstance(created_at, datetime) else 0.0
        if sort_by == "certainty_desc":
            return (-confidence, -timestamp)
        if sort_by == "newest":
            return (-timestamp, confidence)
        if sort_by == "oldest":
            return (timestamp, -confidence)
        if sort_by == "table_asc":
            return (_normalize_text(str(item.get("table_fqn") or item.get("table_name") or "")), -timestamp)
        if sort_by == "tag_asc":
            return (_normalize_text(str(item.get("tag_name") or "")), -timestamp)
        return (confidence, -timestamp)

    filtered_payloads.sort(key=sort_key)
    if limit is None:
        return filtered_payloads
    return filtered_payloads[:limit]


def _batch_action_result(
    *,
    action: str,
    event_ids: list[int],
    succeeded_ids: list[int],
    failed_items: list[dict[str, object]],
) -> dict[str, object]:
    return {
        "action": action,
        "requested": len(dict.fromkeys(event_ids)),
        "succeeded": len(succeeded_ids),
        "failed": len(failed_items),
        "applied_ids": succeeded_ids,
        "failed_items": failed_items,
    }


def batch_apply_tag_intelligence_events(
    session: Session,
    *,
    event_ids: list[int],
    actor_user_id: int | None,
) -> dict[str, object]:
    succeeded_ids: list[int] = []
    failed_items: list[dict[str, object]] = []
    for event_id in dict.fromkeys(event_ids):
        try:
            apply_tag_intelligence_event(session, event_id=event_id, actor_user_id=actor_user_id)
            succeeded_ids.append(event_id)
        except Exception as exc:  # pragma: no cover - defensive aggregation
            failed_items.append({"event_id": event_id, "message": str(exc)})
    return _batch_action_result(action="apply", event_ids=event_ids, succeeded_ids=succeeded_ids, failed_items=failed_items)


def batch_dismiss_tag_intelligence_events(
    session: Session,
    *,
    event_ids: list[int],
    actor_user_id: int | None,
    reason: str | None = None,
) -> dict[str, object]:
    succeeded_ids: list[int] = []
    failed_items: list[dict[str, object]] = []
    for event_id in dict.fromkeys(event_ids):
        try:
            dismiss_tag_intelligence_event(session, event_id=event_id, actor_user_id=actor_user_id, reason=reason)
            succeeded_ids.append(event_id)
        except Exception as exc:  # pragma: no cover - defensive aggregation
            failed_items.append({"event_id": event_id, "message": str(exc)})
    return _batch_action_result(action="block", event_ids=event_ids, succeeded_ids=succeeded_ids, failed_items=failed_items)


def apply_tag_intelligence_event(
    session: Session,
    *,
    event_id: int,
    actor_user_id: int | None,
) -> dict[str, object]:
    event = session.get(TagIntelligenceEvent, event_id)
    if event is None:
        raise ValueError("Tag intelligence event not found")
    tag = session.get(Tag, event.tag_id)
    if tag is None:
        raise ValueError("Tag not found")
    datasource_id = event.datasource_id
    if datasource_id is None:
        if event.entity_type == "table":
            datasource_id = session.scalar(
                select(Database.datasource_id)
                .join(Schema, Schema.database_id == Database.id)
                .join(TableEntity, TableEntity.schema_id == Schema.id)
                .where(TableEntity.id == event.entity_id)
            )
        elif event.entity_type == "column":
            datasource_id = session.scalar(
                select(Database.datasource_id)
                .join(Schema, Schema.database_id == Database.id)
                .join(TableEntity, TableEntity.schema_id == Schema.id)
                .join(ColumnEntity, ColumnEntity.table_id == TableEntity.id)
                .where(ColumnEntity.id == event.entity_id)
            )
    manual_assign_tag(
        session,
        tag_id=tag.id,
        entity_type=event.entity_type,
        entity_id=event.entity_id,
        datasource_id=datasource_id,
        actor_user_id=actor_user_id,
        reason=event.inference_reason or "Sugestão aplicada manualmente.",
    )
    if event.entity_type == "column":
        try:
            with session.begin_nested():
                from t2c_data.features.governance.column_classification import record_column_classification_decision

                record_column_classification_decision(
                    session,
                    column_id=event.entity_id,
                    taxonomy_key=tag.slug,
                    source_kind=event.inference_source or "tag_intelligence",
                    confidence_score=int(event.confidence_score or 0),
                    decision_status="approved",
                    evidence_json=event.evidence or {},
                    notes=event.inference_reason,
                    reviewed_by_user_id=actor_user_id,
                    reviewed_at=datetime.now(timezone.utc),
                    persist_current=True,
                )
        except SQLAlchemyError:
            pass
    event.review_status = "manual_applied"
    event.reviewed_by_user_id = actor_user_id
    event.reviewed_at = datetime.now(timezone.utc)
    session.add(event)
    return {"success": True, "event_id": event_id, "status": event.review_status}


def dismiss_tag_intelligence_event(
    session: Session,
    *,
    event_id: int,
    actor_user_id: int | None,
    reason: str | None = None,
) -> dict[str, object]:
    event = session.get(TagIntelligenceEvent, event_id)
    if event is None:
        raise ValueError("Tag intelligence event not found")
    datasource_id = event.datasource_id
    if datasource_id is None:
        if event.entity_type == "table":
            datasource_id = session.scalar(
                select(Database.datasource_id)
                .join(Schema, Schema.database_id == Database.id)
                .join(TableEntity, TableEntity.schema_id == Schema.id)
                .where(TableEntity.id == event.entity_id)
            )
        elif event.entity_type == "column":
            datasource_id = session.scalar(
                select(Database.datasource_id)
                .join(Schema, Schema.database_id == Database.id)
                .join(TableEntity, TableEntity.schema_id == Schema.id)
                .join(ColumnEntity, ColumnEntity.table_id == TableEntity.id)
                .where(ColumnEntity.id == event.entity_id)
            )
    _set_override_block(
        session,
        tag_id=event.tag_id,
        entity_type=event.entity_type,
        entity_id=event.entity_id,
        datasource_id=datasource_id,
        actor_user_id=actor_user_id,
        reason=reason or event.inference_reason or "Sugestão bloqueada manualmente.",
    )
    if event.entity_type == "column":
        try:
            with session.begin_nested():
                from t2c_data.features.governance.column_classification import record_column_classification_decision

                record_column_classification_decision(
                    session,
                    column_id=event.entity_id,
                    taxonomy_key=event.tag.slug if event.tag is not None else (event.rule_key or "dado_operacional"),
                    source_kind=event.inference_source or "tag_intelligence",
                    confidence_score=int(event.confidence_score or 0),
                    decision_status="rejected",
                    evidence_json=event.evidence or {},
                    notes=reason or event.inference_reason,
                    reviewed_by_user_id=actor_user_id,
                    reviewed_at=datetime.now(timezone.utc),
                    persist_current=False,
                )
        except SQLAlchemyError:
            pass
    event.review_status = "blocked"
    event.reviewed_by_user_id = actor_user_id
    event.reviewed_at = datetime.now(timezone.utc)
    session.add(event)
    return {"success": True, "event_id": event_id, "status": event.review_status}


__all__ = [
    "AUTO_TAG_DEFINITIONS",
    "TagIntelligenceSignal",
    "TagIntelligenceSummary",
    "ensure_core_intelligence_tags",
    "infer_column_signals",
    "infer_table_signals",
    "manual_assign_tag",
    "manual_unassign_tag",
    "load_pending_tag_intelligence_events",
    "apply_tag_intelligence_event",
    "dismiss_tag_intelligence_event",
    "batch_apply_tag_intelligence_events",
    "batch_dismiss_tag_intelligence_events",
    "purge_tag_intelligence_for_entity_ids",
    "reprocess_table_tag_intelligence",
]
