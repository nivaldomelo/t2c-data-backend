from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import re
import unicodedata
from typing import Any, Iterable

from sqlalchemy import Integer, func, select
from sqlalchemy.orm import Session, selectinload

from t2c_data.models.catalog import ColumnEntity, Schema, TableEntity
from t2c_data.models.classification import ColumnClassification, ColumnClassificationVersion


@dataclass(frozen=True)
class ColumnClassificationDefinition:
    taxonomy_key: str
    taxonomy_label: str
    taxonomy_group: str
    confidence: int
    keywords: tuple[str, ...]
    data_types: tuple[str, ...] = ()
    is_personal_data: bool = False
    is_sensitive_data: bool = False
    is_financial_data: bool = False
    is_operational_data: bool = False
    description: str | None = None


COLUMN_CLASSIFICATION_DEFINITIONS: dict[str, ColumnClassificationDefinition] = {
    "cpf": ColumnClassificationDefinition(
        taxonomy_key="cpf",
        taxonomy_label="CPF",
        taxonomy_group="personal",
        confidence=98,
        keywords=("cpf",),
        data_types=("char", "varchar", "text", "bpchar"),
        is_personal_data=True,
        is_sensitive_data=True,
    ),
    "cnpj": ColumnClassificationDefinition(
        taxonomy_key="cnpj",
        taxonomy_label="CNPJ",
        taxonomy_group="personal",
        confidence=98,
        keywords=("cnpj",),
        data_types=("char", "varchar", "text", "bpchar"),
        is_personal_data=True,
        is_sensitive_data=True,
    ),
    "email": ColumnClassificationDefinition(
        taxonomy_key="email",
        taxonomy_label="E-mail",
        taxonomy_group="personal",
        confidence=96,
        keywords=("email", "e-mail", "mail", "mail_address", "email_address"),
        data_types=("char", "varchar", "text", "citext"),
        is_personal_data=True,
        is_sensitive_data=True,
    ),
    "telefone": ColumnClassificationDefinition(
        taxonomy_key="telefone",
        taxonomy_label="Telefone",
        taxonomy_group="personal",
        confidence=95,
        keywords=("telefone", "phone", "celular", "mobile", "whatsapp"),
        data_types=("char", "varchar", "text"),
        is_personal_data=True,
        is_sensitive_data=True,
    ),
    "nome": ColumnClassificationDefinition(
        taxonomy_key="nome",
        taxonomy_label="Nome",
        taxonomy_group="personal",
        confidence=90,
        keywords=("nome", "name", "full_name", "customer_name", "user_name", "titular", "holder"),
        data_types=("char", "varchar", "text"),
        is_personal_data=True,
        is_sensitive_data=True,
    ),
    "endereco": ColumnClassificationDefinition(
        taxonomy_key="endereco",
        taxonomy_label="Endereço",
        taxonomy_group="personal",
        confidence=90,
        keywords=("endereco", "endereço", "address", "logradouro", "rua", "bairro", "cidade", "estado", "cep", "zipcode"),
        data_types=("char", "varchar", "text"),
        is_personal_data=True,
        is_sensitive_data=True,
    ),
    "data_nascimento": ColumnClassificationDefinition(
        taxonomy_key="data_nascimento",
        taxonomy_label="Data de nascimento",
        taxonomy_group="personal",
        confidence=88,
        keywords=("data_nascimento", "nascimento", "birth", "dob", "birth_date"),
        data_types=("date", "datetime", "timestamp", "timestamptz"),
        is_personal_data=True,
        is_sensitive_data=True,
    ),
    "renda": ColumnClassificationDefinition(
        taxonomy_key="renda",
        taxonomy_label="Renda",
        taxonomy_group="financial",
        confidence=90,
        keywords=("renda", "salary", "salario", "salário", "income", "earnings", "proventos"),
        data_types=("numeric", "decimal", "number", "int", "bigint", "float"),
        is_personal_data=True,
        is_sensitive_data=True,
        is_financial_data=True,
    ),
    "dados_bancarios": ColumnClassificationDefinition(
        taxonomy_key="dados_bancarios",
        taxonomy_label="Dados bancários",
        taxonomy_group="financial",
        confidence=95,
        keywords=("bank", "banco", "conta", "account", "agencia", "agência", "agency", "iban", "pix", "bank_account", "account_number"),
        data_types=("char", "varchar", "text", "numeric", "decimal", "bigint"),
        is_personal_data=True,
        is_sensitive_data=True,
        is_financial_data=True,
    ),
    "boleto": ColumnClassificationDefinition(
        taxonomy_key="boleto",
        taxonomy_label="Boleto",
        taxonomy_group="financial",
        confidence=92,
        keywords=("boleto", "linha_digitavel", "linha digitavel", "billing", "invoice", "cobranca", "charge"),
        data_types=("char", "varchar", "text"),
        is_personal_data=True,
        is_sensitive_data=True,
        is_financial_data=True,
    ),
    "parcela": ColumnClassificationDefinition(
        taxonomy_key="parcela",
        taxonomy_label="Parcela",
        taxonomy_group="financial",
        confidence=90,
        keywords=("parcela", "installment", "instalment", "quota_parcela", "installment_number"),
        data_types=("numeric", "decimal", "int", "bigint", "smallint", "number"),
        is_personal_data=True,
        is_sensitive_data=True,
        is_financial_data=True,
    ),
    "saldo_devedor": ColumnClassificationDefinition(
        taxonomy_key="saldo_devedor",
        taxonomy_label="Saldo devedor",
        taxonomy_group="financial",
        confidence=92,
        keywords=("saldo_devedor", "saldo devedor", "debt_balance", "outstanding_balance", "remaining_balance", "balance_due"),
        data_types=("numeric", "decimal", "number", "int", "bigint", "float"),
        is_personal_data=True,
        is_sensitive_data=True,
        is_financial_data=True,
    ),
    "valor_credito": ColumnClassificationDefinition(
        taxonomy_key="valor_credito",
        taxonomy_label="Valor de crédito",
        taxonomy_group="financial",
        confidence=92,
        keywords=("valor_credito", "valor de credito", "valor de crédito", "credit_value", "loan_amount", "financed_value", "credit_amount"),
        data_types=("numeric", "decimal", "number", "int", "bigint", "float"),
        is_personal_data=True,
        is_sensitive_data=True,
        is_financial_data=True,
    ),
    "contrato": ColumnClassificationDefinition(
        taxonomy_key="contrato",
        taxonomy_label="Contrato",
        taxonomy_group="financial",
        confidence=88,
        keywords=("contrato", "contract", "agreement", "contract_number", "contract_id"),
        data_types=("char", "varchar", "text", "numeric", "bigint"),
        is_personal_data=True,
        is_sensitive_data=True,
        is_financial_data=True,
    ),
    "proposta": ColumnClassificationDefinition(
        taxonomy_key="proposta",
        taxonomy_label="Proposta",
        taxonomy_group="financial",
        confidence=88,
        keywords=("proposta", "proposal", "application", "proposal_id", "proposal_number"),
        data_types=("char", "varchar", "text", "numeric", "bigint"),
        is_personal_data=True,
        is_sensitive_data=True,
        is_financial_data=True,
    ),
    "cota": ColumnClassificationDefinition(
        taxonomy_key="cota",
        taxonomy_label="Cota",
        taxonomy_group="financial",
        confidence=90,
        keywords=("cota", "quota", "share", "share_number", "quota_number", "quota_id"),
        data_types=("char", "varchar", "text", "numeric", "bigint", "int"),
        is_personal_data=True,
        is_sensitive_data=True,
        is_financial_data=True,
    ),
    "identificador_financeiro": ColumnClassificationDefinition(
        taxonomy_key="identificador_financeiro",
        taxonomy_label="Identificador financeiro",
        taxonomy_group="financial",
        confidence=85,
        keywords=(
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
        ),
        data_types=("char", "varchar", "text", "numeric", "bigint"),
        is_personal_data=True,
        is_sensitive_data=True,
        is_financial_data=True,
    ),
    "dado_sensivel": ColumnClassificationDefinition(
        taxonomy_key="dado_sensivel",
        taxonomy_label="Dado sensível",
        taxonomy_group="sensitive",
        confidence=80,
        keywords=("dado_sensivel", "dado sensível", "sensivel", "sensível", "confidential", "restricted", "private"),
        data_types=("char", "varchar", "text"),
        is_personal_data=True,
        is_sensitive_data=True,
    ),
    "dado_pessoal": ColumnClassificationDefinition(
        taxonomy_key="dado_pessoal",
        taxonomy_label="Dado pessoal",
        taxonomy_group="personal",
        confidence=82,
        keywords=("dado_pessoal", "dado pessoal", "personal_data", "personal", "person", "cliente", "customer", "client", "titular"),
        data_types=("char", "varchar", "text"),
        is_personal_data=True,
        is_sensitive_data=True,
    ),
    "dado_operacional": ColumnClassificationDefinition(
        taxonomy_key="dado_operacional",
        taxonomy_label="Dado operacional",
        taxonomy_group="operational",
        confidence=76,
        keywords=("status", "state", "stage", "event", "source", "origin", "payload", "metadata", "flag", "indicator", "created_at", "updated_at", "processed_at", "executed_at", "reference", "sequence"),
        data_types=("char", "varchar", "text", "date", "datetime", "timestamp", "timestamptz", "numeric", "decimal", "int", "bigint", "boolean"),
        is_operational_data=True,
    ),
}

_TAXONOMY_PRIORITY = [
    "cpf",
    "cnpj",
    "email",
    "telefone",
    "nome",
    "endereco",
    "data_nascimento",
    "renda",
    "dados_bancarios",
    "boleto",
    "parcela",
    "saldo_devedor",
    "valor_credito",
    "contrato",
    "proposta",
    "cota",
    "identificador_financeiro",
    "dado_sensivel",
    "dado_pessoal",
    "dado_operacional",
]


def _normalize_text(value: str | None) -> str:
    raw = (value or "").strip().lower()
    if not raw:
        return ""
    normalized = unicodedata.normalize("NFKD", raw).encode("ascii", "ignore").decode("ascii")
    normalized = re.sub(r"[^a-z0-9]+", "_", normalized).strip("_")
    return normalized


def _normalize_human_text(value: str | None) -> str:
    raw = (value or "").strip().lower()
    if not raw:
        return ""
    normalized = unicodedata.normalize("NFKD", raw).encode("ascii", "ignore").decode("ascii")
    normalized = normalized.replace("_", " ").replace("-", " ")
    normalized = re.sub(r"[^a-z0-9\s]+", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def _match_score(haystack: str, keywords: Iterable[str]) -> tuple[int, str | None]:
    best = 0
    matched_keyword: str | None = None
    for keyword in keywords:
        normalized = _normalize_human_text(keyword)
        if not normalized:
            continue
        if normalized in haystack:
            score = max(20, min(100, 70 + len(normalized)))
            if score > best:
                best = score
                matched_keyword = keyword
    return best, matched_keyword


def _data_type_matches(definition: ColumnClassificationDefinition, column: ColumnEntity) -> bool:
    normalized_type = _normalize_text(column.data_type)
    normalized_udt = _normalize_text(column.udt_name)
    candidates = [normalized_type, normalized_udt]
    return any(
        any(type_hint and type_hint in candidate for type_hint in definition.data_types)
        for candidate in candidates
    )


def build_column_classification_candidate(
    column: ColumnEntity,
    *,
    table: TableEntity | None = None,
) -> dict[str, object]:
    parts = [
        column.name,
        column.description_source,
        column.description_manual,
        column.dictionary_description,
        column.dictionary_comment,
        column.existing_comment,
        column.slug,
        column.data_type,
        column.udt_name,
    ]
    if table is not None:
        parts.extend(
            [
                table.name,
                table.description_source,
                table.description_manual,
                table.schema.name if table.schema else None,
                table.schema.database.name if table and table.schema and table.schema.database else None,
            ]
        )
    haystack = " ".join(_normalize_human_text(part) for part in parts if part)
    candidates: list[dict[str, object]] = []
    for key in _TAXONOMY_PRIORITY:
        definition = COLUMN_CLASSIFICATION_DEFINITIONS[key]
        score, matched_keyword = _match_score(haystack, definition.keywords)
        if score <= 0 and not _data_type_matches(definition, column):
            continue
        if score <= 0 and _data_type_matches(definition, column):
            score = definition.confidence - 15
        elif _data_type_matches(definition, column):
            score = min(100, score + 5)
        if score <= 0:
            continue
        evidence = {
            "matched_keyword": matched_keyword,
            "data_type": column.data_type,
            "udt_name": column.udt_name,
            "column_name": column.name,
            "table_name": table.name if table else None,
        }
        if table is not None:
            evidence["schema_name"] = table.schema.name if table.schema else None
            evidence["datasource_name"] = table.schema.database.datasource.name if table.schema and table.schema.database else None
        candidates.append(
            {
                "taxonomy_key": definition.taxonomy_key,
                "taxonomy_label": definition.taxonomy_label,
                "taxonomy_group": definition.taxonomy_group,
                "confidence_score": int(max(0, min(100, score))),
                "source_kind": "heuristic",
                "is_personal_data": definition.is_personal_data,
                "is_sensitive_data": definition.is_sensitive_data,
                "is_financial_data": definition.is_financial_data,
                "is_operational_data": definition.is_operational_data,
                "evidence_json": evidence,
                "reason": (
                    f"Nome/tipo de coluna sugere {definition.taxonomy_label.lower()}."
                    if matched_keyword
                    else f"Tipo da coluna sugere {definition.taxonomy_label.lower()}."
                ),
            }
        )

    candidates.sort(
        key=lambda item: (
            int(item["confidence_score"]),
            _TAXONOMY_PRIORITY.index(str(item["taxonomy_key"])) if str(item["taxonomy_key"]) in _TAXONOMY_PRIORITY else 999,
        ),
        reverse=True,
    )
    best = candidates[0] if candidates else None
    return {
        "column_id": column.id,
        "column_name": column.name,
        "table_id": column.table_id,
        "suggestion": best,
        "candidates": candidates[:5],
        "classified": bool(best),
    }


def _classification_flags(taxonomy_key: str) -> dict[str, bool]:
    definition = COLUMN_CLASSIFICATION_DEFINITIONS.get(taxonomy_key)
    if definition is None:
        return {
            "is_personal_data": False,
            "is_sensitive_data": False,
            "is_financial_data": False,
            "is_operational_data": False,
            "taxonomy_group": "operational",
        }
    return {
        "is_personal_data": definition.is_personal_data,
        "is_sensitive_data": definition.is_sensitive_data,
        "is_financial_data": definition.is_financial_data,
        "is_operational_data": definition.is_operational_data,
        "taxonomy_group": definition.taxonomy_group,
    }


def taxonomy_definition(taxonomy_key: str) -> ColumnClassificationDefinition | None:
    return COLUMN_CLASSIFICATION_DEFINITIONS.get(_normalize_text(taxonomy_key))


def load_column_classifications(
    session: Session,
    *,
    column_ids: Iterable[int] | None = None,
    table_id: int | None = None,
) -> dict[int, ColumnClassification]:
    stmt = select(ColumnClassification).options(selectinload(ColumnClassification.column))
    if column_ids:
        stmt = stmt.where(ColumnClassification.column_id.in_(list(dict.fromkeys(int(value) for value in column_ids))))
    elif table_id is not None:
        stmt = stmt.join(ColumnEntity, ColumnEntity.id == ColumnClassification.column_id).where(ColumnEntity.table_id == table_id)
    else:
        return {}
    rows = session.scalars(stmt).all()
    return {int(row.column_id): row for row in rows}


def build_column_classification_map(
    session: Session,
    *,
    table_id: int | None = None,
    column_ids: Iterable[int] | None = None,
    key_by: str = "id",
) -> dict[str, dict[str, object]]:
    rows = load_column_classifications(session, column_ids=column_ids, table_id=table_id)
    if not rows:
        return {}
    mapping: dict[str, dict[str, object]] = {}
    for row in rows.values():
        key = str(row.column.name if key_by == "name" and row.column is not None else row.column_id)
        mapping[key] = {
            "taxonomy_key": row.taxonomy_key,
            "taxonomy_label": row.taxonomy_label,
            "taxonomy_group": row.taxonomy_group,
            "review_status": row.review_status,
            "source_kind": row.source_kind,
            "confidence_score": int(row.confidence_score or 0),
            "is_personal_data": bool(row.is_personal_data),
            "is_sensitive_data": bool(row.is_sensitive_data),
            "is_financial_data": bool(row.is_financial_data),
            "is_operational_data": bool(row.is_operational_data),
            "evidence_json": row.evidence_json or {},
            "reviewed_at": row.reviewed_at.isoformat() if row.reviewed_at else None,
        }
    return mapping


def table_column_classification_summary(session: Session, table_id: int) -> dict[str, object]:
    if table_id <= 0:
        return {
            "total_columns": 0,
            "classified_columns": 0,
            "personal_columns": 0,
            "sensitive_columns": 0,
            "financial_columns": 0,
            "operational_columns": 0,
            "coverage_pct": 0.0,
            "last_reviewed_at": None,
        }
    total_columns = int(session.scalar(select(func.count(ColumnEntity.id)).where(ColumnEntity.table_id == table_id)) or 0)
    stmt = (
        select(
            func.count(ColumnClassification.id).label("classified_columns"),
            func.sum(func.cast(ColumnClassification.is_personal_data, Integer)).label("personal_columns"),
            func.sum(func.cast(ColumnClassification.is_sensitive_data, Integer)).label("sensitive_columns"),
            func.sum(func.cast(ColumnClassification.is_financial_data, Integer)).label("financial_columns"),
            func.sum(func.cast(ColumnClassification.is_operational_data, Integer)).label("operational_columns"),
            func.max(func.coalesce(ColumnClassification.reviewed_at, ColumnClassification.updated_at)).label("last_reviewed_at"),
        )
        .join(ColumnEntity, ColumnEntity.id == ColumnClassification.column_id)
        .where(ColumnEntity.table_id == table_id)
    )
    row = session.execute(stmt).mappings().first() or {}
    classified_columns = int(row.get("classified_columns") or 0)
    coverage_pct = round((classified_columns / total_columns) * 100.0, 1) if total_columns else 0.0
    return {
        "total_columns": total_columns,
        "classified_columns": classified_columns,
        "personal_columns": int(row.get("personal_columns") or 0),
        "sensitive_columns": int(row.get("sensitive_columns") or 0),
        "financial_columns": int(row.get("financial_columns") or 0),
        "operational_columns": int(row.get("operational_columns") or 0),
        "coverage_pct": coverage_pct,
        "last_reviewed_at": row.get("last_reviewed_at"),
    }


def _next_version_number(session: Session, column_id: int) -> int:
    current = session.scalar(
        select(func.max(ColumnClassificationVersion.version_number)).where(ColumnClassificationVersion.column_id == column_id)
    )
    return int(current or 0) + 1


def record_column_classification_decision(
    session: Session,
    *,
    column_id: int,
    taxonomy_key: str,
    source_kind: str,
    confidence_score: int,
    decision_status: str,
    evidence_json: dict[str, object] | None = None,
    notes: str | None = None,
    reviewed_by_user_id: int | None = None,
    reviewed_at: datetime | None = None,
    persist_current: bool = True,
) -> dict[str, object]:
    column = session.get(ColumnEntity, column_id)
    if column is None:
        raise ValueError("Column not found")
    definition = taxonomy_definition(taxonomy_key)
    normalized_key = _normalize_text(taxonomy_key)
    if definition is None:
        definition = ColumnClassificationDefinition(
            taxonomy_key=normalized_key,
            taxonomy_label=taxonomy_key.replace("_", " ").title(),
            taxonomy_group="operational",
            confidence=max(0, min(100, int(confidence_score or 0))),
            keywords=(taxonomy_key,),
        )
    now = reviewed_at or datetime.now(timezone.utc)
    flags = _classification_flags(definition.taxonomy_key)
    current = session.scalar(select(ColumnClassification).where(ColumnClassification.column_id == column_id))

    classification_id: int | None = current.id if current is not None else None
    if persist_current and decision_status != "rejected":
        if current is None:
            current = ColumnClassification(
                column_id=column_id,
                taxonomy_key=definition.taxonomy_key,
                taxonomy_label=definition.taxonomy_label,
                taxonomy_group=flags["taxonomy_group"],
                review_status="approved",
                source_kind=source_kind,
                confidence_score=max(0, min(100, int(confidence_score or 0))),
                is_personal_data=flags["is_personal_data"],
                is_sensitive_data=flags["is_sensitive_data"],
                is_financial_data=flags["is_financial_data"],
                is_operational_data=flags["is_operational_data"],
                evidence_json=evidence_json or {},
                notes=notes,
                reviewed_by_user_id=reviewed_by_user_id,
                reviewed_at=now,
            )
            session.add(current)
            session.flush()
            classification_id = current.id
        else:
            current.taxonomy_key = definition.taxonomy_key
            current.taxonomy_label = definition.taxonomy_label
            current.taxonomy_group = flags["taxonomy_group"]
            current.review_status = "approved"
            current.source_kind = source_kind
            current.confidence_score = max(0, min(100, int(confidence_score or 0)))
            current.is_personal_data = flags["is_personal_data"]
            current.is_sensitive_data = flags["is_sensitive_data"]
            current.is_financial_data = flags["is_financial_data"]
            current.is_operational_data = flags["is_operational_data"]
            current.evidence_json = evidence_json or {}
            current.notes = notes
            current.reviewed_by_user_id = reviewed_by_user_id
            current.reviewed_at = now
            session.add(current)
            session.flush()
            classification_id = current.id

    version = ColumnClassificationVersion(
        column_id=column_id,
        column_classification_id=classification_id,
        version_number=_next_version_number(session, column_id),
        decision_status=decision_status,
        taxonomy_key=definition.taxonomy_key,
        taxonomy_label=definition.taxonomy_label,
        taxonomy_group=flags["taxonomy_group"],
        source_kind=source_kind,
        confidence_score=max(0, min(100, int(confidence_score or 0))),
        is_personal_data=flags["is_personal_data"],
        is_sensitive_data=flags["is_sensitive_data"],
        is_financial_data=flags["is_financial_data"],
        is_operational_data=flags["is_operational_data"],
        evidence_json=evidence_json or {},
        notes=notes,
        decided_by_user_id=reviewed_by_user_id,
        decided_at=now,
    )
    session.add(version)
    session.flush()
    return {
        "classification_id": classification_id,
        "version_id": version.id,
        "version_number": version.version_number,
        "taxonomy_key": definition.taxonomy_key,
        "taxonomy_label": definition.taxonomy_label,
        "taxonomy_group": flags["taxonomy_group"],
        "decision_status": decision_status,
    }


def load_column_classification_history(session: Session, column_id: int) -> list[ColumnClassificationVersion]:
    return session.scalars(
        select(ColumnClassificationVersion)
        .where(ColumnClassificationVersion.column_id == column_id)
        .order_by(ColumnClassificationVersion.version_number.desc(), ColumnClassificationVersion.id.desc())
    ).all()


def column_classification_payload(classification: ColumnClassification | None) -> dict[str, object] | None:
    if classification is None:
        return None
    return {
        "id": classification.id,
        "column_id": classification.column_id,
        "taxonomy_key": classification.taxonomy_key,
        "taxonomy_label": classification.taxonomy_label,
        "taxonomy_group": classification.taxonomy_group,
        "review_status": classification.review_status,
        "source_kind": classification.source_kind,
        "confidence_score": int(classification.confidence_score or 0),
        "is_personal_data": bool(classification.is_personal_data),
        "is_sensitive_data": bool(classification.is_sensitive_data),
        "is_financial_data": bool(classification.is_financial_data),
        "is_operational_data": bool(classification.is_operational_data),
        "evidence_json": classification.evidence_json or {},
        "notes": classification.notes,
        "reviewed_by_user_id": classification.reviewed_by_user_id,
        "reviewed_at": classification.reviewed_at.isoformat() if classification.reviewed_at else None,
        "created_at": classification.created_at.isoformat() if classification.created_at else None,
        "updated_at": classification.updated_at.isoformat() if classification.updated_at else None,
    }


def column_classification_version_payload(version: ColumnClassificationVersion) -> dict[str, object]:
    return {
        "id": version.id,
        "column_id": version.column_id,
        "column_classification_id": version.column_classification_id,
        "version_number": version.version_number,
        "decision_status": version.decision_status,
        "taxonomy_key": version.taxonomy_key,
        "taxonomy_label": version.taxonomy_label,
        "taxonomy_group": version.taxonomy_group,
        "source_kind": version.source_kind,
        "confidence_score": int(version.confidence_score or 0),
        "is_personal_data": bool(version.is_personal_data),
        "is_sensitive_data": bool(version.is_sensitive_data),
        "is_financial_data": bool(version.is_financial_data),
        "is_operational_data": bool(version.is_operational_data),
        "evidence_json": version.evidence_json or {},
        "notes": version.notes,
        "decided_by_user_id": version.decided_by_user_id,
        "decided_at": version.decided_at.isoformat() if version.decided_at else None,
        "created_at": version.created_at.isoformat() if version.created_at else None,
        "updated_at": version.updated_at.isoformat() if version.updated_at else None,
    }
