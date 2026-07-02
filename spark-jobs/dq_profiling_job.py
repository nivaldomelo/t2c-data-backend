from __future__ import annotations

import hashlib
import json
import math
import os
import re
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from dq_common import base_parser, build_spark, load_connection_config, read_table_via_jdbc, write_json_output

_SENSITIVE_NAME_TOKENS = {
    "cpf": "cpf",
    "cnpj": "cnpj",
    "email": "email",
    "e-mail": "email",
    "mail": "email",
    "telefone": "phone",
    "celular": "phone",
    "phone": "phone",
    "nome": "name",
    "address": "address",
    "endereco": "address",
    "endereço": "address",
    "nascimento": "birth_date",
    "birthday": "birth_date",
    "birth": "birth_date",
}

_FINANCIAL_NAME_TOKENS = {
    "valor": "financial_amount",
    "credito": "financial_amount",
    "crédito": "financial_amount",
    "parcela": "financial_amount",
    "saldo": "financial_amount",
    "devedor": "financial_amount",
    "taxa": "financial_amount",
    "boleto": "boleto",
    "contrato": "contract_id",
    "proposta": "proposal_id",
    "cota": "cota_id",
    "grupo": "group_id",
    "administradora": "administradora",
    "administrador": "administradora",
    "status": "status",
    "contempl": "contemplation_date",
    "venc": "due_date",
    "pag": "payment",
}

_PII_REGEXES = {
    "cpf": re.compile(r"^\d{3}\.?\d{3}\.?\d{3}-?\d{2}$"),
    "cnpj": re.compile(r"^\d{2}\.?\d{3}\.?\d{3}/?\d{4}-?\d{2}$"),
    "email": re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$"),
    "phone": re.compile(r"^\+?\d{10,15}$"),
}


def _col(name: str) -> Any:
    escaped = name.replace("`", "``")
    return F.col(f"`{escaped}`")


def _safe_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _is_numeric_type(simple: str) -> bool:
    simple = simple.lower()
    return any(token in simple for token in ("int", "double", "float", "decimal", "long", "short", "bigint", "numeric", "real"))


def _is_date_type(simple: str) -> bool:
    simple = simple.lower()
    return any(token in simple for token in ("date", "time", "timestamp"))


def _is_boolean_type(simple: str) -> bool:
    return "boolean" in simple.lower()


def _schema_signature(df: DataFrame) -> list[dict[str, Any]]:
    return [
        {
            "name": field.name,
            "type": field.dataType.simpleString(),
            "nullable": bool(field.nullable),
        }
        for field in df.schema.fields
    ]


def _schema_hash(df: DataFrame) -> str:
    raw = json.dumps(_schema_signature(df), sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _mask_value(value: Any, pattern_type: str | None) -> str | None:
    text = _safe_str(value)
    if text is None:
        return None
    if pattern_type == "cpf":
        digits = re.sub(r"\D+", "", text)
        if len(digits) >= 11:
            return f"***.***.{digits[-3:-1]}-**"
    if pattern_type == "cnpj":
        digits = re.sub(r"\D+", "", text)
        if len(digits) >= 14:
            return f"**.***.***/****-{digits[-2:]}"
    if pattern_type == "email" and "@" in text:
        local, domain = text.split("@", 1)
        return f"{local[:1]}***@{domain}"
    if pattern_type == "phone":
        digits = re.sub(r"\D+", "", text)
        if len(digits) >= 4:
            return f"***{digits[-4:]}"
    if pattern_type in {"financial_amount", "proposal_id", "cota_id", "contract_id", "group_id", "boleto"}:
        return f"hash:{hashlib.sha256(text.encode('utf-8')).hexdigest()[:10]}"
    if len(text) <= 12:
        return text[:2] + "***"
    return f"hash:{hashlib.sha256(text.encode('utf-8')).hexdigest()[:10]}"


def _name_pattern(column_name: str) -> tuple[str | None, str | None]:
    normalized = column_name.lower()
    for token, pattern in _SENSITIVE_NAME_TOKENS.items():
        if token in normalized:
            return pattern, {
                "cpf": "sensitive_pii",
                "cnpj": "sensitive_pii",
                "email": "sensitive_pii",
                "phone": "sensitive_pii",
                "name": "sensitive_pii",
                "address": "sensitive_pii",
                "birth_date": "sensitive_pii",
            }.get(pattern, "business_attribute")
    for token, pattern in _FINANCIAL_NAME_TOKENS.items():
        if token in normalized:
            return pattern, {
                "financial_amount": "financial",
                "status": "operational",
                "contemplation_date": "financial",
                "due_date": "financial",
                "payment": "financial",
            }.get(pattern, "business_attribute")
    return None, None


def _sample_values(df: DataFrame, column_name: str, limit: int = 60) -> list[str]:
    rows = (
        df.select(_col(column_name).cast("string").alias("value"))
        .where(F.col("value").isNotNull())
        .distinct()
        .limit(limit)
        .collect()
    )
    return [str(row["value"]) for row in rows if row["value"] is not None]


def _regex_hit_rate(values: list[str], pattern: re.Pattern[str]) -> float:
    if not values:
        return 0.0
    hits = sum(1 for value in values if pattern.match(value.strip()) is not None)
    return hits / float(len(values))


def _detect_pattern(column_name: str, data_type: str, values: list[str]) -> tuple[str | None, float, str | None]:
    name_pattern, sensitive_guess = _name_pattern(column_name)
    if name_pattern in _PII_REGEXES:
        return name_pattern, 0.95 if values else 0.8, sensitive_guess
    best_pattern = name_pattern
    best_confidence = 0.0
    for pattern_name, regex in _PII_REGEXES.items():
        hit_rate = _regex_hit_rate(values, regex)
        if hit_rate > best_confidence:
            best_confidence = hit_rate
            best_pattern = pattern_name
    if best_pattern is None and _is_date_type(data_type):
        best_pattern = "date"
        best_confidence = 0.8
    if best_pattern is None and _is_numeric_type(data_type):
        best_pattern = "numeric"
        best_confidence = 0.7
    if best_pattern is None and values:
        best_pattern = name_pattern or "categorical"
        best_confidence = 0.55 if name_pattern else 0.4
    return best_pattern, round(float(best_confidence), 4), sensitive_guess


def _expected_type(column_name: str, data_type: str) -> str | None:
    pattern, _ = _name_pattern(column_name)
    if pattern in {"cpf", "cnpj", "email", "phone", "name", "address", "status", "proposal_id", "cota_id", "group_id", "administradora", "boleto", "contract_id"}:
        return "string"
    if pattern in {"financial_amount"}:
        return "numeric"
    if pattern in {"birth_date", "contemplation_date", "due_date"}:
        return "date"
    if _is_numeric_type(data_type):
        return "numeric"
    if _is_date_type(data_type):
        return "date"
    if _is_boolean_type(data_type):
        return "boolean"
    return None


def _inferred_type(data_type: str, pattern_type: str | None) -> str:
    simple = data_type.lower()
    if _is_boolean_type(simple):
        return "boolean"
    if _is_date_type(simple) or pattern_type in {"birth_date", "contemplation_date", "due_date", "date"}:
        return "date"
    if _is_numeric_type(simple) or pattern_type == "financial_amount":
        return "numeric"
    if pattern_type in {"cpf", "cnpj", "email", "phone", "name", "address", "status", "proposal_id", "cota_id", "group_id", "administradora", "boleto", "contract_id"}:
        return "identifier"
    return "categorical" if pattern_type in {"categorical", "status"} else "text"


def _cardinality_level(distinct_count: int, row_count: int) -> str:
    if row_count <= 0:
        return "unknown"
    ratio = distinct_count / float(row_count)
    if ratio >= 0.95:
        return "very_high"
    if ratio >= 0.70:
        return "high"
    if ratio >= 0.25:
        return "medium"
    return "low"


def _numeric_summary(df: DataFrame, column_name: str) -> dict[str, float | None]:
    try:
        quantiles = df.approxQuantile(column_name, [0.25, 0.5, 0.75], 0.01)
        min_max = df.select(
            F.min(_col(column_name)).alias("min_v"),
            F.max(_col(column_name)).alias("max_v"),
            F.avg(_col(column_name)).alias("mean_v"),
            F.stddev(_col(column_name)).alias("std_v"),
        ).collect()[0]
        return {
            "min": float(min_max["min_v"]) if min_max["min_v"] is not None else None,
            "max": float(min_max["max_v"]) if min_max["max_v"] is not None else None,
            "mean": float(min_max["mean_v"]) if min_max["mean_v"] is not None else None,
            "median": float(quantiles[1]) if len(quantiles) > 1 else None,
            "stddev": float(min_max["std_v"]) if min_max["std_v"] is not None else None,
            "q1": float(quantiles[0]) if len(quantiles) > 0 else None,
            "q3": float(quantiles[2]) if len(quantiles) > 2 else None,
        }
    except Exception:
        return {"min": None, "max": None, "mean": None, "median": None, "stddev": None, "q1": None, "q3": None}


def _outlier_count(df: DataFrame, column_name: str) -> int:
    summary = _numeric_summary(df, column_name)
    if summary["q1"] is None or summary["q3"] is None:
        return 0
    iqr = float(summary["q3"]) - float(summary["q1"])
    if iqr <= 0:
        return 0
    lower = float(summary["q1"]) - 1.5 * iqr
    upper = float(summary["q3"]) + 1.5 * iqr
    try:
        return int(
            df.where((_col(column_name) < F.lit(lower)) | (_col(column_name) > F.lit(upper))).count()
        )
    except Exception:
        return 0


def _top_values(df: DataFrame, column_name: str, pattern_type: str | None, limit: int = 5) -> list[dict[str, Any]]:
    try:
        rows = (
            df.select(_col(column_name).cast("string").alias("value"))
            .groupBy("value")
            .agg(F.count(F.lit(1)).alias("count"))
            .orderBy(F.desc("count"), F.asc("value"))
            .limit(limit)
            .collect()
        )
    except Exception:
        return []
    total = sum(int(row["count"] or 0) for row in rows) or 1
    return [
        {
            "value": _mask_value(row["value"], pattern_type),
            "count": int(row["count"] or 0),
            "ratio": round((int(row["count"] or 0) / float(total)) * 100.0, 2),
        }
        for row in rows
    ]


def _business_key_candidates(columns: list[str]) -> list[list[str]]:
    normalized = [column for column in columns if column]
    candidates = [[column] for column in normalized if column.lower().endswith("_id") or column.lower() in {"cpf", "cnpj", "email", "documento"}]
    grouped_tokens = [
        ["cliente_id", "proposta_id"],
        ["cliente_id", "cota_id"],
        ["grupo_id", "cota_id"],
        ["proposta_id", "cota_id"],
        ["boleto_id"],
        ["contract_id"],
    ]
    for combo in grouped_tokens:
        if all(item in normalized for item in combo):
            candidates.append(combo)
    return candidates


def _masked_examples(values: list[str], pattern_type: str | None, limit: int = 3) -> list[str]:
    return [item for item in (_mask_value(value, pattern_type) for value in values[:limit]) if item is not None]


def _profile_column(df: DataFrame, field, row_count: int) -> dict[str, Any]:
    column_name = field.name
    data_type = field.dataType.simpleString()
    null_count = int(df.select(F.sum(F.when(_col(column_name).isNull(), 1).otherwise(0)).alias("v")).collect()[0]["v"] or 0)
    distinct_count = int(df.select(F.countDistinct(_col(column_name)).alias("v")).collect()[0]["v"] or 0)
    null_ratio = round((null_count / float(row_count)) if row_count > 0 else 0.0, 6)
    fill_ratio = round(max(0.0, 1.0 - null_ratio), 6)
    distinct_ratio = round((distinct_count / float(row_count)) if row_count > 0 else 0.0, 6)
    values = _sample_values(df, column_name)
    pattern_type, pattern_confidence, sensitive_guess = _detect_pattern(column_name, data_type, values)
    inferred_type = _inferred_type(data_type, pattern_type)
    expected_type = _expected_type(column_name, data_type)
    type_mismatch = bool(expected_type and expected_type != inferred_type)
    cardinality_level = _cardinality_level(distinct_count, row_count)
    numeric_summary = _numeric_summary(df, column_name) if _is_numeric_type(data_type) or inferred_type == "numeric" else {}
    outlier_count = _outlier_count(df, column_name) if inferred_type == "numeric" else 0
    duplicate_count = max(0, row_count - distinct_count)
    top_values = _top_values(df, column_name, pattern_type)
    examples = _masked_examples(values, pattern_type)
    return {
        "column_name": column_name,
        "data_type": data_type,
        "null_count": null_count,
        "distinct_count": distinct_count,
        "null_pct": round(float(null_ratio) * 100.0, 4),
        "min_value": None if numeric_summary.get("min") is None else str(numeric_summary["min"]),
        "max_value": None if numeric_summary.get("max") is None else str(numeric_summary["max"]),
        "inferred_type": inferred_type,
        "expected_type": expected_type,
        "type_mismatch": type_mismatch,
        "null_ratio": round(float(null_ratio) * 100.0, 4),
        "fill_ratio": round(float(fill_ratio) * 100.0, 4),
        "distinct_ratio": round(float(distinct_ratio) * 100.0, 4),
        "min_value_masked": None if numeric_summary.get("min") is None else _mask_value(numeric_summary["min"], pattern_type),
        "max_value_masked": None if numeric_summary.get("max") is None else _mask_value(numeric_summary["max"], pattern_type),
        "mean_value": numeric_summary.get("mean"),
        "median_value": numeric_summary.get("median"),
        "stddev_value": numeric_summary.get("stddev"),
        "top_values_json_masked": top_values,
        "pattern_type": pattern_type,
        "pattern_confidence": pattern_confidence,
        "outlier_count": outlier_count,
        "duplicate_count": duplicate_count,
        "cardinality_level": cardinality_level,
        "sensitive_guess": sensitive_guess,
        "examples_masked_json": examples,
    }


def _table_timestamp_hint(df: DataFrame) -> tuple[str | None, str | None, int | None]:
    candidates = [
        field.name
        for field in df.schema.fields
        if _is_date_type(field.dataType.simpleString())
        and any(token in field.name.lower() for token in ("updated", "loaded", "ingest", "refresh", "created", "modified"))
    ]
    if not candidates:
        return None, None, None
    best_column = candidates[0]
    try:
        row = df.select(F.max(_col(best_column)).alias("v")).collect()[0]
        value = row["v"]
        if value is None:
            return best_column, None, None
        if isinstance(value, datetime):
            ts = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        else:
            text = str(value)
            ts = datetime.fromisoformat(text.replace("Z", "+00:00"))
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
        freshness_seconds = int(max(0.0, (datetime.now(timezone.utc) - ts).total_seconds()))
        return best_column, ts.isoformat(), freshness_seconds
    except Exception:
        return best_column, None, None


def _business_key_duplicate_count(df: DataFrame, columns: list[str]) -> int:
    candidates = _business_key_candidates(columns)
    if not candidates:
        return 0
    best = candidates[0]
    for candidate in candidates[1:]:
        if len(candidate) > len(best):
            best = candidate
    try:
        distinct = df.dropDuplicates(best).count()
        return max(0, int(df.count()) - int(distinct))
    except Exception:
        return 0


def _rule_definition_for_suggestion(
    *,
    column_name: str | None,
    template_key: str,
    rule_type: str,
    dimension: str,
    conditions: list[dict[str, Any]],
    unique_columns: list[str] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "version": 1,
        "type": rule_type,
        "dimension": dimension,
        "template_key": template_key,
        "logic": "AND",
        "conditions": conditions,
    }
    if column_name is not None:
        payload["target_column"] = column_name
    if unique_columns:
        payload["unique_columns"] = unique_columns
    return payload


def _suggestions_for_column(column: dict[str, Any], row_count: int) -> list[dict[str, Any]]:
    suggestions: list[dict[str, Any]] = []
    column_name = str(column["column_name"])
    pattern_type = column.get("pattern_type")
    distinct_count = int(column.get("distinct_count") or 0)
    fill_ratio = float(column.get("fill_ratio") or 0.0)
    null_ratio = float(column.get("null_ratio") or 0.0)
    inferred_type = str(column.get("inferred_type") or "")
    if pattern_type == "cpf":
        suggestions.append(
            {
                "dimension": "validade",
                "suggested_rule_type": "domain",
                "rule_definition_json": _rule_definition_for_suggestion(
                    column_name=column_name,
                    template_key="validade.cpf",
                    rule_type="domain",
                    dimension="validade",
                    conditions=[{"column": column_name, "operator": "matches_regex", "value": r"^\d{3}\.?\d{3}\.?\d{3}-?\d{2}$"}],
                ),
                "confidence_score": 0.98,
                "reason": "Nome e padrão da coluna sugerem CPF.",
            }
        )
        if fill_ratio >= 80.0:
            suggestions.append(
                {
                    "dimension": "completude",
                    "suggested_rule_type": "nullability",
                    "rule_definition_json": _rule_definition_for_suggestion(
                        column_name=column_name,
                        template_key="completude.required_column",
                        rule_type="nullability",
                        dimension="completude",
                        conditions=[{"column": column_name, "operator": "not_null"}],
                    ),
                    "confidence_score": 0.88,
                    "reason": "CPF em domínio crítico costuma ser obrigatório.",
                }
            )
        if distinct_count >= max(10, int(row_count * 0.8)):
            suggestions.append(
                {
                    "dimension": "unicidade",
                    "suggested_rule_type": "uniqueness",
                    "rule_definition_json": _rule_definition_for_suggestion(
                        column_name=column_name,
                        template_key="unicidade.coluna",
                        rule_type="uniqueness",
                        dimension="unicidade",
                        conditions=[{"column": column_name, "operator": "unique"}],
                    ),
                    "confidence_score": 0.9,
                    "reason": "CPF geralmente identifica uma entidade de forma única.",
                }
            )
    elif pattern_type == "cnpj":
        suggestions.append(
            {
                "dimension": "validade",
                "suggested_rule_type": "domain",
                "rule_definition_json": _rule_definition_for_suggestion(
                    column_name=column_name,
                    template_key="validade.cnpj",
                    rule_type="domain",
                    dimension="validade",
                    conditions=[{"column": column_name, "operator": "matches_regex", "value": r"^\d{2}\.?\d{3}\.?\d{3}/?\d{4}-?\d{2}$"}],
                ),
                "confidence_score": 0.98,
                "reason": "Nome e padrão da coluna sugerem CNPJ.",
            }
        )
        if fill_ratio >= 80.0:
            suggestions.append(
                {
                    "dimension": "completude",
                    "suggested_rule_type": "nullability",
                    "rule_definition_json": _rule_definition_for_suggestion(
                        column_name=column_name,
                        template_key="completude.required_column",
                        rule_type="nullability",
                        dimension="completude",
                        conditions=[{"column": column_name, "operator": "not_null"}],
                    ),
                    "confidence_score": 0.82,
                    "reason": "CNPJ é um identificador obrigatório para pessoa jurídica.",
                }
            )
    elif pattern_type == "email":
        suggestions.append(
            {
                "dimension": "validade",
                "suggested_rule_type": "domain",
                "rule_definition_json": _rule_definition_for_suggestion(
                    column_name=column_name,
                    template_key="validade.email",
                    rule_type="domain",
                    dimension="validade",
                    conditions=[{"column": column_name, "operator": "matches_regex", "value": r"^[^@\s]+@[^@\s]+\.[^@\s]+$"}],
                ),
                "confidence_score": 0.97,
                "reason": "A coluna se parece com um e-mail.",
            }
        )
        if null_ratio <= 5.0:
            suggestions.append(
                {
                    "dimension": "completude",
                    "suggested_rule_type": "max_null_ratio",
                    "rule_definition_json": _rule_definition_for_suggestion(
                        column_name=column_name,
                        template_key="completude.required_column",
                        rule_type="nullability",
                        dimension="completude",
                        conditions=[{"column": column_name, "operator": "not_null"}],
                    ),
                    "confidence_score": 0.8,
                    "reason": "E-mail tende a ser obrigatório em cadastros de clientes e usuários.",
                }
            )
    elif pattern_type == "phone":
        suggestions.append(
            {
                "dimension": "validade",
                "suggested_rule_type": "domain",
                "rule_definition_json": _rule_definition_for_suggestion(
                    column_name=column_name,
                    template_key="validade.telefone",
                    rule_type="domain",
                    dimension="validade",
                    conditions=[{"column": column_name, "operator": "matches_regex", "value": r"^\+?\d{10,15}$"}],
                ),
                "confidence_score": 0.94,
                "reason": "A coluna se parece com telefone.",
            }
        )
    elif pattern_type == "status" and distinct_count > 1 and distinct_count <= 20:
        top_values = [item.get("value") for item in column.get("top_values_json_masked", []) if item.get("value") is not None]
        if top_values:
            suggestions.append(
                {
                    "dimension": "validade",
                    "suggested_rule_type": "domain_values",
                    "rule_definition_json": _rule_definition_for_suggestion(
                        column_name=column_name,
                        template_key="validade.status_permitido",
                        rule_type="domain",
                        dimension="validade",
                        conditions=[{"column": column_name, "operator": "in_list", "values": top_values[:10]}],
                    ),
                    "confidence_score": 0.76,
                    "reason": "Poucos valores distintos sugerem domínio permitido.",
                }
            )
    elif inferred_type == "numeric":
        mean_value = column.get("mean_value")
        stddev_value = column.get("stddev_value")
        if mean_value is not None and stddev_value is not None and float(stddev_value) > 0:
            lower = float(mean_value) - 3.0 * float(stddev_value)
            upper = float(mean_value) + 3.0 * float(stddev_value)
            suggestions.append(
                {
                    "dimension": "validade",
                    "suggested_rule_type": "outlier_threshold",
                    "rule_definition_json": _rule_definition_for_suggestion(
                        column_name=column_name,
                        template_key="validade.intervalo_numerico",
                        rule_type="column_validation",
                        dimension="validade",
                        conditions=[{"column": column_name, "operator": "between", "value": lower, "value_to": upper}],
                    ),
                    "confidence_score": 0.68,
                    "reason": "A distribuição numérica permite sugerir um intervalo esperado.",
                }
            )
    if column_name.lower().endswith("_id") and distinct_count >= max(10, int(row_count * 0.8)):
        suggestions.append(
            {
                "dimension": "unicidade",
                "suggested_rule_type": "uniqueness",
                "rule_definition_json": _rule_definition_for_suggestion(
                    column_name=column_name,
                    template_key="unicidade.coluna",
                    rule_type="uniqueness",
                    dimension="unicidade",
                    conditions=[{"column": column_name, "operator": "unique"}],
                ),
                "confidence_score": 0.72,
                "reason": "Colunas ID tendem a ser chave candidata.",
            }
        )
    if pattern_type in {"contemplation_date", "due_date"}:
        suggestions.append(
            {
                "dimension": "tempestividade",
                "suggested_rule_type": "freshness_sla",
                "rule_definition_json": _rule_definition_for_suggestion(
                    column_name=column_name,
                    template_key="tempestividade.sla",
                    rule_type="freshness",
                    dimension="tempestividade",
                    conditions=[{"column": column_name, "operator": "not_future"}],
                ),
                "confidence_score": 0.62,
                "reason": "Datas críticas costumam precisar de janela de atualização e validade.",
            }
        )
    return suggestions


def _dimension_scores(payload_columns: list[dict[str, Any]], table_metrics: dict[str, Any]) -> dict[str, float | None]:
    row_count = int(table_metrics.get("row_count") or 0)
    if row_count <= 0:
        return {
            "completude": None,
            "validade": None,
            "consistencia": None,
            "unicidade": None,
            "tempestividade": None,
            "acuracia": None,
            "governanca": None,
            "rastreabilidade": None,
            "classificacao_sensivel": None,
        }
    completeness_values = [100.0 - float(col.get("null_pct") or 0.0) for col in payload_columns]
    validity_values = [
        100.0 if not bool(col.get("type_mismatch")) else 60.0
        for col in payload_columns
        if col.get("expected_type") is not None or col.get("pattern_type") is not None
    ]
    uniqueness_values = [
        max(0.0, 100.0 - ((float(col.get("duplicate_count") or 0.0) / float(row_count)) * 100.0))
        for col in payload_columns
        if col.get("distinct_count") is not None
    ]
    numeric_values = [float(col.get("mean_value")) for col in payload_columns if col.get("mean_value") is not None]
    freshness_seconds = table_metrics.get("freshness_seconds")
    freshness_score = None
    if freshness_seconds is not None:
        freshness_score = max(0.0, 100.0 - min(100.0, float(freshness_seconds) / 3600.0 * 5.0))
    if not completeness_values:
        completeness_score = None
    else:
        completeness_score = round(sum(completeness_values) / len(completeness_values), 4)
    validity_score = round(sum(validity_values) / len(validity_values), 4) if validity_values else None
    uniqueness_score = round(sum(uniqueness_values) / len(uniqueness_values), 4) if uniqueness_values else None
    consistency_score = 100.0 if not table_metrics.get("schema_drift_detected") else 65.0
    accuracy_score = 100.0 if table_metrics.get("duplicate_business_key_count", 0) == 0 else max(
        0.0, 100.0 - min(100.0, float(table_metrics.get("duplicate_business_key_count") or 0))
    )
    sensitive_columns = [col for col in payload_columns if col.get("sensitive_guess")]
    classification_score = 100.0 if sensitive_columns and all(col.get("examples_masked_json") for col in sensitive_columns) else 85.0 if sensitive_columns else None
    governance_score = 100.0 if any(col.get("sensitive_guess") for col in payload_columns) else 80.0
    traceability_score = 100.0 if table_metrics.get("schema_hash") else 75.0
    return {
        "completude": completeness_score,
        "validade": validity_score,
        "consistencia": consistency_score,
        "unicidade": uniqueness_score,
        "tempestividade": freshness_score,
        "acuracia": accuracy_score,
        "governanca": governance_score,
        "rastreabilidade": traceability_score,
        "classificacao_sensivel": classification_score,
    }


def _blend_score(weight_map: dict[str, float], dimension_scores: dict[str, float | None]) -> tuple[float | None, float]:
    weighted_total = 0.0
    usable_weight = 0.0
    for key, weight in weight_map.items():
        score = dimension_scores.get(key)
        if score is None:
            continue
        weighted_total += float(weight) * float(score)
        usable_weight += float(weight)
    if usable_weight <= 0:
        return None, 0.0
    return round(weighted_total / usable_weight, 4), round((usable_weight / max(sum(weight_map.values()), 1.0)) * 100.0, 4)


def _weight_profiles() -> dict[str, dict[str, float]]:
    default = {
        "completude": 20.0,
        "validade": 15.0,
        "consistencia": 20.0,
        "unicidade": 15.0,
        "tempestividade": 15.0,
        "acuracia": 10.0,
        "governanca": 5.0,
        "rastreabilidade": 0.0,
        "classificacao_sensivel": 0.0,
    }
    fintech = {
        "completude": 15.0,
        "validade": 15.0,
        "consistencia": 20.0,
        "unicidade": 10.0,
        "tempestividade": 15.0,
        "acuracia": 10.0,
        "governanca": 5.0,
        "rastreabilidade": 5.0,
        "classificacao_sensivel": 5.0,
    }
    return {"default": default, "fintech": fintech}


def _select_weight_profile(table_fqn: str, table_name: str, payload_columns: list[dict[str, Any]]) -> tuple[str, dict[str, float]]:
    profile = _weight_profiles()["default"]
    lower_fqn = f"{table_fqn} {table_name}".lower()
    if any(token in lower_fqn for token in ("finance", "fina", "consor", "cota", "boleto", "proposta")):
        profile = _weight_profiles()["fintech"]
        return "fintech", profile
    if any(col.get("sensitive_guess") for col in payload_columns):
        return "fintech", profile
    return "default", profile


def _observed_score(dimension_scores: dict[str, float | None], weights: dict[str, float]) -> float | None:
    score, _coverage = _blend_score(weights, dimension_scores)
    return score


def _formal_score() -> float | None:
    return None


def _delta_where_clause(args) -> str | None:
    """Build a safe WHERE clause for delta profiling, or None for a full read."""
    if (getattr(args, "profiling_mode", "full") or "full") != "delta":
        return None
    column = (getattr(args, "watermark_column", None) or "").strip()
    window_start = (getattr(args, "window_start", None) or "").strip()
    window_end = (getattr(args, "window_end", None) or "").strip()
    if not column or not window_start:
        return None
    # Identifier comes from our own catalog, but guard against injection regardless.
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", column):
        return None
    # Timestamp bounds are ISO-8601 strings produced by the backend; reject quotes.
    if "'" in window_start or "'" in window_end:
        return None
    clause = f'"{column}" > \'{window_start}\''
    if window_end:
        clause += f' AND "{column}" <= \'{window_end}\''
    return clause


def main() -> None:
    parser = base_parser()
    parser.add_argument("--columns-json", default="[]")
    parser.add_argument("--sample-fraction", type=float, default=None)
    parser.add_argument("--profiling-mode", default="full")
    parser.add_argument("--watermark-column", default=None)
    parser.add_argument("--window-start", default=None)
    parser.add_argument("--window-end", default=None)
    args = parser.parse_args()
    connection = load_connection_config(args)

    where_clause = _delta_where_clause(args)

    # Particionamento JDBC: no modo delta a coluna de watermark + a janela (window_start/end)
    # são bounds naturais → paraleliza a leitura em N faixas (evita 1 executor segurar tudo).
    num_partitions = int(os.getenv("SPARK_JDBC_NUM_PARTITIONS", "4") or "4")
    fetchsize = int(os.getenv("SPARK_JDBC_FETCHSIZE", "10000") or "10000")
    partition_kwargs: dict[str, Any] = {}
    if (
        (getattr(args, "profiling_mode", "full") or "full") == "delta"
        and (getattr(args, "watermark_column", None) or "").strip()
        and (getattr(args, "window_start", None) or "").strip()
    ):
        partition_kwargs = {
            "partition_column": args.watermark_column.strip(),
            "lower_bound": args.window_start.strip(),
            "upper_bound": (args.window_end or args.window_start).strip(),
            "num_partitions": num_partitions,
        }

    spark = build_spark("t2c-dq-profiling")
    df: DataFrame | None = None
    try:
        df = read_table_via_jdbc(
            spark,
            connection["jdbc_url"],
            connection["jdbc_user"],
            connection["jdbc_password"],
            args.table_fqn,
            where_clause=where_clause,
            fetchsize=fetchsize,
            **partition_kwargs,
        )
        if args.sample_fraction:
            df = df.sample(withReplacement=False, fraction=args.sample_fraction, seed=42)

        selected_columns = json.loads(args.columns_json or "[]")
        if selected_columns:
            keep = [c for c in selected_columns if c in df.columns]
            if keep:
                df = df.select(*keep)

        df = df.persist()
        has_rows = df.limit(1).count() > 0
        columns = df.columns
        if not has_rows:
            payload = {
                "status": "no_data",
                "observation": "Tabela sem linhas no momento do perfilamento.",
                "table_fqn": args.table_fqn,
                "row_count": 0,
                "column_count": len(columns),
                "estimated_size_bytes": None,
                "last_updated_at": None,
                "last_loaded_at": None,
                "schema_hash": _schema_hash(df),
                "schema_current": _schema_signature(df),
                "types_detected": {field.name: field.dataType.simpleString() for field in df.schema.fields},
                "columns_detected": columns,
                "row_count": 0,
                "completeness_pct_avg": None,
                "dq_score": None,
                "duplicates_count": 0,
                "duplicate_business_key_count": 0,
                "failed_rules": 0,
                "freshness_seconds": None,
                "volume_change_ratio": None,
                "columns": [],
                "profiling_intelligence": {
                    "weight_profile": "default",
                    "observed_score": None,
                    "formal_score": None,
                    "coverage_score": 0.0,
                    "consolidated_score": None,
                    "coverage_dimensions": 0,
                    "covered_dimensions": 0,
                    "dimension_scores": {},
                    "rule_suggestions": [],
                    "quality_message": "Tabela vazia",
                },
            }
            write_json_output(args.output_json, payload)
            return

        row_count = int(df.count())
        column_payloads: list[dict[str, Any]] = []
        for field in df.schema.fields:
            column_payloads.append(_profile_column(df, field, row_count))

        completeness_values = [100.0 - float(column["null_pct"] or 0.0) for column in column_payloads]
        completeness_pct_avg = round(sum(completeness_values) / len(completeness_values), 4) if completeness_values else 100.0
        duplicate_rows_count = max(0, row_count - int(df.dropDuplicates().count()))
        duplicate_business_key_count = _business_key_duplicate_count(df, columns)
        last_updated_column, last_updated_at, freshness_seconds = _table_timestamp_hint(df)
        schema_hash = _schema_hash(df)
        estimated_size_bytes = None
        volume_change_ratio = None

        weight_profile_name, weight_map = _select_weight_profile(args.table_fqn, args.table_fqn.split(".")[-1], column_payloads)
        dimension_scores = _dimension_scores(column_payloads, {"row_count": row_count, "freshness_seconds": freshness_seconds, "schema_hash": schema_hash, "duplicate_business_key_count": duplicate_business_key_count})
        observed_score = _observed_score(dimension_scores, weight_map)
        formal_score = _formal_score()
        coverage_score = round(
            (sum(1 for score in dimension_scores.values() if score is not None) / max(len(dimension_scores), 1)) * 100.0,
            4,
        )
        consolidated_score = observed_score
        if consolidated_score is None:
            consolidated_score = round(completeness_pct_avg, 4) if completeness_values else None

        suggestions: list[dict[str, Any]] = []
        for column in column_payloads:
            suggestions.extend(_suggestions_for_column(column, row_count))

        payload = {
            "status": "success",
            "table_fqn": args.table_fqn,
            "row_count": row_count,
            "column_count": len(columns),
            "estimated_size_bytes": estimated_size_bytes,
            "last_updated_at": last_updated_at,
            "last_loaded_at": last_updated_at,
            "last_updated_column": last_updated_column,
            "schema_hash": schema_hash,
            "schema_current": _schema_signature(df),
            "types_detected": {field.name: field.dataType.simpleString() for field in df.schema.fields},
            "columns_detected": columns,
            "completeness_pct_avg": completeness_pct_avg,
            "dq_score": consolidated_score,
            "duplicates_count": duplicate_rows_count,
            "duplicate_business_key_count": duplicate_business_key_count,
            "failed_rules": 0,
            "freshness_seconds": freshness_seconds,
            "volume_change_ratio": volume_change_ratio,
            "columns": column_payloads,
            "profiling_intelligence": {
                "weight_profile": weight_profile_name,
                "observed_score": observed_score,
                "formal_score": formal_score,
                "coverage_score": coverage_score,
                "consolidated_score": consolidated_score,
                "coverage_dimensions": sum(1 for score in dimension_scores.values() if score is not None),
                "covered_dimensions": sum(1 for score in dimension_scores.values() if score is not None),
                "dimension_scores": dimension_scores,
                "rule_suggestions": suggestions,
                "quality_message": (
                    "Boa qualidade observada, mas cobertura formal ainda depende de regras configuradas."
                    if formal_score is None
                    else "Perfilamento e regras formais disponíveis."
                ),
            },
        }
        write_json_output(args.output_json, payload)
    finally:
        try:
            if df is not None:
                df.unpersist()
        except Exception:
            pass
        spark.stop()


if __name__ == "__main__":
    main()
