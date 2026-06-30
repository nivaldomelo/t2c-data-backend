from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Literal

from fastapi import HTTPException, status

from t2c_data.core.sql_utils import safe_identifier
from t2c_data.models.catalog import ColumnEntity, DataSource, Schema, TableEntity

LEGACY_SQL_RULE_REMOVED_REASON = "legacy_sql_rule_removed"


def _validate_comparison_fqn(value: str, *, label: str) -> str:
    """Reject anything that is not a strict 1-3 part dotted identifier.

    The comparison target FQN flows into the Spark JDBC `dbtable` option, which the driver
    wraps as `SELECT * FROM <dbtable>`. Without this guard a free-form string (e.g. a
    subquery) would be executed against the source database — SQL injection. Each part must
    match the identifier grammar, so no quotes/spaces/parentheses/semicolons can pass.
    """
    raw = str(value or "").strip()
    parts = raw.split(".")
    if not raw or len(parts) > 3:
        raise ValueError(f"{label} inválido.")
    try:
        return ".".join(safe_identifier(part, label=label) for part in parts)
    except ValueError as exc:
        raise ValueError(f"{label} inválido.") from exc
RULE_DIMENSION_LABELS: dict[str, str] = {
    "completude": "Completude",
    "validade": "Validade",
    "consistencia": "Consistência",
    "unicidade": "Unicidade",
    "tempestividade": "Tempestividade",
    "acuracia": "Acurácia",
}

RULE_CATEGORY_LABELS: dict[str, str] = {
    "technical": "Técnica",
    "business": "Negócio",
    "operational": "Operacional",
}

RuleLogic = Literal["AND", "OR"]

RULE_TYPE_LABELS: dict[str, str] = {
    "column_validation": "Validação de coluna",
    "nullability": "Validação de nulidade",
    "domain": "Validação de domínio",
    "uniqueness": "Validação de unicidade",
    "freshness": "Validação de freshness",
    "column_comparison": "Comparação entre colunas",
    "reconciliation": "Reconciliação",
}

OPERATOR_LABELS: dict[str, str] = {
    "equal": "igual a",
    "not_equal": "diferente de",
    "greater_than": "maior que",
    "greater_or_equal": "maior ou igual a",
    "less_than": "menor que",
    "less_or_equal": "menor ou igual a",
    "between": "entre",
    "not_between": "fora do intervalo",
    "contains": "contém",
    "not_contains": "não contém",
    "starts_with": "começa com",
    "ends_with": "termina com",
    "is_null": "é nulo",
    "not_null": "não é nulo",
    "matches_regex": "corresponde ao padrão",
    "not_matches_regex": "não corresponde ao padrão",
    "in_list": "está em lista",
    "not_in_list": "não está em lista",
    "unique": "valores únicos",
    "freshness_within_last": "atualizada nos últimos",
    "not_future": "não está no futuro",
    "column_greater_than_column": "maior que outra coluna",
    "column_less_than_column": "menor que outra coluna",
    "column_equal_to_column": "igual a outra coluna",
    "column_required_when_other_present": "preenchida quando outra coluna tem valor",
}

OPERATORS_BY_FAMILY: dict[str, tuple[str, ...]] = {
    "number": (
        "equal",
        "not_equal",
        "greater_than",
        "greater_or_equal",
        "less_than",
        "less_or_equal",
        "between",
        "not_between",
        "in_list",
        "not_in_list",
        "is_null",
        "not_null",
        "matches_regex",
        "not_matches_regex",
        "unique",
        "not_future",
        "column_greater_than_column",
        "column_less_than_column",
        "column_equal_to_column",
        "column_required_when_other_present",
    ),
    "text": (
        "equal",
        "not_equal",
        "contains",
        "not_contains",
        "starts_with",
        "ends_with",
        "in_list",
        "not_in_list",
        "is_null",
        "not_null",
        "matches_regex",
        "not_matches_regex",
        "unique",
        "column_equal_to_column",
        "column_required_when_other_present",
    ),
    "date": (
        "equal",
        "not_equal",
        "greater_than",
        "greater_or_equal",
        "less_than",
        "less_or_equal",
        "between",
        "not_between",
        "is_null",
        "not_null",
        "matches_regex",
        "not_matches_regex",
        "freshness_within_last",
        "not_future",
        "column_greater_than_column",
        "column_less_than_column",
        "column_equal_to_column",
        "column_required_when_other_present",
    ),
    "boolean": (
        "equal",
        "not_equal",
        "is_null",
        "not_null",
        "column_equal_to_column",
        "column_required_when_other_present",
    ),
}

RULE_TEMPLATES: list[dict[str, Any]] = [
    {
        "key": "completude.required_column",
        "label": "Coluna obrigatória",
        "dimension": "completude",
        "category": "technical",
        "rule_type": "nullability",
        "description": "Exige que a coluna selecionada nunca fique nula.",
        "requires_comparison": False,
    },
    {
        "key": "completude.required_when_other_present",
        "label": "Obrigatória quando outra coluna existe",
        "dimension": "completude",
        "category": "technical",
        "rule_type": "column_validation",
        "description": "Exige preenchimento condicional quando outra coluna possui valor.",
        "requires_comparison": False,
    },
    {
        "key": "validade.cpf",
        "label": "CPF válido",
        "dimension": "validade",
        "category": "technical",
        "rule_type": "domain",
        "description": "Valida o formato de CPF com expressão regular.",
        "requires_comparison": False,
    },
    {
        "key": "validade.cnpj",
        "label": "CNPJ válido",
        "dimension": "validade",
        "category": "technical",
        "rule_type": "domain",
        "description": "Valida o formato de CNPJ com expressão regular.",
        "requires_comparison": False,
    },
    {
        "key": "validade.email",
        "label": "E-mail válido",
        "dimension": "validade",
        "category": "technical",
        "rule_type": "domain",
        "description": "Valida o formato de e-mail.",
        "requires_comparison": False,
    },
    {
        "key": "validade.telefone",
        "label": "Telefone válido",
        "dimension": "validade",
        "category": "technical",
        "rule_type": "domain",
        "description": "Valida o formato de telefone.",
        "requires_comparison": False,
    },
    {
        "key": "validade.nao_futura",
        "label": "Data não futura",
        "dimension": "validade",
        "category": "technical",
        "rule_type": "domain",
        "description": "Garante que a data não esteja no futuro.",
        "requires_comparison": False,
    },
    {
        "key": "consistencia.colunas",
        "label": "Comparação entre colunas",
        "dimension": "consistencia",
        "category": "business",
        "rule_type": "column_comparison",
        "description": "Compara duas colunas para detectar inconsistências.",
        "requires_comparison": False,
    },
    {
        "key": "unicidade.coluna",
        "label": "Unicidade por coluna",
        "dimension": "unicidade",
        "category": "technical",
        "rule_type": "uniqueness",
        "description": "Exige valores únicos em uma coluna.",
        "requires_comparison": False,
    },
    {
        "key": "unicidade.chave_composta",
        "label": "Chave composta única",
        "dimension": "unicidade",
        "category": "technical",
        "rule_type": "uniqueness",
        "description": "Exige unicidade combinada em múltiplas colunas.",
        "requires_comparison": False,
    },
    {
        "key": "tempestividade.sla",
        "label": "Freshness dentro do SLA",
        "dimension": "tempestividade",
        "category": "operational",
        "rule_type": "freshness",
        "description": "Exige que a coluna de data esteja dentro da janela de freshness esperada.",
        "requires_comparison": False,
    },
    {
        "key": "acuracia.contagem",
        "label": "Reconciliação de contagem",
        "dimension": "acuracia",
        "category": "business",
        "rule_type": "reconciliation",
        "description": "Compara contagem entre ativo de origem e ativo de destino.",
        "requires_comparison": True,
    },
    {
        "key": "acuracia.soma",
        "label": "Reconciliação de soma",
        "dimension": "acuracia",
        "category": "business",
        "rule_type": "reconciliation",
        "description": "Compara soma agregada entre ativo de origem e ativo de destino.",
        "requires_comparison": True,
    },
]

RULE_DIMENSION_BY_TYPE: dict[str, str] = {
    "column_validation": "validade",
    "nullability": "completude",
    "domain": "validade",
    "uniqueness": "unicidade",
    "freshness": "tempestividade",
    "column_comparison": "consistencia",
    "reconciliation": "acuracia",
}

RULE_CATEGORY_BY_TYPE: dict[str, str] = {
    "column_validation": "technical",
    "nullability": "technical",
    "domain": "technical",
    "uniqueness": "technical",
    "freshness": "operational",
    "column_comparison": "business",
    "reconciliation": "business",
}

STATIC_RULE_BUILDER_OPTIONS = {
    "category_options": [
        {"value": key, "label": label}
        for key, label in RULE_CATEGORY_LABELS.items()
    ],
    "logic_options": [
        {"value": "AND", "label": "Todas as condições (AND)"},
        {"value": "OR", "label": "Qualquer condição (OR)"},
    ],
    "rule_types": [
        {"value": key, "label": label}
        for key, label in RULE_TYPE_LABELS.items()
    ],
    "severities": [
        {"value": "low", "label": "Baixa"},
        {"value": "medium", "label": "Média"},
        {"value": "high", "label": "Alta"},
        {"value": "critical", "label": "Crítica"},
    ],
    "operators": {
        family: [
            {"value": operator, "label": OPERATOR_LABELS[operator]}
            for operator in operators
        ]
        for family, operators in OPERATORS_BY_FAMILY.items()
    },
    "time_units": [
        {"value": "hours", "label": "Horas"},
        {"value": "days", "label": "Dias"},
    ],
}


def classify_column_family(data_type: str | None) -> str:
    normalized = str(data_type or "").strip().lower()
    if not normalized:
        return "text"
    if any(token in normalized for token in ("bool",)):
        return "boolean"
    if any(token in normalized for token in ("date", "time", "timestamp")):
        return "date"
    if any(token in normalized for token in ("int", "numeric", "decimal", "double", "float", "real", "number", "serial")):
        return "number"
    return "text"


def _coerce_number(value: Any) -> int | float:
    if isinstance(value, bool) or value is None:
        raise ValueError("Esperado valor numérico.")
    if isinstance(value, (int, float)):
        return value
    try:
        decimal_value = Decimal(str(value).strip())
    except (InvalidOperation, ValueError) as exc:
        raise ValueError("Valor numérico inválido.") from exc
    return int(decimal_value) if decimal_value == decimal_value.to_integral_value() else float(decimal_value)


def _coerce_text(value: Any) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError("Valor textual obrigatório.")
    if len(normalized) > 500:
        raise ValueError("Valor textual muito longo para a regra.")
    return normalized


def _coerce_boolean(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    normalized = str(value or "").strip().lower()
    if normalized in {"true", "1", "sim"}:
        return True
    if normalized in {"false", "0", "nao", "não"}:
        return False
    raise ValueError("Valor booleano inválido.")


def _coerce_date_like(value: Any) -> str:
    if value is None:
        raise ValueError("Valor de data é obrigatório.")
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    normalized = str(value).strip()
    if not normalized:
        raise ValueError("Valor de data é obrigatório.")
    try:
        if "T" in normalized or " " in normalized:
            datetime.fromisoformat(normalized.replace("Z", "+00:00"))
        else:
            date.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError("Valor de data inválido.") from exc
    return normalized


def _coerce_scalar_by_family(value: Any, family: str) -> Any:
    if family == "number":
        return _coerce_number(value)
    if family == "date":
        return _coerce_date_like(value)
    if family == "boolean":
        return _coerce_boolean(value)
    return _coerce_text(value)


def _ensure_list(values: Any) -> list[Any]:
    if values is None:
        raise ValueError("Lista de valores obrigatória.")
    if not isinstance(values, list):
        raise ValueError("O valor informado deve ser uma lista.")
    if not values:
        raise ValueError("Informe ao menos um valor na lista.")
    if len(values) > 100:
        raise ValueError("A lista informada é muito grande para uma regra de DQ.")
    return values


def _normalize_dimension(value: Any | None, rule_type: str) -> str:
    normalized = str(value or "").strip().lower()
    if normalized:
        if normalized not in RULE_DIMENSION_LABELS:
            raise ValueError("Dimensão de qualidade inválida.")
        return normalized
    inferred = RULE_DIMENSION_BY_TYPE.get(rule_type)
    if inferred is None:
        raise ValueError("Não foi possível inferir a dimensão da regra.")
    return inferred


def _normalize_template_key(value: Any | None) -> str | None:
    normalized = str(value or "").strip()
    return normalized or None


def _normalize_rule_category(value: Any | None, *, rule_type: str) -> str:
    normalized = str(value or "").strip().lower()
    if normalized:
        if normalized not in RULE_CATEGORY_LABELS:
            raise ValueError("Categoria de regra inválida.")
        return normalized
    inferred = RULE_CATEGORY_BY_TYPE.get(rule_type)
    if inferred is None:
        raise ValueError("Não foi possível inferir a categoria da regra.")
    return inferred


def normalize_rule_condition(
    condition: dict[str, Any],
    *,
    columns_by_name: dict[str, ColumnEntity],
) -> dict[str, Any]:
    column_name = str(condition.get("column") or "").strip()
    if not column_name:
        raise ValueError("Cada condição precisa informar uma coluna.")
    column = columns_by_name.get(column_name)
    if column is None:
        raise ValueError(f"Coluna '{column_name}' não encontrada na tabela selecionada.")

    operator = str(condition.get("operator") or "").strip()
    if not operator:
        raise ValueError(f"A condição da coluna '{column_name}' precisa informar um operador.")

    family = classify_column_family(column.data_type)
    allowed_operators = set(OPERATORS_BY_FAMILY.get(family, ()))
    if operator not in allowed_operators:
        raise ValueError(
            f"O operador '{operator}' não é permitido para a coluna '{column_name}' do tipo '{column.data_type}'."
        )

    normalized: dict[str, Any] = {
        "column": column.name,
        "column_id": column.id,
        "column_data_type": column.data_type,
        "column_family": family,
        "operator": operator,
    }

    if operator in {"is_null", "not_null", "unique"}:
        normalized["value_type"] = "none"
        return normalized

    if operator in {"matches_regex", "not_matches_regex"}:
        pattern = _coerce_text(condition.get("value"))
        normalized["value"] = pattern
        normalized["value_type"] = "text"
        return normalized

    if operator == "not_future":
        normalized["value_type"] = "date"
        return normalized

    if operator in {"column_greater_than_column", "column_less_than_column", "column_equal_to_column", "column_required_when_other_present"}:
        compare_column_name = str(condition.get("compare_column") or "").strip()
        compare_column = columns_by_name.get(compare_column_name)
        if compare_column is None:
            raise ValueError(f"A coluna de comparação '{compare_column_name}' não foi encontrada.")
        normalized["compare_column"] = compare_column.name
        normalized["compare_column_id"] = compare_column.id
        normalized["value_type"] = "column"
        return normalized

    if operator == "freshness_within_last":
        amount = _coerce_number(condition.get("value"))
        time_unit = str(condition.get("time_unit") or "").strip().lower()
        if time_unit not in {"hours", "days"}:
            raise ValueError("Freshness exige unidade de tempo em horas ou dias.")
        if float(amount) <= 0:
            raise ValueError("Freshness exige valor maior que zero.")
        normalized["value"] = amount
        normalized["time_unit"] = time_unit
        normalized["value_type"] = "number"
        return normalized

    if operator in {"between", "not_between"}:
        normalized["value"] = _coerce_scalar_by_family(condition.get("value"), family)
        normalized["value_to"] = _coerce_scalar_by_family(condition.get("value_to"), family)
        normalized["value_type"] = family
        return normalized

    if operator in {"in_list", "not_in_list"}:
        values = [_coerce_scalar_by_family(item, family) for item in _ensure_list(condition.get("values"))]
        normalized["values"] = values
        normalized["value_type"] = "list"
        return normalized

    normalized["value"] = _coerce_scalar_by_family(condition.get("value"), family)
    normalized["value_type"] = family
    return normalized


def summarize_rule_condition(condition: dict[str, Any]) -> str:
    operator = str(condition.get("operator") or "")
    column = str(condition.get("column") or "coluna")
    label = OPERATOR_LABELS.get(operator, operator)

    if operator in {"is_null", "not_null", "unique"}:
        return f"{column} {label}"
    if operator in {"matches_regex", "not_matches_regex"}:
        return f"{column} {label} {condition.get('value')}"
    if operator == "not_future":
        return f"{column} {label}"
    if operator in {"column_greater_than_column", "column_less_than_column", "column_equal_to_column", "column_required_when_other_present"}:
        return f"{column} {label} {condition.get('compare_column')}"
    if operator == "freshness_within_last":
        unit = "hora" if condition.get("time_unit") == "hours" and condition.get("value") == 1 else (
            "horas" if condition.get("time_unit") == "hours" else ("dia" if condition.get("value") == 1 else "dias")
        )
        return f"{column} {label} {condition.get('value')} {unit}"
    if operator in {"between", "not_between"}:
        return f"{column} {label} {condition.get('value')} e {condition.get('value_to')}"
    if operator in {"in_list", "not_in_list"}:
        values = ", ".join(str(item) for item in condition.get("values") or [])
        return f"{column} {label} {values}"
    return f"{column} {label} {condition.get('value')}"


def summarize_rule_definition(definition: dict[str, Any]) -> str:
    conditions = definition.get("conditions") if isinstance(definition.get("conditions"), list) else []
    dimension = str(definition.get("dimension") or "").strip()
    category = str(definition.get("category") or "").strip()
    template_key = str(definition.get("template_key") or "").strip()
    prefix_bits: list[str] = []
    if category:
        prefix_bits.append(f"Categoria: {RULE_CATEGORY_LABELS.get(category, category)}")
    if dimension:
        prefix_bits.append(f"Dimensão: {RULE_DIMENSION_LABELS.get(dimension, dimension)}")
    if template_key:
        prefix_bits.append(f"Template: {template_key}")
    if not conditions:
        return " · ".join(prefix_bits) if prefix_bits else "Sem condições definidas"
    logic = str(definition.get("logic") or "AND").upper()
    joiner = " E " if logic == "AND" else " OU "
    summary = joiner.join(summarize_rule_condition(item) for item in conditions)
    if prefix_bits:
        return f"{' · '.join(prefix_bits)} · {summary}"
    return summary


def build_rule_definition(
    *,
    datasource: DataSource,
    schema: Schema,
    table: TableEntity,
    rule_type: str,
    logic: str,
    conditions: list[dict[str, Any]],
    columns_by_name: dict[str, ColumnEntity],
    quality_dimension: str | None = None,
    rule_category: str | None = None,
    template_key: str | None = None,
    unique_columns: list[str] | None = None,
    comparison_target: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_logic = str(logic or "AND").strip().upper()
    if normalized_logic not in {"AND", "OR"}:
        raise ValueError("A lógica entre condições deve ser AND ou OR.")
    normalized_rule_type = str(rule_type or "").strip().lower()
    if normalized_rule_type not in RULE_DIMENSION_BY_TYPE:
        raise ValueError("Tipo de regra inválido.")
    dimension = _normalize_dimension(quality_dimension, normalized_rule_type)
    category = _normalize_rule_category(rule_category, rule_type=normalized_rule_type)
    normalized_template_key = _normalize_template_key(template_key)
    if normalized_template_key and not any(template["key"] == normalized_template_key for template in RULE_TEMPLATES):
        raise ValueError("Template de regra inválido.")

    normalized_unique_columns = [str(item).strip() for item in (unique_columns or []) if str(item).strip()]
    normalized_comparison_target = None
    if comparison_target is not None:
        normalized_comparison_target = {
            "table_id": comparison_target.get("table_id"),
            "datasource_id": comparison_target.get("datasource_id"),
            "schema_name": comparison_target.get("schema_name"),
            "table_name": comparison_target.get("table_name"),
            "table_fqn": comparison_target.get("table_fqn"),
            "metric": str(comparison_target.get("metric") or "count").strip().lower(),
            "column": comparison_target.get("column"),
            "key_columns": [str(item).strip() for item in (comparison_target.get("key_columns") or []) if str(item).strip()],
            "tolerance_abs": comparison_target.get("tolerance_abs"),
            "tolerance_pct": comparison_target.get("tolerance_pct"),
        }
        if normalized_comparison_target["metric"] not in {"count", "sum"}:
            raise ValueError("Métrica de comparação inválida.")
        if not normalized_comparison_target.get("table_fqn") and not normalized_comparison_target.get("table_id"):
            raise ValueError("A regra de acurácia precisa informar a tabela de comparação.")
        if normalized_comparison_target["metric"] == "sum" and not str(normalized_comparison_target.get("column") or "").strip():
            raise ValueError("A comparação por soma precisa informar a coluna de valor.")
        # Harden identifiers that flow into the Spark JDBC `dbtable`/column expressions.
        if normalized_comparison_target.get("table_fqn"):
            normalized_comparison_target["table_fqn"] = _validate_comparison_fqn(
                normalized_comparison_target["table_fqn"], label="table_fqn de comparação"
            )
        if normalized_comparison_target.get("schema_name"):
            normalized_comparison_target["schema_name"] = safe_identifier(
                str(normalized_comparison_target["schema_name"]).strip(), label="schema de comparação"
            )
        if normalized_comparison_target.get("table_name"):
            normalized_comparison_target["table_name"] = safe_identifier(
                str(normalized_comparison_target["table_name"]).strip(), label="tabela de comparação"
            )
        if str(normalized_comparison_target.get("column") or "").strip():
            normalized_comparison_target["column"] = safe_identifier(
                str(normalized_comparison_target["column"]).strip(), label="coluna de comparação"
            )
        normalized_comparison_target["key_columns"] = [
            safe_identifier(item, label="coluna-chave de comparação")
            for item in normalized_comparison_target["key_columns"]
        ]

    normalized_conditions = [
        normalize_rule_condition(item, columns_by_name=columns_by_name)
        for item in conditions
    ]
    if normalized_rule_type != "reconciliation" and not normalized_conditions and not (
        normalized_rule_type == "uniqueness" and normalized_unique_columns
    ):
        raise ValueError("A regra precisa ter ao menos uma condição.")
    if normalized_rule_type == "reconciliation" and normalized_comparison_target is None:
        raise ValueError("A regra de acurácia precisa informar a tabela de comparação.")
    if normalized_rule_type == "uniqueness" and not normalized_unique_columns and not any(
        condition.get("operator") == "unique" for condition in normalized_conditions
    ):
        raise ValueError("A regra de unicidade precisa informar ao menos uma coluna única.")
    return {
        "version": 1,
        "type": normalized_rule_type,
        "dimension": dimension,
        "category": category,
        "template_key": normalized_template_key,
        "target": {
            "datasource_id": datasource.id,
            "datasource_name": datasource.name,
            "schema_name": schema.name,
            "table_name": table.name,
            "table_id": table.id,
        },
        "logic": normalized_logic,
        "conditions": normalized_conditions,
        "unique_columns": normalized_unique_columns,
        "comparison": normalized_comparison_target,
    }


def reject_legacy_sql_payload(payload: Any | None) -> None:
    if payload is None:
        return
    if isinstance(payload, dict):
        legacy_sql = {
            key: payload.get(key)
            for key in ("sql_text", "custom_sql", "raw_sql", "sql_expression")
            if payload.get(key) is not None and str(payload.get(key)).strip()
        }
        rule_type = str(payload.get("rule_type") or "").strip().lower()
    else:
        legacy_sql = {}
        for key in ("sql_text", "custom_sql", "raw_sql", "sql_expression"):
            value = getattr(payload, key, None)
            if value is not None and str(value).strip():
                legacy_sql[key] = value
        rule_type = str(getattr(payload, "rule_type", "") or "").strip().lower()

    if legacy_sql or rule_type in {"custom_sql", "raw_sql", "sql_expression"}:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="SQL livre foi descontinuado. Use o construtor visual de regras.",
        )


def builder_options_payload() -> dict[str, Any]:
    payload = dict(STATIC_RULE_BUILDER_OPTIONS)
    payload["category_options"] = [
        {"value": key, "label": label}
        for key, label in RULE_CATEGORY_LABELS.items()
    ]
    payload["dimension_options"] = [
        {"value": key, "label": label}
        for key, label in RULE_DIMENSION_LABELS.items()
    ]
    payload["templates"] = RULE_TEMPLATES
    return payload
