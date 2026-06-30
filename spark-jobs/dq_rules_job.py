from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from dq_common import base_parser, build_spark, load_connection_config, read_table_via_jdbc, safe_preview, write_json_output


def _column_families(df: DataFrame) -> dict[str, str]:
    families: dict[str, str] = {}
    for field in df.schema.fields:
        simple = field.dataType.simpleString().lower()
        if any(token in simple for token in ("int", "double", "float", "decimal", "bigint", "smallint", "tinyint", "long", "short")):
            families[field.name] = "number"
        elif any(token in simple for token in ("date", "timestamp")):
            families[field.name] = "date"
        elif "boolean" in simple:
            families[field.name] = "boolean"
        else:
            families[field.name] = "text"
    return families


def _safe_valid_expr(expr):
    return F.coalesce(expr.cast("boolean"), F.lit(False))


def _build_condition_expr(df: DataFrame, condition: dict) -> object:
    column_name = str(condition.get("column") or "")
    if column_name not in df.columns:
        raise ValueError(f"Coluna '{column_name}' não encontrada no DataFrame da regra.")
    operator = str(condition.get("operator") or "")
    value = condition.get("value")
    value_to = condition.get("value_to")
    compare_column = condition.get("compare_column")

    column_ref = F.col(column_name)
    string_ref = column_ref.cast("string")

    if operator == "is_null":
        return column_ref.isNull()
    if operator == "not_null":
        return column_ref.isNotNull()
    if operator == "equal":
        return column_ref == F.lit(value)
    if operator == "not_equal":
        return column_ref != F.lit(value)
    if operator == "greater_than":
        return column_ref > F.lit(value)
    if operator == "greater_or_equal":
        return column_ref >= F.lit(value)
    if operator == "less_than":
        return column_ref < F.lit(value)
    if operator == "less_or_equal":
        return column_ref <= F.lit(value)
    if operator == "between":
        return column_ref.between(value, value_to)
    if operator == "not_between":
        return ~column_ref.between(value, value_to)
    if operator == "contains":
        return string_ref.contains(str(value))
    if operator == "not_contains":
        return ~string_ref.contains(str(value))
    if operator == "starts_with":
        return string_ref.startswith(str(value))
    if operator == "ends_with":
        return string_ref.endswith(str(value))
    if operator == "in_list":
        return column_ref.isin(list(condition.get("values") or []))
    if operator == "not_in_list":
        return ~column_ref.isin(list(condition.get("values") or []))
    if operator == "freshness_within_last":
        amount = float(condition.get("value") or 0)
        if amount <= 0:
            raise ValueError("Freshness requer valor maior que zero.")
        threshold = datetime.now(timezone.utc) - timedelta(
            hours=amount if condition.get("time_unit") == "hours" else 0,
            days=amount if condition.get("time_unit") == "days" else 0,
        )
        return column_ref.isNotNull() & (column_ref >= F.lit(threshold.isoformat()))
    if operator == "not_future":
        return column_ref.isNotNull() & (F.to_timestamp(string_ref) <= F.current_timestamp())
    if operator == "matches_regex":
        pattern = str(value or "").strip()
        if not pattern:
            raise ValueError("Regex inválida.")
        return string_ref.rlike(pattern)
    if operator == "not_matches_regex":
        pattern = str(value or "").strip()
        if not pattern:
            raise ValueError("Regex inválida.")
        return ~string_ref.rlike(pattern)
    if operator in {"column_greater_than_column", "column_less_than_column", "column_equal_to_column", "column_required_when_other_present"}:
        if compare_column not in df.columns:
            raise ValueError(f"Coluna de comparação '{compare_column}' não encontrada.")
        compare_ref = F.col(str(compare_column))
        if operator == "column_greater_than_column":
            return column_ref > compare_ref
        if operator == "column_less_than_column":
            return column_ref < compare_ref
        if operator == "column_equal_to_column":
            return column_ref == compare_ref
        return compare_ref.isNull() | column_ref.isNotNull()
    raise ValueError(f"Operador '{operator}' não suportado no executor Spark.")


def _violation_dataframe(df: DataFrame, definition: dict) -> DataFrame:
    conditions = list(definition.get("conditions") or [])
    logic = str(definition.get("logic") or "AND").upper()
    if not conditions:
        unique_columns = _normalize_columns(definition.get("unique_columns"))
        if not unique_columns:
            raise ValueError("A regra não possui condições para execução.")
        window = Window.partitionBy(*[F.col(column_name) for column_name in unique_columns])
        return (
            df.withColumn("__dq_duplicate_count", F.count(F.lit(1)).over(window))
            .filter(F.col("__dq_duplicate_count") > 1)
            .drop("__dq_duplicate_count")
        )

    if len(conditions) == 1 and str(conditions[0].get("operator") or "") == "unique":
        unique_columns = _normalize_columns(definition.get("unique_columns"))
        column_name = unique_columns[0] if unique_columns else str(conditions[0].get("column") or "")
        window = Window.partitionBy(F.col(column_name))
        if len(unique_columns) > 1:
            window = Window.partitionBy(*[F.col(column_name) for column_name in unique_columns])
        return (
            df.withColumn("__dq_duplicate_count", F.count(F.lit(1)).over(window))
            .filter(F.col("__dq_duplicate_count") > 1)
            .drop("__dq_duplicate_count")
        )

    valid_expr = None
    for condition in conditions:
        expr = _safe_valid_expr(_build_condition_expr(df, condition))
        valid_expr = expr if valid_expr is None else (valid_expr & expr if logic == "AND" else valid_expr | expr)
    if valid_expr is None:
        raise ValueError("Não foi possível construir a expressão de validação.")
    return df.filter(~_safe_valid_expr(valid_expr))


def _normalize_columns(columns: list[str] | None) -> list[str]:
    return [str(column).strip() for column in (columns or []) if str(column).strip()]


def _reconciliation_result(
    source_df: DataFrame,
    comparison_df: DataFrame,
    definition: dict,
) -> tuple[int, list[dict], str | None]:
    comparison = definition.get("comparison")
    if not isinstance(comparison, dict):
        raise ValueError("A regra de acurácia não possui comparação estruturada.")

    metric = str(comparison.get("metric") or "count").strip().lower()
    key_columns = _normalize_columns(comparison.get("key_columns"))
    tolerance_abs = comparison.get("tolerance_abs")
    tolerance_pct = comparison.get("tolerance_pct")

    if key_columns:
        missing_source = [column for column in key_columns if column not in source_df.columns]
        missing_comparison = [column for column in key_columns if column not in comparison_df.columns]
        if missing_source:
            raise ValueError(f"Coluna(s) de chave ausente(s) na origem: {', '.join(missing_source)}")
        if missing_comparison:
            raise ValueError(f"Coluna(s) de chave ausente(s) na comparação: {', '.join(missing_comparison)}")

    if key_columns and metric == "count":
        source_keys = source_df.select(*[F.col(column_name) for column_name in key_columns]).distinct()
        comparison_keys = comparison_df.select(*[F.col(column_name) for column_name in key_columns]).distinct()
        missing_in_comparison = source_keys.join(comparison_keys, key_columns, "left_anti")
        missing_in_source = comparison_keys.join(source_keys, key_columns, "left_anti")
        mismatch_preview = missing_in_comparison.unionByName(missing_in_source, allowMissingColumns=True)
        missing_count = int(missing_in_comparison.count() + missing_in_source.count())
        if missing_count > 0:
            return (
                1,
                safe_preview(mismatch_preview, limit=20),
                "Reconciliação por chave com diferenças entre origem e comparação.",
            )
        source_count = int(source_keys.count())
        comparison_count = int(comparison_keys.count())
        delta = source_count - comparison_count
        delta_pct = round((abs(delta) / max(comparison_count, 1)) * 100.0, 2)
        if delta == 0:
            return 0, [], None
        if tolerance_abs is not None and abs(delta) <= float(tolerance_abs):
            return 0, [], None
        if tolerance_pct is not None and delta_pct <= float(tolerance_pct):
            return 0, [], None
        return (
            1,
            [
                {
                    "metric": "count",
                    "source_value": source_count,
                    "comparison_value": comparison_count,
                    "delta": delta,
                    "delta_pct": delta_pct,
                    "key_columns": key_columns,
                }
            ],
            "Reconciliação de contagem por chave fora da tolerância.",
        )

    if metric == "count":
        source_count = int(source_df.count())
        comparison_count = int(comparison_df.count())
        delta = source_count - comparison_count
        delta_pct = round((abs(delta) / max(comparison_count, 1)) * 100.0, 2)
        if delta == 0:
            return 0, [], None
        if tolerance_abs is not None and abs(delta) <= float(tolerance_abs):
            return 0, [], None
        if tolerance_pct is not None and delta_pct <= float(tolerance_pct):
            return 0, [], None
        return (
            1,
            [
                {
                    "metric": "count",
                    "source_value": source_count,
                    "comparison_value": comparison_count,
                    "delta": delta,
                    "delta_pct": delta_pct,
                    "key_columns": key_columns,
                }
            ],
            "Reconciliação de contagem fora da tolerância.",
        )

    column_name = str(comparison.get("column") or "").strip()
    if not column_name:
        raise ValueError("A reconciliação por soma precisa informar uma coluna numérica.")
    if column_name not in source_df.columns:
        raise ValueError(f"Coluna '{column_name}' não encontrada na origem.")
    if column_name not in comparison_df.columns:
        raise ValueError(f"Coluna '{column_name}' não encontrada na comparação.")

    if key_columns:
        source_grouped = source_df.groupBy(*[F.col(column) for column in key_columns]).agg(F.sum(F.col(column_name)).alias("source_value"))
        comparison_grouped = comparison_df.groupBy(*[F.col(column) for column in key_columns]).agg(F.sum(F.col(column_name)).alias("comparison_value"))
        joined = source_grouped.join(comparison_grouped, key_columns, "full_outer")
        joined = joined.fillna(0.0, subset=["source_value", "comparison_value"])
        joined = joined.withColumn("delta", F.col("source_value") - F.col("comparison_value"))
        joined = joined.withColumn(
            "delta_pct",
            F.when(F.col("comparison_value") == 0, F.when(F.col("source_value") == 0, F.lit(0.0)).otherwise(F.lit(100.0))).otherwise(
                F.abs(F.col("delta")) / F.abs(F.col("comparison_value")) * 100.0
            ),
        )
        if tolerance_abs is not None:
            within_abs = F.abs(F.col("delta")) <= float(tolerance_abs)
        else:
            within_abs = F.lit(True)
        if tolerance_pct is not None:
            within_pct = F.col("delta_pct") <= float(tolerance_pct)
        else:
            within_pct = F.lit(True)
        filtered = joined.filter(~(within_abs & within_pct))
        if filtered.count() == 0:
            return 0, [], None
        return (
            1,
            safe_preview(filtered, limit=20),
            "Reconciliação de soma por chave fora da tolerância.",
        )

    source_sum = source_df.select(F.sum(F.col(column_name)).alias("value")).collect()[0]["value"]
    comparison_sum = comparison_df.select(F.sum(F.col(column_name)).alias("value")).collect()[0]["value"]
    source_sum_value = float(source_sum or 0.0)
    comparison_sum_value = float(comparison_sum or 0.0)
    delta = source_sum_value - comparison_sum_value
    delta_pct = round((abs(delta) / max(abs(comparison_sum_value), 1.0)) * 100.0, 2)
    if delta == 0:
        return 0, [], None
    if tolerance_abs is not None and abs(delta) <= float(tolerance_abs):
        return 0, [], None
    if tolerance_pct is not None and delta_pct <= float(tolerance_pct):
        return 0, [], None
    return (
        1,
        [
            {
                "metric": "sum",
                "column": column_name,
                "source_value": source_sum_value,
                "comparison_value": comparison_sum_value,
                "delta": delta,
                "delta_pct": delta_pct,
                "key_columns": key_columns,
            }
        ],
        "Reconciliação de soma fora da tolerância.",
    )


def main() -> None:
    parser = base_parser()
    parser.add_argument("--rules-json", required=True)
    args = parser.parse_args()
    connection = load_connection_config(args)

    spark = build_spark("t2c-dq-rules")
    try:
        df = read_table_via_jdbc(
            spark,
            connection["jdbc_url"],
            connection["jdbc_user"],
            connection["jdbc_password"],
            args.table_fqn,
        )
        rows_checked_total = int(df.count())

        rules = json.loads(args.rules_json)
        results: list[dict] = []
        for rule in rules:
            definition = rule.get("rule_definition_json")
            if not isinstance(definition, dict):
                results.append(
                    {
                        "rule_id": int(rule["id"]),
                        "status": "error",
                        "rows_checked": rows_checked_total,
                        "violations_count": 0,
                        "preview_rows": [],
                        "error_message": "A regra não possui definição visual estruturada.",
                    }
                )
                continue
            try:
                if str(definition.get("type") or "").strip().lower() == "reconciliation" or definition.get("comparison") is not None:
                    comparison = definition.get("comparison")
                    if not isinstance(comparison, dict):
                        raise ValueError("A regra de acurácia não possui comparação estruturada.")
                    comparison_table_fqn = str(comparison.get("table_fqn") or "").strip()
                    if not comparison_table_fqn:
                        raise ValueError("A regra de acurácia precisa informar a tabela de comparação.")
                    comparison_df = read_table_via_jdbc(
                        spark,
                        connection["jdbc_url"],
                        connection["jdbc_user"],
                        connection["jdbc_password"],
                        comparison_table_fqn,
                    )
                    violations_count, preview_rows, error_message = _reconciliation_result(df, comparison_df, definition)
                    status = "fail" if violations_count > 0 else "pass"
                else:
                    violations_df = _violation_dataframe(df, definition)
                    violations_count = int(violations_df.count())
                    preview_rows = safe_preview(violations_df, limit=20)
                    status = "fail" if violations_count > 0 else "pass"
                    error_message = None
            except Exception as exc:  # noqa: BLE001
                violations_count = 0
                preview_rows = []
                status = "error"
                error_message = str(exc)
            results.append(
                {
                    "rule_id": int(rule["id"]),
                    "status": status,
                    "rows_checked": rows_checked_total,
                    "violations_count": violations_count,
                    "preview_rows": preview_rows,
                    "error_message": error_message,
                }
            )

        summary = {
            "total_rules": len(results),
            "failed_rules": sum(1 for r in results if r["status"] == "fail"),
            "error_rules": sum(1 for r in results if r["status"] == "error"),
            "passed_rules": sum(1 for r in results if r["status"] == "pass"),
        }
        write_json_output(
            args.output_json,
            {
                "table_fqn": args.table_fqn,
                "rows_checked_total": rows_checked_total,
                "rules": results,
                "summary": summary,
            },
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
