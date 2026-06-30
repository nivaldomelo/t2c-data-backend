from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from statistics import pstdev
from typing import Any

from t2c_data.models.platform import DataLakeConnection, DataLakeInventoryTable


_DEFAULT_FRESHNESS_SLA_HOURS = 168


@dataclass(slots=True)
class DataLakeQualitySignal:
    key: str
    label: str
    tone: str = "neutral"
    detail: str | None = None


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _safe_ratio(part: float | int | None, total: float | int | None) -> float | None:
    if part is None or total is None:
        return None
    try:
        total_value = float(total)
        if total_value <= 0:
            return None
        return max(0.0, min(1.0, float(part) / total_value))
    except (TypeError, ValueError, ZeroDivisionError):
        return None


def _freshness_sla_from_connection(connection: DataLakeConnection, inventory: DataLakeInventoryTable) -> int:
    if inventory.freshness_sla_hours_override and inventory.freshness_sla_hours_override > 0:
        return int(inventory.freshness_sla_hours_override)
    if inventory.layer == "bronze" and connection.freshness_sla_hours_bronze and connection.freshness_sla_hours_bronze > 0:
        return int(connection.freshness_sla_hours_bronze)
    if inventory.layer == "silver" and connection.freshness_sla_hours_silver and connection.freshness_sla_hours_silver > 0:
        return int(connection.freshness_sla_hours_silver)
    if inventory.layer == "gold" and connection.freshness_sla_hours_gold and connection.freshness_sla_hours_gold > 0:
        return int(connection.freshness_sla_hours_gold)
    if connection.freshness_sla_hours_default and connection.freshness_sla_hours_default > 0:
        return int(connection.freshness_sla_hours_default)
    return _DEFAULT_FRESHNESS_SLA_HOURS


def resolve_data_lake_table_freshness_sla_hours(
    connection: DataLakeConnection,
    inventory: DataLakeInventoryTable,
) -> int:
    return _freshness_sla_from_connection(connection, inventory)


def _freshness_snapshot(last_modified_at: datetime | None, freshness_sla_hours: int) -> dict[str, Any]:
    normalized = _as_utc(last_modified_at)
    if normalized is None:
        return {
            "freshness_status": "unknown",
            "freshness_detail": "Nenhuma atualização observada para esta tabela.",
            "freshness_age_seconds": None,
            "freshness_age_hours": None,
        }
    age_seconds = max(0, int((_now() - normalized).total_seconds()))
    age_hours = age_seconds / 3600.0
    if age_hours <= freshness_sla_hours:
        return {
            "freshness_status": "fresh",
            "freshness_detail": f"Atualização dentro do SLA configurado ({freshness_sla_hours}h).",
            "freshness_age_seconds": age_seconds,
            "freshness_age_hours": age_hours,
        }
    if age_hours <= freshness_sla_hours * 1.5:
        return {
            "freshness_status": "recent",
            "freshness_detail": f"A atualização passou levemente do SLA configurado ({freshness_sla_hours}h).",
            "freshness_age_seconds": age_seconds,
            "freshness_age_hours": age_hours,
        }
    return {
        "freshness_status": "stale",
        "freshness_detail": f"A tabela está fora do SLA configurado ({freshness_sla_hours}h).",
        "freshness_age_seconds": age_seconds,
        "freshness_age_hours": age_hours,
    }


def _partition_tokens(path: str) -> set[str]:
    tokens: set[str] = set()
    for segment in path.split("/"):
        if "=" in segment:
            key = segment.split("=", 1)[0].strip().lower()
            if key:
                tokens.add(key)
    return tokens


def _aggregate_column_metrics(parquet_metadata: list[Any]) -> dict[str, dict[str, Any]]:
    metrics: dict[str, dict[str, Any]] = {}
    for metadata in parquet_metadata:
        row_count = max(0, int(getattr(metadata, "num_rows", 0) or 0))
        column_stats = getattr(metadata, "column_stats", {}) or {}
        for path, values in column_stats.items():
            current = metrics.setdefault(
                path,
                {
                    "num_values": 0,
                    "null_count": 0,
                    "distinct_count": None,
                    "files_present": 0,
                    "files_total_rows": 0,
                },
            )
            current["files_present"] += 1
            current["files_total_rows"] += row_count
            if values.get("num_values") is not None:
                current["num_values"] += max(0, int(values.get("num_values") or 0))
            if values.get("null_count") is not None:
                current["null_count"] += max(0, int(values.get("null_count") or 0))
            distinct_count = values.get("distinct_count")
            if distinct_count is not None:
                previous = current.get("distinct_count")
                if previous is None:
                    current["distinct_count"] = max(0, int(distinct_count))
                else:
                    current["distinct_count"] = min(int(previous), max(0, int(distinct_count)))
    for values in metrics.values():
        total_values = values.get("num_values") or values.get("files_total_rows")
        null_count = values.get("null_count")
        values["null_pct"] = _safe_ratio(null_count, total_values)
        values["all_null"] = bool(total_values and null_count is not None and int(null_count) >= int(total_values))
    return metrics


def calculate_data_lake_table_quality(
    *,
    connection: DataLakeConnection,
    inventory: DataLakeInventoryTable,
    sample_entries: list[dict[str, Any]],
    columns: list[dict[str, Any]],
    parquet_metadata: list[Any],
    errors: list[str],
    exact_coverage: bool,
    row_count: int | None,
    row_count_method: str | None,
    row_count_confidence: str | None,
) -> dict[str, Any]:
    freshness_sla_hours = _freshness_sla_from_connection(connection, inventory)
    freshness = _freshness_snapshot(inventory.last_modified_at, freshness_sla_hours)

    aggregated_metrics = _aggregate_column_metrics(parquet_metadata)
    total_columns = len(columns)
    null_columns = [name for name, values in aggregated_metrics.items() if values.get("all_null")]
    suspicious_columns = [column["path"] for column in columns if column.get("is_suspicious")]
    missing_columns = 0
    primary_paths = {column["path"] for column in columns}
    file_paths = [{column["path"] for column in getattr(metadata, "columns", [])} for metadata in parquet_metadata]
    if primary_paths and file_paths:
        intersection = set(primary_paths)
        for paths in file_paths:
            intersection &= paths
        missing_columns = len(primary_paths - intersection)

    schema_variants_count = len({getattr(metadata, "schema_signature", None) for metadata in parquet_metadata if getattr(metadata, "schema_signature", None)})
    drift_detected = schema_variants_count > 1 or missing_columns > 0
    unreadable_files_count = len(errors)

    partition_token_sets = [_partition_tokens(str(entry.get("key") or "")) for entry in sample_entries if entry.get("key")]
    partition_consistent = len({frozenset(tokens) for tokens in partition_token_sets if tokens}) <= 1
    partition_gap_detected = bool(partition_token_sets and any(not tokens for tokens in partition_token_sets) and any(tokens for tokens in partition_token_sets))

    sample_row_counts = [max(0, int(getattr(metadata, "num_rows", 0) or 0)) for metadata in parquet_metadata if getattr(metadata, "num_rows", None) is not None]
    volume_variation = pstdev(sample_row_counts) if len(sample_row_counts) > 1 else 0.0
    volume_spread = (max(sample_row_counts) / max(1, min(sample_row_counts))) if len(sample_row_counts) > 1 and min(sample_row_counts) > 0 else 1.0

    has_parquet_metadata = bool(parquet_metadata)

    completeness_score = 100.0
    if not has_parquet_metadata:
        completeness_score = 35.0
    if aggregated_metrics:
        null_ratios = [float(values["null_pct"]) for values in aggregated_metrics.values() if values.get("null_pct") is not None]
        completeness_score = max(0.0, 100.0 - (sum(null_ratios) / max(1, len(null_ratios))) * 100.0) if null_ratios else 72.0
    if null_columns:
        completeness_score = max(0.0, completeness_score - min(35.0, len(null_columns) * 8.0))

    structural_score = 100.0
    if not has_parquet_metadata:
        structural_score = 40.0
    if schema_variants_count > 1:
        structural_score -= min(55.0, (schema_variants_count - 1) * 22.0)
    if missing_columns > 0:
        structural_score -= min(25.0, missing_columns * 7.0)
    if partition_gap_detected:
        structural_score -= 12.0
    structural_score = max(0.0, structural_score)

    freshness_score = 100.0
    if freshness["freshness_status"] == "recent":
        freshness_score = 82.0
    elif freshness["freshness_status"] == "stale":
        freshness_score = 45.0
    elif freshness["freshness_status"] == "unknown":
        freshness_score = 55.0

    integrity_score = 100.0
    if not has_parquet_metadata:
        integrity_score = 35.0
    if unreadable_files_count > 0:
        integrity_score -= min(60.0, unreadable_files_count * 25.0)
    if row_count is not None and row_count <= 0:
        integrity_score -= 40.0
    if not exact_coverage:
        integrity_score -= 8.0
    integrity_score = max(0.0, integrity_score)

    metadata_coverage_score = 100.0
    if not has_parquet_metadata:
        metadata_coverage_score = 20.0
    if total_columns == 0:
        metadata_coverage_score = 45.0
    elif row_count_method == "estimated":
        metadata_coverage_score -= 12.0
    if sample_entries and len(sample_entries) < inventory.parquet_files_count:
        metadata_coverage_score -= 8.0
    metadata_coverage_score = max(0.0, metadata_coverage_score)

    quality_score = round(
        (
            completeness_score * 0.25
            + structural_score * 0.25
            + freshness_score * 0.25
            + integrity_score * 0.15
            + metadata_coverage_score * 0.10
        ),
        1,
    )

    if freshness["freshness_status"] == "fresh":
        freshness_tone = "success"
    elif freshness["freshness_status"] == "recent":
        freshness_tone = "warning"
    elif freshness["freshness_status"] == "stale":
        freshness_tone = "danger"
    else:
        freshness_tone = "neutral"

    quality_signals: list[dict[str, Any]] = []
    if not has_parquet_metadata:
        quality_signals.append(
            {
                "key": "no_parquet_metadata",
                "label": "Sem metadados parquet",
                "tone": "warning",
                "detail": "A leitura leve não encontrou footers legíveis para esta tabela.",
            }
        )
    quality_signals.append(
        {
            "key": "freshness",
            "label": "Freshness",
            "tone": freshness_tone,
            "detail": freshness["freshness_detail"],
        }
    )
    quality_signals.append(
        {
            "key": "schema",
            "label": "Schema",
            "tone": "warning" if schema_variants_count > 1 or missing_columns > 0 else "success",
            "detail": (
                "Há divergência estrutural entre arquivos amostrados."
                if schema_variants_count > 1 or missing_columns > 0
                else "Os arquivos amostrados mantêm schema consistente."
            ),
        }
    )
    quality_signals.append(
        {
            "key": "integrity",
            "label": "Integridade Parquet",
            "tone": "danger" if unreadable_files_count > 0 else "success",
            "detail": (
                f"{unreadable_files_count} arquivo(s) não puderam ser lidos com segurança."
                if unreadable_files_count > 0
                else "Leitura leve dos footers concluída com sucesso."
            ),
        }
    )
    quality_signals.append(
        {
            "key": "row_count_method",
            "label": "Contagem de linhas",
            "tone": "neutral" if row_count_method == "exact" else "accent",
            "detail": (
                "Contagem exata baseada nos footers dos arquivos amostrados."
                if row_count_method == "exact"
                else "Estimativa baseada na média dos arquivos parquet amostrados."
            ),
        }
    )
    if null_columns:
        quality_signals.append(
            {
                "key": "null_columns",
                "label": "Colunas totalmente nulas",
                "tone": "warning",
                "detail": f"{len(null_columns)} coluna(s) aparentam não carregar valores úteis nas amostras.",
            }
        )
    if suspicious_columns:
        quality_signals.append(
            {
                "key": "suspicious_columns",
                "label": "Colunas suspeitas",
                "tone": "warning",
                "detail": "Algumas colunas têm nomes genéricos ou gerados automaticamente.",
            }
        )
    if partition_gap_detected or not partition_consistent:
        quality_signals.append(
            {
                "key": "partitions",
                "label": "Partições",
                "tone": "warning",
                "detail": "A amostragem sugere partições inconsistentes ou ausentes em parte dos arquivos.",
            }
        )
    if not exact_coverage:
        quality_signals.append(
            {
                "key": "coverage",
                "label": "Cobertura de amostra",
                "tone": "accent",
                "detail": "A leitura foi amostrada para manter o custo do S3 controlado.",
            }
        )
    if volume_spread >= 5.0:
        quality_signals.append(
            {
                "key": "volume_spread",
                "label": "Volume irregular",
                "tone": "warning",
                "detail": "Há variação relevante de tamanho/linhas entre os arquivos amostrados.",
            }
        )

    operational_signals: list[dict[str, Any]] = []
    if freshness["freshness_status"] == "stale":
        operational_signals.append(
            {
                "key": "stale",
                "label": "Atualização atrasada",
                "tone": "danger",
                "detail": freshness["freshness_detail"],
            }
        )
    if schema_variants_count > 1:
        operational_signals.append(
            {
                "key": "schema_drift",
                "label": "Schema inconsistente",
                "tone": "warning",
                "detail": "Foram detectadas múltiplas assinaturas de schema entre os arquivos amostrados.",
            }
        )
    if partition_gap_detected:
        operational_signals.append(
            {
                "key": "partition_gap",
                "label": "Partições quebradas",
                "tone": "warning",
                "detail": "Parte dos arquivos amostrados não segue o mesmo padrão de partição.",
            }
        )
    if unreadable_files_count > 0:
        operational_signals.append(
            {
                "key": "unreadable_files",
                "label": "Arquivos ilegíveis",
                "tone": "danger",
                "detail": f"{unreadable_files_count} arquivo(s) tiveram falha de leitura do footer parquet.",
            }
        )
    if row_count is not None and row_count <= 0:
        operational_signals.append(
            {
                "key": "empty_table",
                "label": "Tabela vazia",
                "tone": "warning",
                "detail": "A contagem de linhas consolidada ficou zerada.",
            }
        )
    if volume_spread >= 5.0:
        operational_signals.append(
            {
                "key": "volume_anomaly",
                "label": "Crescimento anômalo",
                "tone": "warning",
                "detail": "A distribuição de volume entre arquivos sugere comportamento irregular.",
            }
        )

    quality_breakdown = [
        {
            "key": "completeness",
            "label": "Completude",
            "score": round(completeness_score, 1),
            "tone": "success" if completeness_score >= 80 else "warning" if completeness_score >= 60 else "danger",
            "detail": "Baseado em nulos detectados nos footers parquet e cobertura de colunas.",
        },
        {
            "key": "structural",
            "label": "Consistência estrutural",
            "score": round(structural_score, 1),
            "tone": "success" if structural_score >= 80 else "warning" if structural_score >= 60 else "danger",
            "detail": "Avalia drift de schema, colunas faltantes e partições inconsistentes.",
        },
        {
            "key": "freshness",
            "label": "Freshness",
            "score": round(freshness_score, 1),
            "tone": "success" if freshness_score >= 80 else "warning" if freshness_score >= 60 else "danger",
            "detail": f"SLA considerado: {freshness_sla_hours}h.",
        },
        {
            "key": "integrity",
            "label": "Integridade Parquet",
            "score": round(integrity_score, 1),
            "tone": "success" if integrity_score >= 80 else "warning" if integrity_score >= 60 else "danger",
            "detail": "Avalia arquivos ilegíveis, cobertura exata e presença de sinais inválidos.",
        },
        {
            "key": "metadata",
            "label": "Cobertura de metadados",
            "score": round(metadata_coverage_score, 1),
            "tone": "success" if metadata_coverage_score >= 80 else "warning" if metadata_coverage_score >= 60 else "danger",
            "detail": "Considera cobertura de amostragem e possibilidade de consolidação segura.",
        },
    ]

    return {
        "freshness_sla_hours": freshness_sla_hours,
        "freshness_age_seconds": freshness["freshness_age_seconds"],
        "freshness_age_hours": freshness["freshness_age_hours"],
        "freshness_status": freshness["freshness_status"],
        "freshness_detail": freshness["freshness_detail"],
        "quality_score": quality_score,
        "quality_breakdown": quality_breakdown,
        "quality_signals": quality_signals,
        "operational_signals": operational_signals,
        "null_columns_count": len(null_columns),
        "missing_columns_count": missing_columns,
        "unreadable_files_count": unreadable_files_count,
        "schema_variants_count": schema_variants_count,
        "drift_detected": drift_detected,
        "volume_spread": volume_spread,
        "volume_variation": volume_variation,
    }


def build_data_lake_observation_payload(
    *,
    connection: DataLakeConnection,
    inventory: DataLakeInventoryTable,
    quality_snapshot: dict[str, Any],
    row_count: int | None,
    row_count_method: str | None,
    row_count_confidence: str | None,
    size_total_bytes: int | None,
) -> dict[str, Any]:
    return {
        "connection_id": connection.id,
        "table_id": inventory.id,
        "source_kind": "detail",
        "observed_at": _now(),
        "freshness_status": quality_snapshot.get("freshness_status") or "unknown",
        "freshness_age_seconds": quality_snapshot.get("freshness_age_seconds"),
        "freshness_sla_hours": quality_snapshot.get("freshness_sla_hours"),
        "quality_score": quality_snapshot.get("quality_score"),
        "row_count": row_count,
        "row_count_method": row_count_method,
        "row_count_confidence": row_count_confidence,
        "size_total_bytes": size_total_bytes,
        "schema_variants_count": int(quality_snapshot.get("schema_variants_count") or 0),
        "null_columns_count": int(quality_snapshot.get("null_columns_count") or 0),
        "missing_columns_count": int(quality_snapshot.get("missing_columns_count") or 0),
        "unreadable_files_count": int(quality_snapshot.get("unreadable_files_count") or 0),
        "drift_detected": bool(quality_snapshot.get("drift_detected")),
        "signals_json": quality_snapshot.get("quality_signals") or [],
        "summary_json": {
            "quality_breakdown": quality_snapshot.get("quality_breakdown") or [],
            "operational_signals": quality_snapshot.get("operational_signals") or [],
            "volume_spread": quality_snapshot.get("volume_spread"),
            "volume_variation": quality_snapshot.get("volume_variation"),
        },
    }


__all__ = [
    "build_data_lake_observation_payload",
    "calculate_data_lake_table_quality",
    "resolve_data_lake_table_freshness_sla_hours",
]
