from __future__ import annotations

import base64
import hashlib
import argparse
import json
import os
from pathlib import Path

from cryptography.fernet import Fernet, InvalidToken, MultiFernet
from sqlalchemy import create_engine, text

try:  # pragma: no cover - imported lazily in environments without pyspark
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql import functions as F
except ModuleNotFoundError:  # pragma: no cover - backend tests do not install pyspark
    DataFrame = object  # type: ignore[assignment]
    SparkSession = object  # type: ignore[assignment]
    F = None  # type: ignore[assignment]

DEFAULT_SPARK_REDACTION_REGEX = r"(?i)secret|password|token|access[.]?key|api[.]?key|credential|authorization|jdbc|aws_secret_access_key|aws_session_token"
DEFAULT_DATA_SCHEMA = "t2c_data"
_SECRET_PREFIX = "enc::"


def _derive_fernet(material: str) -> Fernet:
    digest = hashlib.sha256(material.encode("utf-8")).digest()
    return Fernet(base64.urlsafe_b64encode(digest))


def _build_fernet() -> MultiFernet:
    """Mirror the backend's MultiFernet so secrets encrypted with the dedicated
    DATASOURCE_SECRET_KEY *or* with the legacy JWT-derived key both decrypt here.
    Decryption tries each key in order; the first material is the preferred one."""
    materials: list[str] = []
    for value in (os.getenv("DATASOURCE_SECRET_KEY"), os.getenv("JWT_SECRET_KEY"), "change-me"):
        candidate = (value or "").strip()
        if candidate and candidate not in materials:
            materials.append(candidate)
    return MultiFernet([_derive_fernet(material) for material in materials])


_FERNET = _build_fernet()


def base_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--datasource-id", required=True)
    parser.add_argument("--connection-file", required=False, help=argparse.SUPPRESS)
    parser.add_argument("--jdbc-url", required=False, help=argparse.SUPPRESS)
    parser.add_argument("--jdbc-user", required=False, help=argparse.SUPPRESS)
    parser.add_argument("--jdbc-password", required=False, help=argparse.SUPPRESS)
    parser.add_argument("--table-fqn", required=True, help="schema.table")
    parser.add_argument("--run-id", required=False)
    parser.add_argument("--output-json", required=True)
    return parser


def spark_redaction_regex() -> str:
    return os.getenv("SPARK_REDACTION_REGEX", DEFAULT_SPARK_REDACTION_REGEX)


def _extract_secret_value(raw_secret: str) -> str:
    secret = raw_secret.strip()
    if not secret:
        return ""
    try:
        parsed = json.loads(secret)
    except json.JSONDecodeError:
        return secret
    if isinstance(parsed, dict):
        for key in ("password", "jdbc_password", "secret"):
            value = parsed.get(key)
            if value is not None and str(value).strip():
                return str(value).strip()
    return secret


def build_spark(app_name: str) -> SparkSession:
    if F is None:
        raise RuntimeError("pyspark is required to build Spark sessions.")
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.redaction.regex", spark_redaction_regex())
        .getOrCreate()
    )


def load_connection_config(args: argparse.Namespace) -> dict[str, str]:
    if args.jdbc_url or args.jdbc_user or args.jdbc_password:
        raise RuntimeError("Passing JDBC credentials via CLI is blocked. Use datasource_id runtime lookup.")

    datasource_id = int(args.datasource_id)
    database_url = (os.getenv("DATABASE_URL") or "").strip()
    if not database_url:
        raise RuntimeError("DATABASE_URL is required for Spark DQ jobs to resolve datasource credentials.")

    engine = create_engine(database_url, future=True, pool_pre_ping=True)
    candidate_tables = (f"{DEFAULT_DATA_SCHEMA}.data_sources", "data_sources")
    row = None
    try:
        with engine.connect() as conn:
            for table_name in candidate_tables:
                try:
                    row = conn.execute(
                        text(
                            f"""
                            SELECT db_type, host, port, "database" AS database_name, username, password
                            FROM {table_name}
                            WHERE id = :datasource_id
                            """
                        ),
                        {"datasource_id": datasource_id},
                    ).mappings().first()
                except Exception:
                    row = None
                if row is not None:
                    break
    finally:
        engine.dispose()

    if row is None:
        raise RuntimeError(f"Datasource {datasource_id} was not found in the application database.")

    db_type = str(row.get("db_type") or "").strip().lower()
    if db_type and db_type != "postgres":
        raise RuntimeError(f"Unsupported datasource type for Spark DQ jobs: {db_type}")

    host = str(row.get("host") or "").strip()
    database = str(row.get("database_name") or "").strip()
    user = str(row.get("username") or "").strip()
    raw_password = str(row.get("password") or "")
    if not host or not database or not user or not raw_password:
        raise RuntimeError("Datasource credentials are incomplete.")

    if raw_password.startswith(_SECRET_PREFIX):
        token = raw_password[len(_SECRET_PREFIX) :].encode("utf-8")
        try:
            decrypted = _FERNET.decrypt(token)
            raw_password = _extract_secret_value(decrypted.decode("utf-8"))
        except InvalidToken as exc:
            raise RuntimeError("Datasource credentials could not be decrypted.") from exc
    if not raw_password.strip():
        raise RuntimeError("Datasource password is empty.")

    return {
        "jdbc_url": f"jdbc:postgresql://{host}:{int(row.get('port') or 5432)}/{database}",
        "jdbc_user": user,
        "jdbc_password": raw_password,
    }


def read_table_via_jdbc(
    spark: SparkSession,
    jdbc_url: str,
    user: str,
    password: str,
    table_fqn: str,
    where_clause: str | None = None,
    *,
    partition_column: str | None = None,
    lower_bound: str | None = None,
    upper_bound: str | None = None,
    num_partitions: int | None = None,
    fetchsize: int = 10000,
) -> DataFrame:
    if F is None:
        raise RuntimeError("pyspark is required to read tables via JDBC.")
    # When a WHERE clause is supplied (incremental/delta profiling), push it down to the
    # source via a subquery so only the delta window is read over JDBC.
    dbtable = table_fqn
    if where_clause:
        dbtable = f"(SELECT * FROM {table_fqn} WHERE {where_clause}) AS t2c_src"
    reader = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("user", user)
        .option("password", password)
        .option("dbtable", dbtable)
        .option("driver", "org.postgresql.Driver")
        .option("fetchsize", str(fetchsize))  # streaming de linhas, não tudo de uma vez
    )
    # Particionamento: paraleliza a leitura em N faixas de partition_column, evitando 1 executor
    # segurar a tabela inteira (OOM). Requer coluna numérica/data/timestamp + bounds; no profiling
    # delta usamos a coluna de watermark + a janela como bounds.
    if partition_column and lower_bound is not None and upper_bound is not None and num_partitions and int(num_partitions) > 1:
        reader = (
            reader.option("partitionColumn", partition_column)
            .option("lowerBound", str(lower_bound))
            .option("upperBound", str(upper_bound))
            .option("numPartitions", str(int(num_partitions)))
        )
    return reader.load()


def write_json_output(path: str, payload: dict) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, default=str, indent=2))


def scalar_count(df: DataFrame) -> int:
    return int(df.count())


def safe_preview(df: DataFrame, limit: int = 20) -> list[dict]:
    rows = df.limit(limit).toJSON().collect()
    return [json.loads(item) for item in rows]


def column_null_count(df: DataFrame, col_name: str) -> int:
    if F is None:
        raise RuntimeError("pyspark is required for column statistics.")
    return int(df.select(F.sum(F.when(F.col(col_name).isNull(), 1).otherwise(0)).alias("v")).collect()[0]["v"] or 0)


def column_distinct_count(df: DataFrame, col_name: str) -> int:
    if F is None:
        raise RuntimeError("pyspark is required for column statistics.")
    return int(df.select(F.countDistinct(F.col(col_name)).alias("v")).collect()[0]["v"] or 0)
