from __future__ import annotations

import sys
import json
import os
import stat
import base64
import hashlib
import tarfile
from pathlib import Path
from types import SimpleNamespace

from cryptography.fernet import Fernet
from sqlalchemy import create_engine

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from t2c_data.core.redaction import format_command_for_log, redact_command_args, redact_sensitive_string
from t2c_data.features.data_quality.guardrails import sanitize_execution_error
from t2c_data.features.data_quality.spark_worker_support import (
    build_connection_reference_args,
    sanitize_process_output,
    temporary_connection_file,
)
from t2c_data.integrations.spark import SparkSubmitConfig, SparkSubmitRunner, get_spark_submit_config

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "spark-jobs"))

import dq_common  # noqa: E402


def test_redact_command_args_masks_plain_and_embedded_secrets() -> None:
    args = [
        "spark-submit",
        "--jdbc-password",
        "super-secret",
        "--jdbc-url",
        "jdbc:postgresql://user:secret@db.local/catalog",
        "--conf",
        "spark.datasource.password=another-secret",
    ]

    redacted = redact_command_args(args)
    rendered = format_command_for_log(args)

    assert "super-secret" not in redacted
    assert "another-secret" not in rendered
    assert "secret@db.local" not in rendered
    assert "********" in rendered


def test_build_connection_reference_args_uses_datasource_id_only() -> None:
    args = build_connection_reference_args(datasource_id=123)

    assert args == ["--datasource-id", "123"]
    assert "--connection-file" not in args
    assert "--jdbc-password" not in args
    assert "--jdbc-user" not in args
    assert "--jdbc-url" not in args


def test_temporary_connection_file_is_private_and_contains_only_reference_data(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("TMPDIR", str(tmp_path))
    file_path = temporary_connection_file(
        job_type="profiling",
        job_run_id=42,
        datasource_id=7,
    )
    try:
        payload = json.loads(file_path.read_text())
        file_mode = stat.S_IMODE(file_path.stat().st_mode)

        assert payload["datasource_id"] == 7
        assert "jdbc_url" not in payload
        assert "jdbc_user" not in payload
        assert "jdbc_password" not in payload
        assert file_mode == 0o600
    finally:
        file_path.unlink(missing_ok=True)


def test_load_connection_config_resolves_datasource_from_database(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "datasource.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite+pysqlite:///{db_path}")
    monkeypatch.delenv("DATASOURCE_SECRET_KEY", raising=False)
    monkeypatch.delenv("JWT_SECRET_KEY", raising=False)

    engine = create_engine(f"sqlite+pysqlite:///{db_path}", future=True)
    with engine.begin() as conn:
        conn.exec_driver_sql(
            """
            CREATE TABLE data_sources (
                id INTEGER PRIMARY KEY,
                db_type TEXT NOT NULL,
                host TEXT NOT NULL,
                port INTEGER NOT NULL,
                database TEXT NOT NULL,
                username TEXT NOT NULL,
                password TEXT NOT NULL
            )
            """
        )
        conn.exec_driver_sql(
            """
            INSERT INTO data_sources (id, db_type, host, port, database, username, password)
            VALUES (:id, :db_type, :host, :port, :database, :username, :password)
            """,
            {
                "id": 7,
                "db_type": "postgres",
                "host": "db.local",
                "port": 5432,
                "database": "catalog",
                "username": "dq_user",
                "password": "dq_secret",
            },
        )

    args = SimpleNamespace(
        datasource_id=7,
        connection_file=None,
        jdbc_url=None,
        jdbc_user=None,
        jdbc_password=None,
    )

    connection = dq_common.load_connection_config(args)

    assert connection == {
        "jdbc_url": "jdbc:postgresql://db.local:5432/catalog",
        "jdbc_user": "dq_user",
        "jdbc_password": "dq_secret",
    }


def test_load_connection_config_extracts_password_from_encrypted_secret_payload(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "datasource_encrypted.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite+pysqlite:///{db_path}")
    monkeypatch.delenv("DATASOURCE_SECRET_KEY", raising=False)
    monkeypatch.delenv("JWT_SECRET_KEY", raising=False)

    digest = hashlib.sha256(b"change-me").digest()
    fernet = Fernet(base64.urlsafe_b64encode(digest))
    encrypted_secret = "enc::" + fernet.encrypt(b'{"password":"dq_secret"}').decode("utf-8")

    engine = create_engine(f"sqlite+pysqlite:///{db_path}", future=True)
    with engine.begin() as conn:
        conn.exec_driver_sql(
            """
            CREATE TABLE data_sources (
                id INTEGER PRIMARY KEY,
                db_type TEXT NOT NULL,
                host TEXT NOT NULL,
                port INTEGER NOT NULL,
                database TEXT NOT NULL,
                username TEXT NOT NULL,
                password TEXT NOT NULL
            )
            """
        )
        conn.exec_driver_sql(
            """
            INSERT INTO data_sources (id, db_type, host, port, database, username, password)
            VALUES (:id, :db_type, :host, :port, :database, :username, :password)
            """,
            {
                "id": 8,
                "db_type": "postgres",
                "host": "db.local",
                "port": 5432,
                "database": "catalog",
                "username": "dq_user",
                "password": encrypted_secret,
            },
        )

    args = SimpleNamespace(
        datasource_id=8,
        connection_file=None,
        jdbc_url=None,
        jdbc_user=None,
        jdbc_password=None,
    )

    connection = dq_common.load_connection_config(args)

    assert connection == {
        "jdbc_url": "jdbc:postgresql://db.local:5432/catalog",
        "jdbc_user": "dq_user",
        "jdbc_password": "dq_secret",
    }


def test_spark_submit_runner_adds_redaction_regex_without_secret_conf() -> None:
    config = SparkSubmitConfig(
        submit_bin="spark-submit",
        master_url="spark://spark-master:7077",
        jobs_dir="/opt/spark/jobs",
        local_jars_dir="/opt/spark/jars",
        local_jars_cache_dir="/tmp/spark-local-jars",
        packages="org.postgresql:postgresql:42.7.4",
        packages_enabled=True,
        results_dir="/tmp/spark-results",
        driver_host="backend",
        driver_bind_address="0.0.0.0",
        driver_memory="1g",
        executor_memory="1g",
        redaction_regex="(?i)secret|password|token",
        timeout_seconds=900,
    )
    runner = SparkSubmitRunner(config)

    command = runner.build_command(
        "dq_profiling_job.py",
        ["--datasource-id", "7", "--connection-file", "/tmp/conn.json", "--table-fqn", "gold.orders"],
    )

    assert "--conf" in command
    assert "spark.redaction.regex=(?i)secret|password|token" in command
    assert "--jdbc-password" not in command
    assert "/tmp/conn.json" in command


def test_spark_submit_runner_skips_packages_when_disabled() -> None:
    config = SparkSubmitConfig(
        submit_bin="spark-submit",
        master_url="spark://spark-master:7077",
        jobs_dir="/opt/spark/jobs",
        local_jars_dir="/opt/spark/jars",
        local_jars_cache_dir="/tmp/spark-local-jars",
        packages="org.postgresql:postgresql:42.7.4",
        packages_enabled=False,
        results_dir="/tmp/spark-results",
        driver_host="backend",
        driver_bind_address="0.0.0.0",
        driver_memory="1g",
        executor_memory="1g",
        redaction_regex="(?i)secret|password|token",
        timeout_seconds=900,
    )
    runner = SparkSubmitRunner(config)

    command = runner.build_command(
        "dq_profiling_job.py",
        ["--datasource-id", "7", "--table-fqn", "gold.orders", "--output-json", "/tmp/result.json"],
    )

    assert "--packages" not in command


def test_spark_submit_runner_prefers_local_jars_and_extracts_mysql_archive(tmp_path: Path) -> None:
    local_jars_dir = tmp_path / "jars"
    local_jars_dir.mkdir(parents=True)
    cache_dir = tmp_path / "cache"

    postgres_jar = local_jars_dir / "postgresql-42.7.10.jar"
    postgres_jar.write_bytes(b"postgres-jdbc-jar")

    mysql_source_dir = tmp_path / "mysql-connector-j-9.7.0"
    mysql_source_dir.mkdir()
    mysql_jar = mysql_source_dir / "mysql-connector-j-9.7.0.jar"
    mysql_jar.write_bytes(b"mysql-jdbc-jar")

    archive_path = local_jars_dir / "mysql-connector-j-9.7.0.tar.gz"
    with tarfile.open(archive_path, "w:gz") as archive:
        archive.add(mysql_jar, arcname="mysql-connector-j-9.7.0/mysql-connector-j-9.7.0.jar")

    config = SparkSubmitConfig(
        submit_bin="spark-submit",
        master_url="spark://spark-master:7077",
        jobs_dir="/opt/spark/jobs",
        local_jars_dir=str(local_jars_dir),
        local_jars_cache_dir=str(cache_dir),
        packages="org.postgresql:postgresql:42.7.4",
        packages_enabled=True,
        results_dir="/tmp/spark-results",
        driver_host="backend",
        driver_bind_address="0.0.0.0",
        driver_memory="1g",
        executor_memory="1g",
        redaction_regex="(?i)secret|password|token",
        timeout_seconds=900,
    )
    runner = SparkSubmitRunner(config)

    command = runner.build_command(
        "dq_profiling_job.py",
        ["--datasource-id", "7", "--connection-file", "/tmp/conn.json", "--table-fqn", "gold.orders"],
    )

    assert "--jars" in command
    jars_value = command[command.index("--jars") + 1]
    assert "postgresql-42.7.10.jar" in jars_value
    assert "mysql-connector-j-9.7.0.jar" in jars_value
    assert "--packages" not in command


def test_sanitize_process_output_redacts_secrets() -> None:
    output = sanitize_process_output(
        'password=plain-text jdbc:postgresql://user:secret@db.local/catalog Authorization: Bearer token-123'
    )

    assert "plain-text" not in output
    assert "secret@db.local" not in output
    assert "token-123" not in output
    assert "********" in output


def test_redact_sensitive_string_masks_json_payloads() -> None:
    payload = '{"jdbc_password":"secret","api_key":"abc","url":"jdbc:postgresql://user:pass@localhost/db"}'
    redacted = redact_sensitive_string(payload)

    assert "secret" not in redacted
    assert "abc" not in redacted
    assert "pass@localhost" not in redacted


def test_redact_sensitive_string_masks_jdbc_and_aws_secrets() -> None:
    payload = "jdbc:postgresql://user:super-secret@db.local/catalog aws_secret_access_key=SECRET_TEST"
    redacted = redact_sensitive_string(payload)

    assert "super-secret" not in redacted
    assert "SECRET_TEST" not in redacted
    assert redacted.count("********") >= 2


def test_default_spark_redaction_regex_covers_jdbc_and_aws_secret_access_key(monkeypatch) -> None:
    monkeypatch.delenv("SPARK_REDACTION_REGEX", raising=False)

    config = get_spark_submit_config()

    assert "aws_secret_access_key" in config.redaction_regex
    assert "aws_session_token" in config.redaction_regex
    assert "jdbc" in config.redaction_regex.lower()


def test_sanitize_execution_error_redacts_connection_strings() -> None:
    error = RuntimeError("jdbc:postgresql://user:super-secret@db.local/catalog")

    assert sanitize_execution_error(error, default_message="fallback") == "fallback"
