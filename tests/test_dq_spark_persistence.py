from __future__ import annotations

import os
from pathlib import Path

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.core.config import settings
from t2c_data.models.catalog import ColumnEntity, DataSource, Database, Schema, TableEntity
from t2c_data.models.dq import (
    DQColumnMetric,
    DQEvidenceSample,
    DQObservabilityBaseline,
    DQObservabilityEvent,
    DQProfileColumnMetric,
    DQProfileRun,
    DQProfileTableMetric,
    DQRuleSuggestion,
    DQRun,
    DQScoreWeightProfile,
    DQTableMetric,
)
from t2c_data.services.dq_spark import (
    _persist_profiling_output,
    _temporary_result_file,
    _validate_profiling_payload,
)


def _session_factory():
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    ).execution_options(schema_translate_map={settings.db_schema: None})
    with engine.begin() as conn:
        DataSource.__table__.create(bind=conn)
        Database.__table__.create(bind=conn)
        Schema.__table__.create(bind=conn)
        TableEntity.__table__.create(bind=conn)
        ColumnEntity.__table__.create(bind=conn)
        DQRun.__table__.create(bind=conn)
        DQTableMetric.__table__.create(bind=conn)
        DQColumnMetric.__table__.create(bind=conn)
        DQProfileRun.__table__.create(bind=conn)
        DQProfileTableMetric.__table__.create(bind=conn)
        DQProfileColumnMetric.__table__.create(bind=conn)
        DQRuleSuggestion.__table__.create(bind=conn)
        DQScoreWeightProfile.__table__.create(bind=conn)
        DQObservabilityBaseline.__table__.create(bind=conn)
        DQObservabilityEvent.__table__.create(bind=conn)
        DQEvidenceSample.__table__.create(bind=conn)
    return sessionmaker(bind=engine, future=True)


def _seed_table(session):
    datasource = DataSource(name="warehouse", db_type="postgres", host="localhost", port=5432, database="analytics", username="tester")
    datasource.password = "secret"
    session.add(datasource)
    session.flush()

    database = Database(datasource_id=datasource.id, name="analytics")
    session.add(database)
    session.flush()

    schema = Schema(database_id=database.id, name="gold")
    session.add(schema)
    session.flush()

    table = TableEntity(schema_id=schema.id, name="fato_vendas", table_type="table")
    session.add(table)
    session.flush()

    session.add_all(
        [
            ColumnEntity(table_id=table.id, name="id", data_type="integer", ordinal_position=1),
            ColumnEntity(table_id=table.id, name="valor_total", data_type="numeric", ordinal_position=2),
        ]
    )
    session.flush()
    return datasource, schema, table


def test_persist_profiling_output_stores_official_payload_in_database():
    SessionLocal = _session_factory()
    with SessionLocal() as session:
        datasource, schema, table = _seed_table(session)
        payload = {
            "row_count": 42,
            "completeness_pct_avg": 97.5,
            "dq_score": 97.5,
            "duplicates_count": 0,
            "failed_rules": 0,
            "columns": [
                {
                    "column_name": "id",
                    "data_type": "int",
                    "null_count": 0,
                    "distinct_count": 42,
                    "null_pct": 0.0,
                    "min_value": "1",
                    "max_value": "42",
                },
                {
                    "column_name": "valor_total",
                    "data_type": "decimal",
                    "null_count": 1,
                    "distinct_count": 40,
                    "null_pct": 2.38,
                    "min_value": "10.5",
                    "max_value": "999.9",
                },
            ],
        }

        dq_run = _persist_profiling_output(session, table, datasource, schema.name, payload)
        dq_run_id = dq_run.id
        table_id = table.id
        profile_payload = dq_run.profile_payload_json
        session.commit()

    assert dq_run is not None
    assert profile_payload is not None
    assert profile_payload["source_name"] == "warehouse"
    assert profile_payload["schema_name"] == "gold"
    assert profile_payload["table_name"] == "fato_vendas"
    assert profile_payload["row_count"] == 42
    assert profile_payload["column_count"] == 2
    assert profile_payload["metrics_json"]["profiling_intelligence"]["observed_score"] is not None
    assert profile_payload["metrics_json"]["profile_summary"]["row_count"] == 42
    assert profile_payload["metrics_json"]["dq_score"] == 97.5
    assert len(profile_payload["metrics_json"]["columns"]) == 2


def test_invalid_payload_raises_explicit_error():
    try:
        _validate_profiling_payload({"row_count": 10})
    except ValueError as exc:
        assert "columns" in str(exc)
    else:
        raise AssertionError("Expected explicit ValueError for invalid profiling payload")


def test_result_file_for_profiling_is_temporary_and_not_in_spark_results():
    path = _temporary_result_file(job_type="profiling", job_run_id=999)
    try:
        assert path.exists()
        assert "/spark-results/" not in str(path)
        assert Path("/data/spark-results") not in path.parents
    finally:
        path.unlink(missing_ok=True)


def test_persist_profiling_output_marks_empty_table_as_no_data():
    SessionLocal = _session_factory()
    with SessionLocal() as session:
        datasource, schema, table = _seed_table(session)
        payload = {
            "status": "no_data",
            "observation": "Tabela sem linhas no momento do perfilamento.",
            "row_count": 0,
            "completeness_pct_avg": None,
            "dq_score": None,
            "duplicates_count": 0,
            "failed_rules": 0,
            "columns": [],
        }

        dq_run = _persist_profiling_output(session, table, datasource, schema.name, payload)
        dq_run_id = dq_run.id
        profile_payload = dq_run.profile_payload_json
        session.commit()

    assert dq_run is not None
    assert profile_payload is not None
    assert profile_payload["profiling_status"] == "no_data"
    assert profile_payload["observation"] == "Tabela sem linhas no momento do perfilamento."
    assert profile_payload["metrics_json"]["assessment_state"]["score"] is None
    assert profile_payload["metrics_json"]["profile_summary"]["row_count"] == 0
