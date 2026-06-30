from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from t2c_data.api.dq import dq_observability_history_by_table_id
from t2c_data.core.config import settings
from t2c_data.models.catalog import ColumnEntity, DataSource, Database, Schema, TableEntity
from t2c_data.models.dq import DQEvidenceSample, DQObservabilityBaseline, DQObservabilityEvent, DQRun, DQTableMetric
from t2c_data.features.data_quality.observability_store import (
    load_filtered_observability_artifacts,
    load_persisted_observability_artifacts,
    mask_evidence_rows,
    purge_persisted_observability_artifacts,
    persist_evidence_sample,
    persist_observability_artifacts,
)


def _session_factory():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True).execution_options(
        schema_translate_map={settings.db_schema: None}
    )
    with engine.begin() as conn:
        DataSource.__table__.create(bind=conn)
        Database.__table__.create(bind=conn)
        Schema.__table__.create(bind=conn)
        TableEntity.__table__.create(bind=conn)
        ColumnEntity.__table__.create(bind=conn)
        DQRun.__table__.create(bind=conn)
        DQTableMetric.__table__.create(bind=conn)
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

    table = TableEntity(
        schema_id=schema.id,
        name="clientes",
        table_type="table",
        has_sensitive_personal_data=True,
        sensitivity_level="restricted",
    )
    session.add(table)
    session.flush()

    session.add_all(
        [
            ColumnEntity(table_id=table.id, name="nome", data_type="text", ordinal_position=1),
            ColumnEntity(table_id=table.id, name="email", data_type="text", ordinal_position=2),
            ColumnEntity(table_id=table.id, name="valor_total", data_type="numeric", ordinal_position=3),
        ]
    )
    session.flush()
    return datasource, schema, table


def test_mask_evidence_rows_redacts_sensitive_values():
    SessionLocal = _session_factory()
    with SessionLocal() as session:
        _datasource, _schema, table = _seed_table(session)

    masked_rows, masked_fields = mask_evidence_rows(
        [{"nome": "Maria Silva", "email": "maria@example.com", "valor_total": 100.0}],
        table=table,
    )

    assert masked_fields == ["email", "nome", "valor_total"]
    assert masked_rows[0]["nome"]["redacted"] is True
    assert masked_rows[0]["email"]["visibility"] == "masked"
    assert masked_rows[0]["valor_total"]["redacted"] is True


def test_persisted_observability_history_and_evidence_are_readable():
    SessionLocal = _session_factory()
    with SessionLocal() as session:
        _datasource, _schema, table = _seed_table(session)
        previous_run = DQRun(table_id=table.id, datasource_id=1, status="success", execution_engine="spark")
        session.add(previous_run)
        session.flush()
        session.add(
            DQTableMetric(
                run_id=previous_run.id,
                table_id=table.id,
                row_count=100,
                column_count=3,
                completeness_pct_avg=98.0,
                dq_score=95.0,
                duplicates_count=0,
                failed_rules=0,
            )
        )
        session.flush()
        run = DQRun(table_id=table.id, datasource_id=1, status="success", execution_engine="spark")
        session.add(run)
        session.flush()
        metric = DQTableMetric(
            run_id=run.id,
            table_id=table.id,
            row_count=10,
            column_count=3,
            completeness_pct_avg=85.0,
            dq_score=78.0,
            duplicates_count=8,
            failed_rules=1,
        )
        session.add(metric)
        session.flush()

        persisted = persist_observability_artifacts(session, dq_run=run, table=table, table_metric=metric)
        persist_evidence_sample(
            session,
            dq_run=run,
            table=table,
            sample_rows=[{"nome": "Maria Silva", "email": "maria@example.com", "valor_total": 100.0}],
            evidence_type="rule_violation",
            rule_run_id=77,
            rule_id=9,
            affected_rows_count=1,
            details_json={"source": "test"},
        )
        session.commit()

        loaded = load_persisted_observability_artifacts(session, table_id=table.id, limit=10)

    assert persisted["baselines"]
    assert any(event["event_type"] in {"anomaly", "drift"} for event in persisted["events"])
    assert loaded["baselines"]
    assert loaded["evidence_samples"]
    sample = loaded["evidence_samples"][0]
    assert sample["sample_rows_json"][0]["nome"]["redacted"] is True
    assert sample["rule_run_id"] == 77


def test_filtered_history_returns_only_requested_artifacts():
    SessionLocal = _session_factory()
    with SessionLocal() as session:
        _datasource, _schema, table = _seed_table(session)
        run = DQRun(table_id=table.id, datasource_id=1, status="success", execution_engine="spark")
        session.add(run)
        session.flush()
        metric = DQTableMetric(
            run_id=run.id,
            table_id=table.id,
            row_count=10,
            column_count=3,
            completeness_pct_avg=85.0,
            dq_score=78.0,
            duplicates_count=8,
            failed_rules=1,
        )
        session.add(metric)
        session.flush()
        persist_observability_artifacts(session, dq_run=run, table=table, table_metric=metric)
        session.add(
            DQObservabilityEvent(
                run_id=run.id,
                table_id=table.id,
                metric_key="volume",
                event_type="anomaly",
                status="open",
                severity="warning",
                observed_value=10.0,
                expected_value=100.0,
                baseline_value=100.0,
                delta_value=-90.0,
                delta_pct=-90.0,
                detected_at=datetime.now(timezone.utc),
            )
        )
        persist_evidence_sample(
            session,
            dq_run=run,
            table=table,
            sample_rows=[{"nome": "Maria Silva", "email": "maria@example.com"}],
            evidence_type="rule_violation",
            rule_run_id=88,
            rule_id=12,
        )
        session.commit()

        loaded = load_filtered_observability_artifacts(
            session,
            table_id=table.id,
            artifact_type="event",
            severity="warning",
            limit=5,
        )

    assert loaded["baselines"] == []
    assert loaded["events"]
    assert all(event["severity"] == "warning" for event in loaded["events"])
    assert loaded["evidence_samples"] == []


def test_observability_history_route_supports_filters():
    SessionLocal = _session_factory()
    with SessionLocal() as session:
        _datasource, _schema, table = _seed_table(session)
        run = DQRun(table_id=table.id, datasource_id=1, status="success", execution_engine="spark")
        session.add(run)
        session.flush()
        metric = DQTableMetric(
            run_id=run.id,
            table_id=table.id,
            row_count=12,
            column_count=3,
            completeness_pct_avg=88.0,
            dq_score=82.0,
            duplicates_count=1,
            failed_rules=0,
        )
        session.add(metric)
        session.flush()
        persist_observability_artifacts(session, dq_run=run, table=table, table_metric=metric)
        session.add(
            DQObservabilityEvent(
                run_id=run.id,
                table_id=table.id,
                metric_key="volume",
                event_type="anomaly",
                status="open",
                severity="warning",
                observed_value=12.0,
                expected_value=100.0,
                baseline_value=100.0,
                delta_value=-88.0,
                delta_pct=-88.0,
                detected_at=datetime.now(timezone.utc),
            )
        )
        session.commit()

        response = dq_observability_history_by_table_id(
            table_id=table.id,
            artifact_type="event",
            limit=5,
            metric_key=None,
            column_name=None,
            dimension_key=None,
            event_type=None,
            severity="warning",
            evidence_type=None,
            origin=None,
            status=None,
            dq_run_id=None,
            rule_run_id=None,
            rule_id=None,
            db=session,
            current_user=object(),
        )

    assert response.table_id == table.id
    assert response.filters.artifact_type == "event"
    assert response.events
    assert response.evidence_samples == []
    assert all(event["severity"] == "warning" for event in response.events)


def test_observability_retention_purges_old_artifacts():
    SessionLocal = _session_factory()
    old_created_at = datetime.now(timezone.utc) - timedelta(days=400)
    fresh_created_at = datetime.now(timezone.utc) - timedelta(days=5)
    with SessionLocal() as session:
        _datasource, _schema, table = _seed_table(session)

        old_run = DQRun(table_id=table.id, datasource_id=1, status="success", execution_engine="spark", created_at=old_created_at)
        fresh_run = DQRun(table_id=table.id, datasource_id=1, status="success", execution_engine="spark", created_at=fresh_created_at)
        session.add_all([old_run, fresh_run])
        session.flush()

        old_metric = DQTableMetric(
            run_id=old_run.id,
            table_id=table.id,
            row_count=20,
            column_count=3,
            completeness_pct_avg=80.0,
            dq_score=70.0,
            duplicates_count=2,
            failed_rules=1,
            created_at=old_created_at,
        )
        fresh_metric = DQTableMetric(
            run_id=fresh_run.id,
            table_id=table.id,
            row_count=22,
            column_count=3,
            completeness_pct_avg=82.0,
            dq_score=74.0,
            duplicates_count=1,
            failed_rules=0,
            created_at=fresh_created_at,
        )
        session.add_all([old_metric, fresh_metric])
        session.flush()

        session.add(
            DQObservabilityBaseline(
                run_id=old_run.id,
                table_id=table.id,
                metric_key="volume",
                metric_scope="table",
                current_value=20.0,
                baseline_value=25.0,
                window_size=14,
                calculated_at=old_created_at,
                created_at=old_created_at,
            )
        )
        session.add(
            DQObservabilityBaseline(
                run_id=fresh_run.id,
                table_id=table.id,
                metric_key="volume",
                metric_scope="table",
                current_value=22.0,
                baseline_value=25.0,
                window_size=14,
                calculated_at=fresh_created_at,
                created_at=fresh_created_at,
            )
        )
        session.add(
            DQObservabilityEvent(
                run_id=old_run.id,
                table_id=table.id,
                metric_key="volume",
                event_type="anomaly",
                status="open",
                severity="warning",
                detected_at=old_created_at,
                created_at=old_created_at,
            )
        )
        session.add(
            DQObservabilityEvent(
                run_id=fresh_run.id,
                table_id=table.id,
                metric_key="volume",
                event_type="anomaly",
                status="open",
                severity="warning",
                detected_at=fresh_created_at,
                created_at=fresh_created_at,
            )
        )
        session.add(
            DQEvidenceSample(
                dq_run_id=old_run.id,
                table_id=table.id,
                evidence_type="rule_violation",
                origin="dq_rule",
                status="masked",
                sample_size=1,
                created_at=old_created_at,
            )
        )
        session.add(
            DQEvidenceSample(
                dq_run_id=fresh_run.id,
                table_id=table.id,
                evidence_type="rule_violation",
                origin="dq_rule",
                status="masked",
                sample_size=1,
                created_at=fresh_created_at,
            )
        )
        session.commit()

        deleted = purge_persisted_observability_artifacts(session)
        remaining_baselines = session.scalars(select(DQObservabilityBaseline).where(DQObservabilityBaseline.table_id == table.id)).all()
        remaining_events = session.scalars(select(DQObservabilityEvent).where(DQObservabilityEvent.table_id == table.id)).all()
        remaining_evidence = session.scalars(select(DQEvidenceSample).where(DQEvidenceSample.table_id == table.id)).all()

    assert deleted["dq_observability_baselines_deleted"] >= 1
    assert deleted["dq_observability_events_deleted"] >= 1
    assert deleted["dq_evidence_samples_deleted"] >= 1
    assert len(remaining_baselines) == 1
    assert len(remaining_events) == 1
    assert len(remaining_evidence) == 1
