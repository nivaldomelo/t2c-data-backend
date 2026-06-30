from __future__ import annotations

import os
from datetime import datetime, timezone
from types import SimpleNamespace

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.core.config import settings
from t2c_data.features.data_observability import service as observability_service
from t2c_data.features.dashboard.support import TableProfile
from t2c_data.models.catalog import DataSource, Database, Schema, TableEntity


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
    return sessionmaker(bind=engine, future=True)


def _seed_catalog(session):
    ds_main = DataSource(name="warehouse_main", db_type="postgres", host="localhost", port=5432, database="analytics", username="tester")
    ds_aux = DataSource(name="warehouse_aux", db_type="postgres", host="localhost", port=5432, database="analytics_aux", username="tester")
    session.add_all([ds_main, ds_aux])
    session.flush()

    db_main = Database(datasource_id=ds_main.id, name="analytics")
    db_aux = Database(datasource_id=ds_aux.id, name="analytics_aux")
    session.add_all([db_main, db_aux])
    session.flush()

    schema_main = Schema(database_id=db_main.id, name="public")
    schema_aux = Schema(database_id=db_aux.id, name="public")
    session.add_all([schema_main, schema_aux])
    session.flush()

    table_main = TableEntity(schema_id=schema_main.id, name="clientes", table_type="table")
    table_aux = TableEntity(schema_id=schema_aux.id, name="clientes", table_type="table")
    session.add_all([table_main, table_aux])
    session.flush()
    return ds_main, ds_aux, schema_main, schema_aux, table_main, table_aux


def _profile(*, table_id: int, datasource_id: int, database_id: int, schema_id: int, table_name: str, datasource_name: str) -> TableProfile:
    return TableProfile(
        table_id=table_id,
        datasource_id=datasource_id,
        database_id=database_id,
        schema_id=schema_id,
        table_name=table_name,
        table_type="table",
        schema_name="public",
        database_name="analytics",
        datasource_name=datasource_name,
        engine="postgres",
        owner_defined=True,
        description_complete=True,
        dictionary_complete=True,
        classification_defined=True,
        tags_count=1,
        terms_count=1,
        total_columns=3,
        documented_columns=3,
        certification_status="certified",
        certification_criticality="high",
        certification_badges=["official"],
        certification_decided_at=datetime(2026, 5, 26, tzinfo=timezone.utc),
        certification_review_at=datetime(2026, 5, 26, tzinfo=timezone.utc),
        certification_expires_at=None,
        review_recent=True,
        dq_score=91.0,
        completeness_pct_avg=98.0,
        freshness_seconds=1800,
        open_incidents=0,
        critical_open_incidents=0,
        owner_name="Data Owner",
        data_owner_id=1,
        data_owner_is_active=True,
        domain_name="Finanças",
        sensitivity_level="official_use",
        has_personal_data=False,
        has_sensitive_personal_data=False,
        owner_reviewed_at=datetime(2026, 5, 26, tzinfo=timezone.utc),
        privacy_reviewed_at=datetime(2026, 5, 26, tzinfo=timezone.utc),
        last_review_at=datetime(2026, 5, 26, tzinfo=timezone.utc),
        last_sync_at=datetime(2026, 5, 26, tzinfo=timezone.utc),
        last_updated_at=datetime(2026, 5, 26, tzinfo=timezone.utc),
        search_clicks_30d=0,
        active_dq_rules_count=2,
        recent_dq_failure_runs_30d=0,
        sla_defined=True,
        sla_hours=24,
        trust_score=88,
        trust_label="Confiável",
        trust_tone="success",
    )


class _LatestMetricsResponse:
    def __init__(self, table_id: int) -> None:
        self.table_id = table_id

    def model_dump(self):
        return {
            "current": {
                "row_count": 1000 if self.table_id == 1 else 2000,
                "dq_score": 92.0,
                "run_at": "2026-05-26T10:00:00Z",
                "failed_rules": 0,
            },
            "previous": {
                "row_count": 980 if self.table_id == 1 else 1980,
                "dq_score": 90.0,
            },
            "history": [
                {"run_at": "2026-05-25T10:00:00Z", "row_count": 980 if self.table_id == 1 else 1980},
                {"run_at": "2026-05-26T10:00:00Z", "row_count": 1000 if self.table_id == 1 else 2000},
            ],
        }


def _patch_real_signals(monkeypatch, profiles: list[TableProfile], *, metabase_available: bool = False):
    monkeypatch.setattr(
        observability_service,
        "load_dashboard_profiles_with_fallback",
        lambda session, now, table_ids=None, current_user=None: (profiles, "read_model"),
    )
    monkeypatch.setattr(
        observability_service,
        "get_latest_metrics_by_table_id",
        lambda db, table_id, history_runs, current_user: _LatestMetricsResponse(table_id),
    )
    monkeypatch.setattr(
        observability_service,
        "load_filtered_observability_artifacts",
        lambda session, table_id, limit, artifact_type: {"baselines": [], "events": [], "evidence_samples": []},
    )
    monkeypatch.setattr(
        observability_service,
        "load_table_ingestion_summary_from_source",
        lambda session, schema_name, table_name: {"linked": False, "state": "not_linked"},
    )
    monkeypatch.setattr(
        observability_service,
        "load_table_ingestion_detail_from_source",
        lambda session, schema_name, table_name, page, page_size: {"summary": {}, "executions": {"items": [], "total": 0}},
    )
    monkeypatch.setattr(
        observability_service,
        "get_table_metabase_consumption",
        lambda session, table_id: SimpleNamespace(model_dump=lambda: {"available": metabase_available, "dashboards_count": 1 if metabase_available else 0}),
    )


def test_observability_overview_scopes_by_datasource_id_and_returns_only_real_assets(monkeypatch):
    SessionLocal = _session_factory()
    with SessionLocal() as session:
        ds_main, ds_aux, schema_main, schema_aux, table_main, table_aux = _seed_catalog(session)
        profiles = [
            _profile(
                table_id=table_main.id,
                datasource_id=ds_main.id,
                database_id=schema_main.database_id,
                schema_id=schema_main.id,
                table_name="clientes",
                datasource_name=ds_main.name,
            ),
            _profile(
                table_id=table_aux.id,
                datasource_id=ds_aux.id,
                database_id=schema_aux.database_id,
                schema_id=schema_aux.id,
                table_name="clientes",
                datasource_name=ds_aux.name,
            ),
        ]
        _patch_real_signals(monkeypatch, profiles)

        result = observability_service.build_observability_overview(
            session,
            datasource_id=ds_main.id,
            current_user=None,
            schema_name=None,
            table_name=None,
            page=1,
            page_size=10,
        )

    assert result.context.datasource_id == ds_main.id
    assert result.context.datasource_name == ds_main.name
    assert result.total == 1
    assert len(result.items) == 1
    assert result.items[0].table_id == table_main.id
    assert result.items[0].table_name == "clientes"
    assert result.items[0].datasource_id == ds_main.id
    assert result.items[0].is_demo is False
    assert result.items[0].source_origin in {"catalog", "datasource_scan"}
    assert result.summary.total == 1
    assert result.summary.healthy == 1
    assert result.filter_options.domains == ["Finanças"]
    assert result.filter_options.layers == ["public"]
    assert all(not item.table_name.startswith("dw_") for item in result.items)
    assert result.related_signals.metabase == []
    assert result.diagnostics.unlinked_signals >= 1
    assert result.unlinked_signals
    assert result.unlinked_signals[0].context_state == "unlinked"


def test_observability_detail_returns_real_payload_without_demo_flag(monkeypatch):
    SessionLocal = _session_factory()
    with SessionLocal() as session:
        ds_main, _ds_aux, schema_main, _schema_aux, table_main, _table_aux = _seed_catalog(session)
        profiles = [
            _profile(
                table_id=table_main.id,
                datasource_id=ds_main.id,
                database_id=schema_main.database_id,
                schema_id=schema_main.id,
                table_name="clientes",
                datasource_name=ds_main.name,
            )
        ]
        _patch_real_signals(monkeypatch, profiles, metabase_available=True)
        monkeypatch.setattr(
            observability_service,
            "load_table_operational_context",
            lambda session, table_id, datasource_id, database_id, schema_id: {"table_id": table_id, "datasource_id": datasource_id},
        )

        detail = observability_service.build_observability_asset_detail(
            session,
            table_id=table_main.id,
            current_user=None,
        )

    assert detail.table_id == table_main.id
    assert detail.table_name == "clientes"
    assert detail.datasource_id == ds_main.id
    assert detail.is_demo is False
    assert detail.source_origin in {"catalog", "datasource_scan"}
    assert detail.linked_by == "table_id"
    assert detail.linked_confidence == 100
    assert detail.confidence == 100
    assert detail.dq_latest is not None
    assert detail.metabase_consumption is not None
    assert detail.metabase_consumption["available"] is True
    assert detail.operational_context == {"table_id": table_main.id, "datasource_id": ds_main.id}
