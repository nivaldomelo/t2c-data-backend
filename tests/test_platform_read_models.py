from __future__ import annotations

import os
import unittest
from dataclasses import replace

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.features.platform import read_models as platform_read_models
from t2c_data.features.platform import scheduler as platform_scheduler
from t2c_data.features.search.global_search import SearchRecord
from t2c_data.models.platform import SearchReadModel


def _build_session() -> Session:
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    with engine.begin() as conn:
        conn.exec_driver_sql("ATTACH DATABASE ':memory:' AS t2c_data")
        SearchReadModel.__table__.create(bind=conn)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, class_=Session)
    return SessionLocal()


def _record(*, title: str, description: str | None = None, entity_type: str = "table", entity_id: int = 1) -> SearchRecord:
    return SearchRecord(
        entity_type=entity_type,
        entity_id=entity_id,
        title=title,
        subtitle="catalogo",
        description=description,
        context_path="Fontes > banco > schema > tabela",
        target_url=f"/explorer?tableId={entity_id}",
        searchable_name=[title],
        searchable_aliases=[],
        searchable_synonyms=[],
        searchable_descriptions=[description or ""],
        searchable_context=["catalogo", "banco", "schema", "tabela"],
        metadata={"table_id": entity_id},
    )


class PlatformReadModelRefreshTests(unittest.TestCase):
    def setUp(self) -> None:
        self.session = _build_session()
        self.addCleanup(self.session.close)

    def _patch_records(self, records: list[SearchRecord]) -> None:
        self._original_loader = platform_read_models.load_search_records_live
        platform_read_models.load_search_records_live = lambda *args, **kwargs: records  # type: ignore[assignment]
        self.addCleanup(self._restore_loader)

    def _restore_loader(self) -> None:
        platform_read_models.load_search_records_live = self._original_loader  # type: ignore[assignment]

    def test_refresh_search_read_model_handles_empty_table(self) -> None:
        self._patch_records([])

        summary = platform_read_models.refresh_search_read_model(self.session, mode="full")

        assert summary["entries"] == 0
        assert summary["inserted"] == 0
        assert summary["updated"] == 0
        assert summary["removed"] == 0
        assert summary["duplicates_detected"] == 0
        assert self.session.query(SearchReadModel).count() == 0

    def test_refresh_search_read_model_is_idempotent_on_repeated_runs(self) -> None:
        self._patch_records([_record(title="clientes", description="Base de clientes")])

        first = platform_read_models.refresh_search_read_model(self.session, mode="full")
        second = platform_read_models.refresh_search_read_model(self.session, mode="full")
        row = self.session.query(SearchReadModel).one()

        assert first["entries"] == 1
        assert second["entries"] == 1
        assert second["updated"] == 1
        assert second["inserted"] == 0
        assert second["removed"] == 1
        assert row.title == "clientes"
        assert row.description == "Base de clientes"

    def test_refresh_search_read_model_updates_changed_metadata(self) -> None:
        initial = [_record(title="clientes", description="Base de clientes")]
        updated = [_record(title="clientes_v2", description="Base de clientes atualizada")]

        self._patch_records(initial)
        platform_read_models.refresh_search_read_model(self.session, mode="full")

        self._patch_records(updated)
        summary = platform_read_models.refresh_search_read_model(self.session, mode="full")
        row = self.session.query(SearchReadModel).one()

        assert summary["updated"] == 1
        assert summary["inserted"] == 0
        assert summary["removed"] == 1
        assert row.title == "clientes_v2"
        assert row.description == "Base de clientes atualizada"

    def test_refresh_search_read_model_deduplicates_batch_by_entity_key(self) -> None:
        first = _record(title="clientes", description="Versão antiga")
        second = replace(first, title="clientes_normalizado", description="Versão vencedora")
        self._patch_records([first, second])

        summary = platform_read_models.refresh_search_read_model(self.session, mode="full")
        row = self.session.query(SearchReadModel).one()

        assert summary["entries"] == 1
        assert summary["duplicates_detected"] == 1
        assert row.title == "clientes_normalizado"
        assert row.description == "Versão vencedora"

    def test_refresh_search_read_model_skips_when_lock_is_unavailable(self) -> None:
        original_acquire = platform_read_models._acquire_search_refresh_lock
        try:
            platform_read_models._acquire_search_refresh_lock = lambda session: False  # type: ignore[assignment]
            self._patch_records([_record(title="clientes")])

            summary = platform_read_models.refresh_search_read_model(self.session, mode="full")
        finally:
            platform_read_models._acquire_search_refresh_lock = original_acquire  # type: ignore[assignment]

        assert summary["skipped"] == "lock_unavailable"
        assert self.session.query(SearchReadModel).count() == 0

    def test_refresh_platform_read_models_returns_elapsed_metrics(self) -> None:
        self._patch_records([_record(title="clientes", description="Base de clientes")])
        original_dashboard_refresh = platform_read_models.refresh_dashboard_asset_read_model
        try:
            platform_read_models.refresh_dashboard_asset_read_model = lambda session, mode="full": {  # type: ignore[assignment]
                "entries": 0,
                "refreshed_at": "2026-05-23T00:00:00+00:00",
                "mode": mode,
                "updated_tables": 0,
                "elapsed_ms": 0.5,
            }
            summary = platform_read_models.refresh_platform_read_models(self.session, mode="full")
        finally:
            platform_read_models.refresh_dashboard_asset_read_model = original_dashboard_refresh  # type: ignore[assignment]

        assert "elapsed_ms" in summary
        assert summary["dashboard"]["elapsed_ms"] == 0.5
        assert summary["search"]["entries"] == 1


class PlatformMaintenanceSchedulerTests(unittest.TestCase):
    def test_platform_maintenance_cycle_skips_when_process_lock_is_busy(self) -> None:
        original_acquire = platform_scheduler._acquire_maintenance_refresh_lock
        original_enqueue = platform_scheduler.enqueue_platform_maintenance_job
        try:
            platform_scheduler._acquire_maintenance_refresh_lock = lambda: False  # type: ignore[assignment]
            platform_scheduler.enqueue_platform_maintenance_job = lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("enqueue should not run"))  # type: ignore[assignment]

            summary = platform_scheduler.run_platform_maintenance_cycle(trigger="startup", scheduler_mode="embedded")
        finally:
            platform_scheduler._acquire_maintenance_refresh_lock = original_acquire  # type: ignore[assignment]
            platform_scheduler.enqueue_platform_maintenance_job = original_enqueue  # type: ignore[assignment]

        assert summary["skipped"] == "maintenance_already_running"
        assert summary["trigger"] == "startup"


if __name__ == "__main__":
    unittest.main()
