from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from types import SimpleNamespace

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy import create_engine, event, select
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.api.data_owners import delete_data_owner, get_data_owner_reassign_preview
from t2c_data.features.governance import reassign_ownership_assets
from t2c_data.features.governance import owners_summary as owners_summary_module
from t2c_data.features.governance.owners_summary import get_ownership_summary
from t2c_data.main import app
from t2c_data.models import Base
from t2c_data.models.audit import AuditLog
from t2c_data.models.catalog import DataOwner, DataSource, Database, Schema, TableEntity
from t2c_data.schemas.data_owner import OwnershipReassignRequestIn, OwnershipUnownedAssetOut


if not hasattr(SQLiteTypeCompiler, "visit_INET"):
    SQLiteTypeCompiler.visit_INET = lambda self, type_, **kw: "TEXT"  # type: ignore[attr-defined]
if not hasattr(SQLiteTypeCompiler, "visit_JSONB"):
    SQLiteTypeCompiler.visit_JSONB = lambda self, type_, **kw: "JSON"  # type: ignore[attr-defined]


def _build_session() -> Session:
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @event.listens_for(engine, "connect")
    def _attach_schema(dbapi_connection, _connection_record) -> None:  # type: ignore[no-untyped-def]
        cursor = dbapi_connection.cursor()
        cursor.execute("ATTACH DATABASE ':memory:' AS t2c_data")
        cursor.execute("ATTACH DATABASE ':memory:' AS t2c_ops")
        cursor.close()

    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, class_=Session)
    return SessionLocal()


def _seed_owner_pair_with_assets(db: Session) -> tuple[DataOwner, DataOwner, list[TableEntity]]:
    source_owner = DataOwner(name="Owner origem", email="origem@example.com", area="Dados")
    target_owner = DataOwner(name="Owner destino", email="destino@example.com", area="Dados")
    datasource = DataSource(
        name="local",
        db_type="postgres",
        host="localhost",
        port=5432,
        database="andromeda",
        username="user",
    )
    datasource.password = "secret"
    database = Database(name="andromeda", datasource=datasource)
    schema = Schema(name="bronze", database=database)
    tables = [
        TableEntity(
            name="audit_logs",
            table_type="table",
            schema=schema,
            data_owner=source_owner,
            owner=source_owner.name,
            owner_email=source_owner.email,
            certification_status="not_eligible",
            certification_criticality="high",
            has_personal_data=True,
            access_scope="confidential",
        ),
        TableEntity(
            name="customer_profile",
            table_type="table",
            schema=schema,
            data_owner=source_owner,
            owner=source_owner.name,
            owner_email=source_owner.email,
            certification_status="certified",
            certification_criticality="medium",
            has_sensitive_personal_data=True,
            access_scope="public",
        ),
        TableEntity(
            name="orders",
            table_type="table",
            schema=schema,
            data_owner=source_owner,
            owner=source_owner.name,
            owner_email=source_owner.email,
            certification_status="in_review",
            certification_criticality="low",
            access_scope="internal",
        ),
    ]
    db.add_all([source_owner, target_owner, datasource, database, schema, *tables])
    db.commit()
    for item in [source_owner, target_owner, *tables]:
        db.refresh(item)
    return source_owner, target_owner, tables


def _profile_for(table: TableEntity, *, open_incidents: int, dq_score: float | None, active_dq_rules_count: int) -> SimpleNamespace:
    now = datetime.now(timezone.utc)
    return SimpleNamespace(
        table_id=table.id,
        datasource_id=table.schema.database.datasource.id,
        database_id=table.schema.database.id,
        schema_id=table.schema.id,
        table_name=table.name,
        table_type=table.table_type,
        schema_name=table.schema.name,
        database_name=table.schema.database.name,
        datasource_name=table.schema.database.datasource.name,
        engine=table.schema.database.datasource.db_type,
        owner_defined=True,
        description_complete=True,
        dictionary_complete=True,
        classification_defined=True,
        tags_count=1,
        terms_count=1,
        total_columns=1,
        documented_columns=1,
        certification_status=table.certification_status,
        certification_criticality=table.certification_criticality,
        certification_badges=[],
        certification_decided_at=None,
        certification_review_at=None,
        certification_expires_at=None,
        review_recent=True,
        dq_score=dq_score,
        completeness_pct_avg=None,
        freshness_seconds=None,
        open_incidents=open_incidents,
        critical_open_incidents=1 if open_incidents > 0 else 0,
        active_dq_violation=False,
        active_dq_violation_count=0,
        active_dq_rule_names=[],
        owner_name=table.owner,
        data_owner_id=table.data_owner_id,
        domain_name=None,
        sensitivity_level=None,
        has_personal_data=table.has_personal_data,
        has_sensitive_personal_data=table.has_sensitive_personal_data,
        owner_reviewed_at=None,
        privacy_reviewed_at=table.privacy_reviewed_at,
        last_review_at=None,
        last_sync_at=None,
        last_updated_at=now,
        search_clicks_30d=0,
        active_dq_rules_count=active_dq_rules_count,
        recent_dq_failure_runs_30d=0,
        sla_defined=True,
        sla_hours=24,
        trust_score=0,
        trust_label="Sem leitura",
        trust_tone="neutral",
        readiness_score=78,
    )


def _patch_profiles(monkeypatch, tables: list[TableEntity]) -> None:
    seeded_table_ids = [table.id for table in tables]

    def _fake_load_dashboard_profiles_with_fallback(session, now, *, table_ids=None, current_user=None):  # type: ignore[no-untyped-def]
        ids = table_ids or seeded_table_ids
        current_tables = [session.get(TableEntity, table_id) for table_id in ids]
        current_tables = [table for table in current_tables if table is not None and table.id in ids]
        by_name = {table.name: table for table in current_tables}
        return (
            [
            _profile_for(by_name["audit_logs"], open_incidents=2, dq_score=None, active_dq_rules_count=0),
            _profile_for(by_name["customer_profile"], open_incidents=0, dq_score=91.2, active_dq_rules_count=1),
            _profile_for(by_name["orders"], open_incidents=1, dq_score=None, active_dq_rules_count=0),
            ],
            "materialized",
        )

    monkeypatch.setattr(owners_summary_module, "load_dashboard_profiles_with_fallback", _fake_load_dashboard_profiles_with_fallback)


def test_reassign_preview_shows_target_and_filters_assets(monkeypatch) -> None:
    db = _build_session()
    source_owner, target_owner, tables = _seed_owner_pair_with_assets(db)
    _patch_profiles(monkeypatch, tables)

    user = SimpleNamespace(id=1, roles=[SimpleNamespace(name="admin")], email="admin@example.com")

    preview = get_data_owner_reassign_preview(
        owner_id=source_owner.id,
        target_owner_id=target_owner.id,
        asset_ids=[tables[0].id],
        page=1,
        page_size=100,
        db=db,
        current_user=user,
    )

    assert preview.source_owner.id == source_owner.id
    assert preview.target_owner is not None
    assert preview.target_owner.id == target_owner.id
    assert preview.impact.asset_count == 1
    assert preview.impact.certification_pending_assets == 1
    assert preview.impact.personal_data_assets == 1
    assert preview.assets[0].id == tables[0].id
    assert preview.assets[0].recommended_action.startswith("Reatribuir owner")


def test_reassign_selected_assets_updates_ownership(monkeypatch) -> None:
    db = _build_session()
    source_owner, target_owner, tables = _seed_owner_pair_with_assets(db)
    _patch_profiles(monkeypatch, tables)

    user = SimpleNamespace(id=1, roles=[SimpleNamespace(name="admin")], email="admin@example.com")

    result = reassign_ownership_assets(
        db,
        current_user=user,
        owner_id=source_owner.id,
        payload=OwnershipReassignRequestIn(
            target_owner_id=target_owner.id,
            asset_ids=[tables[0].id, tables[2].id],
            mode="selected",
            note="Reatribuição por mudança de responsabilidade.",
        ),
        audit_kwargs={"route": "/v1/data-owners/1/reassign-assets", "method": "POST", "request_id": "req-1"},
    )

    assert result.reassigned_count == 2
    assert result.source_owner_id == source_owner.id
    assert result.target_owner_id == target_owner.id

    moved_first = db.get(TableEntity, tables[0].id)
    moved_third = db.get(TableEntity, tables[2].id)
    untouched_second = db.get(TableEntity, tables[1].id)
    assert moved_first is not None
    assert moved_first.data_owner_id == target_owner.id
    assert moved_first.owner == target_owner.name
    assert moved_first.owner_email == target_owner.email
    assert moved_third is not None
    assert moved_third.data_owner_id == target_owner.id
    assert untouched_second is not None
    assert untouched_second.data_owner_id == source_owner.id

    audit_rows = db.scalars(
        select(AuditLog)
        .where(AuditLog.action == "data_owner.reassign_assets")
        .order_by(AuditLog.id.asc())
    ).all()
    assert audit_rows
    assert any(row.entity_id == str(tables[0].id) for row in audit_rows)
    assert any((row.metadata_json or {}).get("source_owner_id") == source_owner.id for row in audit_rows)
    assert any((row.metadata_json or {}).get("target_owner_id") == target_owner.id for row in audit_rows)
    assert any((row.metadata_json or {}).get("note") == "Reatribuição por mudança de responsabilidade." for row in audit_rows)


def test_reassign_all_then_delete_without_force_succeeds(monkeypatch) -> None:
    db = _build_session()
    source_owner, target_owner, tables = _seed_owner_pair_with_assets(db)
    _patch_profiles(monkeypatch, tables)

    user = SimpleNamespace(id=1, roles=[SimpleNamespace(name="admin")], email="admin@example.com")

    result = reassign_ownership_assets(
        db,
        current_user=user,
        owner_id=source_owner.id,
        payload=OwnershipReassignRequestIn(
            target_owner_id=target_owner.id,
            asset_ids=[],
            mode="all",
            note=None,
        ),
        audit_kwargs={"route": "/v1/data-owners/1/reassign-assets", "method": "POST", "request_id": "req-2"},
    )

    assert result.reassigned_count == 3
    assert all(db.get(TableEntity, table.id).data_owner_id == target_owner.id for table in tables)

    delete_response = delete_data_owner(owner_id=source_owner.id, force=False, db=db, user=user)
    assert delete_response is None
    assert db.get(DataOwner, source_owner.id) is None


def test_reassign_rejects_same_owner(monkeypatch) -> None:
    db = _build_session()
    source_owner, _, tables = _seed_owner_pair_with_assets(db)
    _patch_profiles(monkeypatch, tables)

    user = SimpleNamespace(id=1, roles=[SimpleNamespace(name="admin")], email="admin@example.com")

    try:
        reassign_ownership_assets(
            db,
            current_user=user,
            owner_id=source_owner.id,
            payload=OwnershipReassignRequestIn(
                target_owner_id=source_owner.id,
                asset_ids=[tables[0].id],
                mode="selected",
                note=None,
            ),
            audit_kwargs={"route": "/v1/data-owners/1/reassign-assets", "method": "POST", "request_id": "req-3"},
        )
        raise AssertionError("Expected ValueError was not raised")
    except ValueError as exc:
        assert "different" in str(exc).lower()


def test_reassign_rejects_inactive_target_owner(monkeypatch) -> None:
    db = _build_session()
    source_owner, target_owner, tables = _seed_owner_pair_with_assets(db)
    target_owner.is_active = False
    db.commit()
    _patch_profiles(monkeypatch, tables)

    user = SimpleNamespace(id=1, roles=[SimpleNamespace(name="admin")], email="admin@example.com")

    try:
        reassign_ownership_assets(
            db,
            current_user=user,
            owner_id=source_owner.id,
            payload=OwnershipReassignRequestIn(
                target_owner_id=target_owner.id,
                asset_ids=[tables[0].id],
                mode="selected",
                note=None,
            ),
            audit_kwargs={"route": "/v1/data-owners/1/reassign-assets", "method": "POST", "request_id": "req-4"},
        )
        raise AssertionError("Expected ValueError was not raised")
    except ValueError as exc:
        assert "active" in str(exc).lower()


def test_inactive_owner_with_assets_appears_in_alert_rankings(monkeypatch) -> None:
    db = _build_session()
    source_owner, _, tables = _seed_owner_pair_with_assets(db)
    source_owner.is_active = False
    db.commit()
    _patch_profiles(monkeypatch, tables)

    user = SimpleNamespace(id=1, roles=[SimpleNamespace(name="admin")], email="admin@example.com")
    summary = get_ownership_summary(db, current_user=user, include_unowned=True, page=1, page_size=100)

    assert any(item.owner_id == source_owner.id for item in summary.rankings.inactive_with_assets)
    assert all(
        "<bound method BaseModel.schema" not in priority.description
        for priority in summary.priorities
    )

    unowned_asset = OwnershipUnownedAssetOut(
        id=tables[0].id,
        name=tables[0].name,
        database_name="andromeda",
        schema_name="bronze",
        connection_name="local",
        certification_status="not_eligible",
        recommended_action="Atribuir owner",
    )
    priorities = owners_summary_module._build_ownership_priorities([], [unowned_asset])
    assert priorities[0].description == "bronze.audit_logs está sem owner e precisa de ownership para avançar governança e operação."
