from __future__ import annotations

import os
from datetime import datetime, timezone
from types import SimpleNamespace

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy import create_engine, event
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.features.catalog.operational_context import build_asset_links
from t2c_data.features.dashboard.profile_loader import load_table_profiles
from t2c_data.features.governance.active_governance import build_active_governance_findings
from t2c_data.features.governance.settings import GovernanceSettingsSnapshot
from t2c_data.models import Base
from t2c_data.models.auth import Role, User
from t2c_data.models.catalog import ColumnEntity, DataSource, Database, Schema, TableEntity
from t2c_data.models.dq import DQRun, DQRule, DQRuleRun
from t2c_data.models.search import SearchResultClick


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


def _seed_catalog(db: Session) -> TableEntity:
    role = Role(name="editor")
    db.add(role)

    datasource = DataSource(
        name="local-andromeda",
        db_type="postgres",
        host="localhost",
        port=5432,
        database="andromeda",
        username="nivasmelo",
    )
    datasource.password = "secret"
    database = Database(name="andromeda", datasource=datasource)
    schema = Schema(name="bronze", database=database)
    table = TableEntity(
        name="customers",
        table_type="table",
        schema=schema,
        owner="Governança",
        certification_criticality="high",
        description_manual="Tabela de clientes",
        has_personal_data=True,
    )
    db.add_all([datasource, database, schema, table])
    db.flush()
    db.add_all(
        [
            ColumnEntity(
                table=table,
                name="id",
                data_type="integer",
                is_primary_key=True,
                is_nullable=False,
                ordinal_position=1,
            )
        ]
    )
    db.commit()
    return table


def test_load_table_profiles_aggregates_usage_and_dq_recurrence() -> None:
    db = _build_session()
    table = _seed_catalog(db)
    now = datetime.now(timezone.utc)

    db.add_all(
        [
            SearchResultClick(
                entity_type="table",
                entity_id=table.id,
                query_text="clientes",
                normalized_query="clientes",
                target_url="/explorer?tableId=1",
                created_at=now,
                updated_at=now,
            )
            for _ in range(25)
        ]
    )
    rule = DQRule(
        table_id=table.id,
        table_fqn=f"{table.schema.name}.{table.name}",
        name="not_null_id",
        description="id obrigatório",
        rule_type="row_violation",
        severity="high",
        is_active=True,
    )
    db.add(rule)
    db.flush()
    db.add(
        DQRuleRun(
            rule_id=rule.id,
            status="fail",
            execution_engine="python",
            violations_count=3,
            created_at=now,
            updated_at=now,
        )
    )
    db.add_all(
        [
            DQRun(
                table_id=table.id,
                status="failed",
                execution_engine="spark",
                created_at=now,
                updated_at=now,
            ),
            DQRun(
                table_id=table.id,
                status="failed",
                execution_engine="spark",
                created_at=now,
                updated_at=now,
            ),
        ]
    )
    db.commit()

    profiles = load_table_profiles(db, now)
    profile = next(item for item in profiles if item.table_id == table.id)

    assert profile.search_clicks_30d == 25
    assert profile.active_dq_rules_count == 1
    assert profile.recent_dq_failure_runs_30d == 2


def test_active_governance_findings_cover_phase1_rules() -> None:
    table = SimpleNamespace(
        table_id=1,
        table_name="customers",
        table_fqn="local-andromeda.bronze.customers",
        datasource_name="local-andromeda",
        database_name="andromeda",
        schema_name="bronze",
        domain_name=None,
        owner_name="Governança",
        data_owner_id=None,
        owner_defined=False,
        owner_reviewed_at=None,
        certification_criticality="high",
        certification_status="eligible",
        classification_defined=False,
        sla_defined=False,
        dictionary_complete=False,
        description_complete=False,
        search_clicks_30d=25,
        active_dq_rules_count=0,
        recent_dq_failure_runs_30d=2,
        critical_open_incidents=0,
        open_incidents=0,
        last_updated_at=None,
        last_sync_at=None,
        last_review_at=None,
        datasource_id=1,
        database_id=1,
        schema_id=1,
    )
    links = build_asset_links(table_id=1, datasource_id=1, database_id=1, schema_id=1, data_owner_id=None)
    findings = build_active_governance_findings(
        table,
        settings_snapshot=GovernanceSettingsSnapshot(governance_high_usage_click_threshold=20),
        links=links,
        now=datetime.now(timezone.utc),
    )
    keys = {finding.key for finding in findings}

    assert "no_owner" in keys
    assert "critical_without_dq" in keys
    assert "classification_high_usage" in keys
    assert "no_sla" in keys
    assert "dictionary_high_usage" in keys
    assert "recurring_dq_failure_critical" in keys


if __name__ == "__main__":
    test_load_table_profiles_aggregates_usage_and_dq_recurrence()
    test_active_governance_findings_cover_phase1_rules()
    print("governance active phase1 tests: OK")
