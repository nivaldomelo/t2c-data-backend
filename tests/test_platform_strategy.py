from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy import create_engine, event
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.features.dashboard.strategy_queries import build_platform_strategic_summary
from t2c_data.models import Base
from t2c_data.models.auth import User
from t2c_data.models.catalog import DataOwner, DataSource, Database, Schema, TableEntity
from t2c_data.models.governance import GovernanceTrustSnapshot
from t2c_data.models.platform import DashboardAssetReadModel, PlatformUsageEvent
from t2c_data.models.semantic import SemanticDataProduct, SemanticDomain, SemanticLink


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


def _create_table(db: Session) -> TableEntity:
    datasource = DataSource(
        name="warehouse",
        db_type="postgres",
        host="localhost",
        port=5432,
        database="warehouse",
        username="catalog",
    )
    datasource.password = "secret"
    database = Database(name="warehouse", datasource=datasource)
    schema = Schema(name="finance", database=database)
    owner = DataOwner(name="Finance Analytics", email="finance@example.com", area="Finance", is_active=True)
    table = TableEntity(
        name="orders",
        table_type="table",
        schema=schema,
        data_owner=owner,
        owner="Finance Analytics",
        owner_email="finance@example.com",
    )
    db.add_all([datasource, database, schema, owner, table])
    db.commit()
    return table


def test_platform_strategic_summary_builds_value_and_benchmarks() -> None:
    db = _build_session()
    now = datetime.now(timezone.utc)
    table = _create_table(db)
    user = User(email="leader@example.com", password_hash="hash", name="Leader", full_name="Business Leader", is_active=True)
    db.add(user)
    db.flush()
    db.add(
        DashboardAssetReadModel(
            table_id=table.id,
            datasource_id=table.schema.database.datasource_id,
            database_id=table.schema.database_id,
            schema_id=table.schema_id,
            table_name=table.name,
            table_type=table.table_type,
            schema_name=table.schema.name,
            database_name=table.schema.database.name,
            datasource_name=table.schema.database.datasource.name,
            engine="postgres",
            owner_defined=True,
            description_complete=True,
            dictionary_complete=True,
            classification_defined=True,
            tags_count=3,
            terms_count=2,
            search_clicks_30d=14,
            active_dq_rules_count=2,
            recent_dq_failure_runs_30d=1,
            certification_status="certified",
            certification_criticality="high",
            certification_badges=["trusted"],
            certification_decided_at=now.isoformat(),
            certification_review_at=now.isoformat(),
            certification_expires_at=(now + timedelta(days=30)).isoformat(),
            review_recent=True,
            dq_score=84.0,
            completeness_pct_avg=91.0,
            freshness_seconds=1800,
            open_incidents=1,
            critical_open_incidents=0,
            owner_name=table.owner,
            data_owner_id=table.data_owner_id,
            domain_name="Finance",
            sensitivity_level="internal",
            has_personal_data=False,
            has_sensitive_personal_data=False,
            owner_reviewed_at=now.isoformat(),
            privacy_reviewed_at=now.isoformat(),
            last_review_at=now.isoformat(),
            last_sync_at=now.isoformat(),
            last_updated_at=now.isoformat(),
        )
    )
    domain = SemanticDomain(
        slug="finance",
        name="Finance",
        description="Domínio financeiro",
        owner="finance@example.com",
        steward="finance@example.com",
        criticality="high",
        maturity_status="governed",
        quality_score=82,
        governance_score=78,
        is_active=True,
    )
    product = SemanticDataProduct(
        domain=domain,
        slug="orders",
        name="Orders",
        description="Produto de pedidos",
        owner="finance@example.com",
        steward="finance@example.com",
        consumers=["bi", "ops"],
        sla_text="Atualização diária",
        contract_text="Contrato v1",
        maturity_status="governed",
        quality_score=80,
        governance_score=76,
        is_active=True,
    )
    db.add_all(
        [
            domain,
            product,
            SemanticLink(domain=domain, relation_kind="contains", entity_kind="table", entity_id=table.id, entity_label=table.name, is_primary=True),
            SemanticLink(product=product, relation_kind="contains", entity_kind="table", entity_id=table.id, entity_label=table.name, is_primary=True),
            GovernanceTrustSnapshot(
                table_id=table.id,
                datasource_id=table.schema.database.datasource_id,
                owner_name=table.owner,
                domain_label="Finance",
                score=82,
                label="Confiável",
                tone="success",
                readiness_score=78,
                governance_score=80,
                operational_score=79,
                dq_score=84.0,
                open_incidents=1,
                critical_open_incidents=0,
                active_dq_violation=False,
                recent_dq_failure_runs_30d=1,
                trust_context_json={"penalties": []},
                bucket_date=now - timedelta(days=5),
            ),
            GovernanceTrustSnapshot(
                table_id=table.id,
                datasource_id=table.schema.database.datasource_id,
                owner_name=None,
                domain_label="Finance",
                score=60,
                label="Gerenciado",
                tone="warning",
                readiness_score=52,
                governance_score=58,
                operational_score=55,
                dq_score=72.0,
                open_incidents=3,
                critical_open_incidents=1,
                active_dq_violation=True,
                recent_dq_failure_runs_30d=4,
                trust_context_json={"penalties": [{"key": "no_owner"}, {"key": "no_description"}, {"key": "no_dictionary"}, {"key": "no_tags"}]},
                bucket_date=now - timedelta(days=35),
            ),
            PlatformUsageEvent(
                user_id=user.id,
                event_name="page_view",
                module_name="dashboard",
                page_path="/dashboard/strategy",
                entity_type="table",
                entity_id=table.id,
                target_url="/dashboard/strategy",
                metadata_json={"view": "strategy"},
                created_at=now - timedelta(days=1),
                updated_at=now - timedelta(days=1),
            ),
        ]
    )
    db.commit()

    payload = build_platform_strategic_summary(db, days=30, current_user=user)

    assert payload["window_days"] == 30
    assert payload["value_score"] > payload["value_score_previous"]
    assert payload["value_score_delta"] > 0
    assert payload["adoption"]["active_users"] == 1
    assert payload["adoption"]["top_domains"][0]["label"] == "Finance"
    assert payload["adoption"]["top_products"][0]["label"] == "Orders"
    assert payload["benchmark"]["by_domain"][0]["label"] == "Finance"
    assert payload["roadmap"]
    assert len(payload["value_metrics"]) == 6
    assert payload["narrative"]
