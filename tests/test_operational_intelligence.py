from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy import create_engine, event
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.features.dashboard.operational_intelligence import build_operational_intelligence
from t2c_data.features.dashboard.support import TableProfile
from t2c_data.models import Base
from t2c_data.models.catalog import DataSource, Database, Schema, TableEntity
from t2c_data.models.dq import DQRun
from t2c_data.models.incident import Incident
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


def _create_table_fixture(db: Session) -> TableEntity:
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
    table = TableEntity(name="orders", table_type="table", schema=schema)
    db.add_all([datasource, database, schema, table])
    db.commit()
    return table


def _table_profile(table: TableEntity) -> TableProfile:
    return TableProfile(
        table_id=table.id,
        datasource_id=table.schema.database.datasource.id,
        database_id=table.schema.database.id,
        schema_id=table.schema.id,
        table_name=table.name,
        table_type=table.table_type,
        schema_name=table.schema.name,
        database_name=table.schema.database.name,
        datasource_name=table.schema.database.datasource.name,
        engine="postgres",
        owner_defined=False,
        description_complete=False,
        dictionary_complete=False,
        classification_defined=False,
        tags_count=0,
        terms_count=0,
        total_columns=10,
        documented_columns=1,
        certification_status="eligible",
        certification_criticality=None,
        certification_badges=[],
        certification_decided_at=None,
        certification_review_at=None,
        certification_expires_at=None,
        review_recent=False,
        dq_score=58.0,
        completeness_pct_avg=44.0,
        freshness_seconds=3600 * 96,
        open_incidents=2,
        critical_open_incidents=1,
        active_dq_violation=True,
        active_dq_violation_count=2,
        active_dq_rule_names=["not_null"],
        owner_name=None,
        data_owner_id=None,
        domain_name="Financeiro",
        sensitivity_level="restricted",
        has_personal_data=False,
        has_sensitive_personal_data=False,
        owner_reviewed_at=None,
        privacy_reviewed_at=None,
        last_review_at=None,
        last_sync_at=datetime.now(timezone.utc) - timedelta(days=4),
        last_updated_at=datetime.now(timezone.utc) - timedelta(days=4),
        search_clicks_30d=82,
        active_dq_rules_count=3,
        recent_dq_failure_runs_30d=3,
        trust_score=41,
        trust_label="Em risco",
        trust_tone="danger",
    )


def test_operational_intelligence_prioritizes_risky_assets() -> None:
    db = _build_session()
    table = _create_table_fixture(db)

    domain = SemanticDomain(
        slug="financeiro",
        name="Financeiro",
        description="Domínio financeiro",
        owner="Data Office",
        steward="Steward Financeiro",
        criticality="high",
        maturity_status="managed",
        quality_score=72,
        governance_score=68,
    )
    product = SemanticDataProduct(
        domain=domain,
        slug="orders-product",
        name="Orders Product",
        description="Produto de pedidos",
        owner="Squad Finance",
        steward="Steward Financeiro",
        consumers=["BI", "Operations"],
        sla_text="Atualização diária",
        contract_text="Contrato de pedidos v1",
        maturity_status="managed",
        quality_score=70,
        governance_score=66,
    )
    db.add_all(
        [
            domain,
            product,
            SemanticLink(domain=domain, relation_kind="contains", entity_kind="table", entity_id=table.id, entity_label="finance.orders", is_primary=True),
            SemanticLink(product=product, relation_kind="contains", entity_kind="table", entity_id=table.id, entity_label="finance.orders", is_primary=True),
            Incident(
                title="Falha de ingestão",
                description="Erro recente",
                entity_type="table",
                table_fqn="finance.orders",
                severity="sev1",
                status="open",
                detected_at=datetime.now(timezone.utc) - timedelta(hours=4),
                occurrences=2,
                domain_name="Financeiro",
                owner_team="Data Platform",
                squad_name="Payments",
            ),
            Incident(
                title="Falha de DQ",
                description="DQ abaixo do mínimo",
                entity_type="table",
                table_fqn="finance.orders",
                severity="sev2",
                status="investigating",
                detected_at=datetime.now(timezone.utc) - timedelta(hours=10),
                occurrences=1,
                domain_name="Financeiro",
                owner_team="Data Platform",
                squad_name="Payments",
            ),
            DQRun(
                table_id=table.id,
                scope="table",
                schema_name="finance",
                status="failed",
                error_message="timeout",
            ),
            DQRun(
                table_id=table.id,
                scope="table",
                schema_name="finance",
                status="failed",
                error_message="missing value",
            ),
            DQRun(
                table_id=table.id,
                scope="table",
                schema_name="finance",
                status="success",
            ),
        ]
    )
    db.commit()

    payload = build_operational_intelligence(
        db,
        profiles=[_table_profile(table)],
        recent_incident_map={"finance.orders": 2},
        recent_occurrence_map={"finance.orders": 2},
        ingestion_summary={
            "items": [
                {
                    "table_id": table.id,
                    "table_name": table.name,
                    "table_fqn": "finance.orders",
                    "pipeline_name": "finance-orders",
                    "dag_id": "finance_orders_daily",
                    "latest_status_label": "Failed",
                    "last_success_at": (datetime.now(timezone.utc) - timedelta(days=4)).isoformat(),
                    "last_error": "timeout",
                }
            ]
        },
        critical_changes=[
            {
                "changed_at": datetime.now(timezone.utc).isoformat(),
                "table_id": table.id,
            }
        ],
        window_days=30,
    )

    assert payload["evaluated_assets"] == 1
    assert payload["high_risk_assets"] >= 1
    assert payload["high_risk_domains"] >= 1
    assert payload["high_risk_products"] >= 1
    assert payload["unstable_pipelines"] >= 1
    assert payload["suggested_incidents"] >= 1
    assert payload["by_asset"][0]["suggested_incident"] is True
    assert payload["alerts"]
    assert payload["trend"]
