from __future__ import annotations

import os
from datetime import datetime, timezone

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy import create_engine, event, select
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.features.contracts.service import contract_impact_summary, create_contract, validate_contract
from t2c_data.features.data_quality.scorecards import build_dq_platform_scorecard_summary
from t2c_data.models import Base
from t2c_data.models.auth import Role, User
from t2c_data.models.catalog import ColumnEntity, DataSource, Database, Schema, TableEntity
from t2c_data.models.dq import DQRun, DQRule, DQRuleRun, DQTableMetric


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


def _seed_table(db: Session) -> tuple[User, TableEntity]:
    role = Role(name="admin", description="Admin")
    user = User(email="admin@andromeda.local", password_hash="hash", name="Admin", full_name="Admin User", is_active=True)
    user.roles.append(role)
    datasource = DataSource(
        name="operational-source",
        db_type="postgres",
        host="localhost",
        port=5432,
        database="catalog",
        username="catalog",
    )
    datasource.password = "secret"
    database = Database(name="catalog", datasource=datasource)
    schema = Schema(name="bronze", database=database)
    table = TableEntity(
        name="audit_logs",
        table_type="table",
        schema=schema,
        owner="Governança",
        certification_criticality="high",
        description_manual="Tabela de auditoria",
        has_personal_data=True,
        has_sensitive_personal_data=False,
    )
    db.add_all([role, user, datasource, database, schema, table])
    db.flush()
    db.add_all(
        [
            ColumnEntity(table=table, name="id", data_type="integer", is_primary_key=True, is_nullable=False, ordinal_position=1),
            ColumnEntity(table=table, name="created_at", data_type="timestamp", is_nullable=False, ordinal_position=2),
        ]
    )
    db.commit()
    return user, table


def test_platform_scorecard_consolidates_dq_rules_and_contracts() -> None:
    db = _build_session()
    user, table = _seed_table(db)
    now = datetime.now(timezone.utc)

    db.add(
        DQRun(
            table_id=table.id,
            status="success",
            execution_engine="spark",
            created_at=now,
            updated_at=now,
        )
    )
    db.flush()
    dq_run = db.scalar(select(DQRun).where(DQRun.table_id == table.id).order_by(DQRun.id.desc()))
    db.add(
        DQTableMetric(
            run_id=dq_run.id,
            table_id=table.id,
            row_count=120,
            column_count=2,
            completeness_pct_avg=88.5,
            dq_score=86.0,
            duplicates_count=3,
            failed_rules=1,
        )
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
            violations_count=2,
            created_at=now,
            updated_at=now,
        )
    )
    contract = create_contract(
        db,
        table_id=table.id,
        payload={
            "status": "published",
            "columns": [
                {"column_name": "id", "data_type": "int", "is_primary_key": True, "is_nullable": False},
                {"column_name": "event_type", "data_type": "text", "is_nullable": False},
            ],
        },
        created_by_user_id=user.id,
    )
    validate_contract(db, contract_id=contract.id, created_by_user_id=user.id)

    summary = build_dq_platform_scorecard_summary(db, current_user=user)

    assert summary["totals"]["tables"] == 1
    assert summary["totals"]["tables_with_rules"] == 1
    assert summary["totals"]["tables_without_rules"] == 0
    assert summary["totals"]["contracts_total"] == 1
    assert summary["totals"]["failed_contract_validations"] == 1
    assert summary["top_risks"][0]["table_id"] == table.id
    assert summary["failing_rules"][0]["key"] == str(rule.id)


def test_contract_impact_summary_detects_breaking_changes_and_lineage_context() -> None:
    db = _build_session()
    user, table = _seed_table(db)
    contract = create_contract(
        db,
        table_id=table.id,
        payload={
            "status": "published",
            "columns": [
                {"column_name": "id", "data_type": "integer", "is_primary_key": True, "is_nullable": False},
                {"column_name": "event_type", "data_type": "text", "is_nullable": False},
            ],
        },
        created_by_user_id=user.id,
    )
    validate_contract(db, contract_id=contract.id, created_by_user_id=user.id)

    impact = contract_impact_summary(db, table_id=table.id)

    assert impact["contract_id"] == contract.id
    assert impact["schema_state"] == "breaking"
    assert impact["breaking_changes_count"] >= 1
    assert impact["lineage"]["downstream_count"] == 0
    assert impact["recommendation"]


if __name__ == "__main__":
    test_platform_scorecard_consolidates_dq_rules_and_contracts()
    test_contract_impact_summary_detects_breaking_changes_and_lineage_context()
    print("dq scorecards tests: OK")
