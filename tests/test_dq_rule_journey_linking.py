from __future__ import annotations

import os

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from datetime import datetime, timezone

from sqlalchemy import create_engine, event
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.features.data_quality.rule_management import list_rules_with_filters
from t2c_data.features.data_quality.queries import resolve_table_context_by_fqn
from t2c_data.models import Base
from t2c_data.models.access_control import DataAccessGrant
from t2c_data.models.auth import Role, User
from t2c_data.models.catalog import DataSource, Database, Schema, TableEntity
from t2c_data.models.dq import DQJobRun, DQRule, DQRuleRun
from t2c_data.models.incident import Incident


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


def _seed_catalog(db: Session) -> tuple[User, TableEntity, TableEntity, DQRule, DQRule, DQRule]:
    role = Role(name="editor", description="Editor")
    user = User(email="caio@email.com.br", password_hash="hash", name="Caio Wilson", full_name="Caio Wilson", is_active=True)
    user.roles.append(role)

    datasource = DataSource(
        name="local-andromeda",
        db_type="postgres",
        host="localhost",
        port=5432,
        database="andromeda",
        username="catalog",
    )
    datasource.password = "secret"
    database = Database(name="andromeda", datasource=datasource)
    bronze = Schema(name="bronze", database=database)
    products = TableEntity(name="products", table_type="table", schema=bronze)
    categories = TableEntity(name="categories", table_type="table", schema=bronze)

    db.add_all([role, user, datasource, database, bronze, products, categories])
    db.flush()
    db.add(DataAccessGrant(user=user, effect="allow", schema=bronze))

    product_rule = DQRule(
        table_id=products.id,
        table_fqn="local-andromeda.bronze.products",
        name="Preco maior que",
        severity="critical",
        rule_type="row_violation",
        is_active=True,
        schedule_enabled=True,
        schedule_every_minutes=60,
    )
    category_rule_1 = DQRule(
        table_id=categories.id,
        table_fqn="local-andromeda.bronze.categories",
        name="Nova regra",
        severity="critical",
        rule_type="row_violation",
        is_active=True,
        schedule_enabled=True,
        schedule_every_minutes=60,
    )
    category_rule_2 = DQRule(
        table_id=categories.id,
        table_fqn="local-andromeda.bronze.categories",
        name="Categoria igual a Casa",
        severity="critical",
        rule_type="row_violation",
        is_active=True,
        schedule_enabled=True,
        schedule_every_minutes=60,
    )
    db.add_all([product_rule, category_rule_1, category_rule_2])
    db.flush()

    db.add_all(
        [
            DQRuleRun(rule_id=product_rule.id, status="fail", execution_engine="python", violations_count=3),
            DQJobRun(
                job_type="rules",
                status="success",
                execution_engine="python",
                table_id=products.id,
                table_fqn=product_rule.table_fqn,
                datasource_id=datasource.id,
                result_json={"requested_rule_ids": [product_rule.id]},
            ),
            DQRuleRun(rule_id=category_rule_1.id, status="fail", execution_engine="python", violations_count=2),
            DQJobRun(
                job_type="rules",
                status="success",
                execution_engine="python",
                table_id=categories.id,
                table_fqn=category_rule_1.table_fqn,
                datasource_id=datasource.id,
                result_json={"requested_rule_ids": [category_rule_1.id]},
            ),
            DQRuleRun(rule_id=category_rule_2.id, status="fail", execution_engine="python", violations_count=4),
            DQJobRun(
                job_type="rules",
                status="success",
                execution_engine="python",
                table_id=categories.id,
                table_fqn=category_rule_2.table_fqn,
                datasource_id=datasource.id,
                result_json={"requested_rule_ids": [category_rule_2.id]},
            ),
            Incident(
                title="DQ categories 1",
                description="Incidente vinculado à regra Nova regra",
                entity_type="table",
                source_type="dq_rule",
                source_ref_id=category_rule_1.id,
                table_fqn="local-andromeda.bronze.categories",
                status="open",
                severity="sev1",
                detected_at=datetime.now(timezone.utc),
            ),
            Incident(
                title="DQ categories 2",
                description="Incidente vinculado à regra Categoria igual a Casa",
                entity_type="table",
                source_type="dq_rule",
                source_ref_id=category_rule_2.id,
                table_fqn="local-andromeda.bronze.categories",
                status="open",
                severity="sev1",
                detected_at=datetime.now(timezone.utc),
            ),
        ]
    )
    db.commit()
    db.refresh(user)
    db.refresh(product_rule)
    db.refresh(category_rule_1)
    db.refresh(category_rule_2)
    return user, products, categories, product_rule, category_rule_1, category_rule_2


def test_dq_rules_are_linked_by_table_id_and_normalized_fqn() -> None:
    db = _build_session()
    user, products, _categories, product_rule, *_rest = _seed_catalog(db)

    by_table_id = list_rules_with_filters(
        db=db,
        rule_id=None,
        q=None,
        table_id=products.id,
        table_fqn=None,
        is_active=True,
        severity=None,
        last_status=None,
        current_user=user,
    )
    assert [item.id for item in by_table_id] == [product_rule.id]
    assert by_table_id[0].name == "Preco maior que"
    assert by_table_id[0].last_violations_count == 3

    by_normalized_fqn = list_rules_with_filters(
        db=db,
        rule_id=None,
        q=None,
        table_id=None,
        table_fqn="local-andromeda.andromeda.bronze.products",
        is_active=True,
        severity=None,
        last_status=None,
        current_user=user,
    )
    assert [item.id for item in by_normalized_fqn] == [product_rule.id]
    assert by_normalized_fqn[0].table_fqn == "local-andromeda.bronze.products"
    assert by_normalized_fqn[0].open_incident_id is None


def test_dq_rules_journey_payload_keeps_categories_rules_and_incidents() -> None:
    db = _build_session()
    user, _products, categories, _product_rule, category_rule_1, category_rule_2 = _seed_catalog(db)

    rows = list_rules_with_filters(
        db=db,
        rule_id=None,
        q=None,
        table_id=categories.id,
        table_fqn=None,
        is_active=True,
        severity=None,
        last_status=None,
        current_user=user,
    )

    assert len(rows) == 2
    assert {row.name for row in rows} == {"Nova regra", "Categoria igual a Casa"}
    assert all(row.open_incident_id is not None for row in rows)
    assert all(row.open_incident_status == "open" for row in rows)
    assert {row.last_violations_count for row in rows} == {2, 4}
    assert {row.table_id for row in rows} == {categories.id}
    assert {row.id for row in rows} == {category_rule_1.id, category_rule_2.id}


def test_resolve_table_context_by_fqn_rejects_ambiguous_catalog_match() -> None:
    db = _build_session()
    role = Role(name="editor", description="Editor")
    user = User(email="caio@email.com.br", password_hash="hash", name="Caio Wilson", full_name="Caio Wilson", is_active=True)
    user.roles.append(role)

    datasource = DataSource(
        name="local-andromeda",
        db_type="postgres",
        host="localhost",
        port=5432,
        database="andromeda",
        username="catalog",
    )
    datasource.password = "secret"
    database_one = Database(name="andromeda", datasource=datasource)
    database_two = Database(name="analytics", datasource=datasource)
    schema_one = Schema(name="bronze", database=database_one)
    schema_two = Schema(name="bronze", database=database_two)
    table_one = TableEntity(name="products", table_type="table", schema=schema_one)
    table_two = TableEntity(name="products", table_type="table", schema=schema_two)

    db.add_all([role, user, datasource, database_one, database_two, schema_one, schema_two, table_one, table_two])
    db.commit()

    try:
        resolve_table_context_by_fqn(db, "local-andromeda.bronze.products")
        raise AssertionError("Expected ValueError was not raised")
    except ValueError as exc:
        assert "ambíguo" in str(exc).lower()


if __name__ == "__main__":
    test_dq_rules_are_linked_by_table_id_and_normalized_fqn()
    test_dq_rules_journey_payload_keeps_categories_rules_and_incidents()
    print("dq rule journey linking tests: OK")
