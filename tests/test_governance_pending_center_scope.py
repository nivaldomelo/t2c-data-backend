from __future__ import annotations

import os

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy import create_engine, event
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.core.security import hash_password
from t2c_data.features.governance.queries import get_governance_pending_center, get_governance_pending_center_summary_light
from t2c_data.models import Base
from t2c_data.models.access_control import DataAccessGrant
from t2c_data.models.auth import Role, User
from t2c_data.models.catalog import ColumnEntity, DataSource, Database, Schema, TableEntity


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


def _seed_catalog(db: Session) -> User:
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
    bronze = Schema(name="bronze", database=database)
    demo_datasource = DataSource(
        name="demo-source",
        db_type="postgres",
        host="localhost",
        port=5432,
        database="demo",
        username="nivasmelo",
    )
    demo_datasource.password = "secret"
    demo_database = Database(name="demo", datasource=demo_datasource)
    demo_schema = Schema(name="demo", database=demo_database)

    bronze_table = TableEntity(name="customers", table_type="table", schema=bronze)
    demo_table = TableEntity(name="events", table_type="table", schema=demo_schema)
    db.add_all([datasource, database, bronze, demo_datasource, demo_database, demo_schema, bronze_table, demo_table])
    db.flush()

    db.add_all(
        [
            ColumnEntity(
                table=bronze_table,
                name="id",
                data_type="integer",
                is_primary_key=True,
                is_nullable=False,
                ordinal_position=1,
            ),
            ColumnEntity(
                table=demo_table,
                name="id",
                data_type="integer",
                is_primary_key=True,
                is_nullable=False,
                ordinal_position=1,
            ),
        ]
    )

    user = User(
        email="caio@email.com.br",
        name="Caio Wilson",
        full_name="Caio Wilson",
        password_hash=hash_password("secret123"),
        is_active=True,
    )
    user.roles = [role]
    db.add(user)
    db.flush()
    db.add_all(
        [
            DataAccessGrant(user=user, effect="allow", datasource=datasource),
            DataAccessGrant(user=user, effect="allow", schema=bronze),
        ]
    )
    db.commit()
    return user


def test_pending_center_respects_data_scope_for_restricted_user() -> None:
    db = _build_session()
    user = _seed_catalog(db)

    payload = get_governance_pending_center(db, current_user=user)

    assert payload["total"] > 0
    assert isinstance(payload["risk_queue"], list)
    assert [item["value"] for item in payload["filters"]["datasources"]] == ["local-andromeda"]
    assert [item["value"] for item in payload["filters"]["schemas"]] == ["bronze"]
    assert all(item["datasource_name"] == "local-andromeda" for item in payload["items"])
    assert all(item["schema_name"] == "bronze" for item in payload["items"])
    assert all("demo" not in item["table_fqn"] for item in payload["items"])
    assert all("risk_score" in item for item in payload["risk_queue"])


def test_pending_center_summary_light_excludes_campaigns_payload() -> None:
    db = _build_session()
    user = _seed_catalog(db)

    payload = get_governance_pending_center_summary_light(db, current_user=user)

    assert payload["campaigns"] == []
    assert payload["filters"]["schemas"]
    assert payload["summary_cards"]["stewardship_pending"] >= 0


if __name__ == "__main__":
    test_pending_center_respects_data_scope_for_restricted_user()
    print("governance pending center scope tests: OK")
