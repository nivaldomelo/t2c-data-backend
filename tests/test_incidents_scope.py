from __future__ import annotations

import os

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from datetime import datetime, timezone

from sqlalchemy import create_engine, event, select
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, selectinload, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.core.security import hash_password
from t2c_data.features.incidents.query_support import build_incident_summary, filter_incidents_for_user
from t2c_data.models import Base
from t2c_data.models.access_control import DataAccessGrant
from t2c_data.models.auth import Role, User
from t2c_data.models.catalog import DataSource, Database, Schema, TableEntity
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


def test_incidents_filter_for_restricted_user_does_not_crash_and_keeps_visible_incidents() -> None:
    db = _build_session()
    role = Role(name="editor")
    db.add(role)

    datasource = DataSource(name="local-andromeda", db_type="postgres", host="localhost", port=5432, database="andromeda", username="nivasmelo")
    datasource.password = "secret"
    database = Database(name="andromeda", datasource=datasource)
    schema = Schema(name="bronze", database=database)
    table = TableEntity(name="customers", table_type="table", schema=schema)
    db.add_all([datasource, database, schema, table])
    db.flush()

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
    db.add_all([DataAccessGrant(user=user, effect="allow", datasource=datasource), DataAccessGrant(user=user, effect="allow", schema=schema)])

    incident = Incident(
        title="Falha operacional",
        description="Incidente de tabela",
        entity_type="table",
        table_fqn="local-andromeda.bronze.customers",
        detected_at=datetime.now(timezone.utc),
        status="open",
        severity="sev1",
    )
    db.add(incident)
    db.commit()

    incidents, profile_map = filter_incidents_for_user(db, [incident], user=user)
    summary = build_incident_summary(
        db,
        days=30,
        status=None,
        severity=None,
        entity_type=None,
        owner_id=None,
        reporter_id=None,
        source_type=None,
        source_ref_id=None,
        table_fqn=None,
        q=None,
        date_from=None,
        date_to=None,
        current_user=user,
    )

    assert len(incidents) == 1
    assert profile_map
    assert next(iter(profile_map.values())).datasource_name == "local-andromeda"
    assert summary.total == 1
    assert summary.open == 1


def test_incidents_hidden_when_table_is_explicitly_denied() -> None:
    db = _build_session()
    role = Role(name="viewer")
    db.add(role)

    datasource = DataSource(name="local-andromeda", db_type="postgres", host="localhost", port=5432, database="andromeda", username="nivasmelo")
    datasource.password = "secret"
    database = Database(name="andromeda", datasource=datasource)
    schema = Schema(name="bronze", database=database)
    table = TableEntity(name="customers", table_type="table", schema=schema)
    db.add_all([datasource, database, schema, table])
    db.flush()

    user = User(
        email="blocked@email.com",
        name="Blocked User",
        full_name="Blocked User",
        password_hash=hash_password("secret123"),
        is_active=True,
    )
    user.roles = [role]
    db.add(user)
    db.flush()
    db.add(DataAccessGrant(user=user, effect="deny", table=table))

    incident = Incident(
        title="Falha operacional",
        description="Incidente de tabela",
        entity_type="table",
        table_fqn="local-andromeda.bronze.customers",
        detected_at=datetime.now(timezone.utc),
        status="open",
        severity="sev1",
    )
    db.add(incident)
    db.commit()

    user = db.scalar(select(User).options(selectinload(User.access_grants)).where(User.id == user.id))
    assert user is not None

    incidents, profile_map = filter_incidents_for_user(db, [incident], user=user)
    summary = build_incident_summary(
        db,
        days=30,
        status=None,
        severity=None,
        entity_type=None,
        owner_id=None,
        reporter_id=None,
        source_type=None,
        source_ref_id=None,
        table_fqn=None,
        q=None,
        date_from=None,
        date_to=None,
        current_user=user,
    )

    assert incidents == []
    assert profile_map == {}
    assert summary.total == 0
    assert summary.open == 0


if __name__ == "__main__":
    test_incidents_filter_for_restricted_user_does_not_crash_and_keeps_visible_incidents()
    test_incidents_hidden_when_table_is_explicitly_denied()
    print("incidents scope tests: OK")
