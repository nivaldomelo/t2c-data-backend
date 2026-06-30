from __future__ import annotations

import os

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy import create_engine, event
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.features.data_quality.profiling_schedules import list_profiling_schedules, upsert_profiling_schedule
from t2c_data.models import Base
from t2c_data.models.auth import Role, User
from t2c_data.models.catalog import DataSource, Database, Schema, TableEntity
from t2c_data.schemas.dq import DQProfilingScheduleCreate


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


def _seed_graph(db: Session) -> tuple[User, User, TableEntity]:
    admin_role = Role(name="admin", description="Admin")
    admin = User(email="admin@andromeda.local", password_hash="hash", name="Admin", full_name="Admin User", is_active=True)
    admin.roles.append(admin_role)
    owner = User(email="owner@andromeda.local", password_hash="hash", name="Owner", full_name="Owner User", is_active=True)
    datasource = DataSource(name="warehouse", db_type="postgres", host="localhost", port=5432, database="andromeda", username="catalog")
    datasource.password = "secret"
    database = Database(name="andromeda", datasource=datasource)
    schema = Schema(name="bronze", database=database)
    table = TableEntity(name="categories", table_type="table", schema=schema)
    db.add_all([admin_role, admin, owner, datasource, database, schema, table])
    db.commit()
    db.refresh(admin)
    db.refresh(owner)
    db.refresh(table)
    return admin, owner, table


def test_profiling_schedule_can_be_created_and_listed() -> None:
    db = _build_session()
    admin, owner, table = _seed_graph(db)

    schedule = upsert_profiling_schedule(
        db,
        DQProfilingScheduleCreate(
            scope="table",
            table_id=table.id,
            schedule_mode="daily",
            schedule_enabled=True,
            schedule_time="08:00",
            recipient_user_ids=[owner.id, admin.id, owner.id],
        ),
    )

    assert schedule.scope == "table"
    assert schedule.schedule_mode == "daily"
    assert schedule.schedule_enabled is True
    assert schedule.schedule_summary is not None
    assert "Diário" in schedule.schedule_summary
    assert sorted(recipient.id for recipient in schedule.notification_recipients) == [admin.id, owner.id]
    assert schedule.schedule_next_run_at is not None

    rows = list_profiling_schedules(db, scope="table", table_id=table.id)
    assert len(rows) == 1
    assert rows[0].id == schedule.id
    assert rows[0].notification_recipients[0].email in {admin.email, owner.email}


def test_profiling_schedule_supports_datasource_and_tables_scopes() -> None:
    db = _build_session()
    admin, owner, table = _seed_graph(db)

    datasource_schedule = upsert_profiling_schedule(
        db,
        DQProfilingScheduleCreate(
            scope="datasource",
            datasource_id=table.schema.database.datasource_id,
            name="Profiling do datasource",
            schedule_mode="daily",
            schedule_enabled=True,
            schedule_time="09:00",
            schedule_timezone="America/Sao_Paulo",
            recipient_user_ids=[admin.id, owner.id],
        ),
    )

    tables_schedule = upsert_profiling_schedule(
        db,
        DQProfilingScheduleCreate(
            scope="tables",
            datasource_id=table.schema.database.datasource_id,
            schema_name=table.schema.name,
            table_ids=[table.id],
            name="Profiling das tabelas críticas",
            schedule_mode="weekly",
            schedule_enabled=True,
            schedule_time="10:00",
            schedule_day_of_week=1,
            schedule_timezone="America/Sao_Paulo",
        ),
    )

    assert datasource_schedule.scope == "datasource"
    assert datasource_schedule.name == "Profiling do datasource"
    assert datasource_schedule.schedule_timezone == "America/Sao_Paulo"
    assert datasource_schedule.table_ids == []
    assert datasource_schedule.target_label.startswith("Data Source")

    assert tables_schedule.scope == "tables"
    assert tables_schedule.table_ids == [table.id]
    assert tables_schedule.schedule_timezone == "America/Sao_Paulo"
    assert "tabela" in tables_schedule.target_label.lower()

    rows = list_profiling_schedules(db, scope="datasource", datasource_id=table.schema.database.datasource_id)
    assert len(rows) == 1
    assert rows[0].id == datasource_schedule.id

    rows = list_profiling_schedules(
        db,
        scope="tables",
        datasource_id=table.schema.database.datasource_id,
        schema_name=table.schema.name,
    )
    assert len(rows) == 1
    assert rows[0].id == tables_schedule.id


def test_profiling_schedule_supports_schema_scope_without_table_ids() -> None:
    db = _build_session()
    admin, owner, table = _seed_graph(db)

    schema_schedule = upsert_profiling_schedule(
        db,
        DQProfilingScheduleCreate(
            scope="schema",
            datasource_id=table.schema.database.datasource_id,
            schema_name=table.schema.name,
            name="Profiling do schema",
            schedule_mode="daily",
            schedule_enabled=True,
            schedule_time="07:00",
            schedule_timezone="America/Sao_Paulo",
            recipient_user_ids=[admin.id, owner.id],
        ),
    )

    assert schema_schedule.scope == "schema"
    assert schema_schedule.table_ids == []
    assert schema_schedule.datasource_id == table.schema.database.datasource_id
    assert schema_schedule.schema_name == table.schema.name
    assert schema_schedule.target_label.startswith("Schema")


if __name__ == "__main__":
    test_profiling_schedule_can_be_created_and_listed()
    test_profiling_schedule_supports_datasource_and_tables_scopes()
    print("dq profiling schedule service tests: OK")
