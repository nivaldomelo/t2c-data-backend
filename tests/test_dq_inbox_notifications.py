from __future__ import annotations

import os

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy import create_engine, event, select
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.features.data_quality.incident_signals import handle_profiling_incident_signals
from t2c_data.features.data_quality.notifications import notify_dq_rule_violation
from t2c_data.models import Base
from t2c_data.models.auth import Role, User
from t2c_data.models.catalog import DataOwner, DataSource, Database, Schema, TableEntity
from t2c_data.models.dq import DQRule, DQRun, DQTableMetric
from t2c_data.models.notifications import UserInboxNotification


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


def _seed_catalog(db: Session, *, with_owner: bool = True) -> tuple[User, User | None, TableEntity, DQRule]:
    admin_role = Role(name="admin", description="Admin")
    admin = User(email="admin@andromeda.local", password_hash="hash", name="Admin", full_name="Admin User", is_active=True)
    admin.roles.append(admin_role)
    db.add(admin_role)
    db.add(admin)

    owner_user = None
    data_owner = None
    if with_owner:
        owner_user = User(
            email="owner@andromeda.local",
            password_hash="hash",
            name="Owner",
            full_name="Owner User",
            is_active=True,
        )
        data_owner = DataOwner(name="Owner User", email="owner@andromeda.local", area="Data", is_active=True)
        db.add_all([owner_user, data_owner])

    datasource = DataSource(
        name="warehouse",
        db_type="postgres",
        host="localhost",
        port=5432,
        database="andromeda",
        username="catalog",
    )
    datasource.password = "secret"
    database = Database(name="andromeda", datasource=datasource)
    schema = Schema(name="bronze", database=database)
    table = TableEntity(
        name="categories",
        table_type="table",
        schema=schema,
        data_owner=data_owner,
    )
    db.add_all([datasource, database, schema, table])
    db.flush()

    dq_rule = DQRule(
        table_id=table.id,
        table_fqn=f"{datasource.name}.{schema.name}.{table.name}",
        name="percentual de nulos",
        description="Regra para nulos",
        rule_type="row_violation",
        severity="high",
        is_active=True,
    )
    db.add(dq_rule)
    db.commit()
    db.refresh(admin)
    db.refresh(table)
    db.refresh(dq_rule)
    if owner_user is not None:
        db.refresh(owner_user)
    return admin, owner_user, table, dq_rule


def test_dq_rule_violation_creates_owner_inbox_notification_and_dedupes() -> None:
    db = _build_session()
    admin_user, owner_user, table, dq_rule = _seed_catalog(db, with_owner=True)

    try:
        rule = db.get(DQRule, dq_rule.id)
        assert rule is not None

        notify_dq_rule_violation(
            db,
            rule=rule,
            table=table,
            violations_count=3,
            preview_rows=[{"id": 1, "category": None}],
            run_id=101,
            reporter_user_id=None,
        )

        inbox_items = db.scalars(select(UserInboxNotification)).all()
        assert len(inbox_items) == 2
        inbox_by_user = {item.user_id: item for item in inbox_items}
        assert set(inbox_by_user) == {owner_user.id, admin_user.id}
        owner_item = inbox_by_user[owner_user.id]
        admin_item = inbox_by_user[admin_user.id]
        assert owner_item.category == "data_quality"
        assert owner_item.severity == "high"
        assert owner_item.source_module == "dq"
        assert owner_item.source_entity_type == "dq_rule"
        assert owner_item.href is not None and owner_item.href.startswith("/data-quality/rules")
        assert owner_item.context_json is not None
        assert owner_item.context_json["kind"] == "dq_rule_violation"
        assert owner_item.context_json["rule_id"] == dq_rule.id
        assert owner_item.context_json["table_id"] == table.id
        assert owner_item.context_json["recipient_reason"] == "data_owner"
        assert admin_item.context_json is not None
        assert admin_item.context_json["recipient_reason"] == "admin"

        notify_dq_rule_violation(
            db,
            rule=rule,
            table=table,
            violations_count=3,
            preview_rows=[{"id": 1, "category": None}],
            run_id=102,
            reporter_user_id=admin_user.id,
        )
        inbox_items_after = db.scalars(select(UserInboxNotification)).all()
        assert len(inbox_items_after) == 2
    finally:
        db.rollback()


def test_dq_rule_violation_honors_explicit_recipient_over_owner() -> None:
    db = _build_session()
    admin_user, owner_user, table, dq_rule = _seed_catalog(db, with_owner=True)
    dq_rule.notification_recipient_user_id = admin_user.id
    db.add(dq_rule)
    db.commit()

    try:
        rule = db.get(DQRule, dq_rule.id)
        assert rule is not None

        notify_dq_rule_violation(
            db,
            rule=rule,
            table=table,
            violations_count=5,
            preview_rows=[{"id": 1, "category": None}],
            run_id=201,
            reporter_user_id=None,
        )

        inbox_items = db.scalars(select(UserInboxNotification)).all()
        assert len(inbox_items) == 2
        inbox_by_user = {item.user_id: item for item in inbox_items}
        assert set(inbox_by_user) == {admin_user.id, owner_user.id}
        assert inbox_by_user[admin_user.id].context_json is not None
        assert inbox_by_user[admin_user.id].context_json["recipient_reason"] == "explicit"
        assert inbox_by_user[owner_user.id].context_json is not None
        assert inbox_by_user[owner_user.id].context_json["recipient_reason"] == "data_owner"
    finally:
        db.rollback()


def test_dq_profile_issue_uses_admin_fallback_and_dedupes() -> None:
    db = _build_session()
    admin, _owner_user, table, _dq_rule = _seed_catalog(db, with_owner=False)
    assert table.data_owner is None

    previous_run = DQRun(datasource_id=1, table_id=table.id, status="success", execution_engine="spark")
    previous_metric = DQTableMetric(
        run=previous_run,
        table_id=table.id,
        row_count=100,
        column_count=2,
        completeness_pct_avg=92.0,
        dq_score=96.0,
        duplicates_count=0,
        failed_rules=0,
        metrics_json={},
    )
    current_run = DQRun(datasource_id=1, table_id=table.id, status="success", execution_engine="spark")
    current_metric = DQTableMetric(
        run=current_run,
        table_id=table.id,
        row_count=100,
        column_count=2,
        completeness_pct_avg=54.0,
        dq_score=52.0,
        duplicates_count=1,
        failed_rules=3,
        metrics_json={},
    )
    db.add_all([previous_run, previous_metric, current_run, current_metric])
    db.commit()
    db.refresh(current_run)
    db.refresh(current_metric)

    incident = handle_profiling_incident_signals(
        db,
        table=table,
        schema_name=table.schema.name,
        dq_run=current_run,
        table_metric=current_metric,
        reporter_user_id=None,
    )
    assert incident is not None

    inbox_items = db.scalars(select(UserInboxNotification)).all()
    assert len(inbox_items) == 1
    item = inbox_items[0]
    assert item.user_id == admin.id
    assert item.category == "data_quality"
    assert item.severity == "critical"
    assert item.source_entity_type == "dq_profile"
    assert item.href is not None and item.href.startswith("/data-quality?tableId=")
    assert item.context_json is not None
    assert item.context_json["kind"] == "dq_profile_issue"
    assert item.context_json["table_id"] == table.id
    assert "dq_score_below_60" in item.context_json["trigger_codes"]

    handle_profiling_incident_signals(
        db,
        table=table,
        schema_name=table.schema.name,
        dq_run=current_run,
        table_metric=current_metric,
        reporter_user_id=None,
    )
    inbox_items_after = db.scalars(select(UserInboxNotification)).all()
    assert len(inbox_items_after) == 1


if __name__ == "__main__":
    test_dq_rule_violation_creates_owner_inbox_notification_and_dedupes()
    test_dq_profile_issue_uses_admin_fallback_and_dedupes()
    print("dq inbox notifications tests: OK")
