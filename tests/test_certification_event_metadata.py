from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from starlette.requests import Request
from sqlalchemy import create_engine, event, select
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.api.certification import _record_certification_event, decide_table_certification, submit_table_certification
from t2c_data.models import Base
from t2c_data.models.auth import User
from t2c_data.models.audit import AuditLog
from t2c_data.models.catalog import DataSource, Database, Schema, TableEntity
from t2c_data.models.governance import CertificationDecisionEvent
from t2c_data.schemas.catalog import TableCertificationDecisionIn, TableCertificationSubmitIn, TableCertificationSummaryOut


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


def _summary(status: str, *, readiness_score: int, checklist: list[dict[str, str | bool]]) -> TableCertificationSummaryOut:
    now = datetime.now(timezone.utc)
    return TableCertificationSummaryOut(
        id=1,
        name="products",
        schema_name="bronze",
        database_name="andromeda",
        datasource_name="local-andromeda",
        owner="Owner",
        owner_email="owner@andromeda.local",
        data_owner_id=None,
        data_owner=None,
        certification_status=status,
        certification_criticality="high",
        certification_badges=[],
        certification_notes=None,
        certification_status_source="automatic",
        certification_status_rule="automatic_readiness_not_eligible",
        certification_status_reason="Prontidão insuficiente.",
        certification_submitted_by_user_id=None,
        certification_submitted_by_user_name=None,
        certification_submitted_by_user_email=None,
        certification_submitted_at=None,
        certification_decided_by_user_id=None,
        certification_decided_by_user_name=None,
        certification_decided_by_user_email=None,
        certification_decided_at=now,
        certification_review_at=now,
        certification_expires_at=now,
        certification_sla_due_at=None,
        certification_sla_status="within_sla",
        certification_sla_label="Dentro do SLA",
        certification_revalidation_required=status == "revalidation_pending",
        certification_next_step="Revisar checklist",
        active_dq_violation=status == "revalidation_pending",
        active_dq_violation_count=2 if status == "revalidation_pending" else 0,
        active_dq_rule_names=["Preco maior que zero"] if status == "revalidation_pending" else [],
        owner_reviewed_by_user_id=None,
        owner_reviewed_by_user_name=None,
        owner_reviewed_by_user_email=None,
        owner_reviewed_at=None,
        certification_status_label="Pendente de revalidação" if status == "revalidation_pending" else "Elegível",
        trust_score=70,
        trust_label="Atenção",
        trust_tone="warning",
        readiness_score=readiness_score,
        readiness_completed=5,
        readiness_total=8,
        eligible_for_certification=status in {"eligible", "revalidation_pending", "certified"},
        checklist=checklist,
        created_at=now,
        updated_at=now,
    )


def test_certification_event_stores_effective_status_and_pending_checklist() -> None:
    db = _build_session()
    datasource = DataSource(name="local-andromeda", db_type="postgres", host="localhost", port=5432, database="andromeda", username="catalog")
    datasource.password = "secret"
    database = Database(name="andromeda", datasource=datasource)
    schema = Schema(name="bronze", database=database)
    table = TableEntity(name="products", table_type="table", schema=schema, certification_status="eligible")
    user = User(email="reviewer@andromeda.local", password_hash="hash", name="Reviewer", full_name="Reviewer User", is_active=True)
    db.add_all([datasource, database, schema, table, user])
    db.commit()
    db.refresh(table)
    db.refresh(user)

    before_summary = _summary(
        "eligible",
        readiness_score=82,
        checklist=[
            {"key": "owner_defined", "label": "Owner definido", "passed": True, "detail": "Owner informado."},
        ],
    )
    after_summary = _summary(
        "revalidation_pending",
        readiness_score=71,
        checklist=[
            {"key": "owner_defined", "label": "Owner definido", "passed": True, "detail": "Owner informado."},
            {"key": "dq_score", "label": "Sem DQ", "passed": False, "detail": "Score abaixo do mínimo."},
        ],
    )

    event = _record_certification_event(
        db=db,
        table=table,
        user=user,
        before_summary=before_summary,
        after_summary=after_summary,
        previous_status="eligible",
        new_status="revalidation_pending",
        notes="Revalidação operacional.",
        decision_source="manual",
        explicit_reason=None,
    )

    assert event.metadata_json is not None
    assert event.metadata_json["effective_status"] == "revalidation_pending"
    assert event.metadata_json["readiness_completed"] == 5
    assert event.metadata_json["readiness_total"] == 8
    assert event.metadata_json["active_dq_violation_count"] == 2
    assert event.metadata_json["active_dq_rule_names"] == ["Preco maior que zero"]
    assert len(event.metadata_json["pending_checklist"]) == 1
    assert event.metadata_json["primary_pending_check"]["key"] == "dq_score"


def _request(path: str) -> Request:
    return Request(
        {
            "type": "http",
            "method": "POST",
            "path": path,
            "headers": [],
            "client": ("127.0.0.1", 1234),
            "scheme": "http",
            "server": ("testserver", 80),
        }
    )


def test_certification_submit_and_decision_persist_workflow_and_audit() -> None:
    db = _build_session()
    datasource = DataSource(name="local-andromeda", db_type="postgres", host="localhost", port=5432, database="andromeda", username="catalog")
    datasource.password = "secret"
    database = Database(name="andromeda", datasource=datasource)
    schema = Schema(name="bronze", database=database)
    now = datetime.now(timezone.utc)
    table = TableEntity(
        name="products",
        table_type="table",
        schema=schema,
        owner="Owner",
        owner_email="owner@andromeda.local",
        certification_status="eligible",
        certification_criticality="high",
        owner_reviewed_at=now,
        privacy_reviewed_at=now,
    )
    user = User(email="reviewer@andromeda.local", password_hash="hash", name="Reviewer", full_name="Reviewer User", is_active=True)
    db.add_all([datasource, database, schema, table, user])
    db.commit()
    db.refresh(table)
    db.refresh(user)

    checklist = [{"key": f"k{i}", "label": f"Check {i}", "passed": True, "detail": "ok"} for i in range(10)]
    with (
        patch("t2c_data.features.certification.api_support.build_certification_checklist", return_value=(checklist, 10, True)),
        patch("t2c_data.features.certification.api_support._has_open_critical_incident", return_value=False),
        patch("t2c_data.features.certification.api_support._active_dq_violation_summary", return_value=(False, 0, [])),
    ):
        submitted = submit_table_certification(
            table_id=table.id,
            payload=TableCertificationSubmitIn(certification_notes="Solicitação formal de certificação."),
            request=_request(f"/api/v1/certification/tables/{table.id}/submit"),
            db=db,
            user=user,
        )
        review_at = datetime.now(timezone.utc) + timedelta(days=30)
        expires_at = datetime.now(timezone.utc) + timedelta(days=90)
        decided = decide_table_certification(
            table_id=table.id,
            payload=TableCertificationDecisionIn(
                decision="certified",
                certification_notes="Aprovado pelo comitê.",
                certification_review_at=review_at,
                certification_expires_at=expires_at,
            ),
            request=_request(f"/api/v1/certification/tables/{table.id}/decision"),
            db=db,
            user=user,
        )

    assert submitted.certification_status == "in_review"
    assert submitted.certification_submitted_by_user_id == user.id
    assert submitted.certification_review_at is not None
    assert decided.certification_status == "certified"
    assert decided.certification_decided_by_user_id == user.id
    assert decided.certification_review_at == review_at.replace(tzinfo=None)
    assert decided.certification_expires_at == expires_at.replace(tzinfo=None)

    events = db.scalars(
        select(CertificationDecisionEvent)
        .where(CertificationDecisionEvent.asset_id == table.id)
        .order_by(CertificationDecisionEvent.id.asc())
    ).all()
    assert len(events) == 2
    assert events[0].new_status == "in_review"
    assert events[0].decision_type == "status_change"
    assert events[0].reviewer_user_id == user.id
    assert events[1].new_status == "certified"
    assert events[1].decision_type == "certification"
    assert events[1].valid_until == expires_at.replace(tzinfo=None)
    assert events[1].revalidation_due_at == review_at.replace(tzinfo=None)
    assert events[1].metadata_json is not None
    assert events[1].metadata_json["workflow_stage"] == "certified"
    assert events[1].metadata_json["workflow_gates_pending"] == 0

    audit_rows = db.scalars(
        select(AuditLog)
        .where(AuditLog.action == "table.certification.patch", AuditLog.entity_id == str(table.id))
        .order_by(AuditLog.id.asc())
    ).all()
    assert audit_rows
    assert any(row.field_name == "certification_status" for row in audit_rows)
    assert any((row.metadata_json or {}).get("message") == "Table certification updated" for row in audit_rows)


if __name__ == "__main__":
    test_certification_event_stores_effective_status_and_pending_checklist()
    print("certification event metadata tests: OK")
