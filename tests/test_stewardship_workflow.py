from __future__ import annotations

import os

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from fastapi import HTTPException
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.models import Base
from t2c_data.models.auth import User
from t2c_data.models.catalog import ColumnEntity, DataOwner, DataSource, Database, Schema, TableEntity
from t2c_data.schemas.stewardship import StewardshipDecisionIn, StewardshipRequestCreateIn
from t2c_data.features.stewardship.workflow import create_stewardship_request, decide_stewardship_request, get_stewardship_requests


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


def _seed_minimum_graph(db: Session) -> tuple[User, TableEntity, DataOwner]:
    user = User(email="admin@andromeda.local", password_hash="hash", name="Admin", full_name="Admin User", is_active=True)
    datasource = DataSource(name="local-andromeda", db_type="postgres", host="localhost", port=5432, database="andromeda", username="catalog")
    datasource.password = "secret"
    database = Database(name="andromeda", datasource=datasource)
    schema = Schema(name="bronze", database=database)
    table = TableEntity(name="audit_logs", table_type="table", schema=schema)
    owner = DataOwner(name="Nivaldo Melo", email="owner@andromeda.local", is_active=True)
    db.add_all([user, datasource, database, schema, table, owner])
    db.commit()
    db.refresh(user)
    db.refresh(table)
    db.refresh(owner)
    return user, table, owner


def test_stewardship_request_create_duplicate_and_approve() -> None:
    db = _build_session()
    user, table, _owner = _seed_minimum_graph(db)

    request_item = create_stewardship_request(
        db,
        payload=StewardshipRequestCreateIn(
            table_id=table.id,
            request_type="table_description",
            description_manual="Descrição proposta para auditoria operacional.",
            requester_comment="Precisamos esclarecer o propósito da tabela.",
        ),
        user=user,
        audit_kwargs=None,
    )

    assert request_item.status == "pending"
    assert request_item.events[-1].event_type == "created"

    try:
        create_stewardship_request(
            db,
            payload=StewardshipRequestCreateIn(
                table_id=table.id,
                request_type="table_description",
                description_manual="Outra proposta",
            ),
            user=user,
            audit_kwargs=None,
        )
        raise AssertionError("Expected duplicate pending request to be blocked")
    except HTTPException as exc:
        assert exc.status_code == 409

    approved = decide_stewardship_request(
        db,
        request_id=request_item.id,
        decision="approved",
        actor=user,
        payload=StewardshipDecisionIn(decision_comment="Aprovação inicial do stewardship."),
        audit_kwargs=None,
    )

    db.refresh(table)
    assert approved.status == "approved"
    assert table.description_manual == "Descrição proposta para auditoria operacional."
    assert len(approved.events) == 2
    assert approved.events[-1].event_type == "approved"


def test_stewardship_connects_reviews_and_certification() -> None:
    db = _build_session()
    user, table, owner = _seed_minimum_graph(db)
    table.data_owner_id = owner.id
    table.description_manual = "Tabela de auditoria operacional com descrição suficiente para avaliação de certificação."
    db.add(
        ColumnEntity(
            table_id=table.id,
            name="event_id",
            data_type="varchar",
            is_primary_key=True,
            is_nullable=False,
            ordinal_position=1,
            dictionary_description="Identificador do evento operacional auditado.",
        )
    )
    db.commit()

    owner_review_request = create_stewardship_request(
        db,
        payload=StewardshipRequestCreateIn(
            table_id=table.id,
            request_type="owner_review",
            requester_comment="Confirmar accountability do ativo.",
        ),
        user=user,
        audit_kwargs=None,
    )
    certification_request = create_stewardship_request(
        db,
        payload=StewardshipRequestCreateIn(
            table_id=table.id,
            request_type="certification_review",
            requester_comment="Ativo pronto para entrar em revisão de certificação.",
        ),
        user=user,
        audit_kwargs=None,
    )

    payload = get_stewardship_requests(db, current_user=user)
    assert payload["inbox"]["pending_total"] == 2
    assert payload["inbox"]["review_pending"] == 1
    assert payload["inbox"]["certification_pending"] == 1
    assert payload["inbox"]["by_owner"][0]["count"] == 2

    decide_stewardship_request(
        db,
        request_id=owner_review_request.id,
        decision="approved",
        actor=user,
        payload=StewardshipDecisionIn(decision_comment="Owner confirmado."),
        audit_kwargs=None,
    )
    decide_stewardship_request(
        db,
        request_id=certification_request.id,
        decision="approved",
        actor=user,
        payload=StewardshipDecisionIn(decision_comment="Pode iniciar revisão."),
        audit_kwargs=None,
    )

    db.refresh(table)
    assert table.owner_reviewed_at is not None
    assert table.owner_reviewed_by_user_id == user.id
    assert table.certification_status == "in_review"
    assert table.certification_submitted_by_user_id == user.id
    assert table.certification_submitted_at is not None


if __name__ == "__main__":
    test_stewardship_request_create_duplicate_and_approve()
    test_stewardship_connects_reviews_and_certification()
    print("stewardship workflow tests: OK")
