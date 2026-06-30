from __future__ import annotations

import os
from datetime import datetime, timezone

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from sqlalchemy import create_engine, event
from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.features.collaboration.service import (
    build_collaboration_activity_summary,
    create_collaboration_comment,
    create_collaboration_task,
    update_collaboration_task,
)
from t2c_data.models import Base
from t2c_data.models.auth import User
from t2c_data.models.catalog import DataOwner, DataSource, Database, Schema, TableEntity
from t2c_data.models.dq import DQRule
from t2c_data.models.incident import Incident
from t2c_data.models.platform import DashboardAssetReadModel
from t2c_data.models.semantic import SemanticDataProduct, SemanticDomain
from t2c_data.schemas.collaboration import CollaborationCommentIn, CollaborationTaskIn, CollaborationTaskUpdateIn


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


def _create_user(db: Session, *, email: str = "collaboration@example.com", name: str = "Collaboration User") -> User:
    user = User(email=email, password_hash="hash", name=name, full_name=name, is_active=True)
    db.add(user)
    db.commit()
    return user


def _create_table(db: Session, *, owner_email: str) -> TableEntity:
    datasource = DataSource(name="source", db_type="postgres", host="localhost", port=5432, database="warehouse", username="catalog")
    datasource.password = "secret"
    database = Database(name="warehouse", datasource=datasource)
    schema = Schema(name="public", database=database)
    owner = DataOwner(name="Owner", email=owner_email, area="Analytics", is_active=True)
    table = TableEntity(
        name="orders",
        table_type="table",
        schema=schema,
        data_owner=owner,
        owner="Owner",
        owner_email=owner_email,
    )
    db.add_all([datasource, database, schema, owner, table])
    db.commit()
    return table


def _create_domain_and_product(db: Session, *, owner_email: str) -> tuple[SemanticDomain, SemanticDataProduct]:
    domain = SemanticDomain(
        slug="sales",
        name="Sales",
        description="Domínio comercial",
        owner=owner_email,
        steward=None,
        criticality="high",
        maturity_status="growing",
        quality_score=78,
        governance_score=72,
        is_active=True,
    )
    product = SemanticDataProduct(
        slug="orders",
        name="Orders",
        description="Produto de dados de pedidos",
        owner=owner_email,
        steward=None,
        consumers=["bi", "analytics"],
        sla_text="Atualização diária",
        contract_text="Schema compatível v1",
        maturity_status="growing",
        quality_score=82,
        governance_score=75,
        is_active=True,
    )
    domain.products.append(product)
    db.add(domain)
    db.commit()
    return domain, product


def _create_asset_read_model(db: Session, table: TableEntity, domain: SemanticDomain) -> None:
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
            owner_defined=False,
            description_complete=False,
            dictionary_complete=False,
            classification_defined=False,
            tags_count=0,
            terms_count=0,
            search_clicks_30d=0,
            active_dq_rules_count=0,
            recent_dq_failure_runs_30d=0,
            certification_status="not_eligible",
            certification_criticality=None,
            certification_badges=[],
            certification_decided_at=None,
            certification_review_at=None,
            certification_expires_at=None,
            review_recent=False,
            dq_score=72,
            completeness_pct_avg=84.5,
            freshness_seconds=7200,
            open_incidents=0,
            critical_open_incidents=0,
            owner_name=table.owner,
            data_owner_id=table.data_owner_id,
            domain_name=domain.name,
            sensitivity_level=None,
            has_personal_data=False,
            has_sensitive_personal_data=False,
            owner_reviewed_at=None,
            privacy_reviewed_at=None,
            last_review_at=None,
            last_sync_at=None,
            last_updated_at=None,
        )
    )
    db.commit()


def _create_incident(db: Session, *, owner_id: int, reporter_id: int) -> Incident:
    incident = Incident(
        title="Falha recorrente de ingestão",
        description="Incidente criado para validação de colaboração",
        entity_type="table",
        table_fqn="source.warehouse.public.orders",
        detected_at=datetime.now(timezone.utc),
        status="open",
        severity="sev1",
        source_type="dq",
        source_ref_id=10,
        domain_name="Sales",
        owner_team="Data",
        squad_name="Platform",
        recurrence_count=0,
        occurrences=1,
        owner_user_id=owner_id,
        reporter_user_id=reporter_id,
        tags=["collaboration"],
    )
    db.add(incident)
    db.commit()
    return incident


def _create_dq_rule(db: Session, table: TableEntity) -> DQRule:
    rule = DQRule(
        table_fqn="source.warehouse.public.orders",
        name="Completeness mínima",
        description="Regra criada para a camada colaborativa",
        rule_type="row_violation",
        severity="high",
        is_active=True,
    )
    db.add(rule)
    db.commit()
    return rule


def test_collaboration_task_comment_and_summary_aggregates_operational_signals() -> None:
    db = _build_session()
    user = _create_user(db)
    table = _create_table(db, owner_email=user.email)
    domain, _product = _create_domain_and_product(db, owner_email=user.email)
    _create_asset_read_model(db, table, domain)
    incident = _create_incident(db, owner_id=user.id, reporter_id=user.id)

    task = create_collaboration_task(
        db,
        payload=CollaborationTaskIn(
            entity_type="table",
            entity_id=table.id,
            entity_label="source.warehouse.public.orders",
            title="Revisar owner e documentação",
            description="Tarefa para validar ownership e descrição do ativo.",
            task_type="update_documentation",
            status="open",
            priority="high",
            responsibility_role="steward",
            assigned_to_user_id=user.id,
            due_at=datetime.now(timezone.utc),
            comment="Abrir revisão com o time de domínio.",
        ),
        current_user=user,
        audit_kwargs={},
    )

    assert task.id is not None

    comment = create_collaboration_comment(
        db,
        payload=CollaborationCommentIn(
            entity_type="incident",
            entity_id=incident.id,
            entity_label=incident.title,
            body="Incidente correlacionado com a fila colaborativa.",
            comment_kind="decision",
            context_json={"severity": incident.severity},
        ),
        current_user=user,
        audit_kwargs={},
    )

    assert comment.id is not None

    summary = build_collaboration_activity_summary(db)
    assert summary["total_tasks"] >= 1
    assert summary["total_comments"] >= 2
    assert summary["open_tasks"] >= 1
    assert summary["recent_events"] >= 3
    assert summary["assets_without_owner"] >= 1
    assert summary["domains_without_steward"] >= 1
    assert summary["documentation_stale"] >= 1


def test_collaboration_resolves_incident_dq_rule_domain_and_product_entities() -> None:
    db = _build_session()
    user = _create_user(db, email="ops@example.com", name="Ops User")
    table = _create_table(db, owner_email=user.email)
    domain, product = _create_domain_and_product(db, owner_email=user.email)
    incident = _create_incident(db, owner_id=user.id, reporter_id=user.id)
    dq_rule = _create_dq_rule(db, table)

    task = create_collaboration_task(
        db,
        payload=CollaborationTaskIn(
            entity_type="semantic_product",
            entity_id=product.id,
            entity_label=product.name,
            title="Revisar contrato do produto",
            description="Validar cobertura de contrato e consumidores.",
            task_type="review_contract",
            status="in_progress",
            priority="medium",
            responsibility_role="product_owner",
            assigned_to_user_id=user.id,
        ),
        current_user=user,
        audit_kwargs={},
    )

    assert task.entity_type == "semantic_product"
    assert task.entity_label == product.name

    incident_comment = create_collaboration_comment(
        db,
        payload=CollaborationCommentIn(
            entity_type="incident",
            entity_id=incident.id,
            entity_label=incident.title,
            body="Comentário no incidente para validar o fluxo colaborativo.",
        ),
        current_user=user,
        audit_kwargs={},
    )
    dq_comment = create_collaboration_comment(
        db,
        payload=CollaborationCommentIn(
            entity_type="dq_rule",
            entity_id=dq_rule.id,
            entity_label=dq_rule.name,
            body="Validar a regra com o steward e o responsável pela qualidade.",
        ),
        current_user=user,
        audit_kwargs={},
    )

    assert incident_comment.entity_type == "incident"
    assert dq_comment.entity_type == "dq_rule"

    updated = update_collaboration_task(
        db,
        task_id=task.id,
        payload=CollaborationTaskUpdateIn(status="done", priority="high", title="Revisar contrato do produto"),
        current_user=user,
        audit_kwargs={},
    )

    assert updated.status == "done"
    assert updated.completed_at is not None
