from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import or_, select
from sqlalchemy.orm import Session, selectinload

from t2c_data.features.dashboard.profile_loader import load_table_profiles
from t2c_data.features.governance.scoring import build_governance_score_for_profile
from t2c_data.features.governance.settings import get_governance_settings_snapshot
from t2c_data.features.governance.trust_score import build_trust_score_for_profile
from t2c_data.features.privacy_access import can_view_table
from t2c_data.models.catalog import ColumnEntity, Database, Schema, TableEntity
from t2c_data.schemas.catalog import ExplorerSearchResultOut


def search_tree(*, db: Session, q: str, limit: int, current_user, governance_maturity: str | None = None) -> list[ExplorerSearchResultOut]:
    pattern = f"%{q}%"
    results: list[ExplorerSearchResultOut] = []
    seen: set[tuple[str, int | None, int | None]] = set()
    settings_snapshot = get_governance_settings_snapshot(db)
    maturity_filter = (governance_maturity or "").strip().lower() or None

    def _matches_maturity(score_payload: dict[str, object] | None) -> bool:
        if maturity_filter is None:
            return True
        if not score_payload:
            return False
        label = str(score_payload.get("label") or "").lower()
        normalized = {
            "forte": "forte",
            "boa": "boa",
            "em evolução": "em evolução",
            "em evolucao": "em evolução",
            "crítica": "crítica",
            "critica": "crítica",
        }.get(maturity_filter, maturity_filter)
        return label == normalized

    table_ids_for_scores: set[int] = set()

    schemas = db.execute(
        select(Schema.id, Schema.name, Database.datasource_id)
        .join(Database, Schema.database_id == Database.id)
        .where(Schema.name.ilike(pattern))
        .limit(limit)
    ).all()
    if maturity_filter is None:
        for schema_id, schema_name, datasource_id in schemas:
            schema_tables = db.scalars(
                select(TableEntity)
                .where(TableEntity.schema_id == schema_id)
                .options(selectinload(TableEntity.data_owner))
                .limit(25)
            ).all()
            if not any(can_view_table(current_user, table) for table in schema_tables):
                continue
            table_ids_for_scores.update(table.id for table in schema_tables if can_view_table(current_user, table))
            key = ("schema", schema_id, None)
            if key in seen:
                continue
            seen.add(key)
            results.append(
                ExplorerSearchResultOut(
                    match_type="schema",
                    name=schema_name,
                    datasource_id=datasource_id,
                    schema_id=schema_id,
                    table_id=None,
                    column_name=None,
                )
            )

    tables = db.execute(
        select(TableEntity.id, TableEntity.name, Schema.id, Database.datasource_id)
        .join(Schema, TableEntity.schema_id == Schema.id)
        .join(Database, Schema.database_id == Database.id)
        .where(TableEntity.name.ilike(pattern))
        .limit(limit)
    ).all()
    for table_id, table_name, schema_id, datasource_id in tables:
        table = db.scalar(select(TableEntity).options(selectinload(TableEntity.data_owner)).where(TableEntity.id == table_id))
        if not table or not can_view_table(current_user, table):
            continue
        table_ids_for_scores.add(int(table_id))
    column_rows = db.execute(
        select(
            ColumnEntity.name,
            TableEntity.id,
            TableEntity.name,
            Schema.id,
            Database.datasource_id,
        )
        .join(TableEntity, ColumnEntity.table_id == TableEntity.id)
        .join(Schema, TableEntity.schema_id == Schema.id)
        .join(Database, Schema.database_id == Database.id)
        .where(
            or_(
                ColumnEntity.name.ilike(pattern),
                ColumnEntity.description_source.ilike(pattern),
                ColumnEntity.description_manual.ilike(pattern),
            )
        )
        .limit(limit)
    ).all()
    for _column_name, table_id, _table_name, _schema_id, _datasource_id in column_rows:
        table_ids_for_scores.add(int(table_id))

    profiles = {
        table.table_id: table
        for table in load_table_profiles(db, datetime.now(timezone.utc), table_ids=sorted(table_ids_for_scores))
    }
    governance_scores = {
        table_id: build_governance_score_for_profile(profile, settings_snapshot=settings_snapshot)
        for table_id, profile in profiles.items()
    }
    trust_scores = {
        table_id: build_trust_score_for_profile(profile, settings_snapshot=settings_snapshot)
        for table_id, profile in profiles.items()
    }

    for table_id, table_name, schema_id, datasource_id in tables:
        table = db.scalar(select(TableEntity).options(selectinload(TableEntity.data_owner)).where(TableEntity.id == table_id))
        if not table or not can_view_table(current_user, table):
            continue
        profile = profiles.get(int(table_id))
        score_payload = governance_scores.get(int(table_id))
        if not _matches_maturity(score_payload):
            continue
        key = ("table", schema_id, table_id)
        if key in seen:
            continue
        seen.add(key)
        results.append(
            ExplorerSearchResultOut(
                match_type="table",
                name=table_name,
                datasource_id=datasource_id,
                schema_id=schema_id,
                table_id=table_id,
                column_name=None,
                governance_score=int(score_payload["score"]) if score_payload else None,
                governance_label=str(score_payload["label"]) if score_payload else None,
                governance_tone=str(score_payload["tone"]) if score_payload else None,
                certification_status=table.certification_status,
                readiness_score=int(profile.readiness_score) if profile else None,
                trust_score=int(trust_scores.get(int(table_id)).score) if profile and int(table_id) in trust_scores else None,
                trust_label=str(trust_scores.get(int(table_id)).label) if profile and int(table_id) in trust_scores else None,
                trust_tone=str(trust_scores.get(int(table_id)).tone) if profile and int(table_id) in trust_scores else None,
                active_dq_violation=bool(profile.active_dq_violation) if profile else False,
                owner_defined=bool(profile.owner_defined) if profile else False,
            )
        )

    for column_name, table_id, table_name, schema_id, datasource_id in column_rows:
        table = db.scalar(select(TableEntity).options(selectinload(TableEntity.data_owner)).where(TableEntity.id == table_id))
        if not table or not can_view_table(current_user, table):
            continue
        profile = profiles.get(int(table_id))
        score_payload = governance_scores.get(int(table_id))
        if not _matches_maturity(score_payload):
            continue
        key = ("column", schema_id, table_id)
        if key in seen:
            continue
        seen.add(key)
        results.append(
            ExplorerSearchResultOut(
                match_type="column",
                name=table_name,
                datasource_id=datasource_id,
                schema_id=schema_id,
                table_id=table_id,
                column_name=column_name,
                governance_score=int(score_payload["score"]) if score_payload else None,
                governance_label=str(score_payload["label"]) if score_payload else None,
                governance_tone=str(score_payload["tone"]) if score_payload else None,
                certification_status=table.certification_status,
                readiness_score=int(profile.readiness_score) if profile else None,
                trust_score=int(trust_scores.get(int(table_id)).score) if profile and int(table_id) in trust_scores else None,
                trust_label=str(trust_scores.get(int(table_id)).label) if profile and int(table_id) in trust_scores else None,
                trust_tone=str(trust_scores.get(int(table_id)).tone) if profile and int(table_id) in trust_scores else None,
                active_dq_violation=bool(profile.active_dq_violation) if profile else False,
                owner_defined=bool(profile.owner_defined) if profile else False,
            )
        )

    return results[:limit]
