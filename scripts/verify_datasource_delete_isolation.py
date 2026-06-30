from __future__ import annotations

from uuid import uuid4

from sqlalchemy import func, select

from t2c_data.core.db import SessionLocal
from t2c_data.models.catalog import ColumnEntity, DataSource, Database, Schema, TableEntity
from t2c_data.models.glossary import GlossaryAssignment, GlossaryTerm
from t2c_data.models.scan import ScanRun
from t2c_data.models.tag import Tag, TagAssignment
from t2c_data.services.datasource import hard_delete_datasource


def main() -> None:
    suffix = uuid4().hex[:8]
    a_name = f"ds_a_{suffix}"
    b_name = f"ds_b_{suffix}"

    with SessionLocal() as session:
        ds_a = DataSource(
            name=a_name,
            db_type="postgres",
            host="localhost",
            port=5432,
            database="andromeda",
            username="tester",
            password="secret-a",
            is_active=True,
        )
        ds_b = DataSource(
            name=b_name,
            db_type="postgres",
            host="localhost",
            port=5432,
            database="andromeda",
            username="tester",
            password="secret-b",
            is_active=True,
        )
        session.add_all([ds_a, ds_b])
        session.flush()

        db_a = Database(datasource_id=ds_a.id, name=f"db_a_{suffix}")
        db_b = Database(datasource_id=ds_b.id, name=f"db_b_{suffix}")
        session.add_all([db_a, db_b])
        session.flush()

        sch_a = Schema(database_id=db_a.id, name=f"schema_a_{suffix}")
        sch_b = Schema(database_id=db_b.id, name=f"schema_b_{suffix}")
        session.add_all([sch_a, sch_b])
        session.flush()

        tbl_a = TableEntity(schema_id=sch_a.id, name=f"table_a_{suffix}", table_type="table")
        tbl_b = TableEntity(schema_id=sch_b.id, name=f"table_b_{suffix}", table_type="table")
        session.add_all([tbl_a, tbl_b])
        session.flush()

        session.add_all(
            [
                ColumnEntity(
                    table_id=tbl_a.id,
                    name="id",
                    data_type="integer",
                    is_nullable=False,
                    is_primary_key=True,
                    ordinal_position=1,
                ),
                ColumnEntity(
                    table_id=tbl_b.id,
                    name="id",
                    data_type="integer",
                    is_nullable=False,
                    is_primary_key=True,
                    ordinal_position=1,
                ),
            ]
        )

        session.add_all(
            [
                ScanRun(datasource_id=ds_a.id, status="done", summary={"ok": True}),
                ScanRun(datasource_id=ds_b.id, status="done", summary={"ok": True}),
            ]
        )

        tag = Tag(name=f"tag_{suffix}")
        term = GlossaryTerm(name=f"term_{suffix}", definition="definition")
        session.add_all([tag, term])
        session.flush()

        session.add_all(
            [
                TagAssignment(tag_id=tag.id, datasource_id=ds_a.id, entity_type="table", entity_id=tbl_a.id),
                TagAssignment(tag_id=tag.id, datasource_id=ds_b.id, entity_type="table", entity_id=tbl_b.id),
                GlossaryAssignment(
                    term_id=term.id,
                    datasource_id=ds_a.id,
                    entity_type="table",
                    entity_id=tbl_a.id,
                ),
                GlossaryAssignment(
                    term_id=term.id,
                    datasource_id=ds_b.id,
                    entity_type="table",
                    entity_id=tbl_b.id,
                ),
            ]
        )
        session.commit()
        ds_a_id = ds_a.id
        ds_b_id = ds_b.id

    with SessionLocal() as session:
        ok = hard_delete_datasource(session, ds_a_id)
        if not ok:
            raise SystemExit("Failed to delete datasource A")

    with SessionLocal() as session:
        counts = {
            "ds_a": session.scalar(select(func.count()).select_from(DataSource).where(DataSource.id == ds_a_id)),
            "ds_b": session.scalar(select(func.count()).select_from(DataSource).where(DataSource.id == ds_b_id)),
            "db_b": session.scalar(select(func.count()).select_from(Database).where(Database.datasource_id == ds_b_id)),
            "tables_b": session.scalar(
                select(func.count())
                .select_from(TableEntity)
                .join(Schema, TableEntity.schema_id == Schema.id)
                .join(Database, Schema.database_id == Database.id)
                .where(Database.datasource_id == ds_b_id)
            ),
            "scan_runs_b": session.scalar(select(func.count()).select_from(ScanRun).where(ScanRun.datasource_id == ds_b_id)),
            "tag_assign_b": session.scalar(
                select(func.count()).select_from(TagAssignment).where(TagAssignment.datasource_id == ds_b_id)
            ),
            "glossary_assign_b": session.scalar(
                select(func.count()).select_from(GlossaryAssignment).where(GlossaryAssignment.datasource_id == ds_b_id)
            ),
        }

    print("COUNTS", counts)
    assert counts["ds_a"] == 0
    assert counts["ds_b"] == 1
    assert counts["db_b"] == 1
    assert counts["tables_b"] == 1
    assert counts["scan_runs_b"] == 1
    assert counts["tag_assign_b"] == 1
    assert counts["glossary_assign_b"] == 1
    print("OK: datasource delete is isolated")


if __name__ == "__main__":
    main()
