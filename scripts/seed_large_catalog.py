"""Seed a large synthetic catalog for scale validation.

This script is intentionally manual and should not be executed from unit tests.

Example:
    python backend/scripts/seed_large_catalog.py --tables 5000 --columns-per-table 20
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone

from sqlalchemy import insert, select

from t2c_data.core.db import SessionLocal
from t2c_data.models.catalog import ColumnEntity, DataOwner, DataSource, Database, Schema, TableEntity


def _chunked(items: list[dict], size: int):
    for idx in range(0, len(items), size):
        yield items[idx : idx + size]


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed large synthetic catalog data for scale validation.")
    parser.add_argument("--tables", type=int, default=5000)
    parser.add_argument("--columns-per-table", type=int, default=20)
    parser.add_argument("--owners", type=int, default=250)
    parser.add_argument("--schemas", type=int, default=25)
    parser.add_argument("--chunk-size", type=int, default=1000)
    args = parser.parse_args()

    now = datetime.now(timezone.utc)
    session = SessionLocal()
    try:
        datasource = session.scalar(select(DataSource).where(DataSource.name == "scale-benchmark-datasource"))
        if datasource is None:
            datasource = DataSource(
                name="scale-benchmark-datasource",
                db_type="postgres",
                host="benchmark.local",
                port=5432,
                database="benchmark",
                username="benchmark",
            )
            datasource.password = "benchmark"
            session.add(datasource)
            session.flush()

        database = session.scalar(select(Database).where(Database.datasource_id == datasource.id, Database.name == "scale_benchmark"))
        if database is None:
            database = Database(datasource_id=datasource.id, name="scale_benchmark", owner="bench", lifecycle_status="active")
            session.add(database)
            session.flush()

        existing_owner_emails = {
            row[0]
            for row in session.execute(
                select(DataOwner.email).where(DataOwner.email.like("scale-owner-%@example.local"))
            ).all()
        }
        owner_rows: list[dict] = []
        for idx in range(1, args.owners + 1):
            email = f"scale-owner-{idx:04d}@example.local"
            if email in existing_owner_emails:
                continue
            owner_rows.append(
                {
                    "name": f"Scale Owner {idx:04d}",
                    "email": email,
                    "area": f"domain-{idx % 12:02d}",
                    "description": "Synthetic owner for scale validation",
                    "is_active": idx % 17 != 0,
                    "created_at": now,
                    "updated_at": now,
                }
            )
        if owner_rows:
            for chunk in _chunked(owner_rows, args.chunk_size):
                session.execute(insert(DataOwner), chunk)
            session.commit()

        owners = session.scalars(
            select(DataOwner).where(DataOwner.email.like("scale-owner-%@example.local")).order_by(DataOwner.id.asc())
        ).all()
        owner_ids = [owner.id for owner in owners]

        schema_names = [f"scale_schema_{idx:03d}" for idx in range(1, args.schemas + 1)]
        existing_schemas = {
            row[0]
            for row in session.execute(select(Schema.name).where(Schema.database_id == database.id, Schema.name.in_(schema_names))).all()
        }
        for schema_name in schema_names:
            if schema_name in existing_schemas:
                continue
            session.add(Schema(database_id=database.id, name=schema_name, owner="bench", lifecycle_status="active"))
        session.commit()
        schemas = session.scalars(select(Schema).where(Schema.database_id == database.id).order_by(Schema.id.asc())).all()

        existing_table_names = {
            row[0]
            for row in session.execute(
                select(TableEntity.name)
                .join(Schema, TableEntity.schema_id == Schema.id)
                .where(Schema.database_id == database.id, TableEntity.name.like("scale_table_%"))
            ).all()
        }

        table_rows: list[dict] = []
        target_table_names: list[str] = []
        for idx in range(1, args.tables + 1):
            target_table_names.append(f"scale_table_{idx:05d}")
            if target_table_names[-1] in existing_table_names:
                continue
            schema = schemas[(idx - 1) % len(schemas)]
            has_personal = idx % 5 == 0
            has_sensitive = idx % 17 == 0
            status = "certified" if idx % 7 == 0 else "in_review" if idx % 3 == 0 else "not_eligible"
            table_rows.append(
                {
                    "schema_id": schema.id,
                    "data_owner_id": owner_ids[(idx - 1) % len(owner_ids)] if owner_ids and idx % 11 != 0 else None,
                    "name": f"scale_table_{idx:05d}",
                    "table_type": "BASE TABLE",
                    "description_manual": f"Synthetic scale table {idx}",
                    "owner": f"Scale Owner {(idx - 1) % len(owner_ids) + 1:04d}" if owner_ids and idx % 11 != 0 else None,
                    "owner_email": f"scale-owner-{(idx - 1) % len(owner_ids) + 1:04d}@example.local" if owner_ids and idx % 11 != 0 else None,
                    "lifecycle_status": "active",
                    "certification_status": status,
                    "certification_criticality": "critical" if idx % 19 == 0 else "high" if idx % 9 == 0 else "medium",
                    "certification_review_at": now - timedelta(days=idx % 120),
                    "certification_expires_at": now + timedelta(days=30 - (idx % 20)),
                    "has_personal_data": has_personal or has_sensitive,
                    "has_sensitive_personal_data": has_sensitive,
                    "legal_basis": "legitimate_interest" if has_personal or has_sensitive else None,
                    "privacy_purpose": "Synthetic scale benchmark" if has_personal or has_sensitive else None,
                    "retention_policy": "365d" if has_personal or has_sensitive else None,
                    "access_scope": "restricted" if has_sensitive else "authenticated" if has_personal else "public",
                    "sensitivity_level": "sensitive" if has_sensitive else "personal" if has_personal else "internal",
                    "privacy_reviewed_at": now - timedelta(days=idx % 90) if has_personal or has_sensitive else None,
                    "owner_reviewed_at": now - timedelta(days=idx % 60),
                    "schema_hash": f"scale-hash-{idx:05d}",
                    "created_at": now,
                    "updated_at": now,
                }
            )
        if table_rows:
            for chunk in _chunked(table_rows, args.chunk_size):
                session.execute(insert(TableEntity), chunk)
            session.commit()

        tables = session.execute(
            select(TableEntity.id, TableEntity.name)
            .join(Schema, TableEntity.schema_id == Schema.id)
            .where(Schema.database_id == database.id, TableEntity.name.in_(target_table_names))
            .order_by(TableEntity.id.asc())
        ).all()
        table_by_name = {name: table_id for table_id, name in tables}

        existing_column_table_ids = {
            row[0]
            for row in session.execute(
                select(ColumnEntity.table_id)
                .where(ColumnEntity.name == "col_001", ColumnEntity.table_id.in_(list(table_by_name.values())))
                .distinct()
            ).all()
        }
        column_rows: list[dict] = []
        for idx in range(1, args.tables + 1):
            table_name = f"scale_table_{idx:05d}"
            table_id = table_by_name.get(table_name)
            if table_id is None or table_id in existing_column_table_ids:
                continue
            for ordinal in range(1, args.columns_per_table + 1):
                column_rows.append(
                    {
                        "table_id": table_id,
                        "data_owner_id": owner_ids[(idx - 1) % len(owner_ids)] if owner_ids and idx % 11 != 0 else None,
                        "name": f"col_{ordinal:03d}",
                        "data_type": "text" if ordinal % 5 else "timestamp",
                        "is_primary_key": ordinal == 1,
                        "is_nullable": ordinal % 7 != 0,
                        "ordinal_position": ordinal,
                        "description_manual": f"Column {ordinal} of {table_name}",
                        "dictionary_description": f"Synthetic dictionary description for {table_name}.{ordinal}",
                        "created_at": now,
                        "updated_at": now,
                    }
                )
                if len(column_rows) >= args.chunk_size:
                    session.execute(insert(ColumnEntity), column_rows)
                    session.commit()
                    column_rows = []
        if column_rows:
            session.execute(insert(ColumnEntity), column_rows)
            session.commit()

        print(
            {
                "datasource_id": datasource.id,
                "database_id": database.id,
                "schemas": len(schemas),
                "tables_target": args.tables,
                "columns_target": args.tables * args.columns_per_table,
                "owners_target": args.owners,
            }
        )
    finally:
        session.close()


if __name__ == "__main__":
    main()
