#!/usr/bin/env python3
from __future__ import annotations

import argparse

from sqlalchemy import select

from t2c_data.core.db import SessionLocal
from t2c_data.models.catalog import Database, Schema, TableEntity
from t2c_data.features.tags.intelligence import reprocess_table_tag_intelligence


def main() -> None:
    parser = argparse.ArgumentParser(description="Reprocess tag intelligence for catalog tables.")
    parser.add_argument("--datasource-id", type=int, default=None)
    parser.add_argument("--database-name", type=str, default=None)
    parser.add_argument("--schema-name", type=str, default=None)
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    with SessionLocal() as session:
        stmt = select(TableEntity.id).join(Schema, Schema.id == TableEntity.schema_id).join(Database, Database.id == Schema.database_id)
        if args.datasource_id is not None:
            stmt = stmt.where(Database.datasource_id == args.datasource_id)
        if args.database_name:
            stmt = stmt.where(Database.name == args.database_name)
        if args.schema_name:
            stmt = stmt.where(Schema.name == args.schema_name)
        if args.limit and args.limit > 0:
            stmt = stmt.limit(args.limit)
        table_ids = [int(row[0]) for row in session.execute(stmt).all()]

        processed: list[int] = []
        for table_id in table_ids:
            reprocess_table_tag_intelligence(
                session,
                table_id=table_id,
                actor_user_id=None,
                source_module="tags.script",
                metadata={"trigger": "batch_reprocess"},
            )
            processed.append(table_id)
        session.commit()

    print(f"Reprocessamento concluído. Tabelas processadas: {len(processed)}")


if __name__ == "__main__":
    main()
