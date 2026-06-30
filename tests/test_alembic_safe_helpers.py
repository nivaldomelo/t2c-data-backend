from __future__ import annotations

from sqlalchemy import Column, Integer, MetaData, String, Table, create_engine, inspect
from alembic.migration import MigrationContext
from alembic.operations import Operations

import t2c_data.core.alembic_safe as alembic_safe


def test_safe_migration_helpers_are_idempotent() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    metadata = MetaData()
    Table("demo", metadata, Column("id", Integer, primary_key=True))
    metadata.create_all(engine)

    with engine.begin() as connection:
        original_op = alembic_safe.op
        try:
            alembic_safe.op = Operations(MigrationContext.configure(connection))

            assert alembic_safe.column_exists(connection, "demo", "id") is True
            assert alembic_safe.column_exists(connection, "demo", "reason_code") is False

            assert alembic_safe.safe_add_column(connection, "demo", Column("reason_code", String(80))) is True
            assert alembic_safe.safe_add_column(connection, "demo", Column("reason_code", String(80))) is False

            assert alembic_safe.safe_create_table(
                connection,
                "demo_history",
                Column("id", Integer, primary_key=True),
                Column("reason_code", String(80)),
            ) is True
            assert alembic_safe.safe_create_table(
                connection,
                "demo_history",
                Column("id", Integer, primary_key=True),
                Column("reason_code", String(80)),
            ) is False

            assert alembic_safe.safe_create_index(connection, "ix_demo_reason_code", "demo", ["reason_code"]) is True
            assert alembic_safe.safe_create_index(connection, "ix_demo_reason_code", "demo", ["reason_code"]) is False
        finally:
            alembic_safe.op = original_op

    inspector = inspect(engine)
    assert "reason_code" in [col["name"] for col in inspector.get_columns("demo")]
    assert "demo_history" in inspector.get_table_names()
    assert "ix_demo_reason_code" in [index["name"] for index in inspector.get_indexes("demo")]
