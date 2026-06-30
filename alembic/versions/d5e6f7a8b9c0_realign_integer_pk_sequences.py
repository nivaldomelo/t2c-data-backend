"""realign integer primary key sequences

Revision ID: d5e6f7a8b9c0
Revises: c3d4e5f6a7b
Create Date: 2026-03-28 18:30:00.000000
"""

from __future__ import annotations

import re

from alembic import context, op
import sqlalchemy as sa

from t2c_data.core.config import settings


revision = "d5e6f7a8b9c0"
down_revision = "c3d4e5f6a7b"
branch_labels = None
depends_on = None

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _quoted(identifier: str) -> str:
    if not _IDENTIFIER_RE.fullmatch(identifier):
        raise ValueError(f"Identificador inválido para alinhamento de sequence: {identifier!r}")
    return f'"{identifier}"'


def _align_pk_sequence(schema: str, table_name: str, column_name: str) -> None:
    bind = op.get_bind()
    qualified_table = f"{schema}.{table_name}"
    sequence_name = bind.scalar(
        sa.text("SELECT pg_get_serial_sequence(:qualified_table, :column_name)"),
        {"qualified_table": qualified_table, "column_name": column_name},
    )

    if not sequence_name:
        sequence_base_name = f"{table_name}_{column_name}_seq"
        sequence_name = f"{schema}.{sequence_base_name}"
        bind.execute(sa.text(f"CREATE SEQUENCE IF NOT EXISTS {_quoted(schema)}.{_quoted(sequence_base_name)}"))
        bind.execute(
            sa.text(
                f"ALTER SEQUENCE {_quoted(schema)}.{_quoted(sequence_base_name)} "
                f"OWNED BY {_quoted(schema)}.{_quoted(table_name)}.{_quoted(column_name)}"
            )
        )
        bind.execute(
            sa.text(
                f"ALTER TABLE {_quoted(schema)}.{_quoted(table_name)} "
                f"ALTER COLUMN {_quoted(column_name)} "
                f"SET DEFAULT nextval('{sequence_name}'::regclass)"
            )
        )

    max_value = int(
        bind.scalar(
            sa.text(
                f"SELECT COALESCE(MAX({_quoted(column_name)}), 0) "
                f"FROM {_quoted(schema)}.{_quoted(table_name)}"
            )
        )
        or 0
    )
    if max_value > 0:
        bind.execute(
            sa.text("SELECT setval(to_regclass(:sequence_name), :target_value, true)"),
            {"sequence_name": sequence_name, "target_value": max_value},
        )
    else:
        bind.execute(
            sa.text("SELECT setval(to_regclass(:sequence_name), 1, false)"),
            {"sequence_name": sequence_name},
        )


def upgrade() -> None:
    if context.is_offline_mode():
        return

    schema = settings.db_schema
    bind = op.get_bind()
    rows = bind.execute(
        sa.text(
            """
            SELECT cls.relname AS table_name, att.attname AS column_name
            FROM pg_constraint con
            JOIN pg_class cls ON cls.oid = con.conrelid
            JOIN pg_namespace ns ON ns.oid = cls.relnamespace
            JOIN pg_attribute att ON att.attrelid = cls.oid AND att.attnum = con.conkey[1]
            WHERE con.contype = 'p'
              AND ns.nspname = :schema
              AND array_length(con.conkey, 1) = 1
              AND format_type(att.atttypid, att.atttypmod) IN ('smallint', 'integer', 'bigint')
            ORDER BY cls.relname
            """
        ),
        {"schema": schema},
    ).fetchall()

    for table_name, column_name in rows:
        _align_pk_sequence(schema, str(table_name), str(column_name))


def downgrade() -> None:
    # Reparação de sequence; não há alteração estrutural reversível útil aqui.
    return None
