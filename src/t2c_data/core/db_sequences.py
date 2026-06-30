from __future__ import annotations

import re
from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.orm import Session


_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_identifier(value: str, *, label: str) -> str:
    if not _IDENTIFIER_RE.fullmatch(value):
        raise ValueError(f"{label} inválido para operação de sequence: {value!r}")
    return value


def _quoted(identifier: str) -> str:
    safe = _validate_identifier(identifier, label="identificador")
    return f'"{safe}"'


@dataclass(frozen=True)
class SequenceAlignmentResult:
    table_name: str
    column_name: str
    sequence_name: str
    max_value: int
    created_sequence: bool


def align_integer_pk_sequence(
    session: Session,
    *,
    schema: str,
    table_name: str,
    column_name: str = "id",
    use_advisory_lock: bool = True,
) -> SequenceAlignmentResult:
    safe_schema = _validate_identifier(schema, label="schema")
    safe_table = _validate_identifier(table_name, label="tabela")
    safe_column = _validate_identifier(column_name, label="coluna")
    qualified_table_literal = f"{safe_schema}.{safe_table}"
    quoted_table = f"{_quoted(safe_schema)}.{_quoted(safe_table)}"
    quoted_column = _quoted(safe_column)

    if use_advisory_lock:
        session.execute(
            text("SELECT pg_advisory_xact_lock(hashtext(:lock_key))"),
            {"lock_key": f"{qualified_table_literal}.{safe_column}"},
        )

    sequence_name = session.scalar(
        text("SELECT pg_get_serial_sequence(:qualified_table, :column_name)"),
        {"qualified_table": qualified_table_literal, "column_name": safe_column},
    )

    created_sequence = False
    if not sequence_name:
        sequence_base_name = f"{safe_table}_{safe_column}_seq"
        quoted_sequence = f"{_quoted(safe_schema)}.{_quoted(sequence_base_name)}"
        sequence_name = f"{safe_schema}.{sequence_base_name}"
        session.execute(text(f"CREATE SEQUENCE IF NOT EXISTS {quoted_sequence}"))
        session.execute(text(f"ALTER SEQUENCE {quoted_sequence} OWNED BY {quoted_table}.{quoted_column}"))
        session.execute(
            text(
                f"ALTER TABLE {quoted_table} "
                f"ALTER COLUMN {quoted_column} "
                f"SET DEFAULT nextval('{sequence_name}'::regclass)"
            )
        )
        created_sequence = True

    max_value = int(
        session.scalar(text(f"SELECT COALESCE(MAX({quoted_column}), 0) FROM {quoted_table}")) or 0
    )
    if max_value > 0:
        session.execute(
            text("SELECT setval(to_regclass(:sequence_name), :target_value, true)"),
            {"sequence_name": sequence_name, "target_value": max_value},
        )
    else:
        session.execute(
            text("SELECT setval(to_regclass(:sequence_name), 1, false)"),
            {"sequence_name": sequence_name},
        )

    return SequenceAlignmentResult(
        table_name=safe_table,
        column_name=safe_column,
        sequence_name=sequence_name,
        max_value=max_value,
        created_sequence=created_sequence,
    )
