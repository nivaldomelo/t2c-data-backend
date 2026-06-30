#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from sqlalchemy import text

# Ensure "app" package is importable when script is run directly.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from t2c_data.core.config import settings  # noqa: E402
from t2c_data.core.db import engine  # noqa: E402

PROTECTED_TABLES = {"users", "alembic_version"}


def _truncate_app_schema_tables(schema_name: str, excluded: set[str]) -> tuple[int, list[str]]:
    with engine.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT tablename
                FROM pg_tables
                WHERE schemaname = :schema_name
                ORDER BY tablename
                """
            ),
            {"schema_name": schema_name},
        ).fetchall()
        table_names = [str(r[0]) for r in rows]
        to_truncate = [name for name in table_names if name not in excluded]

        if not to_truncate:
            return 0, []

        quoted = ", ".join(f'"{schema_name}"."{name}"' for name in to_truncate)
        conn.execute(text(f"TRUNCATE TABLE {quoted} RESTART IDENTITY CASCADE"))
        return len(to_truncate), to_truncate

def main() -> None:
    parser = argparse.ArgumentParser(description="Reset test data (keeps users and alembic_version).")
    parser.add_argument("--force", action="store_true", help="Allow execution outside ENV=dev")
    args = parser.parse_args()

    if settings.env.lower() != "dev" and not args.force:
        raise SystemExit("Refusing reset: ENV is not 'dev'. Use --force to override.")

    truncated_count, truncated_tables = _truncate_app_schema_tables(settings.db_schema, PROTECTED_TABLES)

    print("Reset concluido.")
    print(
        f"Postgres schema={settings.db_schema}: {truncated_count} tabelas truncadas "
        f"(excluidas: {', '.join(sorted(PROTECTED_TABLES))})."
    )
    if truncated_tables:
        print("  Tabelas:", ", ".join(truncated_tables))


if __name__ == "__main__":
    main()
