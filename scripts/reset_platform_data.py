#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy.engine.url import make_url
from sqlalchemy.orm import Session

# Ensure "app" package is importable when script is run directly.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from t2c_data.core.config import settings  # noqa: E402
from t2c_data.core.db import engine  # noqa: E402
from t2c_data.core.platform_data_reset import (  # noqa: E402
    build_reset_plan,
    truncate_tables,
    validate_reset_plan,
    validate_post_reset_state,
)

SAFE_ENVIRONMENTS = {"dev", "development", "local", "test"}


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Reset platform data safely while preserving authentication tables."
    )
    parser.add_argument(
        "--confirm-reset",
        action="store_true",
        help="Required to execute the reset. Without it, the script only prints the diagnosis and plan.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Allow execution outside dev/local/test environments.",
    )
    parser.add_argument(
        "--schema",
        default=settings.db_schema,
        help=f"Schema to clean (default: {settings.db_schema}).",
    )
    parser.add_argument(
        "--backup-file",
        default=None,
        help="Optional file path for a pg_dump backup before the reset.",
    )
    return parser


def _database_url_for_backup() -> str:
    url = make_url(settings.database_url)
    if url.drivername.startswith("sqlite"):
        raise RuntimeError("pg_dump backup is not supported for SQLite databases.")
    return str(url.set(drivername="postgresql"))


def _suggest_backup_command(backup_path: Path) -> str:
    return f'pg_dump "$DATABASE_URL" > {backup_path}'


def _run_backup(backup_path: Path) -> None:
    backup_path.parent.mkdir(parents=True, exist_ok=True)
    command = ["pg_dump", _database_url_for_backup(), "-f", str(backup_path)]
    subprocess.run(command, check=True)


def _print_plan(
    schema_name: str,
    available_tables: list[str],
    preserved_tables: list[str],
    missing_preserved_tables: list[str],
    truncated_tables: list[str],
    backup_path: Path | None,
) -> None:
    print("Diagnóstico do reset da plataforma")
    print(f"ENV: {settings.env}")
    print(f"Schema alvo: {schema_name}")
    print(f"Tabelas detectadas: {len(available_tables)}")
    print()
    print("Tabelas preservadas:")
    for table_name in preserved_tables:
        print(f"- {table_name}")
    if missing_preserved_tables:
        print("Tabelas preservadas não encontradas no schema atual:")
        for table_name in missing_preserved_tables:
            print(f"- {table_name}")
    print()
    print("Tabelas truncadas:")
    for table_name in truncated_tables:
        print(f"- {table_name}")
    if backup_path is not None:
        print()
        print(f"Backup configurado: {backup_path}")
    else:
        print()
        suggested_backup_path = Path("backups") / f"backup_before_reset_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.sql"
        print("Backup recomendado antes de confirmar:")
        print(_suggest_backup_command(suggested_backup_path))


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    env_value = (settings.env or "").lower()
    schema_name = (args.schema or settings.db_schema).strip() or settings.db_schema

    if env_value not in SAFE_ENVIRONMENTS and not args.force:
        raise SystemExit(
            f"Refusing reset: ENV={env_value!r} is not a safe environment. Use --force only if you know what you are doing."
        )

    with engine.connect() as connection:
        plan = build_reset_plan(connection, schema_name)
        validate_reset_plan(plan)

    backup_path = Path(args.backup_file).expanduser().resolve() if args.backup_file else None
    _print_plan(
        schema_name=plan.schema_name,
        available_tables=plan.available_tables,
        preserved_tables=plan.preserved_tables,
        missing_preserved_tables=plan.missing_preserved_tables,
        truncated_tables=plan.truncated_tables,
        backup_path=backup_path,
    )

    if not args.confirm_reset:
        print()
        print("Modo diagnóstico somente. Use --confirm-reset para aplicar o truncate.")
        return 0

    if backup_path is not None:
        print()
        print(f"Executando backup em {backup_path}...")
        _run_backup(backup_path)
        print("Backup concluído.")
    else:
        print()
        print("Nenhum backup foi executado automaticamente; revise o comando sugerido acima antes de prosseguir.")

    with engine.begin() as connection:
        session = Session(bind=connection)
        try:
            truncate_tables(connection, plan.schema_name, plan.truncated_tables)
            validation = validate_post_reset_state(session, plan.schema_name, plan.truncated_tables)
        finally:
            session.close()

    print()
    print("Reset concluído com sucesso.")
    print(f"Usuários preservados: {validation.users_total}")
    print(f"Usuários ativos: {validation.active_users_total}")
    print(f"Admin bootstrap ativo: {'sim' if validation.admin_exists else 'não'}")
    for table_name in sorted(validation.preserved_counts):
        print(f"{table_name}: {validation.preserved_counts[table_name]}")
    print(f"Migrations preservadas: {'sim' if validation.preserved_counts.get('alembic_version', 0) > 0 else 'não'}")
    if validation.truncated_non_empty:
        print("Atenção: algumas tabelas truncadas ainda têm registros:")
        for table_name in validation.truncated_non_empty:
            print(f"- {table_name}")
    else:
        print("Tabelas operacionais truncadas: vazias")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
