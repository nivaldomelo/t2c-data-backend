#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from sqlalchemy.orm import Session

# Ensure "app" package is importable when script is run directly.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from t2c_data.core.config import settings  # noqa: E402
from t2c_data.core.db import engine  # noqa: E402
from t2c_data.core.installation_reset import (  # noqa: E402
    DEFAULT_PRESERVED_TABLES,
    list_schema_tables,
    reset_installation_state,
)

CONFIRMATION_TOKEN = "RESET_T2C_DATA"
SAFE_ENVIRONMENTS = {"dev", "development", "local", "test"}


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Reset installation data safely and reseed the minimal bootstrap user."
    )
    parser.add_argument(
        "--confirm",
        required=True,
        help=f"Type exactly {CONFIRMATION_TOKEN} to authorize the reset.",
    )
    parser.add_argument(
        "--confirm-env",
        required=True,
        help="Type the current environment value to prove you reviewed the target before running.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Allow execution outside safe environments (dev/local/test).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only show which tables would be cleared and which bootstrap records would be recreated.",
    )
    return parser


def _validate_execution(args: argparse.Namespace) -> None:
    env_value = (settings.env or "").lower()
    confirm_value = (args.confirm or "").strip()
    confirm_env_value = (args.confirm_env or "").strip().lower()
    if confirm_value != CONFIRMATION_TOKEN:
        raise SystemExit(f"Refusing reset: --confirm must be exactly {CONFIRMATION_TOKEN}.")
    if confirm_env_value != env_value:
        raise SystemExit(
            f"Refusing reset: --confirm-env must match the current ENV value ({env_value!r})."
        )
    if env_value not in SAFE_ENVIRONMENTS and not args.force:
        raise SystemExit(
            f"Refusing reset: ENV={env_value!r} is not a safe environment. Use --force only if you know what you are doing."
        )


def _print_plan() -> None:
    with engine.connect() as connection:
        table_names = list_schema_tables(connection, settings.db_schema)

    print("Reset de instalação autorizado.")
    print(f"ENV: {settings.env}")
    print(f"Schema: {settings.db_schema}")
    print(f"Tables to clear: {len([name for name in table_names if name not in DEFAULT_PRESERVED_TABLES])}")
    if table_names:
        print("Detected tables:", ", ".join(table_names))
    print("Bootstrap seed after reset:")
    print(f"  admin: {settings.bootstrap_admin_name} <{settings.bootstrap_admin_email}>")
    print(f"  preserved tables: {', '.join(sorted(DEFAULT_PRESERVED_TABLES))}")


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    _validate_execution(args)
    _print_plan()

    if args.dry_run:
        print("Dry-run concluido. Nenhuma alteração foi aplicada.")
        return

    with engine.begin() as connection:
        session = Session(bind=connection)
        try:
            report = reset_installation_state(session, schema_name=settings.db_schema)
        finally:
            session.close()

    print("Reset concluido com sucesso.")
    print(f"Schema: {report.schema_name}")
    print(f"Dialect: {report.dialect}")
    print(f"Tabelas preservadas: {', '.join(report.preserved_tables) if report.preserved_tables else 'nenhuma'}")
    print(f"Tabelas truncadas ({len(report.truncated_tables)}): {', '.join(report.truncated_tables) if report.truncated_tables else 'nenhuma'}")
    print(f"Seed aplicado: {'sim' if report.seed_applied else 'não'}")
    print(f"Admin bootstrap: {report.bootstrap_admin_email}")


if __name__ == "__main__":
    main()
