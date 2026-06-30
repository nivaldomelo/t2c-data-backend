#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import tempfile
import shutil
from pathlib import Path

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import SQLAlchemyError

ROOT = Path(__file__).resolve().parents[1]
CONFIRM_CLEAN_TOKEN = "CLEAN_T2C_DATA"

# Ensure "app" package is importable when script is run directly.
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Migrate the t2c_data schema contents from a source PostgreSQL database "
            "into the current catalog PostgreSQL target."
        )
    )
    parser.add_argument(
        "--source-url",
        default=os.getenv("T2C_DATA_MIGRATION_SOURCE_DATABASE_URL") or os.getenv("SOURCE_DATABASE_URL"),
        help="Source PostgreSQL URL to export from (accepts SQLAlchemy URL form).",
    )
    parser.add_argument(
        "--target-url",
        default=os.getenv("DATABASE_URL"),
        help="Target PostgreSQL URL to import into (defaults to DATABASE_URL).",
    )
    parser.add_argument(
        "--schema",
        default=os.getenv("DB_SCHEMA", "t2c_data"),
        help="Schema to migrate (default: t2c_data).",
    )
    parser.add_argument(
        "--clean-target",
        action="store_true",
        help="Truncate target schema tables before importing data.",
    )
    parser.add_argument(
        "--confirm-clean",
        help=f"Type exactly {CONFIRM_CLEAN_TOKEN} to authorize --clean-target.",
    )
    parser.add_argument(
        "--skip-alembic",
        action="store_true",
        help="Skip alembic upgrade head on the target database.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the planned actions without executing them.",
    )
    return parser


def _normalize_postgres_url(raw_url: str | None) -> str | None:
    if raw_url is None:
        return None
    url = raw_url.strip()
    if not url:
        return None
    return url


def _libpq_url(raw_url: str) -> str:
    return raw_url.replace("postgresql+psycopg://", "postgresql://", 1)


def _mask_url(raw_url: str | None) -> str:
    if not raw_url:
        return "<empty>"
    try:
        return make_url(raw_url).render_as_string(hide_password=True)
    except Exception:  # noqa: BLE001
        return raw_url


def _ensure_tools() -> None:
    missing = [tool for tool in ("alembic", "pg_dump", "psql") if not shutil.which(tool)]
    if missing:
        raise SystemExit(f"Missing required command(s): {', '.join(missing)}")


def _target_has_rows(target_url: str, schema_name: str) -> tuple[bool, str | None]:
    engine = create_engine(target_url)
    try:
        inspector = inspect(engine)
        table_names = [name for name in inspector.get_table_names(schema=schema_name) if name != "alembic_version"]
        with engine.connect() as connection:
            for table_name in table_names:
                has_row = connection.execute(
                    text(f'SELECT 1 FROM "{schema_name}"."{table_name}" LIMIT 1')
                ).first()
                if has_row is not None:
                    return True, table_name
        return False, None
    finally:
        engine.dispose()


def _common_table_names(source_url: str, target_url: str, schema_name: str) -> list[str]:
    source_engine = create_engine(source_url)
    target_engine = create_engine(target_url)
    try:
        source_tables = set(inspect(source_engine).get_table_names(schema=schema_name))
        target_tables = set(inspect(target_engine).get_table_names(schema=schema_name))
        common = sorted((source_tables & target_tables) - {"alembic_version"})
        return common
    finally:
        source_engine.dispose()
        target_engine.dispose()


def _run_alembic_upgrade(target_url: str) -> None:
    env = os.environ.copy()
    env["DATABASE_URL"] = target_url
    subprocess.run(["alembic", "upgrade", "head"], cwd=ROOT, env=env, check=True)


def _truncate_target_schema(target_url: str, schema_name: str) -> list[str]:
    engine = create_engine(target_url)
    try:
        with engine.begin() as connection:
            inspector = inspect(connection)
            table_names = [
                table_name
                for table_name in inspector.get_table_names(schema=schema_name)
                if table_name != "alembic_version"
            ]
            if not table_names:
                return []

            quoted_tables = ", ".join(f'"{schema_name}"."{table_name}"' for table_name in table_names)
            connection.exec_driver_sql(f"TRUNCATE TABLE {quoted_tables} RESTART IDENTITY CASCADE")
            return table_names
    finally:
        engine.dispose()


def _dump_source_schema(source_url: str, schema_name: str, table_names: list[str], dump_file: Path) -> None:
    table_args: list[str] = []
    for table_name in table_names:
        table_args.extend(["--table", f"{schema_name}.{table_name}"])
    subprocess.run(
        [
            "pg_dump",
            "--data-only",
            "--no-owner",
            "--no-privileges",
            "--exclude-table-data",
            f"{schema_name}.alembic_version",
            *table_args,
            "--file",
            str(dump_file),
            _libpq_url(source_url),
        ],
        check=True,
    )


def _restore_to_target(target_url: str, dump_file: Path) -> None:
    subprocess.run(
        [
            "psql",
            "--set",
                "ON_ERROR_STOP=1",
                "--file",
                str(dump_file),
                _libpq_url(target_url),
        ],
        check=True,
    )


def _sanitize_dump_file(dump_file: Path) -> None:
    lines = dump_file.read_text(encoding="utf-8").splitlines()
    filtered = [
        line
        for line in lines
        if line.strip().lower() not in {"set transaction_timeout = 0;", "set transaction_timeout = '0';"}
    ]
    dump_file.write_text("\n".join(filtered) + "\n", encoding="utf-8")


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    source_url = _normalize_postgres_url(args.source_url)
    target_url = _normalize_postgres_url(args.target_url)
    schema_name = args.schema.strip()

    if not source_url:
        raise SystemExit(
            "Source database URL not provided. Set --source-url or T2C_DATA_MIGRATION_SOURCE_DATABASE_URL."
        )
    if not target_url:
        raise SystemExit("Target database URL not provided. Set --target-url or DATABASE_URL.")
    if source_url == target_url:
        raise SystemExit("Source and target URLs are identical; refusing to migrate.")

    if args.clean_target and (args.confirm_clean or "").strip() != CONFIRM_CLEAN_TOKEN:
        raise SystemExit(f"Refusing clean target: --confirm-clean must be exactly {CONFIRM_CLEAN_TOKEN}.")

    print("Migração do catálogo iniciada.")
    print(f"Schema: {schema_name}")
    print(f"Source: {_mask_url(source_url)}")
    print(f"Target: {_mask_url(target_url)}")
    print(f"Alembic on target: {'disabled' if args.skip_alembic else 'enabled'}")
    print(f"Clean target: {'yes' if args.clean_target else 'no'}")

    if args.dry_run:
        print("Dry-run concluido. Nenhuma alteração foi aplicada.")
        return

    try:
        _ensure_tools()
        if not args.clean_target:
            has_rows, table_name = _target_has_rows(target_url, schema_name)
            if has_rows:
                raise SystemExit(
                    "Target schema already contains data in "
                    f'"{schema_name}"."{table_name}". Re-run with --clean-target --confirm-clean {CONFIRM_CLEAN_TOKEN} '
                    "or empty the schema before importing."
                )

        if not args.skip_alembic:
            print("Running Alembic upgrade on target...")
            _run_alembic_upgrade(target_url)

        if args.clean_target:
            print("Truncating target schema before import...")
            truncated_tables = _truncate_target_schema(target_url, schema_name)
            if truncated_tables:
                print(f"Truncated tables ({len(truncated_tables)}): {', '.join(truncated_tables)}")
            else:
                print("No tables needed truncation.")

        with tempfile.TemporaryDirectory(prefix="t2c-data-migration-") as tmpdir:
            dump_file = Path(tmpdir) / f"{schema_name}.data.sql"
            table_names = _common_table_names(source_url, target_url, schema_name)
            if not table_names:
                print("No common tables found between source and target schemas; skipping data import.")
                return
            print(f"Tables selected for migration ({len(table_names)}): {', '.join(table_names)}")
            print("Exporting source data...")
            _dump_source_schema(source_url, schema_name, table_names, dump_file)
            _sanitize_dump_file(dump_file)
            print("Importing into target...")
            _restore_to_target(target_url, dump_file)

        print("Migração concluida com sucesso.")
        print("Tabelas, dados e views modeladas pelo Alembic estão prontos no banco central.")
    except (subprocess.CalledProcessError, SQLAlchemyError) as exc:
        raise SystemExit(f"Migração falhou: {exc}") from exc


if __name__ == "__main__":
    main()
