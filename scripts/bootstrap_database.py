#!/usr/bin/env python3
"""Bootstrap a fresh database so the system runs immediately.

Based on the current Alembic state, this script takes an EMPTY database to a
fully runnable state in one shot:

  1. creates the application schema (``settings.db_schema``) if missing;
  2. runs ``alembic upgrade head`` — this creates every table AND applies the
     data/seed migrations, including the canonical RBAC baseline (all roles,
     permissions and role->permission mappings) and the bootstrap admin user
     (migration r1a2b3c4d600);
  3. optionally seeds the dev demo accounts (viewer) with ``--with-viewer``;
  4. prints a verification summary.

It is idempotent: running it again on an already-initialized database only
ensures missing pieces and reports the current state. It does NOT drop anything.

Usage (from the backend/ directory or container):

    python scripts/bootstrap_database.py                 # prod-style bootstrap
    python scripts/bootstrap_database.py --with-viewer   # also seed dev viewer
    python scripts/bootstrap_database.py --dry-run        # show the plan only

The admin credentials come from INITIAL_ADMIN_* / ADMIN_* env vars, which
Settings already validates to be non-default outside dev/test.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from sqlalchemy import inspect, text
from sqlalchemy.orm import Session

# Ensure the "app" package is importable when run directly.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from t2c_data.core.config import settings  # noqa: E402
from t2c_data.core.db import engine  # noqa: E402


def _alembic_config():
    from alembic.config import Config

    cfg = Config(str(ROOT / "alembic.ini"))
    # Make paths absolute so the script works from any working directory.
    cfg.set_main_option("script_location", str(ROOT / "alembic"))
    cfg.set_main_option("sqlalchemy.url", settings.database_url)
    return cfg


def _alembic_head(cfg) -> str | None:
    from alembic.script import ScriptDirectory

    return ScriptDirectory.from_config(cfg).get_current_head()


def _current_revision() -> str | None:
    from alembic.migration import MigrationContext

    with engine.connect() as connection:
        return MigrationContext.configure(connection).get_current_revision()


def _create_schema() -> None:
    with engine.begin() as connection:
        connection.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{settings.db_schema}"'))


def _summary() -> None:
    inspector = inspect(engine)
    tables = sorted(inspector.get_table_names(schema=settings.db_schema))
    print(f"  schema           : {settings.db_schema}")
    print(f"  tables created   : {len(tables)}")

    with engine.connect() as connection:
        def _count(sql: str) -> int:
            try:
                return int(connection.execute(text(sql)).scalar() or 0)
            except Exception:  # noqa: BLE001
                return -1

        roles = connection.execute(
            text(f'SELECT name FROM "{settings.db_schema}".roles ORDER BY name')
        ).scalars().all() if "roles" in tables else []
        permissions = _count(f'SELECT count(*) FROM "{settings.db_schema}".permissions') if "permissions" in tables else -1
        admin_exists = _count(
            f"SELECT count(*) FROM \"{settings.db_schema}\".users WHERE email = '{settings.bootstrap_admin_email}'"
        ) if "users" in tables else -1

    print(f"  roles            : {', '.join(roles) if roles else 'none'}")
    print(f"  permissions      : {permissions}")
    print(f"  bootstrap admin  : {settings.bootstrap_admin_email} ({'present' if admin_exists > 0 else 'MISSING'})")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--with-viewer", action="store_true", help="Also seed dev demo viewer accounts.")
    parser.add_argument("--dry-run", action="store_true", help="Show the plan without changing anything.")
    args = parser.parse_args()

    cfg = _alembic_config()
    head = _alembic_head(cfg)
    current = _current_revision()

    print("=== Bootstrap do banco t2c_data ===")
    print(f"  ENV              : {settings.env}")
    print(f"  database schema  : {settings.db_schema}")
    print(f"  alembic current  : {current or '(empty database)'}")
    print(f"  alembic head     : {head}")
    print(f"  seed viewer      : {'yes' if args.with_viewer else 'no'}")

    if args.dry_run:
        print("\nDry-run: would (1) CREATE SCHEMA IF NOT EXISTS, (2) alembic upgrade head, "
              + ("(3) ensure_installation_seed(create_viewer=True)." if args.with_viewer else "(3) ensure baseline seed."))
        return

    # 1) Schema
    print("\n[1/3] Ensuring schema...")
    _create_schema()

    # 2) Tables + data/seed migrations (RBAC baseline + bootstrap admin live here).
    print("[2/3] Running alembic upgrade head (tables + inserts)...")
    from alembic import command

    command.upgrade(cfg, "head")

    # 3) Idempotent safety net. The migration already seeds the baseline; this also
    #    creates dev viewer accounts when requested.
    print("[3/3] Ensuring RBAC seed + bootstrap admin...")
    from t2c_data.seed import ensure_installation_seed

    with engine.begin() as connection:
        session = Session(bind=connection)
        try:
            ensure_installation_seed(session, create_viewer=args.with_viewer, commit=False)
            session.flush()
        finally:
            session.close()

    print("\n=== Concluído. Sistema pronto para subir. ===")
    _summary()


if __name__ == "__main__":
    main()
