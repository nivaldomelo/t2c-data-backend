#!/usr/bin/env python3
"""Generate a standalone, self-contained bootstrap SQL file.

Writes ``scripts/sql/bootstrap.sql`` — a single script that, run on an EMPTY
PostgreSQL database with ``psql``, creates the schema, all tables and the RBAC
seed (roles, permissions, mappings) plus a default admin so the system runs
without Python/Alembic. It is the "break-glass" backup for the Python bootstrap.

How it stays correct and safe:
  * DDL comes from the SQLAlchemy models (``Base.metadata``), compiled for the
    PostgreSQL dialect via a mock engine — version-independent (no pg_dump) and
    always in sync with the ORM.
  * The RBAC seed (roles/permissions/role_permissions) is read from the live
    head database and emitted as idempotent, id-independent (name-keyed) INSERTs.
    Only these seed-only tables are read — no application/dev data, no secrets.
  * ``alembic_version`` is stamped to head so a later ``alembic upgrade`` is a
    no-op and future migrations apply cleanly.

The generated file is meant for an EMPTY database and is wrapped in a single
transaction (all-or-nothing). Schema creation and the RBAC/admin/alembic inserts
are guarded (IF NOT EXISTS / ON CONFLICT DO NOTHING); the table DDL expects an
empty schema. For an already-initialized database use Alembic or the Python
bootstrap instead.

Run from backend/:  python scripts/generate_bootstrap_sql.py
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import create_mock_engine, text  # noqa: E402

from t2c_data.core.config import settings  # noqa: E402
from t2c_data.core.db import engine  # noqa: E402
from t2c_data.core.security import hash_password  # noqa: E402
from t2c_data.models import Base  # noqa: E402

SCHEMA = settings.db_schema
OUT_PATH = ROOT / "scripts" / "sql" / "bootstrap.sql"
# Default credentials for the break-glass admin. CHANGE IMMEDIATELY after first login.
DEFAULT_ADMIN_EMAIL = "admin@andromeda.com"
DEFAULT_ADMIN_NAME = "Andromeda Admin"
DEFAULT_ADMIN_PASSWORD = "admin123"  # documented default; not a real secret


def _sql_str(value: str | None) -> str:
    if value is None:
        return "NULL"
    return "'" + value.replace("'", "''") + "'"


def _ddl() -> str:
    statements: list[str] = []

    def executor(sql, *args, **kwargs):  # noqa: ANN001
        statements.append(str(sql.compile(dialect=mock_engine.dialect)).strip())

    mock_engine = create_mock_engine("postgresql+psycopg://", executor)
    Base.metadata.create_all(mock_engine, checkfirst=False)
    return ";\n".join(s for s in statements if s) + ";"


def _alembic_head() -> str:
    from alembic.config import Config
    from alembic.script import ScriptDirectory

    cfg = Config(str(ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(ROOT / "alembic"))
    return ScriptDirectory.from_config(cfg).get_current_head()


def _seed_inserts() -> str:
    lines: list[str] = []
    with engine.connect() as conn:
        roles = conn.execute(
            text(f'SELECT name, description FROM "{SCHEMA}".roles ORDER BY name')
        ).all()
        permissions = conn.execute(
            text(f'SELECT name, description FROM "{SCHEMA}".permissions ORDER BY name')
        ).all()
        mappings = conn.execute(
            text(
                f'''
                SELECT r.name AS role_name, p.name AS permission_name
                FROM "{SCHEMA}".role_permissions rp
                JOIN "{SCHEMA}".roles r ON r.id = rp.role_id
                JOIN "{SCHEMA}".permissions p ON p.id = rp.permission_id
                ORDER BY r.name, p.name
                '''
            )
        ).all()

    lines.append("-- Roles")
    for name, description in roles:
        lines.append(
            f'INSERT INTO "{SCHEMA}".roles (name, description, created_at, updated_at) '
            f"VALUES ({_sql_str(name)}, {_sql_str(description)}, now(), now()) "
            "ON CONFLICT (name) DO NOTHING;"
        )

    lines.append("\n-- Permissions")
    for name, description in permissions:
        lines.append(
            f'INSERT INTO "{SCHEMA}".permissions (name, description, created_at, updated_at) '
            f"VALUES ({_sql_str(name)}, {_sql_str(description)}, now(), now()) "
            "ON CONFLICT (name) DO NOTHING;"
        )

    lines.append("\n-- Role -> permission mappings (id-independent, keyed by name)")
    for role_name, permission_name in mappings:
        lines.append(
            f'INSERT INTO "{SCHEMA}".role_permissions (role_id, permission_id) '
            f'SELECT r.id, p.id FROM "{SCHEMA}".roles r JOIN "{SCHEMA}".permissions p '
            f"ON r.name = {_sql_str(role_name)} AND p.name = {_sql_str(permission_name)} "
            "ON CONFLICT DO NOTHING;"
        )

    return "\n".join(lines)


def _admin_insert() -> str:
    password_hash = hash_password(DEFAULT_ADMIN_PASSWORD)
    return (
        "-- Break-glass admin user. CHANGE THE PASSWORD IMMEDIATELY after first login.\n"
        f'INSERT INTO "{SCHEMA}".users (email, name, full_name, password_hash, is_active, token_version, mfa_enabled) '
        f"VALUES ({_sql_str(DEFAULT_ADMIN_EMAIL)}, {_sql_str(DEFAULT_ADMIN_NAME)}, {_sql_str(DEFAULT_ADMIN_NAME)}, "
        f"{_sql_str(password_hash)}, TRUE, 0, FALSE) ON CONFLICT (email) DO NOTHING;\n"
        f'INSERT INTO "{SCHEMA}".user_role (user_id, role_id) '
        f'SELECT u.id, r.id FROM "{SCHEMA}".users u JOIN "{SCHEMA}".roles r '
        f"ON u.email = {_sql_str(DEFAULT_ADMIN_EMAIL)} AND r.name = 'admin' ON CONFLICT DO NOTHING;"
    )


def _schemas() -> list[str]:
    found = {table.schema for table in Base.metadata.tables.values() if table.schema}
    found.add(SCHEMA)
    return sorted(found)


def main() -> None:
    head = _alembic_head()
    ddl = _ddl()
    seed = _seed_inserts()
    admin = _admin_insert()
    create_schemas = "\n".join(f'CREATE SCHEMA IF NOT EXISTS "{name}";' for name in _schemas())

    header = f"""-- =====================================================================
-- t2c_data — standalone bootstrap (schema + tables + RBAC seed + admin)
-- Generated by scripts/generate_bootstrap_sql.py from the ORM models and the
-- canonical RBAC seed. Brings an EMPTY PostgreSQL database to a runnable state
-- without Python/Alembic. For an EMPTY database; runs in a single transaction.
--
-- Usage:  psql "<DATABASE_URL>" -v ON_ERROR_STOP=1 -f scripts/sql/bootstrap.sql
--
-- NOTE: Airflow read-model VIEWS (vw_airflow_*) are created by Alembic/runtime,
-- not here; they are optional for the core system to run.
-- The default admin password is "{DEFAULT_ADMIN_PASSWORD}" — CHANGE IT IMMEDIATELY.
-- Prefer scripts/bootstrap_database.py in production (strong configured creds).
-- =====================================================================

BEGIN;

{create_schemas}
SET search_path TO "{SCHEMA}", public;

-- ---------------------------------------------------------------------
-- Schema (tables, constraints, indexes) — from SQLAlchemy models
-- ---------------------------------------------------------------------
{ddl}

-- ---------------------------------------------------------------------
-- Alembic version stamp (so `alembic upgrade head` is a no-op afterwards)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS "{SCHEMA}".alembic_version (
    version_num VARCHAR(32) NOT NULL,
    CONSTRAINT alembic_version_pkc PRIMARY KEY (version_num)
);
INSERT INTO "{SCHEMA}".alembic_version (version_num) VALUES ('{head}')
ON CONFLICT (version_num) DO NOTHING;

-- ---------------------------------------------------------------------
-- RBAC seed
-- ---------------------------------------------------------------------
{seed}

-- ---------------------------------------------------------------------
-- Bootstrap admin
-- ---------------------------------------------------------------------
{admin}

COMMIT;
"""

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(header)
    print(f"Wrote {OUT_PATH} (alembic head={head})")


if __name__ == "__main__":
    main()
