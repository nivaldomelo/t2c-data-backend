"""seed canonical RBAC baseline and bootstrap admin on every install

Revision ID: r1a2b3c4d600
Revises: q1a2b3c4d5ff
Create Date: 2026-06-26

The startup seed (run_startup_seed_if_enabled) only runs in dev, and ENABLE_DB_SEED
is forbidden outside dev/test. So a fresh PRODUCTION database would otherwise have
no roles, permissions, role->permission mappings or bootstrap admin user.

This migration runs on every `alembic upgrade head` (the install step in all
environments) and ensures the canonical RBAC state plus the bootstrap admin user.
It delegates to ``ensure_installation_seed`` so seed.py stays the single source of
truth, and it is fully idempotent (get-or-create everywhere), so it is a no-op on
databases that were already seeded.

The admin user is created with the configured bootstrap credentials
(INITIAL_ADMIN_* / ADMIN_*), which Settings already validates to be non-default
outside dev/test. ``create_viewer=False`` keeps dev-only demo accounts out of
production.
"""

from __future__ import annotations

from alembic import op
from sqlalchemy.orm import Session


revision = "r1a2b3c4d600"
down_revision = "q1a2b3c4d5ff"
branch_labels = None
depends_on = None


def upgrade() -> None:
    from t2c_data.seed import ensure_installation_seed

    session = Session(bind=op.get_bind())
    try:
        ensure_installation_seed(session, create_viewer=False, commit=False)
        session.flush()
    finally:
        session.close()


def downgrade() -> None:
    # Intentionally a no-op: dropping baseline roles/permissions/admin would break
    # access control. Role/permission removals are handled by their own migrations.
    pass
