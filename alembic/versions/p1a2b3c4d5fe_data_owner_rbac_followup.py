"""data_owner RBAC follow-up: drop datasource:read, restore stewardship queues

Revision ID: p1a2b3c4d5fe
Revises: o1a2b3c4d5fd
Create Date: 2026-06-26

- data_owner loses the explicit datasource:read (datasource access is admin-only).
- data_owner regains the stewardship review-queue decision permissions
  (stewardship:approve / stewardship:reject).
"""

from __future__ import annotations

from alembic import op


revision = "p1a2b3c4d5fe"
down_revision = "o1a2b3c4d5fd"
branch_labels = None
depends_on = None


def _execute(sql: str) -> None:
    op.get_bind().exec_driver_sql(sql)


def _grant(role_names: tuple[str, ...], permission_names: tuple[str, ...]) -> None:
    roles = ", ".join(f"'{name}'" for name in role_names)
    perms = ", ".join(f"'{name}'" for name in permission_names)
    _execute(
        f"""
        INSERT INTO t2c_data.role_permissions (role_id, permission_id)
        SELECT r.id, p.id
        FROM t2c_data.roles r
        JOIN t2c_data.permissions p ON p.name IN ({perms})
        WHERE r.name IN ({roles})
        ON CONFLICT DO NOTHING
        """
    )


def _revoke(role_names: tuple[str, ...], permission_names: tuple[str, ...]) -> None:
    roles = ", ".join(f"'{name}'" for name in role_names)
    perms = ", ".join(f"'{name}'" for name in permission_names)
    _execute(
        f"""
        DELETE FROM t2c_data.role_permissions rp
        USING t2c_data.roles r, t2c_data.permissions p
        WHERE rp.role_id = r.id
          AND rp.permission_id = p.id
          AND r.name IN ({roles})
          AND p.name IN ({perms})
        """
    )


def upgrade() -> None:
    _revoke(("data_owner",), ("datasource:read", "datasource:write"))
    _grant(("data_owner",), ("stewardship:approve", "stewardship:reject"))


def downgrade() -> None:
    _revoke(("data_owner",), ("stewardship:approve", "stewardship:reject"))
    _grant(("data_owner",), ("datasource:read",))
