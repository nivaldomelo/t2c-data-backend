"""sync rbac roles and permissions for stewardship/data owner

Revision ID: a4b5c6d7e8f9
Revises: a3b4c5d6e7f8
Create Date: 2026-04-07 00:00:00.000000
"""

from __future__ import annotations

from alembic import op


revision = "a4b5c6d7e8f9"
down_revision = "a3b4c5d6e7f8"
branch_labels = None
depends_on = None


ROLE_DESCRIPTIONS = {
    "stewardship": "Stewardship approvals and review",
    "data_owner": "Data owner approvals and review",
}


def _execute(sql: str) -> None:
    op.get_bind().exec_driver_sql(sql)


def upgrade() -> None:
    _execute(
        """
        INSERT INTO t2c_data.roles (name, description, created_at, updated_at)
        VALUES
          ('stewardship', 'Stewardship approvals and review', now(), now()),
          ('data_owner', 'Data owner approvals and review', now(), now())
        ON CONFLICT (name) DO NOTHING
        """
    )

    _execute(
        """
        DELETE FROM t2c_data.role_permissions rp
        USING t2c_data.roles r, t2c_data.permissions p
        WHERE rp.role_id = r.id
          AND rp.permission_id = p.id
          AND r.name IN ('editor', 'viewer')
          AND p.name IN ('datasource:read', 'datasource:write')
        """
    )

    _execute(
        """
        INSERT INTO t2c_data.role_permissions (role_id, permission_id)
        SELECT r.id, p.id
        FROM t2c_data.roles r
        JOIN t2c_data.permissions p
          ON p.name IN ('*:read', 'datasource:read', 'user:read')
        WHERE r.name IN ('stewardship', 'data_owner')
        ON CONFLICT DO NOTHING
        """
    )

    _execute(
        """
        INSERT INTO t2c_data.role_permissions (role_id, permission_id)
        SELECT r.id, p.id
        FROM t2c_data.roles r
        JOIN t2c_data.permissions p
          ON p.name IN ('*:read', 'user:read')
        WHERE r.name = 'viewer'
        ON CONFLICT DO NOTHING
        """
    )

    _execute(
        """
        INSERT INTO t2c_data.role_permissions (role_id, permission_id)
        SELECT r.id, p.id
        FROM t2c_data.roles r
        JOIN t2c_data.permissions p
          ON p.name = 'user:read'
        WHERE r.name = 'editor'
        ON CONFLICT DO NOTHING
        """
    )


def downgrade() -> None:
    _execute(
        """
        DELETE FROM t2c_data.role_permissions rp
        USING t2c_data.roles r
        WHERE rp.role_id = r.id
          AND r.name IN ('stewardship', 'data_owner')
        """
    )

    _execute(
        """
        DELETE FROM t2c_data.roles
        WHERE name IN ('stewardship', 'data_owner')
        """
    )

    _execute(
        """
        INSERT INTO t2c_data.role_permissions (role_id, permission_id)
        SELECT r.id, p.id
        FROM t2c_data.roles r
        JOIN t2c_data.permissions p
          ON p.name IN ('datasource:read', 'datasource:write')
        WHERE r.name = 'editor'
        ON CONFLICT DO NOTHING
        """
    )

    _execute(
        """
        INSERT INTO t2c_data.role_permissions (role_id, permission_id)
        SELECT r.id, p.id
        FROM t2c_data.roles r
        JOIN t2c_data.permissions p
          ON p.name = 'datasource:read'
        WHERE r.name = 'viewer'
        ON CONFLICT DO NOTHING
        """
    )
