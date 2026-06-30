"""grant audit:export to editor (editor manages all content + audit)

Revision ID: q1a2b3c4d5ff
Revises: p1a2b3c4d5fe
Create Date: 2026-06-26

Editors can now view and export the audit trail (route-level access opened
separately). This completes the editor export set with audit:export.
"""

from __future__ import annotations

from alembic import op


revision = "q1a2b3c4d5ff"
down_revision = "p1a2b3c4d5fe"
branch_labels = None
depends_on = None


def _execute(sql: str) -> None:
    op.get_bind().exec_driver_sql(sql)


def upgrade() -> None:
    _execute(
        """
        INSERT INTO t2c_data.role_permissions (role_id, permission_id)
        SELECT r.id, p.id
        FROM t2c_data.roles r
        JOIN t2c_data.permissions p ON p.name = 'audit:export'
        WHERE r.name = 'editor'
        ON CONFLICT DO NOTHING
        """
    )


def downgrade() -> None:
    _execute(
        """
        DELETE FROM t2c_data.role_permissions rp
        USING t2c_data.roles r, t2c_data.permissions p
        WHERE rp.role_id = r.id
          AND rp.permission_id = p.id
          AND r.name = 'editor'
          AND p.name = 'audit:export'
        """
    )
