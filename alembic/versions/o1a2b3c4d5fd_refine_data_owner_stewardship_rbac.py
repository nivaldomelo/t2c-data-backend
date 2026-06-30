"""refine data_owner and stewardship RBAC

Revision ID: o1a2b3c4d5fd
Revises: n1a2b3c4d5fc
Create Date: 2026-06-26

Scopes the data_owner and stewardship roles to viewer-level read plus a single
extra capability each, without granting any administrative power:

- New permissions: asset.owner:write, stewardship:approve, stewardship:reject.
- editor gains *:read (read parity with viewer) and asset.owner:write.
- data_owner gains asset.owner:write (keeps *:read, user:read, datasource:read).
- stewardship gains stewardship:approve/reject and LOSES the explicit
  datasource:read (it relies on *:read for general reads, mirroring the viewer).
"""

from __future__ import annotations

from alembic import op


revision = "o1a2b3c4d5fd"
down_revision = "n1a2b3c4d5fc"
branch_labels = None
depends_on = None


NEW_PERMISSIONS = (
    ("asset.owner:write", "Assign owner/steward of tables and assets"),
    ("stewardship:approve", "Approve stewardship/governance review items"),
    ("stewardship:reject", "Reject stewardship/governance review items"),
)


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
    for name, description in NEW_PERMISSIONS:
        _execute(
            f"""
            INSERT INTO t2c_data.permissions (name, description, created_at, updated_at)
            VALUES ('{name}', '{description}', now(), now())
            ON CONFLICT (name) DO NOTHING
            """
        )

    _grant(("editor",), ("*:read", "asset.owner:write"))
    _grant(("data_owner",), ("asset.owner:write",))
    _grant(("stewardship",), ("stewardship:approve", "stewardship:reject"))

    # Stewardship must not carry an explicit datasource permission.
    _revoke(("stewardship",), ("datasource:read", "datasource:write"))


def downgrade() -> None:
    _revoke(("editor",), ("*:read", "asset.owner:write"))
    _revoke(("data_owner",), ("asset.owner:write",))
    _revoke(("stewardship",), ("stewardship:approve", "stewardship:reject"))
    _grant(("stewardship",), ("datasource:read",))
