"""add scale phase 5 indexes

Revision ID: 5a6b7c8d9e0f
Revises: 4e5f6a7b8c9d
Create Date: 2026-05-26 10:30:00.000000
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op


revision: str = "5a6b7c8d9e0f"
down_revision: Union[str, Sequence[str], None] = "4e5f6a7b8c9d"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

SCHEMA = "t2c_data"


def upgrade() -> None:
    op.create_index("ix_dq_runs_status_created", "dq_runs", ["status", "created_at"], unique=False, schema=SCHEMA)
    op.create_index("ix_dq_runs_table_status_created", "dq_runs", ["table_id", "status", "created_at"], unique=False, schema=SCHEMA)
    op.create_index(
        "ix_dq_runs_datasource_status_created",
        "dq_runs",
        ["datasource_id", "status", "created_at"],
        unique=False,
        schema=SCHEMA,
    )
    op.create_index(
        "ix_dashboard_asset_read_model_certification_status",
        "dashboard_asset_read_model",
        ["certification_status"],
        unique=False,
        schema=SCHEMA,
    )
    op.create_index(
        "ix_dashboard_asset_read_model_privacy_flags",
        "dashboard_asset_read_model",
        ["has_personal_data", "has_sensitive_personal_data"],
        unique=False,
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_index("ix_dashboard_asset_read_model_privacy_flags", table_name="dashboard_asset_read_model", schema=SCHEMA)
    op.drop_index(
        "ix_dashboard_asset_read_model_certification_status",
        table_name="dashboard_asset_read_model",
        schema=SCHEMA,
    )
    op.drop_index("ix_dq_runs_datasource_status_created", table_name="dq_runs", schema=SCHEMA)
    op.drop_index("ix_dq_runs_table_status_created", table_name="dq_runs", schema=SCHEMA)
    op.drop_index("ix_dq_runs_status_created", table_name="dq_runs", schema=SCHEMA)
