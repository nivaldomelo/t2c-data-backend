"""add scale validation indexes

Revision ID: 4e5f6a7b8c9d
Revises: 3d4e5f6a7b8c
Create Date: 2026-05-25 22:10:00.000000
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op


revision: str = "4e5f6a7b8c9d"
down_revision: Union[str, Sequence[str], None] = "3d4e5f6a7b8c"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

SCHEMA = "t2c_data"


def upgrade() -> None:
    op.create_index("ix_tables_schema_updated_at", "tables", ["schema_id", "updated_at"], unique=False, schema=SCHEMA)
    op.create_index(
        "ix_tables_certification_status_review_at",
        "tables",
        ["certification_status", "certification_review_at"],
        unique=False,
        schema=SCHEMA,
    )
    op.create_index(
        "ix_tables_privacy_flags_reviewed_at",
        "tables",
        ["has_personal_data", "has_sensitive_personal_data", "privacy_reviewed_at"],
        unique=False,
        schema=SCHEMA,
    )
    op.create_index(
        "ix_tables_access_scope_sensitivity",
        "tables",
        ["access_scope", "sensitivity_level"],
        unique=False,
        schema=SCHEMA,
    )

    op.create_index(
        "ix_dq_rules_table_archived_active",
        "dq_rules",
        ["table_id", "archived", "is_active"],
        unique=False,
        schema=SCHEMA,
    )
    op.create_index(
        "ix_dq_rules_table_severity",
        "dq_rules",
        ["table_id", "severity"],
        unique=False,
        schema=SCHEMA,
    )
    op.create_index(
        "ix_dq_rule_runs_rule_created",
        "dq_rule_runs",
        ["rule_id", "created_at"],
        unique=False,
        schema=SCHEMA,
    )
    op.create_index(
        "ix_dq_rule_runs_rule_status_created",
        "dq_rule_runs",
        ["rule_id", "status", "created_at"],
        unique=False,
        schema=SCHEMA,
    )
    op.create_index(
        "ix_dq_job_runs_table_created",
        "dq_job_runs",
        ["table_id", "created_at"],
        unique=False,
        schema=SCHEMA,
    )
    op.create_index(
        "ix_dq_job_runs_status_created",
        "dq_job_runs",
        ["status", "created_at"],
        unique=False,
        schema=SCHEMA,
    )
    op.create_index(
        "ix_dq_job_runs_datasource_created",
        "dq_job_runs",
        ["datasource_id", "created_at"],
        unique=False,
        schema=SCHEMA,
    )

    op.create_index("ix_scan_runs_datasource_created", "scan_runs", ["datasource_id", "created_at"], unique=False, schema=SCHEMA)
    op.create_index("ix_scan_runs_status_created", "scan_runs", ["status", "created_at"], unique=False, schema=SCHEMA)
    op.create_index("ix_scan_snapshots_run_entity", "scan_snapshots", ["scan_run_id", "entity_type"], unique=False, schema=SCHEMA)
    op.create_index("ix_scan_diffs_run_diff_type", "scan_diffs", ["scan_run_id", "diff_type"], unique=False, schema=SCHEMA)


def downgrade() -> None:
    op.drop_index("ix_scan_diffs_run_diff_type", table_name="scan_diffs", schema=SCHEMA)
    op.drop_index("ix_scan_snapshots_run_entity", table_name="scan_snapshots", schema=SCHEMA)
    op.drop_index("ix_scan_runs_status_created", table_name="scan_runs", schema=SCHEMA)
    op.drop_index("ix_scan_runs_datasource_created", table_name="scan_runs", schema=SCHEMA)
    op.drop_index("ix_dq_job_runs_datasource_created", table_name="dq_job_runs", schema=SCHEMA)
    op.drop_index("ix_dq_job_runs_status_created", table_name="dq_job_runs", schema=SCHEMA)
    op.drop_index("ix_dq_job_runs_table_created", table_name="dq_job_runs", schema=SCHEMA)
    op.drop_index("ix_dq_rule_runs_rule_status_created", table_name="dq_rule_runs", schema=SCHEMA)
    op.drop_index("ix_dq_rule_runs_rule_created", table_name="dq_rule_runs", schema=SCHEMA)
    op.drop_index("ix_dq_rules_table_severity", table_name="dq_rules", schema=SCHEMA)
    op.drop_index("ix_dq_rules_table_archived_active", table_name="dq_rules", schema=SCHEMA)
    op.drop_index("ix_tables_access_scope_sensitivity", table_name="tables", schema=SCHEMA)
    op.drop_index("ix_tables_privacy_flags_reviewed_at", table_name="tables", schema=SCHEMA)
    op.drop_index("ix_tables_certification_status_review_at", table_name="tables", schema=SCHEMA)
    op.drop_index("ix_tables_schema_updated_at", table_name="tables", schema=SCHEMA)
