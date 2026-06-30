"""add column classification models

Revision ID: e1a2b3c4d5f6
Revises: d9e0f1a2b3c4
Create Date: 2026-05-28 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "e1a2b3c4d5f6"
down_revision = "d9e0f1a2b3c4"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "column_classifications",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column("column_id", sa.Integer(), sa.ForeignKey("columns.id", ondelete="CASCADE"), nullable=False),
        sa.Column("taxonomy_key", sa.String(length=80), nullable=False),
        sa.Column("taxonomy_label", sa.String(length=120), nullable=False),
        sa.Column("taxonomy_group", sa.String(length=40), server_default="operational", nullable=False),
        sa.Column("review_status", sa.String(length=30), server_default="approved", nullable=False),
        sa.Column("source_kind", sa.String(length=40), server_default="manual", nullable=False),
        sa.Column("confidence_score", sa.Integer(), server_default="100", nullable=False),
        sa.Column("is_personal_data", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("is_sensitive_data", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("is_financial_data", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("is_operational_data", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("evidence_json", sa.JSON(), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("reviewed_by_user_id", sa.Integer(), sa.ForeignKey("users.id", ondelete="SET NULL"), nullable=True),
        sa.Column("reviewed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.UniqueConstraint("column_id", name="uq_column_classifications_column"),
    )
    op.create_index("ix_column_classifications_taxonomy_key", "column_classifications", ["taxonomy_key"])
    op.create_index("ix_column_classifications_review_status", "column_classifications", ["review_status"])

    op.create_table(
        "column_classification_versions",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column("column_id", sa.Integer(), sa.ForeignKey("columns.id", ondelete="CASCADE"), nullable=False),
        sa.Column(
            "column_classification_id",
            sa.Integer(),
            sa.ForeignKey("column_classifications.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("version_number", sa.Integer(), nullable=False),
        sa.Column("decision_status", sa.String(length=30), nullable=False),
        sa.Column("taxonomy_key", sa.String(length=80), nullable=False),
        sa.Column("taxonomy_label", sa.String(length=120), nullable=False),
        sa.Column("taxonomy_group", sa.String(length=40), server_default="operational", nullable=False),
        sa.Column("source_kind", sa.String(length=40), nullable=False),
        sa.Column("confidence_score", sa.Integer(), server_default="0", nullable=False),
        sa.Column("is_personal_data", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("is_sensitive_data", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("is_financial_data", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("is_operational_data", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("evidence_json", sa.JSON(), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("decided_by_user_id", sa.Integer(), sa.ForeignKey("users.id", ondelete="SET NULL"), nullable=True),
        sa.Column("decided_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
    )
    op.create_index("ix_column_classification_versions_column_id", "column_classification_versions", ["column_id"])
    op.create_index("ix_column_classification_versions_decided_at", "column_classification_versions", ["decided_at"])
    op.create_index("ix_column_classification_versions_status", "column_classification_versions", ["decision_status"])
    op.create_unique_constraint(
        "uq_column_classification_versions_column_version",
        "column_classification_versions",
        ["column_id", "version_number"],
    )


def downgrade() -> None:
    op.drop_constraint("uq_column_classification_versions_column_version", "column_classification_versions", type_="unique")
    op.drop_index("ix_column_classification_versions_status", table_name="column_classification_versions")
    op.drop_index("ix_column_classification_versions_decided_at", table_name="column_classification_versions")
    op.drop_index("ix_column_classification_versions_column_id", table_name="column_classification_versions")
    op.drop_table("column_classification_versions")

    op.drop_index("ix_column_classifications_review_status", table_name="column_classifications")
    op.drop_index("ix_column_classifications_taxonomy_key", table_name="column_classifications")
    op.drop_table("column_classifications")

