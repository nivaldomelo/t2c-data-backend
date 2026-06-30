"""add intelligent tag inference tables

Revision ID: 1f2e3d4c5b6b
Revises: e9f0a1b2c3d4
Create Date: 2026-04-10 09:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "1f2e3d4c5b6b"
down_revision = "e9f0a1b2c3d4"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "tag_assignments",
        sa.Column("confidence_score", sa.Integer(), nullable=False, server_default="100"),
        schema="t2c_data",
    )
    op.add_column(
        "tag_assignments",
        sa.Column("inference_source", sa.String(length=80), nullable=True),
        schema="t2c_data",
    )
    op.add_column(
        "tag_assignments",
        sa.Column("inference_reason", sa.Text(), nullable=True),
        schema="t2c_data",
    )
    op.add_column(
        "tag_assignments",
        sa.Column("applied_automatically", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        schema="t2c_data",
    )
    op.add_column(
        "tag_assignments",
        sa.Column("review_status", sa.String(length=30), nullable=False, server_default="manual_applied"),
        schema="t2c_data",
    )
    op.add_column(
        "tag_assignments",
        sa.Column("rule_key", sa.String(length=120), nullable=True),
        schema="t2c_data",
    )
    op.add_column(
        "tag_assignments",
        sa.Column("rule_label", sa.String(length=160), nullable=True),
        schema="t2c_data",
    )
    op.add_column(
        "tag_assignments",
        sa.Column("reviewed_by_user_id", sa.Integer(), sa.ForeignKey("t2c_data.users.id", ondelete="SET NULL"), nullable=True),
        schema="t2c_data",
    )
    op.add_column(
        "tag_assignments",
        sa.Column("reviewed_at", sa.DateTime(timezone=True), nullable=True),
        schema="t2c_data",
    )

    op.create_index(
        "ix_tag_assignments_entity_type_entity_id",
        "tag_assignments",
        ["entity_type", "entity_id"],
        unique=False,
        schema="t2c_data",
    )
    op.create_index(
        "ix_tag_assignments_tag_id_entity",
        "tag_assignments",
        ["tag_id", "entity_type", "entity_id"],
        unique=False,
        schema="t2c_data",
    )

    op.create_table(
        "tag_assignment_overrides",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("tag_id", sa.Integer(), sa.ForeignKey("t2c_data.tags.id", ondelete="CASCADE"), nullable=False),
        sa.Column(
            "datasource_id",
            sa.Integer(),
            sa.ForeignKey("t2c_data.data_sources.id", ondelete="CASCADE"),
            nullable=True,
        ),
        sa.Column("entity_type", sa.String(length=40), nullable=False),
        sa.Column("entity_id", sa.Integer(), nullable=False),
        sa.Column("state", sa.String(length=30), nullable=False, server_default="blocked"),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column(
            "created_by_user_id",
            sa.Integer(),
            sa.ForeignKey("t2c_data.users.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.UniqueConstraint("tag_id", "entity_type", "entity_id", name="uq_tag_assignment_override_entity"),
        schema="t2c_data",
    )
    op.create_index(
        "ix_tag_assignment_overrides_entity_type_entity_id",
        "tag_assignment_overrides",
        ["entity_type", "entity_id"],
        unique=False,
        schema="t2c_data",
    )
    op.create_index(
        "ix_tag_assignment_overrides_datasource_id",
        "tag_assignment_overrides",
        ["datasource_id"],
        unique=False,
        schema="t2c_data",
    )

    op.create_table(
        "tag_intelligence_events",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("tag_id", sa.Integer(), sa.ForeignKey("t2c_data.tags.id", ondelete="CASCADE"), nullable=False),
        sa.Column(
            "datasource_id",
            sa.Integer(),
            sa.ForeignKey("t2c_data.data_sources.id", ondelete="CASCADE"),
            nullable=True,
        ),
        sa.Column("entity_type", sa.String(length=40), nullable=False),
        sa.Column("entity_id", sa.Integer(), nullable=False),
        sa.Column("rule_key", sa.String(length=120), nullable=True),
        sa.Column("rule_label", sa.String(length=160), nullable=True),
        sa.Column("inference_source", sa.String(length=80), nullable=True),
        sa.Column("inference_reason", sa.Text(), nullable=True),
        sa.Column("confidence_score", sa.Integer(), nullable=False, server_default="100"),
        sa.Column("applied_automatically", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("review_status", sa.String(length=30), nullable=False, server_default="suggested"),
        sa.Column("evidence", sa.JSON(), nullable=True),
        sa.Column(
            "created_by_user_id",
            sa.Integer(),
            sa.ForeignKey("t2c_data.users.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "reviewed_by_user_id",
            sa.Integer(),
            sa.ForeignKey("t2c_data.users.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("reviewed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        schema="t2c_data",
    )
    op.create_index(
        "ix_tag_intelligence_events_entity_type_entity_id",
        "tag_intelligence_events",
        ["entity_type", "entity_id"],
        unique=False,
        schema="t2c_data",
    )
    op.create_index(
        "ix_tag_intelligence_events_datasource_id",
        "tag_intelligence_events",
        ["datasource_id"],
        unique=False,
        schema="t2c_data",
    )


def downgrade() -> None:
    op.drop_index("ix_tag_intelligence_events_datasource_id", table_name="tag_intelligence_events", schema="t2c_data")
    op.drop_index("ix_tag_intelligence_events_entity_type_entity_id", table_name="tag_intelligence_events", schema="t2c_data")
    op.drop_table("tag_intelligence_events", schema="t2c_data")

    op.drop_index("ix_tag_assignment_overrides_datasource_id", table_name="tag_assignment_overrides", schema="t2c_data")
    op.drop_index("ix_tag_assignment_overrides_entity_type_entity_id", table_name="tag_assignment_overrides", schema="t2c_data")
    op.drop_table("tag_assignment_overrides", schema="t2c_data")

    op.drop_index("ix_tag_assignments_tag_id_entity", table_name="tag_assignments", schema="t2c_data")
    op.drop_index("ix_tag_assignments_entity_type_entity_id", table_name="tag_assignments", schema="t2c_data")
    op.drop_column("tag_assignments", "reviewed_at", schema="t2c_data")
    op.drop_column("tag_assignments", "reviewed_by_user_id", schema="t2c_data")
    op.drop_column("tag_assignments", "rule_label", schema="t2c_data")
    op.drop_column("tag_assignments", "rule_key", schema="t2c_data")
    op.drop_column("tag_assignments", "review_status", schema="t2c_data")
    op.drop_column("tag_assignments", "applied_automatically", schema="t2c_data")
    op.drop_column("tag_assignments", "inference_reason", schema="t2c_data")
    op.drop_column("tag_assignments", "inference_source", schema="t2c_data")
    op.drop_column("tag_assignments", "confidence_score", schema="t2c_data")
