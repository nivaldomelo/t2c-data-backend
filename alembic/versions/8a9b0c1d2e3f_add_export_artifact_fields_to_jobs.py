"""add export artifact fields to integration sync jobs

Revision ID: 8a9b0c1d2e3f
Revises: 3e4f5a6b7c8d
Create Date: 2026-05-26 16:30:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "8a9b0c1d2e3f"
down_revision = "3e4f5a6b7c8d"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("integration_sync_jobs", sa.Column("artifact_public_id", sa.String(length=64), nullable=True))
    op.add_column("integration_sync_jobs", sa.Column("artifact_filename", sa.String(length=255), nullable=True))
    op.add_column("integration_sync_jobs", sa.Column("artifact_content_type", sa.String(length=255), nullable=True))
    op.add_column("integration_sync_jobs", sa.Column("artifact_storage_path", sa.String(length=1024), nullable=True))
    op.add_column("integration_sync_jobs", sa.Column("artifact_available_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("integration_sync_jobs", sa.Column("artifact_expires_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("integration_sync_jobs", sa.Column("artifact_size_bytes", sa.BigInteger(), nullable=True))
    op.add_column(
        "integration_sync_jobs",
        sa.Column("artifact_download_count", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column("integration_sync_jobs", sa.Column("artifact_last_downloaded_at", sa.DateTime(timezone=True), nullable=True))
    op.create_unique_constraint("uq_integration_sync_jobs_artifact_public_id", "integration_sync_jobs", ["artifact_public_id"])
    op.create_index(
        "ix_integration_sync_jobs_artifact_expires_at",
        "integration_sync_jobs",
        ["artifact_expires_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_integration_sync_jobs_artifact_expires_at", table_name="integration_sync_jobs")
    op.drop_constraint("uq_integration_sync_jobs_artifact_public_id", "integration_sync_jobs", type_="unique")
    op.drop_column("integration_sync_jobs", "artifact_last_downloaded_at")
    op.drop_column("integration_sync_jobs", "artifact_download_count")
    op.drop_column("integration_sync_jobs", "artifact_size_bytes")
    op.drop_column("integration_sync_jobs", "artifact_expires_at")
    op.drop_column("integration_sync_jobs", "artifact_available_at")
    op.drop_column("integration_sync_jobs", "artifact_storage_path")
    op.drop_column("integration_sync_jobs", "artifact_content_type")
    op.drop_column("integration_sync_jobs", "artifact_filename")
    op.drop_column("integration_sync_jobs", "artifact_public_id")
