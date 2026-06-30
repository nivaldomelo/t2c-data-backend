"""add airflow operational read models

Revision ID: b6c7d8e9f0a1
Revises: c6d7e8f9a1b2
Create Date: 2026-04-14 23:30:00.000000
"""

from __future__ import annotations

from alembic import op

from t2c_data.features.integrations.airflow_read_models import ensure_airflow_operational_read_models


revision = "b6c7d8e9f0a1"
down_revision = "c6d7e8f9a1b2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    ensure_airflow_operational_read_models(bind)


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS t2c_data.vw_airflow_operacional")
    op.execute("DROP VIEW IF EXISTS t2c_data.vw_airflow_tasks_falhas")
    op.execute("DROP VIEW IF EXISTS t2c_data.vw_airflow_dags_resumo")
    op.execute("DROP VIEW IF EXISTS t2c_data.vw_airflow_dag_runs_recentes")
