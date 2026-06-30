"""refresh airflow read-model views to the current rich definition

Revision ID: n1a2b3c4d5fc
Revises: m1a2b3c4d5fb
Create Date: 2026-06-26

The original migration created the Airflow read-model views, but
``ensure_airflow_operational_read_models`` only (re)creates a view when it is
absent. Since then the view definitions gained richer columns (description,
tags, 24h activity counts, timetable/next-run/import-error metadata), so the
already-deployed views became stale and never picked up the new columns
(``CREATE OR REPLACE VIEW`` cannot change the output column set anyway).

This migration drops the four Airflow views and re-runs the ensure routine so
they are rebuilt from the current builders. It is a no-op where the Airflow
source schema is unavailable (ensure skips creation), and the drops are guarded
with IF EXISTS, so it is safe across environments.
"""

from __future__ import annotations

from alembic import op

from t2c_data.features.integrations.airflow_read_models import ensure_airflow_operational_read_models


revision = "n1a2b3c4d5fc"
down_revision = "m1a2b3c4d5fb"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("DROP VIEW IF EXISTS t2c_data.vw_airflow_operacional")
    op.execute("DROP VIEW IF EXISTS t2c_data.vw_airflow_tasks_falhas")
    op.execute("DROP VIEW IF EXISTS t2c_data.vw_airflow_dags_resumo")
    op.execute("DROP VIEW IF EXISTS t2c_data.vw_airflow_dag_runs_recentes")
    ensure_airflow_operational_read_models(op.get_bind())


def downgrade() -> None:
    # Non-reversible content change; leave the rebuilt views in place.
    pass
