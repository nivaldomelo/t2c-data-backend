from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.orm import Session

from t2c_data.core.config import settings
from t2c_data.seed import ensure_installation_seed


def rbac_tables_exist(session: Session) -> bool:
    role_reg = session.execute(
        text("SELECT to_regclass(:regname)"),
        {"regname": f"{settings.db_schema}.roles"},
    ).scalar_one()
    return role_reg is not None


def dq_rules_table_exists(session: Session) -> bool:
    table_reg = session.execute(
        text("SELECT to_regclass(:regname)"),
        {"regname": f"{settings.db_schema}.dq_rules"},
    ).scalar_one()
    return table_reg is not None


def ensure_rbac_seed(session: Session) -> None:
    ensure_installation_seed(session, create_viewer=True, commit=True)
