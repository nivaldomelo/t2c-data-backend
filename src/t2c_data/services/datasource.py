from __future__ import annotations

"""Backward-compatible datasource service wrappers.

New datasource write-side orchestration lives under `app.features.datasource`.
This module stays in place to preserve older imports while delegating to the
feature-layer implementation.
"""

from sqlalchemy.orm import Session

from t2c_data.features.datasource.persistence import hard_delete_datasource as _hard_delete_datasource

__all__ = ["hard_delete_datasource"]


def hard_delete_datasource(session: Session, datasource_id: int) -> bool:
    return _hard_delete_datasource(session=session, datasource_id=datasource_id)
