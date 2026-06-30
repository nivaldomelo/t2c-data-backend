"""Compatibility bridge for dashboard read-model queries."""

from __future__ import annotations

from sqlalchemy.orm import Session

from t2c_data.features.dashboard.queries import get_dashboard_summary as _get_dashboard_summary


def get_dashboard_summary(session: Session) -> dict:
    return _get_dashboard_summary(session)


__all__ = ["get_dashboard_summary"]
