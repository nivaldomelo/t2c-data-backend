from __future__ import annotations

import logging

from sqlalchemy.orm import Session

from t2c_data.core.secret_store import decrypt_secret_mapping
from t2c_data.models.platform_settings import PlatformSettings

logger = logging.getLogger(__name__)

SETTINGS_ROW_ID = 1


def get_settings_row(session: Session | None) -> PlatformSettings | None:
    """Best-effort read of the single settings row. Returns None (→ env/default fallback)
    if there is no session, the table is missing, or the row was never created."""
    if session is None:
        return None
    try:
        return session.get(PlatformSettings, SETTINGS_ROW_ID)
    except Exception:  # pragma: no cover - table may not exist yet (pre-migration)
        logger.debug("platform_settings row unavailable; using env/default config", exc_info=True)
        return None


def get_settings_row_or_create(session: Session) -> PlatformSettings:
    """Read the settings row, creating the empty singleton if absent (used by the admin API)."""
    row = session.get(PlatformSettings, SETTINGS_ROW_ID)
    if row is None:
        row = PlatformSettings(id=SETTINGS_ROW_ID)
        session.add(row)
        session.flush()
    return row


def decrypt_control_db_password(row: PlatformSettings | None) -> str | None:
    if not row or not row.control_db_password_encrypted:
        return None
    return decrypt_secret_mapping(row.control_db_password_encrypted).get("password") or None


def decrypt_metabase_auth_secret(row: PlatformSettings | None) -> str | None:
    if not row or not row.metabase_auth_secret_encrypted:
        return None
    return decrypt_secret_mapping(row.metabase_auth_secret_encrypted).get("auth_secret") or None
