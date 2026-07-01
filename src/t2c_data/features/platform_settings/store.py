from __future__ import annotations

import json
import logging
from typing import Any

from sqlalchemy.orm import Session

from t2c_data.core.secret_store import decrypt_text, encrypt_text
from t2c_data.models.platform_settings import PlatformSettings

logger = logging.getLogger(__name__)

SETTINGS_ROW_ID = 1

# Secret keys inside the config document — never returned to clients in cleartext.
SECRET_KEYS: tuple[str, ...] = ("metabase_auth_secret", "control_db_password")

# All non-secret, admin-editable keys (used for output/audit projection).
NONSECRET_KEYS: tuple[str, ...] = (
    "spark_master_url", "spark_results_dir", "spark_jobs_dir", "spark_local_jars_dir",
    "spark_driver_host", "spark_driver_memory", "spark_executor_memory",
    "spark_submit_timeout_seconds", "spark_packages_enabled", "spark_packages",
    "metabase_enabled", "metabase_base_url", "metabase_auth_type", "metabase_auth_username",
    "metabase_timeout_seconds", "metabase_sync_dashboards", "metabase_sync_questions",
    "metabase_sync_collections",
    "control_db_host", "control_db_port", "control_db_name", "control_db_user",
    "control_db_schema", "control_db_sslmode",
    "dq_execution_engine",
)


def get_settings_row(session: Session | None) -> PlatformSettings | None:
    if session is None:
        return None
    try:
        return session.get(PlatformSettings, SETTINGS_ROW_ID)
    except Exception:  # pragma: no cover - table may not exist yet (pre-migration)
        logger.debug("platform_settings row unavailable; using env/default config", exc_info=True)
        return None


def get_settings_row_or_create(session: Session) -> PlatformSettings:
    row = session.get(PlatformSettings, SETTINGS_ROW_ID)
    if row is None:
        row = PlatformSettings(id=SETTINGS_ROW_ID)
        session.add(row)
        session.flush()
    return row


def read_settings_dict(session: Session | None) -> dict[str, Any]:
    """Decrypt and return the stored config overrides as a typed dict.

    Empty dict when there is no session/row/blob → everything inherits from env/defaults.
    Keys absent from the dict are inherited; secret keys are included (decrypted) for
    server-side use only and must never be echoed to clients."""
    row = get_settings_row(session)
    if row is None or not row.settings_encrypted:
        return {}
    raw = decrypt_text(row.settings_encrypted)
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        logger.warning("platform_settings blob could not be decoded; treating as empty")
        return {}
    return data if isinstance(data, dict) else {}


def write_settings_dict(session: Session, data: dict[str, Any], *, user_id: int | None) -> PlatformSettings:
    """Encrypt and persist the full config document (drops empty/None values → inherit)."""
    cleaned = {k: v for k, v in data.items() if v is not None and not (isinstance(v, str) and v.strip() == "")}
    row = get_settings_row_or_create(session)
    row.settings_encrypted = encrypt_text(json.dumps(cleaned, ensure_ascii=True, sort_keys=True)) if cleaned else None
    row.updated_by_user_id = user_id
    session.add(row)
    return row
