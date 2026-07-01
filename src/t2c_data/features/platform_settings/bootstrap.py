from __future__ import annotations

import logging

from sqlalchemy.orm import Session

from t2c_data.features.platform_settings.store import get_settings_row_or_create, write_settings_dict

logger = logging.getLogger(__name__)

# Sentinel "author" for the automatic reference seed (not a real user id).
SEED_SYSTEM_USER_ID = 0

# Non-secret, ENVIRONMENT-INVARIANT defaults seeded as reference on a fresh install so the
# config page opens with a documented baseline (shown as "personalizado"). Deliberately EXCLUDES
# anything that legitimately varies per environment — spark master URL / driver host / results
# dir, Metabase, and the control DB — so those keep inheriting each deployment's env vars.
DEFAULT_SEED: dict[str, object] = {
    "spark_jobs_dir": "/opt/spark/jobs",
    "spark_local_jars_dir": "/app/jars",
    "spark_driver_memory": "1g",
    "spark_executor_memory": "1g",
    "spark_submit_timeout_seconds": 900,
    "spark_packages_enabled": False,
    "spark_packages": "org.postgresql:postgresql:42.7.4",
    "dq_execution_engine": "spark",
    "control_db_schema": "controle",
    "metabase_timeout_seconds": 15,
    "metabase_sync_dashboards": True,
    "metabase_sync_questions": True,
    "metabase_sync_collections": True,
}


def ensure_platform_settings_seed(session: Session) -> bool:
    """Seed the reference defaults ONCE, only on a pristine row (never touched by anyone).

    Idempotent: after seeding, updated_by_user_id is the system sentinel, so it never re-runs;
    once an admin saves (updated_by = their id) it also never re-runs. Returns True if it seeded."""
    try:
        row = get_settings_row_or_create(session)
        if row.settings_encrypted is not None or row.updated_by_user_id is not None:
            return False  # already seeded or edited by an admin
        write_settings_dict(session, dict(DEFAULT_SEED), user_id=SEED_SYSTEM_USER_ID)
        session.commit()
        logger.info("platform_settings reference defaults seeded (%d keys)", len(DEFAULT_SEED))
        return True
    except Exception:  # noqa: BLE001 - never block startup on a seed failure
        session.rollback()
        logger.exception("platform_settings seed skipped due to error")
        return False
