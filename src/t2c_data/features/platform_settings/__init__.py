"""Runtime, admin-editable platform configuration.

Config resolution is layered: DB (platform_settings, edited in the admin UI) → env var →
hardcoded default. An empty/absent row reproduces today's env-only behaviour exactly.
Secrets are encrypted at rest (Fernet, see t2c_data.core.secret_store).
"""

from t2c_data.features.platform_settings.resolvers import (
    resolve_control_db_url,
    resolve_metabase_config,
    resolve_spark_config,
    resolve_spark_runner,
)
from t2c_data.features.platform_settings.store import (
    get_settings_row,
    get_settings_row_or_create,
)

__all__ = [
    "get_settings_row",
    "get_settings_row_or_create",
    "resolve_control_db_url",
    "resolve_metabase_config",
    "resolve_spark_config",
    "resolve_spark_runner",
]
