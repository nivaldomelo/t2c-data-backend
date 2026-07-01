from __future__ import annotations

from sqlalchemy import Integer, Text
from sqlalchemy.orm import Mapped, mapped_column

from t2c_data.models.base import Base
from t2c_data.models.common import TimestampMixin


class PlatformSettings(TimestampMixin, Base):
    """Runtime, admin-editable platform configuration (single row, id=1).

    The entire configuration document (Spark / Metabase / control-DB / advanced) is stored
    as ONE Fernet-encrypted JSON blob in `settings_encrypted` — nothing is kept in plaintext
    at rest, credentials or otherwise. An empty/absent blob means "everything inherits from
    the environment / defaults" (see app/features/platform_settings). Encryption uses the
    shared cipher in t2c_data.core.secret_store (`enc::` prefix, key from DATASOURCE_SECRET_KEY).
    """

    __tablename__ = "platform_settings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, default=1)
    # Fernet-encrypted JSON document of the stored overrides (keys absent = inherit).
    settings_encrypted: Mapped[str | None] = mapped_column(Text, nullable=True)
    updated_by_user_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
