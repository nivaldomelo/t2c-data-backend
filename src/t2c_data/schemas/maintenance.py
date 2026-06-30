from __future__ import annotations

from typing import Literal

from pydantic import BaseModel


class DestructiveActionConfirmIn(BaseModel):
    confirmation: Literal["APAGAR"]

