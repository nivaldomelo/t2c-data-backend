from datetime import datetime

from pydantic import BaseModel


class LoginRequest(BaseModel):
    email: str
    password: str
    mfa_code: str | None = None


class LoginResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    roles: list[str]
    permissions: list[str]
    mfa_enabled: bool = False
    # When the user logged in WITHOUT MFA during the grace window, how many grace
    # logins remain before the account is locked (None once MFA is enrolled).
    mfa_grace_remaining: int | None = None
    mfa_warning: str | None = None
    password_expires_at: datetime | None = None
    password_days_remaining: int | None = None
    password_warning: str | None = None


class LogoutResponse(BaseModel):
    ok: bool = True
    revoked_session: bool = False
