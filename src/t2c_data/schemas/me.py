from datetime import datetime

from pydantic import BaseModel, Field


class ChangePasswordRequest(BaseModel):
    current_password: str
    new_password: str


class ChangePasswordResponse(BaseModel):
    ok: bool
    message: str


class MeResponse(BaseModel):
    id: int
    name: str | None
    email: str
    roles: list[str]
    permissions: list[str]
    is_admin: bool
    unread_notifications: int = 0
    password_changed_at: datetime | None = None
    password_expires_at: datetime | None = None
    password_days_remaining: int | None = None
    ui_theme: str = "atual"


class ThemeUpdateRequest(BaseModel):
    theme: str


class ThemeUpdateResponse(BaseModel):
    ui_theme: str


class NotificationPreferenceUpdateRequest(BaseModel):
    in_app_enabled: bool = True
    email_enabled: bool = False
    governance_enabled: bool = True
    stewardship_enabled: bool = True
    operational_enabled: bool = True
    only_assigned_items: bool = False
    daily_digest_enabled: bool = False


class NotificationPreferenceResponse(BaseModel):
    in_app_enabled: bool
    email_enabled: bool
    governance_enabled: bool
    stewardship_enabled: bool
    operational_enabled: bool
    only_assigned_items: bool
    daily_digest_enabled: bool
    last_daily_digest_at: datetime | None = None
    next_daily_digest_at: datetime | None = None
    last_daily_digest_status: str | None = None
    updated_at: datetime | None = None


class MfaStatusResponse(BaseModel):
    enabled: bool
    setup_pending: bool = False
    issuer: str
    account_name: str
    manual_secret: str | None = None
    otpauth_uri: str | None = None
    updated_at: datetime | None = None


class MfaActionResponse(MfaStatusResponse):
    message: str


class MfaVerifyRequest(BaseModel):
    code: str


class MfaDisableRequest(BaseModel):
    current_password: str


class InboxCategoryCountOut(BaseModel):
    key: str
    count: int


class InboxSummaryOut(BaseModel):
    total: int = 0
    unread: int = 0
    due_delivery: int = 0
    by_category: list[InboxCategoryCountOut] = Field(default_factory=list)


class InboxNotificationOut(BaseModel):
    id: int
    category: str
    severity: str
    source_module: str
    source_entity_type: str
    source_entity_id: str
    title: str
    message: str
    href: str | None = None
    state: str
    delivery_state: str
    context_json: dict | None = None
    forwarded_from_notification_id: int | None = None
    forwarded_by_user_id: int | None = None
    forwarded_by_user_name: str | None = None
    forwarded_by_user_email: str | None = None
    forwarded_at: datetime | None = None
    first_seen_at: datetime
    last_seen_at: datetime
    last_notified_at: datetime | None = None
    next_delivery_at: datetime | None = None
    read_at: datetime | None = None
    archived_at: datetime | None = None
    created_at: datetime
    updated_at: datetime


class InboxListOut(BaseModel):
    generated_at: str
    total: int
    page: int = 1
    page_size: int = 100
    has_more: bool = False
    items: list[InboxNotificationOut] = Field(default_factory=list)


class InboxRecipientOut(BaseModel):
    id: int
    display_name: str
    email: str


class ForwardInboxNotificationRequest(BaseModel):
    recipient_user_id: int
