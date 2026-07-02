from __future__ import annotations

import base64
import hashlib
import hmac
import os
import re
from datetime import datetime, timedelta, timezone
from urllib.parse import quote
from uuid import uuid4

import jwt
from passlib.context import CryptContext

from t2c_data.core.config import settings


pwd_context = CryptContext(
    schemes=["argon2", "bcrypt"],  # argon2 como padrão, bcrypt compat
    deprecated="auto",
)


def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)


def generate_token_jti() -> str:
    return uuid4().hex


def validate_password_policy(password: str) -> None:
    candidate = password or ""
    if len(candidate) < 12:
        raise ValueError("New password must have at least 12 chars and 3 of 4 character types")
    classes = 0
    classes += 1 if re.search(r"[a-z]", candidate) else 0
    classes += 1 if re.search(r"[A-Z]", candidate) else 0
    classes += 1 if re.search(r"\d", candidate) else 0
    classes += 1 if re.search(r"[^A-Za-z0-9]", candidate) else 0
    if classes < 3:
        raise ValueError("New password must have at least 12 chars and 3 of 4 character types")


def generate_totp_secret() -> str:
    raw_secret = base64.b32encode(os.urandom(20)).decode("utf-8")
    return raw_secret.rstrip("=")


def normalize_totp_secret(secret: str) -> str:
    return re.sub(r"\s+", "", (secret or "").strip()).upper().rstrip("=")


def build_totp_provisioning_uri(
    secret: str,
    account_name: str,
    *,
    issuer: str,
    digits: int = 6,
    period: int = 30,
) -> str:
    normalized_secret = normalize_totp_secret(secret)
    normalized_issuer = (issuer or "").strip() or "t2c_data"
    normalized_account = (account_name or "").strip() or normalized_issuer
    label = quote(f"{normalized_issuer}:{normalized_account}")
    return (
        f"otpauth://totp/{label}"
        f"?secret={normalized_secret}"
        f"&issuer={quote(normalized_issuer)}"
        f"&digits={int(digits)}"
        f"&period={int(period)}"
        f"&algorithm=SHA1"
    )


def _totp_counter(for_time: datetime | None = None, *, period: int = 30) -> int:
    moment = for_time or datetime.now(timezone.utc)
    normalized = moment if moment.tzinfo is not None else moment.replace(tzinfo=timezone.utc)
    return int(normalized.timestamp() // max(1, period))


def _hotp_code(secret: str, counter: int, *, digits: int = 6) -> str:
    normalized_secret = normalize_totp_secret(secret)
    padding = (-len(normalized_secret)) % 8
    if padding:
        normalized_secret = f"{normalized_secret}{'=' * padding}"
    key = base64.b32decode(normalized_secret, casefold=True)
    message = counter.to_bytes(8, "big")
    digest = hmac.new(key, message, hashlib.sha1).digest()
    offset = digest[-1] & 0x0F
    code_int = (
        ((digest[offset] & 0x7F) << 24)
        | ((digest[offset + 1] & 0xFF) << 16)
        | ((digest[offset + 2] & 0xFF) << 8)
        | (digest[offset + 3] & 0xFF)
    )
    return str(code_int % (10**digits)).zfill(digits)


def find_totp_counter(
    secret: str,
    code: str,
    *,
    for_time: datetime | None = None,
    period: int = 30,
    digits: int = 6,
    window: int = 1,
) -> int | None:
    """Return the absolute HOTP counter that matches ``code`` (for anti-replay tracking), or None.

    Callers should persist the returned counter and reject any future code whose counter is
    ``<=`` the last accepted one, so a captured code cannot be replayed within its window."""
    normalized_code = re.sub(r"\s+", "", (code or "").strip())
    if not normalized_code.isdigit() or len(normalized_code) != digits:
        return None
    try:
        counter = _totp_counter(for_time, period=period)
    except Exception:  # noqa: BLE001
        return None
    for offset in range(-window, window + 1):
        candidate = _hotp_code(secret, counter + offset, digits=digits)
        if hmac.compare_digest(candidate, normalized_code):
            return counter + offset
    return None


def verify_totp_code(
    secret: str,
    code: str,
    *,
    for_time: datetime | None = None,
    period: int = 30,
    digits: int = 6,
    window: int = 1,
) -> bool:
    return find_totp_counter(secret, code, for_time=for_time, period=period, digits=digits, window=window) is not None


def generate_totp_code(
    secret: str,
    *,
    for_time: datetime | None = None,
    period: int = 30,
    digits: int = 6,
) -> str:
    return _hotp_code(secret, _totp_counter(for_time, period=period), digits=digits)


def create_access_token(
    subject: str,
    expires_minutes: int | None = None,
    *,
    token_version: int = 0,
    session_jti: str | None = None,
) -> str:
    now = datetime.now(timezone.utc)
    expire = now + timedelta(minutes=expires_minutes or settings.access_token_expire_minutes)
    jti = (session_jti or "").strip() or None
    payload = {
        "sub": subject,
        "tv": int(token_version),
        "iat": int(now.timestamp()),
        "exp": int(expire.timestamp()),
    }
    if jti:
        payload["jti"] = jti
    return jwt.encode(payload, settings.jwt_secret_key, algorithm=settings.jwt_algorithm)


def decode_token_payload(token: str) -> dict | None:
    try:
        payload = jwt.decode(token, settings.jwt_secret_key, algorithms=[settings.jwt_algorithm])
    except jwt.InvalidTokenError:
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def decode_token(token: str) -> str | None:
    payload = decode_token_payload(token)
    if payload is None:
        return None
    return payload.get("sub")
