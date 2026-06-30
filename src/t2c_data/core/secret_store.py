from __future__ import annotations

import base64
import hashlib
import json
import logging
from collections.abc import Mapping
from typing import Any

from cryptography.fernet import Fernet, InvalidToken, MultiFernet

from t2c_data.core.config import is_dev_environment, settings

_SECRET_PREFIX = "enc::"
_WEAK_KEY_MATERIALS = {"", "change-me", "dev-only-change-me", "change-me-dev-jwt-secret"}
logger = logging.getLogger(__name__)


class PlaintextSecretError(ValueError):
    """Raised when an unencrypted secret is read outside an explicit dev-only mode."""


def _derive_fernet(material: str) -> Fernet:
    digest = hashlib.sha256(material.encode("utf-8")).digest()
    return Fernet(base64.urlsafe_b64encode(digest))


def _build_fernet() -> MultiFernet:
    """Build the secret cipher.

    The primary (encrypt) key is derived from the dedicated DATASOURCE_SECRET_KEY so that
    secret-at-rest protection is decoupled from the JWT signing key. A JWT-derived key is
    kept as a *decrypt-only* fallback so payloads encrypted before the dedicated key was
    introduced remain readable (transparent re-encryption happens on next write).
    """
    primary_material = (settings.datasource_secret_key or "").strip()
    keys: list[Fernet] = []
    if primary_material and primary_material not in _WEAK_KEY_MATERIALS:
        keys.append(_derive_fernet(primary_material))

    jwt_material = (settings.jwt_secret_key or "").strip()
    legacy_usable = jwt_material and jwt_material not in _WEAK_KEY_MATERIALS and jwt_material != primary_material
    if legacy_usable:
        keys.append(_derive_fernet(jwt_material))

    if not keys:
        # Only reachable in dev/test when ALLOW_PLAINTEXT_SECRETS relaxes the config guard.
        # Use a clearly-ephemeral derivation so we never silently fall back to a public default.
        if is_dev_environment(settings.env):
            logger.warning(
                "No strong DATASOURCE_SECRET_KEY/JWT_SECRET_KEY configured; using an in-process "
                "dev cipher. Set DATASOURCE_SECRET_KEY to persist and protect secrets."
            )
            keys.append(_derive_fernet(f"dev-ephemeral::{primary_material}::{jwt_material}"))
        else:
            raise RuntimeError(
                "DATASOURCE_SECRET_KEY must be set to a strong, non-default value to encrypt secrets."
            )
    return MultiFernet(keys)


_FERNET = _build_fernet()


def is_encrypted_secret_payload(raw: str | None) -> bool:
    return bool(raw and raw.startswith(_SECRET_PREFIX))


def plaintext_secret_allowed() -> bool:
    return bool(settings.allow_plaintext_secrets and is_dev_environment(settings.env))


def encrypt_secret_mapping(values: Mapping[str, Any] | None) -> str:
    normalized = {
        str(key): str(value)
        for key, value in (values or {}).items()
        if value is not None and str(value).strip()
    }
    if not normalized:
        return ""
    payload = json.dumps(normalized, ensure_ascii=True, separators=(",", ":")).encode("utf-8")
    return f"{_SECRET_PREFIX}{_FERNET.encrypt(payload).decode('utf-8')}"


def decrypt_secret_mapping(raw: str | None) -> dict[str, str]:
    if not raw:
        return {}
    if not raw.startswith(_SECRET_PREFIX):
        if plaintext_secret_allowed():
            logger.warning("plaintext secret compatibility fallback used in dev/test; rotate or migrate this credential")
            return {"password": raw}
        raise PlaintextSecretError("Plaintext secret is not allowed. Rotate or migrate this credential.")
    token = raw[len(_SECRET_PREFIX) :].encode("utf-8")
    try:
        decrypted = _FERNET.decrypt(token)
    except InvalidToken:
        return {}
    try:
        payload = json.loads(decrypted.decode("utf-8"))
    except json.JSONDecodeError:
        return {}
    if not isinstance(payload, dict):
        return {}
    return {
        str(key): str(value)
        for key, value in payload.items()
        if value is not None and str(value).strip()
    }
