from __future__ import annotations

import re
import shlex
from collections.abc import Mapping, Sequence
from typing import Any

_SENSITIVE_KEY_RE = re.compile(
    r"(password|passwd|pwd|secret|token|api[_-]?key|access[_-]?key|secret[_-]?key|aws[_-]?secret[_-]?access[_-]?key|aws[_-]?session[_-]?token|authorization|webhook|credential)",
    re.IGNORECASE,
)
_URI_SECRET_RE = re.compile(r"((?:jdbc:)?[a-z][a-z0-9+.\-]*://[^:\s/@]+:)[^@\s]+(@)", re.IGNORECASE)
_JSON_SECRET_RE = re.compile(
    r'((?:password|passwd|pwd|secret|token|api[_-]?key|access[_-]?key|secret[_-]?key|aws_secret_access_key|aws_session_token|authorization|webhook_url|jdbc_password)"\s*:\s*")[^"]*(")',
    re.IGNORECASE,
)
_TEXT_SECRET_REPLACEMENTS: tuple[tuple[re.Pattern[str], str], ...] = (
    (_URI_SECRET_RE, r"\1********\2"),
    (_JSON_SECRET_RE, r"\1********\2"),
    (
        re.compile(
            r"((?:password|passwd|pwd|secret|token|authorization|api[_-]?key|access[_-]?key|secret[_-]?key|aws_secret_access_key|aws_session_token|jdbc_password)\s*=\s*)[^\s,;]+",
            re.IGNORECASE,
        ),
        r"\1********",
    ),
    (
        re.compile(
            r"((?:password|passwd|pwd|secret|token|authorization|api[_-]?key|access[_-]?key|secret[_-]?key|aws_secret_access_key|aws_session_token|jdbc_password)\s*:\s*)[^\s,;]+",
            re.IGNORECASE,
        ),
        r"\1********",
    ),
    (re.compile(r"((?:x-api-key|authorization)\s*:\s*)[^\s,;]+(?:\s+[^\s,;]+)?", re.IGNORECASE), r"\1********"),
)
_SENSITIVE_CLI_FLAGS = {
    "--password",
    "--passwd",
    "--pwd",
    "--secret",
    "--token",
    "--api-key",
    "--access-key",
    "--secret-key",
    "--authorization",
    "--jdbc-password",
    "--aws-secret-access-key",
    "--aws-session-token",
}


def is_sensitive_key(value: str | None) -> bool:
    return bool(value and _SENSITIVE_KEY_RE.search(value))


def redact_sensitive_string(value: str) -> str:
    redacted = value
    for pattern, replacement in _TEXT_SECRET_REPLACEMENTS:
        redacted = pattern.sub(replacement, redacted)
    return redacted


def redact_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, Mapping):
        sanitized: dict[str, Any] = {}
        for key, item in value.items():
            key_str = str(key)
            sanitized[key_str] = "********" if is_sensitive_key(key_str) else redact_value(item)
        return sanitized
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [redact_value(item) for item in value]
    if isinstance(value, str):
        return redact_sensitive_string(value)
    return value


def redact_command_args(args: Sequence[str]) -> list[str]:
    redacted: list[str] = []
    index = 0
    while index < len(args):
        current = str(args[index])
        normalized = current.lower()
        if "=" in current:
            flag, raw_value = current.split("=", 1)
            if is_sensitive_key(flag) or flag.lower() in _SENSITIVE_CLI_FLAGS:
                redacted.append(f"{flag}=********")
            else:
                redacted.append(f"{flag}={redact_sensitive_string(raw_value)}")
            index += 1
            continue
        redacted.append(current)
        if normalized in _SENSITIVE_CLI_FLAGS:
            if index + 1 < len(args):
                redacted.append("********")
                index += 2
                continue
        elif index + 1 < len(args):
            next_value = str(args[index + 1])
            if normalized in {"--jdbc-url", "--url", "--connection-uri", "--dsn"}:
                redacted.append(redact_sensitive_string(next_value))
                index += 2
                continue
        index += 1
    return redacted


def format_command_for_log(args: Sequence[str]) -> str:
    return " ".join(shlex.quote(part) for part in redact_command_args(args))


__all__ = [
    "format_command_for_log",
    "is_sensitive_key",
    "redact_command_args",
    "redact_sensitive_string",
    "redact_value",
]
