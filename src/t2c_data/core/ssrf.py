"""Shared SSRF guards for outbound requests built from user-supplied input.

These helpers validate URLs/regions BEFORE the server makes an outbound request, blocking
requests to loopback/private/link-local/cloud-metadata addresses. This is a best-effort
mitigation (DNS rebinding can still occur between validation and the request); pair it with
``follow_redirects=False`` on the HTTP client and, where possible, an egress allow-list.
"""

from __future__ import annotations

import ipaddress
import re
import socket
from urllib.parse import urlparse

_AWS_REGION_RE = re.compile(r"^[a-z]{2}-[a-z]+-\d$")
_ALLOWED_SCHEMES = {"http", "https"}


class SsrfValidationError(ValueError):
    """Raised when a user-supplied URL/region is not safe to request."""


def _is_blocked_ip(ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    return (
        ip.is_private
        or ip.is_loopback
        or ip.is_link_local
        or ip.is_reserved
        or ip.is_multicast
        or ip.is_unspecified
    )


def validate_public_http_url(url: str, *, label: str = "URL", allow_private: bool = False) -> str:
    """Validate that ``url`` is an http(s) URL whose host does not resolve to a private/internal
    address. Returns the normalized URL or raises ``SsrfValidationError``.
    """
    raw = (url or "").strip()
    if not raw:
        raise SsrfValidationError(f"{label} é obrigatória.")
    parsed = urlparse(raw)
    if parsed.scheme.lower() not in _ALLOWED_SCHEMES:
        raise SsrfValidationError(f"{label} deve usar http ou https.")
    host = parsed.hostname
    if not host:
        raise SsrfValidationError(f"{label} inválida: host ausente.")
    if allow_private:
        return raw
    try:
        infos = socket.getaddrinfo(host, parsed.port or (443 if parsed.scheme == "https" else 80), proto=socket.IPPROTO_TCP)
    except socket.gaierror as exc:
        raise SsrfValidationError(f"{label} inválida: host não resolvível.") from exc
    if not infos:
        raise SsrfValidationError(f"{label} inválida: host não resolvível.")
    for info in infos:
        sockaddr = info[4]
        try:
            ip = ipaddress.ip_address(sockaddr[0])
        except ValueError:
            raise SsrfValidationError(f"{label} inválida.")
        if _is_blocked_ip(ip):
            raise SsrfValidationError(f"{label} aponta para um endereço interno/privado não permitido.")
    return raw


def validate_aws_region(region: str, *, label: str = "region") -> str:
    """Validate an AWS region token (e.g. ``us-east-1``) so it cannot be abused to alter the
    host of S3/STS endpoint URLs."""
    raw = (region or "").strip()
    if not _AWS_REGION_RE.match(raw):
        raise SsrfValidationError(f"{label} inválido. Use um formato como 'us-east-1'.")
    return raw


__all__ = ["SsrfValidationError", "validate_public_http_url", "validate_aws_region"]
