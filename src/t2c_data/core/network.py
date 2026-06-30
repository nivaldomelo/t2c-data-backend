from __future__ import annotations

from typing import Any


def get_request_client_ip(request: Any) -> str | None:
    client = getattr(request, "client", None)
    client_host = getattr(client, "host", None)
    if isinstance(client_host, str) and client_host.strip():
        return client_host.strip()
    return None
