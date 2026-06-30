from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable

import httpx

from t2c_data.features.operations.failures import classify_operational_error

logger = logging.getLogger(__name__)


class MetabaseClientError(RuntimeError):
    pass


def _normalize_url(url: str) -> str:
    return url.rstrip("/")


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _flatten_collection_tree(items: Iterable[Any]) -> list[dict[str, Any]]:
    flat: list[dict[str, Any]] = []

    def walk(item: Any, parent_id: str | None = None) -> None:
        data = _as_dict(item)
        if not data:
            return
        normalized = {
            "id": data.get("id") or data.get("collection_id") or data.get("collectionId"),
            "name": data.get("name") or data.get("label") or data.get("title"),
            "description": data.get("description"),
            "archived": bool(data.get("archived") or data.get("is_archived")),
            "updated_at": data.get("updated_at") or data.get("updatedAt"),
            "location": data.get("location"),
            "parent_id": parent_id or data.get("parent_id") or data.get("parentId"),
            "raw": data,
        }
        if normalized["id"] is not None:
            flat.append(normalized)
        children = data.get("children") or data.get("items") or data.get("collections") or []
        if isinstance(children, list):
            for child in children:
                walk(child, parent_id=str(normalized["id"]) if normalized["id"] is not None else parent_id)

    for item in items:
        walk(item)
    return flat


@dataclass(slots=True)
class MetabaseClientConfig:
    base_url: str
    auth_type: str | None
    auth_username: str | None
    auth_secret: str | None
    timeout_seconds: int = 10


class MetabaseClient:
    def __init__(self, config: MetabaseClientConfig):
        self.config = config
        self.base_url = _normalize_url(config.base_url)
        self.timeout = httpx.Timeout(max(float(config.timeout_seconds or 10), 1.0))
        self._session_id: str | None = None
        # follow_redirects disabled to prevent SSRF redirect-bypass to internal targets.
        self._client = httpx.Client(base_url=self.base_url, timeout=self.timeout, follow_redirects=False)

    @staticmethod
    def _retry_delay(attempt: int) -> float:
        base_delay = min(4.0, 0.5 * (2 ** max(attempt - 1, 0)))
        return base_delay + random.uniform(0.0, max(0.05, base_delay * 0.2))

    @staticmethod
    def _should_retry_request_error(exc: Exception) -> bool:
        _category, _severity, retryable = classify_operational_error(exc, source="metabase.client")
        return retryable

    def _request(self, method: str, path: str, *, params: dict[str, Any] | None = None, json_body: Any = None) -> httpx.Response:
        retryable_statuses = {408, 429, 500, 502, 503, 504}
        last_exc: Exception | None = None
        attempts = 3
        for attempt in range(1, attempts + 1):
            try:
                method_upper = method.upper()
                if method_upper == "GET":
                    response = self._client.get(path, params=params, headers=self._headers())
                elif method_upper == "POST":
                    response = self._client.post(path, params=params, json=json_body, headers=self._headers())
                else:
                    response = self._client.request(
                        method_upper,
                        path,
                        params=params,
                        json=json_body,
                        headers=self._headers(),
                    )
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                if attempt >= attempts or not self._should_retry_request_error(exc):
                    break
                time.sleep(self._retry_delay(attempt))
                continue
            if response.status_code in retryable_statuses and attempt < attempts:
                time.sleep(self._retry_delay(attempt))
                last_exc = MetabaseClientError(f"Metabase request failed {method} {path}: {response.status_code} {response.text[:300]}")
                continue
            return response
        if last_exc is not None:
            if isinstance(last_exc, Exception):
                raise MetabaseClientError(f"Metabase request failed {method} {path} against {self.base_url}: {last_exc}") from last_exc
            raise last_exc
        raise MetabaseClientError(f"Metabase request failed {method} {path}: retry exhausted")

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "MetabaseClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _headers(self) -> dict[str, str]:
        headers: dict[str, str] = {"Accept": "application/json"}
        auth_type = (self.config.auth_type or "").strip().lower()
        if auth_type == "session" and self._session_id:
            headers["X-Metabase-Session"] = self._session_id
        elif auth_type in {"bearer", "api_key"} and self.config.auth_secret:
            headers["Authorization"] = f"Bearer {self.config.auth_secret}"
        elif auth_type == "header" and self.config.auth_secret:
            headers["X-Metabase-Session"] = self.config.auth_secret
        return headers

    def authenticate(self) -> None:
        auth_type = (self.config.auth_type or "none").strip().lower()
        if auth_type in {"", "none"}:
            return
        if auth_type == "session":
            if not self.config.auth_username or not self.config.auth_secret:
                raise MetabaseClientError("Metabase session auth requires username and secret")
            try:
                response = self._request(
                    "POST",
                    "/api/session",
                    json_body={"username": self.config.auth_username, "password": self.config.auth_secret},
                )
            except Exception as exc:  # noqa: BLE001
                raise MetabaseClientError(f"Metabase session auth failed against {self.base_url}: {exc}") from exc
            if response.is_error:
                raise MetabaseClientError(f"Metabase session auth failed: {response.status_code} {response.text[:300]}")
            payload = response.json()
            session_id = payload.get("id") or payload.get("session_id") or payload.get("sessionId")
            if not session_id:
                raise MetabaseClientError("Metabase session auth response did not return a session id")
            self._session_id = str(session_id)
            return
        if auth_type in {"bearer", "api_key", "header"}:
            if not self.config.auth_secret:
                raise MetabaseClientError("Metabase auth secret is required for token auth")
            return
        raise MetabaseClientError(f"Unsupported Metabase auth type: {auth_type}")

    def request_json(self, method: str, path: str, *, params: dict[str, Any] | None = None, json_body: Any = None) -> Any:
        try:
            response = self._request(method, path, params=params, json_body=json_body)
        except Exception as exc:  # noqa: BLE001
            raise MetabaseClientError(f"Metabase request failed {method} {path} against {self.base_url}: {exc}") from exc
        if response.is_error:
            raise MetabaseClientError(f"Metabase request failed {method} {path}: {response.status_code} {response.text[:300]}")
        if not response.content:
            return None
        try:
            return response.json()
        except ValueError as exc:
            raise MetabaseClientError(f"Metabase returned invalid JSON for {method} {path}") from exc

    def probe_health(self) -> dict[str, Any]:
        try:
            response = self._request("GET", "/api/health")
        except Exception as exc:  # noqa: BLE001
            raise MetabaseClientError(f"Metabase health check failed GET /api/health against {self.base_url}: {exc}") from exc
        if response.is_error:
            raise MetabaseClientError(
                f"Metabase health check failed GET /api/health: {response.status_code} {response.text[:300]}"
            )
        try:
            payload = response.json()
        except ValueError:
            payload = {}
        return payload if isinstance(payload, dict) else {}

    def list_dashboards(self) -> list[dict[str, Any]]:
        payload = self.request_json("GET", "/api/dashboard")
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        if isinstance(payload, dict):
            for key in ("data", "items", "dashboards", "results"):
                value = payload.get(key)
                if isinstance(value, list):
                    return [item for item in value if isinstance(item, dict)]
        return []

    def get_dashboard(self, dashboard_id: str | int) -> dict[str, Any]:
        payload = self.request_json("GET", f"/api/dashboard/{dashboard_id}")
        return _as_dict(payload)

    def list_cards(self) -> list[dict[str, Any]]:
        payload = self.request_json("GET", "/api/card")
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        if isinstance(payload, dict):
            for key in ("data", "items", "cards", "results"):
                value = payload.get(key)
                if isinstance(value, list):
                    return [item for item in value if isinstance(item, dict)]
        return []

    def get_card(self, card_id: str | int) -> dict[str, Any]:
        payload = self.request_json("GET", f"/api/card/{card_id}")
        return _as_dict(payload)

    def list_collections(self) -> list[dict[str, Any]]:
        for path in ("/api/collection/tree", "/api/collection"):
            try:
                payload = self.request_json("GET", path)
            except MetabaseClientError:
                continue
            if isinstance(payload, list):
                flat = _flatten_collection_tree(payload)
                if flat:
                    return flat
            if isinstance(payload, dict):
                for key in ("data", "items", "collections", "roots"):
                    value = payload.get(key)
                    if isinstance(value, list):
                        flat = _flatten_collection_tree(value)
                        if flat:
                            return flat
        return []

    def get_collection(self, collection_id: str | int) -> dict[str, Any]:
        payload = self.request_json("GET", f"/api/collection/{collection_id}")
        return _as_dict(payload)

    def get_database_metadata(self, database_id: str | int) -> dict[str, Any]:
        for path in (f"/api/database/{database_id}/metadata", f"/api/database/{database_id}"):
            try:
                payload = self.request_json("GET", path)
            except MetabaseClientError:
                continue
            if isinstance(payload, dict):
                return payload
        return {}

    @staticmethod
    def parse_remote_datetime(value: str | None) -> datetime | None:
        return _parse_iso(value)


__all__ = ["MetabaseClient", "MetabaseClientConfig", "MetabaseClientError"]
