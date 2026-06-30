from __future__ import annotations

from sqlalchemy.orm.exc import DetachedInstanceError

from t2c_data.models.auth import User

ROLE_ADMIN = "admin"
ROLE_EDITOR = "editor"
ROLE_VIEWER = "viewer"
ROLE_STEWARDSHIP = "stewardship"
ROLE_DATA_OWNER = "data_owner"

READ_ONLY_WRITE_EXCEPTIONS = {"/search/track-query", "/search/track-click", "/search/favorites", "/assistant/explain"}


def normalize_role_name(value: str | None) -> str:
    raw = (value or "").strip().lower()
    if not raw:
        return ""
    aliases = {
        "administrador": ROLE_ADMIN,
        "admin": ROLE_ADMIN,
        "editor": ROLE_EDITOR,
        "analista": ROLE_EDITOR,
        "analyst": ROLE_EDITOR,
        "viewer": ROLE_VIEWER,
        "visualizador": ROLE_VIEWER,
        "reader": ROLE_VIEWER,
        "leitor": ROLE_VIEWER,
        "stewardship": ROLE_STEWARDSHIP,
        "steward": ROLE_STEWARDSHIP,
        "data_owner": ROLE_DATA_OWNER,
        "data-owner": ROLE_DATA_OWNER,
        "data owner": ROLE_DATA_OWNER,
        "dataowner": ROLE_DATA_OWNER,
        "owner": ROLE_DATA_OWNER,
    }
    return aliases.get(raw, raw)


def user_role_names(user: User | None) -> set[str]:
    if user is None:
        return set()
    roles = getattr(user, "__dict__", {}).get("roles")
    if roles is None:
        try:
            roles = getattr(user, "roles", [])
        except DetachedInstanceError:
            roles = []
    return {normalize_role_name(role.name) for role in roles if normalize_role_name(role.name)}


def is_admin_role(roles: set[str]) -> bool:
    return ROLE_ADMIN in roles


def is_editor_role(roles: set[str]) -> bool:
    return ROLE_EDITOR in roles and ROLE_ADMIN not in roles


def is_viewer_role(roles: set[str]) -> bool:
    return ROLE_VIEWER in roles and ROLE_ADMIN not in roles and ROLE_EDITOR not in roles


def is_stewardship_role(roles: set[str]) -> bool:
    return ROLE_STEWARDSHIP in roles and ROLE_ADMIN not in roles


def is_data_owner_role(roles: set[str]) -> bool:
    return ROLE_DATA_OWNER in roles and ROLE_ADMIN not in roles


def _normalize_api_scope_path(path: str) -> str:
    if path.startswith("/api/v1"):
        normalized = path[len("/api/v1") :]
    elif path.startswith("/api"):
        normalized = path[len("/api") :]
    else:
        normalized = path
    return normalized or "/"


def _is_profile_path(path: str) -> bool:
    return path == "/me" or path.startswith("/me/")


def _resource_from_path(path: str) -> str:
    if path == "/" or path.startswith("/home"):
        return "home"
    if path.startswith("/dashboard"):
        return "dashboard"
    if path.startswith("/search"):
        return "search"
    if path.startswith("/assistant"):
        return "assistant"
    if path.startswith("/explorer"):
        return "explorer"
    if path.startswith("/governance/stewardship") or path == "/stewardship":
        return "stewardship"
    if path.startswith("/governance"):
        return "governance"
    if path.startswith("/lineage"):
        return "lineage"
    if path.startswith("/data-quality") or path.startswith("/dq"):
        return "dataQuality"
    if path.startswith("/incidents"):
        return "incidents"
    if path.startswith("/glossary"):
        return "glossary"
    if path.startswith("/tags"):
        return "tags"
    if path.startswith("/certification"):
        return "certification"
    if path.startswith("/privacy-access"):
        return "privacyAccess"
    if path.startswith("/admin/governance"):
        return "configuration"
    if path.startswith("/me") or path.startswith("/profile"):
        return "profile"
    if path.startswith("/inbox"):
        return "inbox"
    if path.startswith("/data-owners"):
        return "dataOwners"
    if path.startswith("/datasources"):
        return "datasources"
    if path.startswith("/audit"):
        return "audit"
    if path.startswith("/ops"):
        return "ops"
    if path.startswith("/admin"):
        return "admin"
    return "other"


def can_access_path(roles: set[str], method: str, path: str) -> bool:
    if not roles:
        return False
    if is_admin_role(roles):
        return True

    normalized_path = _normalize_api_scope_path(path)
    resource = _resource_from_path(normalized_path)
    http_method = method.upper()

    if _is_profile_path(normalized_path):
        return True
    if resource == "inbox":
        return True
    if resource == "configuration":
        return is_editor_role(roles)

    if is_stewardship_role(roles) or is_data_owner_role(roles):
        if resource == "admin":
            return False
        if resource == "assistant":
            return True
        if http_method == "GET":
            return True
        return resource == "stewardship"

    if is_editor_role(roles):
        if resource == "configuration":
            return http_method == "GET"
        if resource in {"datasources", "audit", "admin"}:
            return False
        if resource == "stewardship" and http_method != "GET":
            return False
        return True

    if is_viewer_role(roles):
        if http_method != "GET":
            return (
                normalized_path in READ_ONLY_WRITE_EXCEPTIONS
                or normalized_path.startswith("/search/favorites/")
                or normalized_path.startswith("/assistant/explain/")
            )
        allowed_prefixes = (
            "/home",
            "/dashboard",
            "/search",
            "/explorer",
            "/lineage",
            "/privacy-access",
            "/certification",
        )
        return any(normalized_path == prefix or normalized_path.startswith(f"{prefix}/") for prefix in allowed_prefixes)

    return False
