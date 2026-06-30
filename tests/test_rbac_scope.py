from __future__ import annotations

import pytest
from fastapi import HTTPException
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm.exc import DetachedInstanceError

from t2c_data.core.deps import enforce_role_scope_for_request, get_current_user


class DummyRole:
    def __init__(self, name: str, permissions: list[object] | None = None) -> None:
        self.name = name
        self.permissions = permissions or []


class DummyPermission:
    def __init__(self, name: str) -> None:
        self.name = name


class DummyUser:
    def __init__(self, roles: list[str], permissions_by_role: dict[str, list[str]] | None = None) -> None:
        permissions_by_role = permissions_by_role or {}
        self.roles = [DummyRole(name, [DummyPermission(permission) for permission in permissions_by_role.get(name, [])]) for name in roles]


class DetachedUser:
    __slots__ = ()

    @property
    def roles(self):  # type: ignore[override]
        raise DetachedInstanceError("detached")


def test_admin_can_access_anything() -> None:
    user = DummyUser(["admin"])
    enforce_role_scope_for_request(user, "DELETE", "/api/v1/admin/users/1")
    enforce_role_scope_for_request(user, "POST", "/api/v1/datasources")


@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("GET", "/api/v1/datasources"),
        ("POST", "/api/v1/datasources"),
        ("PUT", "/api/v1/datasources/1"),
        ("GET", "/api/v1/admin/users"),
    ],
)
def test_editor_blocked_on_datasources_and_admin(method: str, path: str) -> None:
    user = DummyUser(["editor"])
    with pytest.raises(HTTPException) as exc:
        enforce_role_scope_for_request(user, method, path)
    assert exc.value.status_code == 403


@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("GET", "/api/v1/audit/logs"),
        ("GET", "/api/v1/audit/history"),
        ("POST", "/api/v1/scan-runs/datasource/1"),
        ("POST", "/api/v1/platform/jobs/run"),
        ("POST", "/api/v1/operations/maintenance/run"),
        ("POST", "/api/v1/platform/actions/tables/1/profiling/rerun"),
    ],
)
def test_editor_can_access_audit_and_run_operations(method: str, path: str) -> None:
    # Editors read the audit trail and run operational actions (scans, jobs, ops);
    # genuinely admin-only routes (datasource connections, backups, API keys) keep
    # their own require_roles("admin").
    user = DummyUser(["editor"])
    enforce_role_scope_for_request(user, method, path)


def test_editor_allowed_on_other_write_endpoints() -> None:
    user = DummyUser(["editor"])
    enforce_role_scope_for_request(user, "PUT", "/api/v1/tables/1")
    enforce_role_scope_for_request(user, "POST", "/api/v1/dq/rules")


def test_editor_blocked_from_stewardship_write() -> None:
    user = DummyUser(["editor"])
    with pytest.raises(HTTPException) as exc:
        enforce_role_scope_for_request(user, "POST", "/api/v1/stewardship/requests")
    assert exc.value.status_code == 403


def test_editor_can_access_configuration_read_only() -> None:
    user = DummyUser(["editor"])
    enforce_role_scope_for_request(user, "GET", "/api/v1/admin/governance-settings")
    enforce_role_scope_for_request(user, "GET", "/api/v1/admin/governance-retention-summary")
    enforce_role_scope_for_request(user, "GET", "/api/v1/platform/visibility/rules")
    enforce_role_scope_for_request(user, "GET", "/api/v1/admin/governance")

    from t2c_data.core.deps import require_roles

    assert require_roles("admin", "editor")(current_user=user) is user


def test_editor_blocked_from_admin_only_route_dependency() -> None:
    from t2c_data.core.deps import require_roles

    user = DummyUser(["editor"])
    with pytest.raises(HTTPException) as exc:
        require_roles("admin")(current_user=user)
    assert exc.value.status_code == 403


@pytest.mark.parametrize("path", ["/api/v1/admin/governance", "/api/v1/admin/governance-settings"])
def test_editor_blocked_from_configuration_writes(path: str) -> None:
    user = DummyUser(["editor"])
    with pytest.raises(HTTPException) as exc:
        enforce_role_scope_for_request(user, "POST", path)
    assert exc.value.status_code == 403


@pytest.mark.parametrize(
    "roles",
    [
        ["viewer"],
        ["stewardship"],
        ["data_owner"],
    ],
)
def test_non_admin_profiles_blocked_from_configuration_area(roles: list[str]) -> None:
    user = DummyUser(roles)
    with pytest.raises(HTTPException) as exc:
        enforce_role_scope_for_request(user, "GET", "/api/v1/admin/governance-settings")
    assert exc.value.status_code == 403

    with pytest.raises(HTTPException) as exc:
        enforce_role_scope_for_request(user, "GET", "/api/v1/admin/governance")
    assert exc.value.status_code == 403

    with pytest.raises(HTTPException) as exc:
        enforce_role_scope_for_request(user, "POST", "/api/v1/admin/governance-settings")
    assert exc.value.status_code == 403


@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("GET", "/api/v1/home/summary"),
        ("GET", "/api/v1/catalog/tree"),
        ("GET", "/api/v1/lineage/spec/by-fqn"),
        ("GET", "/api/v1/tables/1/tags"),
        ("GET", "/api/v1/me"),
        ("POST", "/api/v1/me/change-password"),
    ],
)
def test_viewer_allowed_read_only_and_profile(method: str, path: str) -> None:
    user = DummyUser(["viewer"])
    enforce_role_scope_for_request(user, method, path)


@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("POST", "/api/v1/lineage/spec/tables/1"),
        ("PATCH", "/api/v1/tables/1"),
        ("GET", "/api/v1/dq/rules"),
        ("GET", "/api/v1/incidents"),
        ("GET", "/api/v1/datasources"),
        ("GET", "/api/v1/ingestion/overview"),
        ("GET", "/api/v1/admin/users"),
    ],
)
def test_viewer_blocked_from_forbidden_areas_and_writes(method: str, path: str) -> None:
    user = DummyUser(["viewer"])
    with pytest.raises(HTTPException) as exc:
        enforce_role_scope_for_request(user, method, path)
    assert exc.value.status_code == 403


@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("GET", "/api/v1/dq/rules"),
        ("GET", "/api/v1/datasources"),
        ("GET", "/api/v1/audit/logs"),
        ("GET", "/api/v1/ops/cockpit"),
        ("GET", "/api/v1/governance/pending-center"),
    ],
)
def test_stewardship_and_data_owner_can_read_everything_except_admin(method: str, path: str) -> None:
    for role_name in ("stewardship", "data_owner"):
        user = DummyUser([role_name])
        enforce_role_scope_for_request(user, method, path)


def test_stewardship_can_write_stewardship_but_not_owner_path() -> None:
    user = DummyUser(["stewardship"])
    # Stewardship acts on its own review queues.
    enforce_role_scope_for_request(user, "POST", "/api/v1/stewardship/requests")
    enforce_role_scope_for_request(user, "POST", "/api/v1/stewardship/requests/1/approve")
    # ...but cannot touch the owner mutation surface or general table metadata.
    for path in ("/api/v1/tables/1/owner", "/api/v1/tables/1"):
        with pytest.raises(HTTPException) as exc:
            enforce_role_scope_for_request(user, "PATCH", path)
        assert exc.value.status_code == 403


def test_data_owner_can_write_owner_path_and_stewardship() -> None:
    user = DummyUser(["data_owner"])
    # Data owner may reassign the asset owner/steward and act on stewardship queues...
    enforce_role_scope_for_request(user, "PATCH", "/api/v1/tables/1/owner")
    enforce_role_scope_for_request(user, "POST", "/api/v1/stewardship/requests")
    enforce_role_scope_for_request(user, "POST", "/api/v1/stewardship/requests/1/approve")
    # ...but not edit general table metadata.
    with pytest.raises(HTTPException) as exc:
        enforce_role_scope_for_request(user, "PATCH", "/api/v1/tables/1")
    assert exc.value.status_code == 403


def test_stewardship_and_data_owner_blocked_from_admin() -> None:
    for role_name in ("stewardship", "data_owner"):
        user = DummyUser([role_name])
        with pytest.raises(HTTPException) as exc:
            enforce_role_scope_for_request(user, "GET", "/api/v1/admin/users")
        assert exc.value.status_code == 403


def test_datasource_read_permission_is_not_granted_by_reader_wildcard() -> None:
    from t2c_data.core.deps import require_permission

    user = DummyUser(["viewer"], permissions_by_role={"viewer": ["*:read"]})
    with pytest.raises(HTTPException) as exc:
        require_permission("datasource:read")(current_user=user)
    assert exc.value.status_code == 403


def test_datasource_read_is_admin_only() -> None:
    from t2c_data.core.deps import require_permission

    # Datasource read is admin-only now; neither stewardship nor data_owner carry it,
    # and *:read never covers datasource:read.
    for role_name in ("stewardship", "data_owner"):
        perms = ["*:read", "user:read", "stewardship:approve", "stewardship:reject"]
        if role_name == "data_owner":
            perms.append("asset.owner:write")
        user = DummyUser([role_name], permissions_by_role={role_name: perms})
        with pytest.raises(HTTPException) as exc:
            require_permission("datasource:read")(current_user=user)
        assert exc.value.status_code == 403


def test_asset_owner_write_permission_scope() -> None:
    from t2c_data.core.deps import require_permission

    # Granted to data_owner (and editor); denied to viewer/stewardship.
    for role_name in ("data_owner", "editor"):
        user = DummyUser([role_name], permissions_by_role={role_name: ["*:read", "asset.owner:write"]})
        assert require_permission("asset.owner:write")(current_user=user) is user
    for role_name in ("viewer", "stewardship"):
        user = DummyUser([role_name], permissions_by_role={role_name: ["*:read", "user:read"]})
        with pytest.raises(HTTPException) as exc:
            require_permission("asset.owner:write")(current_user=user)
        assert exc.value.status_code == 403


def test_stewardship_decision_permission_scope() -> None:
    from t2c_data.core.deps import require_permission

    # Both stewardship and data_owner can decide review-queue items.
    for role_name in ("stewardship", "data_owner"):
        user = DummyUser([role_name], permissions_by_role={role_name: ["*:read", "stewardship:approve", "stewardship:reject"]})
        assert require_permission("stewardship:approve")(current_user=user) is user
        assert require_permission("stewardship:reject")(current_user=user) is user

    # Editor/viewer cannot.
    for role_name in ("editor", "viewer"):
        user = DummyUser([role_name], permissions_by_role={role_name: ["*:read", "user:read"]})
        with pytest.raises(HTTPException) as exc:
            require_permission("stewardship:approve")(current_user=user)
        assert exc.value.status_code == 403


def test_export_permission_is_denied_without_specific_scope() -> None:
    from t2c_data.core.deps import require_permission

    user = DummyUser(["editor"], permissions_by_role={"editor": ["user:read"]})
    with pytest.raises(HTTPException) as excinfo:
        require_permission("privacy_access:export")(current_user=user)
    assert excinfo.value.status_code == 403


def test_export_permission_is_allowed_with_specific_scope() -> None:
    from t2c_data.core.deps import require_permission

    user = DummyUser(["editor"], permissions_by_role={"editor": ["privacy_access:export", "governance:export"]})
    assert require_permission("privacy_access:export")(current_user=user) is user


@pytest.mark.parametrize(
    "permission_name",
    [
        "catalog:export",
        "glossary:export",
        "integrations.export",
        "lineage:export",
        "owners.export",
        "ops.export",
        "tag:export",
        "privacy_access:export",
        "certification:export",
        "audit:export",
    ],
)
def test_specific_export_permissions_require_explicit_scope(permission_name: str) -> None:
    from t2c_data.core.deps import require_permission

    user = DummyUser(["editor"], permissions_by_role={"editor": ["*:read", "user:read"]})
    with pytest.raises(HTTPException) as excinfo:
        require_permission(permission_name)(current_user=user)
    assert excinfo.value.status_code == 403


@pytest.mark.parametrize(
    "permission_name",
    [
        "catalog:export",
        "glossary:export",
        "integrations.export",
        "lineage:export",
        "owners.export",
        "ops.export",
        "tag:export",
    ],
)
def test_new_export_permissions_are_allowed_with_explicit_scope(permission_name: str) -> None:
    from t2c_data.core.deps import require_permission

    user = DummyUser(["editor"], permissions_by_role={"editor": [permission_name]})
    assert require_permission(permission_name)(current_user=user) is user


def test_user_role_names_handles_detached_instances_gracefully() -> None:
    from t2c_data.core.rbac import user_role_names

    assert user_role_names(DetachedUser()) == set()


def test_get_current_user_returns_503_when_database_authentication_fails(monkeypatch) -> None:
    class _FakeDB:
        def scalar(self, _query):
            raise SQLAlchemyError('password authentication failed for user "nivasmelo"')

    request = type(
        "Request",
        (),
        {
            "state": type("State", (), {})(),
            "method": "GET",
            "url": type("URL", (), {"path": "/api/v1/dq/tables/id/1/summary"})(),
        },
    )()

    monkeypatch.setattr("t2c_data.core.deps.decode_token", lambda token: "user@example.com")

    with pytest.raises(HTTPException) as exc:
        get_current_user(request=request, db=_FakeDB(), token="token")

    assert exc.value.status_code == 503
    assert "Serviço de autenticação indisponível" in str(exc.value.detail)
