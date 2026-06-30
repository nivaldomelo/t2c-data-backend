from __future__ import annotations

from t2c_data.features.access_control.abac import can_access_resource
from t2c_data.features.access_control.policy import can_view_table
from t2c_data.features.platform.sensitive_data import can_view_sensitive_data
from t2c_data.models.access_control import DataAccessGrant
from t2c_data.models.auth import Permission, Role, User
from t2c_data.models.catalog import DataOwner, DataSource, Database, Schema, TableEntity


def _build_table(
    *,
    sensitivity_level: str | None = None,
    environment: str | None = "shared",
    domain_name: str | None = "Finance",
    owner_email: str | None = "owner@example.com",
    has_personal_data: bool = False,
    has_sensitive_personal_data: bool = False,
) -> TableEntity:
    datasource = DataSource(
        id=30,
        name="ds",
        db_type="postgres",
        host="localhost",
        port=5432,
        database="db",
        username="user",
        environment=environment,
    )
    database = Database(id=20, datasource_id=datasource.id, name="db")
    schema = Schema(id=10, database_id=database.id, name="public")
    data_owner = DataOwner(id=5, name="Owner", email=owner_email or "owner@example.com", area=domain_name, is_active=True)
    database.datasource = datasource
    schema.database = database
    table = TableEntity(
        id=10,
        schema_id=schema.id,
        name="orders",
        table_type="table",
        description_source=None,
        description_manual=None,
        owner="Owner",
        owner_email=owner_email,
        lifecycle_status=None,
        certification_status="not_eligible",
        has_personal_data=has_personal_data,
        has_sensitive_personal_data=has_sensitive_personal_data,
        sensitivity_level=sensitivity_level,
        is_masked=False,
        external_sharing=False,
    )
    table.schema = schema
    table.data_owner = data_owner
    return table


def _user(
    *,
    roles: list[str],
    permissions: list[str] | None = None,
    allowed_domains: list[str] | None = None,
    allowed_environments: list[str] | None = None,
) -> User:
    user = User(id=1, email="user@example.com", password_hash="x", is_active=True)
    user.roles = [Role(id=index + 1, name=role, permissions=[]) for index, role in enumerate(roles)]
    if permissions:
        user.roles[0].permissions = [Permission(id=index + 100, name=permission) for index, permission in enumerate(permissions)]
    user.allowed_domains = allowed_domains
    user.allowed_environments = allowed_environments
    return user


def test_sensitive_table_requires_sensitive_read_or_trusted_owner() -> None:
    table = _build_table(sensitivity_level="restricted", has_personal_data=True, has_sensitive_personal_data=True)
    user = _user(roles=["viewer"], allowed_domains=["finance"], allowed_environments=["shared"])
    user.access_grants = [DataAccessGrant(id=1, effect="allow", schema_id=table.schema_id)]

    assert can_view_table(user, table) is False
    assert can_view_sensitive_data(user, table=table) is False


def test_sensitive_table_allows_explicit_sensitive_read_with_domain_and_env() -> None:
    table = _build_table(sensitivity_level="restricted", environment="production", has_personal_data=True)
    user = _user(
        roles=["stewardship"],
        permissions=["sensitive:read"],
        allowed_domains=["finance"],
        allowed_environments=["prod"],
    )
    user.access_grants = [DataAccessGrant(id=1, effect="allow", schema_id=table.schema_id)]

    assert can_view_table(user, table) is True
    assert can_view_sensitive_data(user, table=table) is True


def test_production_environment_requires_environment_access() -> None:
    table = _build_table(sensitivity_level=None, environment="production", domain_name=None, has_personal_data=False)
    user = _user(roles=["viewer"])

    assert can_access_resource(user, action="read", table=table) is False

    user.allowed_environments = ["prod"]
    assert can_access_resource(user, action="read", table=table) is True


def test_sensitive_export_requires_specific_permission() -> None:
    table = _build_table(sensitivity_level="restricted", has_sensitive_personal_data=True)
    user = _user(roles=["viewer"], permissions=["sensitive:read"], allowed_domains=["finance"], allowed_environments=["shared"])

    assert can_access_resource(user, action="export", table=table) is False

    user.roles[0].permissions.append(Permission(id=999, name="sensitive:export"))
    assert can_access_resource(user, action="export", table=table) is True
