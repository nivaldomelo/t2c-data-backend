from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Iterable

from t2c_data.features.privacy_access.policy import _can_view_table_privacy_only
from t2c_data.features.access_control.abac import can_access_resource
from t2c_data.models.access_control import DataAccessGrant
from t2c_data.models.auth import User
from t2c_data.models.catalog import DataSource, Schema, TableEntity
from t2c_data.core.rbac import is_admin_role, user_role_names

GRANT_EFFECT_ALLOW = "allow"
GRANT_EFFECT_DENY = "deny"
SCOPE_DATA_SOURCE = "datasource"
SCOPE_SCHEMA = "schema"
SCOPE_OBJECT = "object"


@dataclass(slots=True)
class DataScopeDecision:
    visible: bool
    explicit: bool
    reason: str | None = None


def _normalize_effect(value: str | None) -> str:
    raw = (value or "").strip().lower()
    return raw if raw in {GRANT_EFFECT_ALLOW, GRANT_EFFECT_DENY} else GRANT_EFFECT_ALLOW


def _principal_grants(user: User | None) -> list[DataAccessGrant]:
    if user is None:
        return []
    grants: dict[int, DataAccessGrant] = {}
    for grant in getattr(user, "access_grants", []) or []:
        if grant.id is not None:
            grants[int(grant.id)] = grant
    for group in getattr(user, "access_groups", []) or []:
        for grant in getattr(group, "grants", []) or []:
            if grant.id is not None:
                grants[int(grant.id)] = grant
    return list(grants.values())


def user_has_data_scope_rules(user: User | None) -> bool:
    return bool(_principal_grants(user))


def _table_datasource_id(table: TableEntity) -> int | None:
    schema = getattr(table, "schema", None)
    database = getattr(schema, "database", None) if schema else None
    datasource = getattr(database, "datasource", None) if database else None
    datasource_id = getattr(database, "datasource_id", None)
    if datasource_id is not None:
        return int(datasource_id)
    if datasource is not None:
        return int(getattr(datasource, "id", 0) or 0) or None
    return None


def _grant_matches_table(grant: DataAccessGrant, table: TableEntity) -> bool:
    if grant.table_id is not None:
        return int(grant.table_id) == int(table.id)
    if grant.schema_id is not None:
        return int(grant.schema_id) == int(table.schema_id)
    if grant.datasource_id is not None:
        table_datasource_id = _table_datasource_id(table)
        return table_datasource_id is not None and int(grant.datasource_id) == int(table_datasource_id)
    return False


def _grant_matches_schema(grant: DataAccessGrant, schema: Schema) -> bool:
    if grant.schema_id is not None:
        return int(grant.schema_id) == int(schema.id)
    if grant.datasource_id is not None:
        database = getattr(schema, "database", None)
        datasource_id = getattr(database, "datasource_id", None)
        return datasource_id is not None and int(grant.datasource_id) == int(datasource_id)
    return False


def _grant_matches_datasource(grant: DataAccessGrant, datasource: DataSource) -> bool:
    if grant.datasource_id is not None:
        return int(grant.datasource_id) == int(datasource.id)
    return False


def _evaluate_grants(grants: Iterable[DataAccessGrant], *, matches: Callable[[DataAccessGrant], bool]) -> DataScopeDecision:
    matched = [grant for grant in grants if matches(grant)]
    if not matched:
        return DataScopeDecision(visible=False, explicit=False, reason="no_match")
    if any(_normalize_effect(grant.effect) == GRANT_EFFECT_DENY for grant in matched):
        return DataScopeDecision(visible=False, explicit=True, reason="deny")
    if any(_normalize_effect(grant.effect) == GRANT_EFFECT_ALLOW for grant in matched):
        return DataScopeDecision(visible=True, explicit=True, reason="allow")
    return DataScopeDecision(visible=False, explicit=False, reason="no_match")


def can_view_table(user: User | None, table: TableEntity) -> bool:
    if not _can_view_table_privacy_only(user, table):
        return False
    if user is None:
        return False
    role_names = user_role_names(user)
    if is_admin_role(role_names):
        return True

    grants = _principal_grants(user)
    if not grants:
        return can_access_resource(user, action="read", table=table)

    matched = [grant for grant in grants if _grant_matches_table(grant, table)]
    if not matched:
        return False
    if any(_normalize_effect(grant.effect) == GRANT_EFFECT_DENY for grant in matched):
        return False
    if not any(_normalize_effect(grant.effect) == GRANT_EFFECT_ALLOW for grant in matched):
        return False
    return can_access_resource(user, action="read", table=table)


def can_view_schema(user: User | None, schema: Schema, tables: list[TableEntity] | None = None) -> bool:
    if user is None:
        return False
    role_names = user_role_names(user)
    if is_admin_role(role_names):
        return True
    grants = _principal_grants(user)
    if not grants:
        return bool(tables and any(can_view_table(user, table) for table in tables))
    decision = _evaluate_grants(grants, matches=lambda grant: _grant_matches_schema(grant, schema))
    if decision.visible:
        return True
    if decision.explicit:
        return False
    if tables:
        return any(can_view_table(user, table) for table in tables)
    return False


def can_view_datasource(user: User | None, datasource: DataSource, schemas: list[Schema] | None = None, tables: list[TableEntity] | None = None) -> bool:
    if user is None:
        return False
    role_names = user_role_names(user)
    if is_admin_role(role_names):
        return True
    grants = _principal_grants(user)
    if not grants:
        return bool(tables and any(can_view_table(user, table) for table in tables))
    decision = _evaluate_grants(grants, matches=lambda grant: _grant_matches_datasource(grant, datasource))
    if decision.visible:
        return True
    if decision.explicit:
        return False
    if tables:
        return any(can_view_table(user, table) for table in tables)
    if schemas:
        return any(can_view_schema(user, schema) for schema in schemas)
    return False


def visible_table_ids(user: User | None, tables: Iterable[TableEntity]) -> list[int]:
    return [int(table.id) for table in tables if can_view_table(user, table)]
