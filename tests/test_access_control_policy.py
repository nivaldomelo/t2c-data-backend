from __future__ import annotations

import unittest

from t2c_data.features.access_control.policy import can_view_schema, can_view_table, user_has_data_scope_rules
from t2c_data.models.access_control import AccessGroup, DataAccessGrant
from t2c_data.models.auth import Role, User
from t2c_data.models.catalog import DataSource, Database, Schema, TableEntity


def _build_table(table_id: int = 10, schema_id: int = 20, datasource_id: int = 30, *, sensitivity_level: str | None = None):
    datasource = DataSource(id=datasource_id, name="ds", db_type="postgres", host="localhost", port=5432, database="db", username="user")
    database = Database(id=schema_id + 1, datasource_id=datasource_id, name="db")
    schema = Schema(id=schema_id, database_id=database.id, name="public")
    database.datasource = datasource
    schema.database = database
    table = TableEntity(
        id=table_id,
        schema_id=schema_id,
        name="orders",
        table_type="table",
        description_source=None,
        description_manual=None,
        owner=None,
        owner_email=None,
        lifecycle_status=None,
        certification_status="not_eligible",
        has_personal_data=False,
        has_sensitive_personal_data=False,
        is_masked=False,
        external_sharing=False,
        sensitivity_level=sensitivity_level,
    )
    table.schema = schema
    return datasource, database, schema, table


class AccessControlPolicyTest(unittest.TestCase):
    def test_table_visible_without_scope_rules_falls_back_to_role(self) -> None:
        _, _, schema, table = _build_table()
        user = User(id=1, email="editor@example.com", password_hash="x", is_active=True)
        user.roles = [Role(id=1, name="editor")]
        self.assertFalse(user_has_data_scope_rules(user))
        self.assertTrue(can_view_table(user, table))
        self.assertTrue(can_view_schema(user, schema, [table]))

    def test_schema_allow_grants_visibility(self) -> None:
        _, _, schema, table = _build_table()
        user = User(id=1, email="viewer@example.com", password_hash="x", is_active=True)
        user.roles = [Role(id=1, name="viewer")]
        user.access_grants = [
            DataAccessGrant(id=1, effect="allow", schema_id=schema.id),
        ]
        self.assertTrue(user_has_data_scope_rules(user))
        self.assertTrue(can_view_table(user, table))
        self.assertTrue(can_view_schema(user, schema, [table]))

    def test_explicit_deny_wins_over_allow(self) -> None:
        _, _, schema, table = _build_table()
        group = AccessGroup(id=1, name="bi", description=None, is_active=True)
        group.grants = [DataAccessGrant(id=1, effect="allow", schema_id=schema.id)]
        user = User(id=1, email="viewer@example.com", password_hash="x", is_active=True)
        user.roles = [Role(id=1, name="viewer")]
        user.access_groups = [group]
        user.access_grants = [DataAccessGrant(id=2, effect="deny", table_id=table.id)]
        self.assertTrue(user_has_data_scope_rules(user))
        self.assertFalse(can_view_table(user, table))


if __name__ == "__main__":
    unittest.main()

