from __future__ import annotations

import os
import unittest
from datetime import datetime, timezone

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")
os.environ.setdefault("INITIAL_ADMIN_NAME", "Reset Admin")
os.environ.setdefault("INITIAL_ADMIN_EMAIL", "reset-admin@example.com")
os.environ.setdefault("INITIAL_ADMIN_PASSWORD", "reset-admin-pass")

from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import sessionmaker

from t2c_data.core.config import settings
from t2c_data.core.platform_data_reset import build_reset_plan, truncate_tables, validate_post_reset_state, validate_reset_plan
from t2c_data.models.access_control import AccessGroup
from t2c_data.models.auth import Permission, Role, User, UserSession, role_permission, user_access_group, user_role
from t2c_data.models.catalog import DataSource, Database, Schema, TableEntity


class PlatformDataResetTest(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = create_engine("sqlite+pysqlite:///:memory:", future=True).execution_options(
            schema_translate_map={settings.db_schema: None}
        )
        with self.engine.begin() as connection:
            connection.exec_driver_sql(
                "CREATE TABLE alembic_version (version_num VARCHAR(32) NOT NULL PRIMARY KEY)"
            )
            User.__table__.create(bind=connection)
            Role.__table__.create(bind=connection)
            Permission.__table__.create(bind=connection)
            user_role.create(bind=connection)
            role_permission.create(bind=connection)
            AccessGroup.__table__.create(bind=connection)
            user_access_group.create(bind=connection)
            UserSession.__table__.create(bind=connection)
            DataSource.__table__.create(bind=connection)
            Database.__table__.create(bind=connection)
            Schema.__table__.create(bind=connection)
            TableEntity.__table__.create(bind=connection)
            connection.exec_driver_sql("INSERT INTO alembic_version (version_num) VALUES ('head')")
        self.SessionLocal = sessionmaker(bind=self.engine, future=True)

    def tearDown(self) -> None:
        self.engine.dispose()

    def _seed_data(self) -> None:
        with self.SessionLocal() as session:
            admin_role = Role(name="admin", description="Full access")
            viewer_role = Role(name="viewer", description="Read only")
            access_perm = Permission(name="admin:access", description="Access admin area")
            admin = User(
                email=settings.bootstrap_admin_email,
                name=settings.bootstrap_admin_name,
                full_name=settings.bootstrap_admin_name,
                password_hash="hash",
                is_active=True,
            )
            legacy_user = User(
                email="legacy@example.com",
                name="Legacy User",
                full_name="Legacy User",
                password_hash="hash-legacy",
                is_active=True,
            )
            admin.roles.append(admin_role)
            admin_role.permissions.append(access_perm)

            access_group = AccessGroup(name="stewards", description="Steward group", is_active=True)
            access_group.users.append(admin)

            user_session = UserSession(
                user=admin,
                jti="jti-001",
                started_at=datetime.now(timezone.utc),
                last_seen_at=datetime.now(timezone.utc),
                expires_at=datetime.now(timezone.utc),
                success=True,
            )

            datasource = DataSource(
                name="legacy-source",
                db_type="postgres",
                host="localhost",
                port=5432,
                database="legacy",
                username="legacy",
                is_active=True,
            )
            database = Database(datasource=datasource, name="legacy_db")
            schema = Schema(database=database, name="legacy_schema")
            table = TableEntity(schema=schema, name="legacy_table", table_type="table")

            session.add_all([admin_role, viewer_role, access_perm, admin, legacy_user, access_group, user_session, datasource, database, schema, table])
            session.commit()

    def test_safe_reset_plan_preserves_login_tables_and_truncates_business_tables(self) -> None:
        self._seed_data()

        with self.engine.connect() as connection:
            plan = build_reset_plan(connection, settings.db_schema)
            validate_reset_plan(plan)

        self.assertIn("users", plan.preserved_tables)
        self.assertIn("roles", plan.preserved_tables)
        self.assertIn("permissions", plan.preserved_tables)
        self.assertIn("user_role", plan.preserved_tables)
        self.assertIn("role_permissions", plan.preserved_tables)
        self.assertIn("alembic_version", plan.preserved_tables)
        self.assertIn("data_sources", plan.truncated_tables)
        self.assertIn("databases", plan.truncated_tables)
        self.assertIn("schemas", plan.truncated_tables)
        self.assertIn("tables", plan.truncated_tables)

        with self.engine.begin() as connection:
            session = self.SessionLocal(bind=connection)
            try:
                truncate_tables(connection, settings.db_schema, plan.truncated_tables)
                validation = validate_post_reset_state(session, settings.db_schema, plan.truncated_tables)

                remaining_users = session.scalars(select(User)).all()
                admin = session.scalar(select(User).where(User.email == settings.bootstrap_admin_email))
                datasource_rows = session.scalar(select(func.count(DataSource.id)))
                access_group_rows = session.scalar(select(func.count(AccessGroup.id)))
                user_access_group_rows = session.scalar(select(func.count()).select_from(user_access_group))
                user_session_rows = session.scalar(select(func.count(UserSession.id)))
                role_count = session.scalar(select(func.count(Role.id)))
                permission_count = session.scalar(select(func.count(Permission.id)))
                user_role_rows = session.scalar(select(func.count()).select_from(user_role))
            finally:
                session.close()

        self.assertEqual(validation.users_total, 2)
        self.assertTrue(validation.admin_exists)
        self.assertEqual(len(remaining_users), 2)
        self.assertIsNotNone(admin)
        self.assertEqual(datasource_rows, 0)
        self.assertEqual(access_group_rows, 1)
        self.assertEqual(user_access_group_rows, 1)
        self.assertEqual(user_session_rows, 1)
        self.assertEqual(role_count, 2)
        self.assertEqual(permission_count, 1)
        self.assertEqual(user_role_rows, 1)
        self.assertEqual(validation.preserved_counts.get("users"), 2)
        self.assertEqual(validation.preserved_counts.get("roles"), 2)
        self.assertEqual(validation.preserved_counts.get("permissions"), 1)
        self.assertEqual(validation.preserved_counts.get("user_access_groups"), 1)
        self.assertEqual(validation.preserved_counts.get("user_sessions"), 1)
        self.assertEqual(validation.preserved_counts.get("access_groups"), 1)
        self.assertEqual(validation.preserved_counts.get("alembic_version"), 1)
        self.assertEqual(validation.truncated_non_empty, [])


if __name__ == "__main__":
    unittest.main()
