from __future__ import annotations

import os
import unittest

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")
os.environ.setdefault("INITIAL_ADMIN_NAME", "Reset Admin")
os.environ.setdefault("INITIAL_ADMIN_EMAIL", "reset-admin@example.com")
os.environ.setdefault("INITIAL_ADMIN_PASSWORD", "reset-admin-pass")

from sqlalchemy import create_engine, select
from sqlalchemy.orm import selectinload, sessionmaker

from t2c_data.core.config import settings
from t2c_data.core.installation_reset import reset_installation_state
from t2c_data.core.security import verify_password
from t2c_data.models.auth import Permission, Role, User, role_permission, user_role
from t2c_data.models.catalog import DataSource


class InstallationResetTest(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = create_engine("sqlite+pysqlite:///:memory:", future=True).execution_options(
            schema_translate_map={settings.db_schema: None}
        )
        with self.engine.begin() as conn:
            conn.exec_driver_sql(
                "CREATE TABLE alembic_version (version_num VARCHAR(32) NOT NULL PRIMARY KEY)"
            )
            User.__table__.create(bind=conn)
            Role.__table__.create(bind=conn)
            Permission.__table__.create(bind=conn)
            user_role.create(bind=conn)
            role_permission.create(bind=conn)
            DataSource.__table__.create(bind=conn)
        self.SessionLocal = sessionmaker(bind=self.engine, future=True)

    def tearDown(self) -> None:
        self.engine.dispose()

    def test_reset_truncates_all_app_tables_and_reseeds_admin_bootstrap(self) -> None:
        with self.SessionLocal() as session:
            datasource = DataSource(
                name="legacy-source",
                db_type="postgres",
                host="localhost",
                port=5432,
                database="legacy",
                username="legacy",
            )
            datasource.set_secret_values({"password": "legacy"})
            legacy_role = Role(name="legacy", description="Legacy")
            legacy_perm = Permission(name="legacy:access", description="Legacy access")
            legacy_user = User(
                email="old@example.com",
                name="Old User",
                full_name="Old User",
                password_hash="old-hash",
                is_active=True,
            )
            session.add_all([datasource, legacy_role, legacy_perm, legacy_user])
            session.commit()

        with self.SessionLocal() as session:
            report = reset_installation_state(session)

            roles = {role.name for role in session.scalars(select(Role)).all()}
            permissions = {perm.name for perm in session.scalars(select(Permission)).all()}
            users = session.scalars(select(User)).all()
            admin = session.scalar(
                select(User).options(selectinload(User.roles)).where(User.email == settings.bootstrap_admin_email)
            )
            admin_roles = [role.name for role in admin.roles] if admin else []

        self.assertIn("data_sources", report.truncated_tables)
        self.assertIn("users", report.truncated_tables)
        self.assertEqual(report.bootstrap_admin_email, settings.bootstrap_admin_email)
        self.assertTrue(report.seed_applied)
        self.assertEqual(roles, {"admin", "editor", "viewer", "stewardship", "data_owner"})
        self.assertIn("admin:access", permissions)
        self.assertNotIn("legacy:access", permissions)
        self.assertEqual(len(users), 1)
        self.assertIsNotNone(admin)
        self.assertTrue(admin.is_active)
        self.assertEqual(admin.name, settings.bootstrap_admin_name)
        self.assertEqual(admin.full_name, settings.bootstrap_admin_name)
        self.assertTrue(verify_password(settings.bootstrap_admin_password, admin.password_hash))
        self.assertEqual(admin_roles, ["admin"])


if __name__ == "__main__":
    unittest.main()
