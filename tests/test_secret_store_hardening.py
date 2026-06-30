from __future__ import annotations

import os

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from t2c_data.core.config import Settings, settings
from t2c_data.core.secret_audit import audit_plaintext_secrets
from t2c_data.core.secret_store import PlaintextSecretError, decrypt_secret_mapping, encrypt_secret_mapping
from t2c_data.models.catalog import DataSource


def _build_session() -> Session:
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @event.listens_for(engine, "connect")
    def _attach_schema(dbapi_connection, _connection_record) -> None:  # type: ignore[no-untyped-def]
        cursor = dbapi_connection.cursor()
        cursor.execute("ATTACH DATABASE ':memory:' AS t2c_data")
        cursor.close()

    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, class_=Session)
    return SessionLocal()


def test_secret_roundtrip_uses_encrypted_payload() -> None:
    encrypted = encrypt_secret_mapping({"password": "super-secret"})

    assert encrypted.startswith("enc::")
    assert "super-secret" not in encrypted
    assert decrypt_secret_mapping(encrypted) == {"password": "super-secret"}


def test_datasource_password_setter_stores_encrypted_secret() -> None:
    datasource = DataSource(
        name="warehouse",
        db_type="postgres",
        host="localhost",
        port=5432,
        database="catalog",
        username="reader",
    )
    datasource.password = "super-secret"

    assert datasource._secret_payload.startswith("enc::")
    assert "super-secret" not in datasource._secret_payload


def test_plaintext_secret_fails_closed_outside_dev(monkeypatch) -> None:
    monkeypatch.setattr(settings, "env", "prod")
    monkeypatch.setattr(settings, "allow_plaintext_secrets", False)

    with pytest.raises(PlaintextSecretError):
        decrypt_secret_mapping("super-secret")


def test_plaintext_secret_requires_explicit_dev_compatibility(monkeypatch) -> None:
    monkeypatch.setattr(settings, "env", "test")
    monkeypatch.setattr(settings, "allow_plaintext_secrets", True)

    assert decrypt_secret_mapping("local-secret") == {"password": "local-secret"}


def test_insecure_plaintext_secret_config_fails_outside_dev() -> None:
    with pytest.raises(ValueError, match="ALLOW_PLAINTEXT_SECRETS"):
        Settings(
            _env_file=None,
            database_url="sqlite+pysqlite:///:memory:",
            env="prod",
            jwt_secret_key="prod-jwt-secret",
            datasource_secret_key="prod-datasource-secret",
            admin_password="strong-admin-password",
            viewer_password="strong-viewer-password",
            dq_scheduler_mode="worker",
            dq_profiling_scheduler_mode="worker",
            datasource_scan_scheduler_mode="worker",
            data_lake_scan_scheduler_mode="worker",
            platform_scheduler_mode="worker",
            allow_plaintext_secrets=True,
        )


def test_plaintext_secret_audit_detects_and_fixes_without_values() -> None:
    session = _build_session()
    session.execute(text("CREATE TABLE t2c_data.data_sources (id INTEGER PRIMARY KEY, password TEXT NOT NULL)"))
    session.execute(text("INSERT INTO t2c_data.data_sources (id, password) VALUES (1, 'super-secret')"))
    session.commit()

    detected = audit_plaintext_secrets(session, fix=False)
    assert sum(int(item["detected"]) for item in detected) == 1
    assert "super-secret" not in str(detected)

    fixed = audit_plaintext_secrets(session, fix=True)
    assert sum(int(item["fixed"]) for item in fixed) == 1
    stored = session.execute(text("SELECT password FROM t2c_data.data_sources WHERE id = 1")).scalar_one()
    assert str(stored).startswith("enc::")
    assert "super-secret" not in str(stored)
