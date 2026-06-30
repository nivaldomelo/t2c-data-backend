from __future__ import annotations

import os

os.environ.setdefault("DATABASE_URL", "postgresql+psycopg://u:p@localhost:5432/db")
os.environ.setdefault("ENV", "test")

import logging

import pytest
from fastapi import HTTPException
from sqlalchemy.engine import make_url

from t2c_data.core import db as db_module
from t2c_data.core.config import settings
from t2c_data.features.integrations import data_lake
from t2c_data.models.platform import DataLakeConnection


# ----------------------- db.py transport hardening -----------------------

def test_sslmode_parsing():
    assert db_module._sslmode(make_url("postgresql+psycopg://u:p@h:5432/d?sslmode=require")) == "require"
    assert db_module._sslmode(make_url("postgresql+psycopg://u:p@h:5432/d")) == ""


def test_local_host_without_ssl_does_not_warn(caplog):
    with caplog.at_level(logging.WARNING, logger="t2c_data.core.db"):
        db_module._warn_if_insecure_transport(make_url("postgresql+psycopg://u:p@localhost:5432/d"))
    assert "UNENCRYPTED" not in caplog.text


def test_remote_host_without_ssl_warns(caplog):
    with caplog.at_level(logging.WARNING, logger="t2c_data.core.db"):
        db_module._warn_if_insecure_transport(make_url("postgresql+psycopg://u:p@db.example.com:5432/d"))
    assert "UNENCRYPTED" in caplog.text


def test_remote_host_with_ssl_does_not_warn(caplog):
    with caplog.at_level(logging.WARNING, logger="t2c_data.core.db"):
        db_module._warn_if_insecure_transport(
            make_url("postgresql+psycopg://u:p@db.example.com:5432/d?sslmode=require")
        )
    assert "UNENCRYPTED" not in caplog.text


# ------------------ data lake: default-env credentials gate ------------------

def test_env_credential_blocked_when_flag_off(monkeypatch):
    monkeypatch.setattr(settings, "datalake_allow_default_env_credentials", False)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIAEXAMPLE")
    assert data_lake._env_credential("AWS_ACCESS_KEY_ID") == ""


def test_env_credential_allowed_when_flag_on(monkeypatch):
    monkeypatch.setattr(settings, "datalake_allow_default_env_credentials", True)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIAEXAMPLE")
    assert data_lake._env_credential("AWS_ACCESS_KEY_ID") == "AKIAEXAMPLE"


def test_default_environment_disabled_by_default(monkeypatch):
    monkeypatch.setattr(settings, "datalake_allow_default_env_credentials", False)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIAEXAMPLE")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret")
    conn = DataLakeConnection(
        name="c", bucket="b", region="us-east-1",
        auth_type="default_environment",
        access_key_id=None, role_arn=None,
    )
    with pytest.raises(HTTPException) as exc:
        data_lake._aws_credentials_for_connection(conn)
    assert exc.value.status_code == 422
    assert "default_environment" in str(exc.value.detail)


def test_default_environment_works_when_enabled(monkeypatch):
    monkeypatch.setattr(settings, "datalake_allow_default_env_credentials", True)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIAEXAMPLE")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret")
    monkeypatch.delenv("AWS_SESSION_TOKEN", raising=False)
    conn = DataLakeConnection(
        name="c", bucket="b", region="us-east-1",
        auth_type="default_environment",
        access_key_id=None, role_arn=None,
    )
    creds, mode = data_lake._aws_credentials_for_connection(conn)
    assert mode == "default_environment"
    assert creds["aws_access_key_id"] == "AKIAEXAMPLE"
