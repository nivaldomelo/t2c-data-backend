from __future__ import annotations

from types import SimpleNamespace

from sqlalchemy.engine import URL

from t2c_data.features.ingestion.service import _column_map_cache_key
from t2c_data.features.shared_cache import (
    safe_connection_fingerprint,
    safe_connection_label,
    session_cache_key,
)


def _session_for_url(url_obj) -> SimpleNamespace:
    engine = SimpleNamespace(url=url_obj)
    bind = SimpleNamespace(engine=engine)
    return SimpleNamespace(get_bind=lambda: bind)


def test_safe_connection_fingerprint_ignores_password() -> None:
    url_a = URL.create(
        "postgresql+psycopg",
        username="reader",
        password="secret-a",
        host="db.local",
        port=5432,
        database="catalog",
    )
    url_b = URL.create(
        "postgresql+psycopg",
        username="reader",
        password="secret-b",
        host="db.local",
        port=5432,
        database="catalog",
    )

    assert safe_connection_fingerprint(url_a) == safe_connection_fingerprint(url_b)


def test_safe_connection_fingerprint_changes_with_host_or_database() -> None:
    base = URL.create(
        "postgresql+psycopg",
        username="reader",
        password="secret-a",
        host="db.local",
        port=5432,
        database="catalog",
    )
    other_host = URL.create(
        "postgresql+psycopg",
        username="reader",
        password="secret-a",
        host="db-2.local",
        port=5432,
        database="catalog",
    )
    other_db = URL.create(
        "postgresql+psycopg",
        username="reader",
        password="secret-a",
        host="db.local",
        port=5432,
        database="analytics",
    )

    assert safe_connection_fingerprint(base) != safe_connection_fingerprint(other_host)
    assert safe_connection_fingerprint(base) != safe_connection_fingerprint(other_db)


def test_session_cache_key_does_not_contain_password() -> None:
    session = _session_for_url(
        URL.create(
            "postgresql+psycopg",
            username="reader",
            password="super-secret",
            host="db.local",
            port=5432,
            database="catalog",
        )
    )

    key = session_cache_key(session)

    assert "super-secret" not in key
    assert "connfp:" in key


def test_column_map_cache_key_does_not_contain_password() -> None:
    session = _session_for_url(
        URL.create(
            "postgresql+psycopg",
            username="reader",
            password="jdbc-password",
            host="db.local",
            port=5432,
            database="catalog",
        )
    )

    key = _column_map_cache_key(session)

    assert "jdbc-password" not in key
    assert "connfp:" in key


def test_safe_connection_label_redacts_password() -> None:
    label = safe_connection_label(
        URL.create(
            "postgresql+psycopg",
            username="reader",
            password="top-secret",
            host="db.local",
            port=5432,
            database="catalog",
        )
    )

    assert "top-secret" not in label
    assert "***" in label or "********" in label
