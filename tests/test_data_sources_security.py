from __future__ import annotations

from sqlalchemy.engine import URL

from t2c_data.features.data_quality.profiling import _connection_url
from t2c_data.features.shared_cache import safe_connection_fingerprint, safe_connection_label
from t2c_data.models.catalog import DataSource


def _build_datasource(*, password: str, host: str = "db.local", database: str = "catalog") -> DataSource:
    datasource = DataSource(
        name="warehouse",
        db_type="postgres",
        host=host,
        port=5432,
        database=database,
        username="reader",
    )
    datasource.password = password
    return datasource


def test_dq_profiling_connection_helper_returns_url_object() -> None:
    datasource = _build_datasource(password="super-secret")

    connection = _connection_url(datasource)

    assert isinstance(connection, URL)
    assert "super-secret" not in safe_connection_label(connection)


def test_datasource_fingerprint_is_password_independent() -> None:
    datasource_a = _build_datasource(password="secret-a")
    datasource_b = _build_datasource(password="secret-b")

    assert safe_connection_fingerprint(_connection_url(datasource_a)) == safe_connection_fingerprint(_connection_url(datasource_b))


def test_datasource_fingerprint_changes_when_target_changes() -> None:
    datasource_a = _build_datasource(password="secret-a", host="db.local", database="catalog")
    datasource_b = _build_datasource(password="secret-a", host="db-2.local", database="catalog")

    assert safe_connection_fingerprint(_connection_url(datasource_a)) != safe_connection_fingerprint(_connection_url(datasource_b))
