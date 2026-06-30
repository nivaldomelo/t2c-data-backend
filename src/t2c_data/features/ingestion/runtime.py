from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import logging
from threading import Lock
from typing import Iterator

from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine import URL, make_url
from sqlalchemy.orm import Session, sessionmaker

from t2c_data.core.config import settings
from t2c_data.features.datasource.api_support import resolved_connection
from t2c_data.features.operations.failures import classify_operational_error
from t2c_data.features.ingestion.service import IngestionIntegrationUnavailable
from t2c_data.models.catalog import DataSource

logger = logging.getLogger(__name__)
_OPERATIONAL_SOURCE_CACHE_LOCK = Lock()
_OPERATIONAL_SOURCE_CACHE_TTL_SECONDS = 300


@dataclass
class OperationalSourceCacheEntry:
    checked_at: datetime
    url: URL | None
    unavailable_message: str | None = None
    unavailable_log_emitted: bool = False


_OPERATIONAL_SOURCE_CACHE: OperationalSourceCacheEntry | None = None


def _build_operational_url() -> URL | None:
    url_value = (settings.operational_ingestion_database_url or "").strip()
    if url_value:
        return make_url(url_value)
    host = (settings.ingestion_operational_host or "").strip()
    database = (settings.ingestion_operational_database or "").strip()
    username = (settings.ingestion_operational_username or "").strip()
    password = settings.ingestion_operational_password or ""
    port = int(settings.ingestion_operational_port or 5432)
    if not host or not database or not username:
        return None
    return URL.create(
        "postgresql+psycopg",
        username=username,
        password=password,
        host=host,
        port=port,
        database=database,
    )


def _operational_connect_timeout() -> int:
    return max(int(settings.operational_ingestion_connect_timeout_seconds or 3), 1)


def _probe_operational_url(url: URL) -> None:
    engine = create_engine(
        url,
        future=True,
        pool_pre_ping=True,
        connect_args={"connect_timeout": _operational_connect_timeout()},
    )
    try:
        with engine.connect() as connection:
            connection.exec_driver_sql("select 1")
    finally:
        engine.dispose()


def _resolve_operational_url() -> URL:
    global _OPERATIONAL_SOURCE_CACHE

    now = datetime.now(timezone.utc)
    with _OPERATIONAL_SOURCE_CACHE_LOCK:
        cache = _OPERATIONAL_SOURCE_CACHE
        if cache is not None and (now - cache.checked_at).total_seconds() < _OPERATIONAL_SOURCE_CACHE_TTL_SECONDS:
            if cache.url is not None:
                return cache.url
            raise IngestionIntegrationUnavailable(
                cache.unavailable_message or "A fonte operacional externa de ingestão não está configurada."
            )

    previous_cache = _OPERATIONAL_SOURCE_CACHE
    url = _build_operational_url()
    if url is None:
        message = "A fonte operacional externa de ingestão não está configurada."
        should_log = (
            previous_cache is None
            or previous_cache.url is not None
            or previous_cache.unavailable_message != message
            or not previous_cache.unavailable_log_emitted
        )
        if should_log:
            logger.warning("operational ingestion source unavailable: %s", message)
        with _OPERATIONAL_SOURCE_CACHE_LOCK:
            _OPERATIONAL_SOURCE_CACHE = OperationalSourceCacheEntry(
                checked_at=now,
                url=None,
                unavailable_message=message,
                unavailable_log_emitted=should_log,
            )
        raise IngestionIntegrationUnavailable(message)

    try:
        _probe_operational_url(url)
    except SQLAlchemyError as exc:
        category, _severity, _retryable = classify_operational_error(exc, source="ingestion.operational_source")
        message = (
            "A fonte operacional externa de ingestão está indisponível."
            if category == "CONNECTIVITY_ERROR"
            else "A fonte operacional externa de ingestão está mal configurada."
        )
        should_log = (
            previous_cache is None
            or previous_cache.url is not None
            or previous_cache.unavailable_message != message
            or not previous_cache.unavailable_log_emitted
        )
        if should_log:
            logger.warning("operational ingestion source unavailable: %s", message)
        with _OPERATIONAL_SOURCE_CACHE_LOCK:
            _OPERATIONAL_SOURCE_CACHE = OperationalSourceCacheEntry(
                checked_at=now,
                url=None,
                unavailable_message=message,
                unavailable_log_emitted=should_log,
            )
        raise IngestionIntegrationUnavailable(message) from exc
    except Exception as exc:  # noqa: BLE001
        message = "A fonte operacional externa de ingestão está mal configurada."
        should_log = (
            previous_cache is None
            or previous_cache.url is not None
            or previous_cache.unavailable_message != message
            or not previous_cache.unavailable_log_emitted
        )
        if should_log:
            logger.warning("operational ingestion source unavailable: %s", message)
        with _OPERATIONAL_SOURCE_CACHE_LOCK:
            _OPERATIONAL_SOURCE_CACHE = OperationalSourceCacheEntry(
                checked_at=now,
                url=None,
                unavailable_message=message,
                unavailable_log_emitted=should_log,
            )
        raise IngestionIntegrationUnavailable(message) from exc

    if previous_cache is not None and previous_cache.url is None:
        logger.info("operational ingestion source available again")
    with _OPERATIONAL_SOURCE_CACHE_LOCK:
        _OPERATIONAL_SOURCE_CACHE = OperationalSourceCacheEntry(
            checked_at=now,
            url=url,
            unavailable_message=None,
            unavailable_log_emitted=False,
        )
    return url


def _datasource_url(datasource: DataSource) -> URL:
    if datasource.db_type != "postgres":
        raise IngestionIntegrationUnavailable("A integração operacional de ingestão suporta apenas datasources PostgreSQL no MVP.")

    connection = resolved_connection(datasource)
    host = str(connection.get("host") or datasource.host or "").strip()
    database = str(connection.get("database") or datasource.database or "").strip()
    username = str(connection.get("username") or datasource.username or "").strip()
    password = datasource.get_secret("password") or ""
    port = int(connection.get("port") or datasource.port or 5432)

    if not host or not database or not username:
        raise IngestionIntegrationUnavailable("O datasource vinculado não possui credenciais suficientes para consultar a camada operacional.")

    return URL.create(
        "postgresql+psycopg",
        username=username,
        password=password,
        host=host,
        port=port,
        database=database,
    )


@contextmanager
def operational_session_for_datasource(datasource: DataSource) -> Iterator[Session]:
    engine = create_engine(_datasource_url(datasource), future=True, pool_pre_ping=True)
    session_factory = sessionmaker(bind=engine, autoflush=False, autocommit=False, class_=Session)
    session = session_factory()
    try:
        yield session
    finally:
        session.close()
        engine.dispose()


@contextmanager
def operational_session(bind_session: Session | None = None) -> Iterator[Session]:
    try:
        operational_url = _resolve_operational_url()
        engine = create_engine(operational_url, future=True, pool_pre_ping=True)
    except IngestionIntegrationUnavailable:
        raise
    except Exception as exc:  # noqa: BLE001
        message = "A fonte operacional externa de ingestão está mal configurada."
        logger.warning("operational ingestion source unavailable: %s", message)
        raise IngestionIntegrationUnavailable(message) from exc

    session_factory = sessionmaker(bind=engine, autoflush=False, autocommit=False, class_=Session)
    session = session_factory()
    try:
        yield session
    finally:
        session.close()
        engine.dispose()


def operational_source_diagnostics() -> dict[str, object]:
    url_value = (settings.operational_ingestion_database_url or "").strip()
    config = settings.operational_ingestion_config
    source_kind = "database_url" if url_value else ("host_parts" if config.as_url() else "unavailable")
    return {
        "configured": bool(settings.operational_ingestion_configured),
        "schema": settings.operational_ingestion_config.schema_name or settings.operational_db_schema or "controle",
        "source_kind": source_kind,
        "has_url": bool(url_value),
        "has_host_parts": bool(
            (settings.ingestion_operational_host or "").strip()
            and (settings.ingestion_operational_database or "").strip()
            and (settings.ingestion_operational_username or "").strip()
        ),
        "host": (settings.ingestion_operational_host or "").strip() or None,
        "port": int(settings.ingestion_operational_port or 5432) if (settings.ingestion_operational_port or 0) else 5432,
        "database": (settings.ingestion_operational_database or "").strip() or None,
        "user": (settings.ingestion_operational_username or "").strip() or None,
        "connect_timeout_seconds": _operational_connect_timeout(),
    }
