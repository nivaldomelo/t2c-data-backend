import logging

from sqlalchemy import create_engine
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session, sessionmaker

from t2c_data.core.config import is_dev_environment, settings

logger = logging.getLogger(__name__)

_LOCAL_HOSTS = {"", "localhost", "127.0.0.1", "::1", "host.docker.internal"}
_TLS_SSLMODES = {"require", "verify-ca", "verify-full"}


def _sslmode(url) -> str:
    value = url.query.get("sslmode")
    if isinstance(value, (tuple, list)):
        value = value[0] if value else None
    return (value or "").strip().lower()


def _warn_if_insecure_transport(url) -> None:
    """Defense-in-depth: a remote Postgres without sslmode=require/verify-* connects in
    cleartext. The transport mode is config-driven (set in DATABASE_URL by ops), so we only
    warn — loudly outside dev — instead of failing, to avoid breaking valid local setups."""
    host = (url.host or "").lower()
    if host in _LOCAL_HOSTS:
        return
    if _sslmode(url) in _TLS_SSLMODES:
        return
    message = (
        "DATABASE_URL points to a remote host (%s) without sslmode=require/verify-ca/verify-full; "
        "the connection may be UNENCRYPTED. Set sslmode (e.g. ?sslmode=require) in production."
    )
    if is_dev_environment(settings.env):
        logger.warning(message, host)
    else:
        # Fail-secure outside dev: refuse to connect in cleartext to a remote DB.
        logger.error(message, host)
        raise RuntimeError(message % host)


engine_kwargs = {
    "future": True,
    "pool_pre_ping": True,   # drop stale connections (managed DBs / RDS idle timeouts)
    "pool_recycle": 1800,    # recycle connections every 30 min
    "echo": False,
}
try:
    parsed_url = make_url(settings.database_url)
    if parsed_url.drivername.startswith("postgresql"):
        engine_kwargs["connect_args"] = {
            "options": f"-csearch_path={settings.db_schema},public",
            "connect_timeout": 10,
        }
        _warn_if_insecure_transport(parsed_url)
except Exception:  # noqa: BLE001 - never block startup on URL introspection
    pass

engine = create_engine(settings.database_url, **engine_kwargs)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, class_=Session)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
