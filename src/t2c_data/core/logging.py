from __future__ import annotations

import logging

from t2c_data.core.redaction import redact_sensitive_string
from t2c_data.core.request_context import correlation_id_ctx, request_id_ctx, request_method_ctx, request_path_ctx


DEFAULT_LOG_FORMAT = (
    "%(asctime)s %(levelname)s [%(name)s] "
    "[request_id=%(request_id)s correlation_id=%(correlation_id)s method=%(request_method)s path=%(request_path)s] %(message)s"
)

class RedactSecretsFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.msg, str):
            if record.args:
                try:
                    rendered = record.msg % record.args
                    record.msg = redact_sensitive_string(rendered)
                    record.args = ()
                except Exception:  # noqa: BLE001
                    record.msg = redact_sensitive_string(record.msg)
                    if isinstance(record.args, dict):
                        record.args = {k: redact_sensitive_string(str(v)) for k, v in record.args.items()}
                    elif isinstance(record.args, tuple):
                        record.args = tuple(redact_sensitive_string(str(v)) for v in record.args)
                    else:
                        record.args = redact_sensitive_string(str(record.args))
            else:
                record.msg = redact_sensitive_string(record.msg)

        if record.exc_info:
            exc_type, exc_val, exc_tb = record.exc_info
            if exc_val is not None:
                safe_exc = Exception(redact_sensitive_string(str(exc_val)))
                record.exc_info = (exc_type, safe_exc, exc_tb)
        return True


class RequestContextFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = getattr(record, "request_id", request_id_ctx.get())
        record.correlation_id = getattr(record, "correlation_id", correlation_id_ctx.get())
        record.request_path = getattr(record, "request_path", request_path_ctx.get())
        record.request_method = getattr(record, "request_method", request_method_ctx.get())
        return True


def setup_logging() -> None:
    redact_filter = RedactSecretsFilter()
    request_context_filter = RequestContextFilter()
    root_logger = logging.getLogger()
    root_logger.addFilter(redact_filter)
    root_logger.addFilter(request_context_filter)

    for handler in root_logger.handlers:
        handler.addFilter(redact_filter)
        handler.addFilter(request_context_filter)
        if handler.formatter is None:
            handler.setFormatter(logging.Formatter(DEFAULT_LOG_FORMAT))

    for logger_name in ("uvicorn", "uvicorn.error", "uvicorn.access", "sqlalchemy.engine"):
        logger = logging.getLogger(logger_name)
        logger.addFilter(redact_filter)
        logger.addFilter(request_context_filter)
        logger.setLevel(logging.WARNING)
        for handler in logger.handlers:
            handler.addFilter(redact_filter)
            handler.addFilter(request_context_filter)

    _maybe_enable_json_logging(redact_filter, request_context_filter)


def _maybe_enable_json_logging(redact_filter: logging.Filter, request_context_filter: logging.Filter) -> None:
    """Logs estruturados JSON em stdout (padrão Turn2C de observabilidade).

    Opt-in via env ``LOG_JSON=true`` (recomendado no cluster). Mantém os filtros de redação e
    de contexto de request. Sem a lib instalada, mantém o formato de log atual (não quebra).
    """
    import os
    import sys

    if os.getenv("LOG_JSON", "false").strip().lower() not in {"1", "true", "yes", "on"}:
        return
    try:
        from pythonjsonlogger import jsonlogger
    except Exception:  # noqa: BLE001
        logging.getLogger(__name__).warning("python-json-logger indisponível; mantendo formato de log padrão")
        return

    json_handler = logging.StreamHandler(sys.stdout)
    json_handler.setFormatter(
        jsonlogger.JsonFormatter(
            "%(asctime)s %(levelname)s %(name)s %(message)s "
            "%(request_id)s %(correlation_id)s %(request_method)s %(request_path)s"
        )
    )
    json_handler.addFilter(redact_filter)
    json_handler.addFilter(request_context_filter)
    root_logger = logging.getLogger()
    root_logger.handlers = [json_handler]
