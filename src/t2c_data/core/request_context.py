from __future__ import annotations

from contextvars import ContextVar
from typing import Any, Callable

request_id_ctx: ContextVar[str] = ContextVar("request_id", default="-")
correlation_id_ctx: ContextVar[str] = ContextVar("correlation_id", default="-")
request_path_ctx: ContextVar[str] = ContextVar("request_path", default="-")
request_method_ctx: ContextVar[str] = ContextVar("request_method", default="-")


def set_request_context(*, request_id: str, correlation_id: str | None = None, path: str, method: str) -> tuple[object, object, object, object]:
    token_request_id = request_id_ctx.set(request_id)
    token_correlation_id = correlation_id_ctx.set(correlation_id or request_id)
    token_path = request_path_ctx.set(path)
    token_method = request_method_ctx.set(method)
    return token_request_id, token_correlation_id, token_path, token_method


def clear_request_context(tokens: tuple[object, object, object, object]) -> None:
    token_request_id, token_correlation_id, token_path, token_method = tokens
    request_id_ctx.reset(token_request_id)
    correlation_id_ctx.reset(token_correlation_id)
    request_path_ctx.reset(token_path)
    request_method_ctx.reset(token_method)


def capture_request_context() -> dict[str, str]:
    return {
        "request_id": request_id_ctx.get(),
        "correlation_id": correlation_id_ctx.get(),
        "path": request_path_ctx.get(),
        "method": request_method_ctx.get(),
    }


def run_with_request_context(
    context: dict[str, str],
    fn: Callable[..., Any],
    /,
    *args: Any,
    **kwargs: Any,
) -> Any:
    tokens = set_request_context(
        request_id=context.get("request_id", "-"),
        correlation_id=context.get("correlation_id", context.get("request_id", "-")),
        path=context.get("path", "-"),
        method=context.get("method", "-"),
    )
    try:
        return fn(*args, **kwargs)
    finally:
        clear_request_context(tokens)
