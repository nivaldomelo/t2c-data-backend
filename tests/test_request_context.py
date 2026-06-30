from __future__ import annotations

import os
import unittest

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from t2c_data.core.request_context import (
    capture_request_context,
    correlation_id_ctx,
    request_id_ctx,
    request_method_ctx,
    request_path_ctx,
    run_with_request_context,
    set_request_context,
    clear_request_context,
)


class RequestContextTests(unittest.TestCase):
    def test_capture_and_run_with_request_context(self) -> None:
        tokens = set_request_context(
            request_id="req-123",
            correlation_id="corr-456",
            path="/api/v1/dq/profile",
            method="POST",
        )
        try:
            context = capture_request_context()
        finally:
            clear_request_context(tokens)

        self.assertEqual(context["request_id"], "req-123")
        self.assertEqual(context["correlation_id"], "corr-456")
        self.assertEqual(context["path"], "/api/v1/dq/profile")
        self.assertEqual(context["method"], "POST")

        observed: dict[str, str] = {}

        def _target() -> None:
            observed["request_id"] = request_id_ctx.get()
            observed["correlation_id"] = correlation_id_ctx.get()
            observed["path"] = request_path_ctx.get()
            observed["method"] = request_method_ctx.get()

        run_with_request_context(context, _target)

        self.assertEqual(
            observed,
            {
                "request_id": "req-123",
                "correlation_id": "corr-456",
                "path": "/api/v1/dq/profile",
                "method": "POST",
            },
        )
        self.assertEqual(request_id_ctx.get(), "-")
        self.assertEqual(correlation_id_ctx.get(), "-")
        self.assertEqual(request_path_ctx.get(), "-")
        self.assertEqual(request_method_ctx.get(), "-")


if __name__ == "__main__":
    unittest.main()
