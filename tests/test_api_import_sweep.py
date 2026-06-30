from __future__ import annotations

import importlib
import os
from pathlib import Path

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")


def _iter_api_modules() -> list[str]:
    modules: list[str] = []
    api_root = Path(__file__).resolve().parents[1] / "app" / "api"
    for path in sorted(api_root.rglob("*.py")):
        if path.name == "__init__.py":
            continue
        module = ".".join(path.relative_to(api_root.parent.parent).with_suffix("").parts)
        modules.append(module)
    return modules


def test_all_api_modules_import_cleanly() -> None:
    failures: list[tuple[str, str]] = []

    for module_name in _iter_api_modules():
        try:
            importlib.import_module(module_name)
        except Exception as exc:  # noqa: BLE001
            failures.append((module_name, repr(exc)))

    assert failures == [], f"API import sweep failed: {failures}"


def test_fastapi_app_imports_cleanly() -> None:
    app_main = importlib.import_module("t2c_data.main")
    assert hasattr(app_main, "app")

