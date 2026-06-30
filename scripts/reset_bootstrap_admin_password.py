#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from sqlalchemy.orm import Session

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from t2c_data.core.admin_recovery import reset_bootstrap_admin_password  # noqa: E402
from t2c_data.core.config import settings  # noqa: E402
from t2c_data.core.db import SessionLocal  # noqa: E402

CONFIRMATION_TOKEN = "RESET_BOOTSTRAP_ADMIN_PASSWORD"
SAFE_ENVIRONMENTS = {"dev", "development", "local", "test"}


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Reset the bootstrap admin password to the effective environment password."
    )
    parser.add_argument(
        "--confirm",
        required=True,
        help=f"Type exactly {CONFIRMATION_TOKEN} to authorize the reset.",
    )
    parser.add_argument(
        "--confirm-env",
        required=True,
        help="Type the current environment value to prove you reviewed the target before running.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Allow execution outside safe environments (dev/local/test).",
    )
    parser.add_argument(
        "--email",
        default=settings.bootstrap_admin_email,
        help="Bootstrap admin email to reset (default: settings.bootstrap_admin_email).",
    )
    parser.add_argument(
        "--password",
        default=None,
        help="Password to apply. Defaults to the effective bootstrap admin password from config.",
    )
    return parser


def _validate_execution(args: argparse.Namespace) -> None:
    env_value = (settings.env or "").lower()
    confirm_value = (args.confirm or "").strip()
    confirm_env_value = (args.confirm_env or "").strip().lower()
    if confirm_value != CONFIRMATION_TOKEN:
        raise SystemExit(f"Refusing reset: --confirm must be exactly {CONFIRMATION_TOKEN}.")
    if confirm_env_value != env_value:
        raise SystemExit(f"Refusing reset: --confirm-env must match the current ENV value ({env_value!r}).")
    if env_value not in SAFE_ENVIRONMENTS and not args.force:
        raise SystemExit(
            f"Refusing reset: ENV={env_value!r} is not a safe environment. Use --force only if you know what you are doing."
        )


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    _validate_execution(args)

    target_password = args.password or settings.bootstrap_admin_password

    with SessionLocal() as session:
        result = reset_bootstrap_admin_password(
            session,
            email=args.email,
            password=target_password,
            commit=True,
        )

    print("Bootstrap admin password reset concluido com sucesso.")
    print(f"ENV: {settings.env}")
    print(f"Admin: {result.email}")
    print(f"Criado: {'sim' if result.created else 'não'}")
    print(f"Reativado: {'sim' if result.reactivated else 'não'}")
    print(f"Hash scheme: {result.hash_scheme}")


if __name__ == "__main__":
    raise SystemExit(main())
