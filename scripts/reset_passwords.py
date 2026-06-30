from __future__ import annotations

import os
import sys
from pathlib import Path

from sqlalchemy import select

# Ensure "app" package is importable when script is run directly.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from t2c_data.core.db import SessionLocal  # noqa: E402
from t2c_data.core.security import hash_password  # noqa: E402
from t2c_data.models.auth import Role, User  # noqa: E402


TARGET_LOCALPARTS = {"admin", "view", "viewer"}
TARGET_ROLES = {"admin", "view", "viewer"}


def should_reset(user: User) -> bool:
    local = (user.email or "").split("@", 1)[0].strip().lower()
    if local in TARGET_LOCALPARTS:
        return True

    role_names = {role.name.lower() for role in user.roles}
    return bool(role_names.intersection(TARGET_ROLES))


def main() -> int:
    new_password = os.getenv("RESET_PASSWORD")
    if not new_password:
        print("ERROR: RESET_PASSWORD env var is required", file=sys.stderr)
        return 1

    with SessionLocal() as session:
        users = session.scalars(select(User)).all()
        targets = [u for u in users if should_reset(u)]

        if not targets:
            print("No matching users found for password reset")
            return 0

        new_hash = hash_password(new_password)
        for user in targets:
            user.password_hash = new_hash

        session.commit()

        print(f"Password reset completed for {len(targets)} user(s)")
        for user in targets:
            roles = ",".join(sorted({r.name for r in user.roles})) or "(no-roles)"
            print(f"- {user.email} roles=[{roles}] hash_scheme={new_hash.split('$')[1]}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
