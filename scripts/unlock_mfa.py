#!/usr/bin/env python3
"""Break-glass: unlock a user locked for not enrolling MFA, and reset their grace window.

Use only for operational recovery (e.g. the sole admin got locked). Normal unlocks
go through the admin UI (POST /admin/users/{id}/mfa/unlock).

    python scripts/unlock_mfa.py user@example.com
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import func, select  # noqa: E402

from t2c_data.core.db import SessionLocal  # noqa: E402
from t2c_data.models.auth import User  # noqa: E402


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: python scripts/unlock_mfa.py <email>")
    email = sys.argv[1].strip().lower()
    session = SessionLocal()
    try:
        user = session.scalar(select(User).where(func.lower(User.email) == email))
        if user is None:
            raise SystemExit(f"user not found: {email}")
        user.mfa_locked = False
        user.mfa_locked_at = None
        user.mfa_grace_logins_used = 0
        session.add(user)
        session.commit()
        print(f"unlocked {user.email}: mfa_locked=False, grace reset to 0")
    finally:
        session.close()


if __name__ == "__main__":
    main()
