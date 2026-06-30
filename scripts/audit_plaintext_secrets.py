"""Audit and optionally encrypt legacy plaintext secrets.

The script reports only table/field counts. It never prints secret values.

Example:
    python backend/scripts/audit_plaintext_secrets.py
    python backend/scripts/audit_plaintext_secrets.py --fix
"""

from __future__ import annotations

import argparse
from t2c_data.core.secret_audit import audit_plaintext_secrets
from t2c_data.core.db import SessionLocal


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit legacy plaintext secrets without printing secret values.")
    parser.add_argument("--fix", action="store_true", help="Encrypt detected legacy plaintext values in place.")
    args = parser.parse_args()

    with SessionLocal() as session:
        results = audit_plaintext_secrets(session, fix=args.fix)

    total_detected = sum(int(item["detected"]) for item in results)
    total_fixed = sum(int(item["fixed"]) for item in results)
    print("Plaintext secret audit summary")
    for item in results:
        print(
            f"- {item['table']}.{item['field']}: "
            f"status={item['status']} detected={item['detected']} fixed={item['fixed']}"
        )
    print(f"Total detected={total_detected} fixed={total_fixed}")


if __name__ == "__main__":
    main()
