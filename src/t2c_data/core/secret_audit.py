from __future__ import annotations

import json
from dataclasses import dataclass

from sqlalchemy import inspect, text
from sqlalchemy.orm import Session

from t2c_data.core.config import settings
from t2c_data.core.secret_store import encrypt_secret_mapping, is_encrypted_secret_payload


@dataclass(frozen=True)
class SecretField:
    table: str
    column: str
    default_key: str
    parse_json: bool = False


SECRET_FIELDS = (
    SecretField("data_sources", "password", "password"),
    SecretField("metabase_instances", "auth_secret", "auth_secret"),
    SecretField("lineage_source_configs", "auth_secret", "auth_secret"),
    SecretField("data_lake_connections", "credentials_payload", "aws_secret_access_key", parse_json=True),
)


def _mapping_for_legacy_value(field: SecretField, raw: str) -> dict[str, str]:
    if field.parse_json:
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            payload = None
        if isinstance(payload, dict):
            return {
                str(key): str(value)
                for key, value in payload.items()
                if value is not None and str(value).strip()
            }
    return {field.default_key: raw}


def audit_plaintext_secrets(session: Session, *, fix: bool = False) -> list[dict[str, object]]:
    inspector = inspect(session.get_bind())
    schema = settings.db_schema
    results: list[dict[str, object]] = []

    for field in SECRET_FIELDS:
        if not inspector.has_table(field.table, schema=schema):
            results.append(
                {
                    "table": field.table,
                    "field": field.column,
                    "status": "missing_table",
                    "detected": 0,
                    "fixed": 0,
                }
            )
            continue

        rows = session.execute(
            text(
                f"""
                SELECT id, {field.column} AS secret_payload
                FROM {schema}.{field.table}
                WHERE {field.column} IS NOT NULL
                  AND TRIM({field.column}) <> ''
                """
            )
        ).mappings()
        detected: list[tuple[int, str]] = []
        for row in rows:
            raw = str(row["secret_payload"] or "")
            if raw and not is_encrypted_secret_payload(raw):
                detected.append((int(row["id"]), raw))

        fixed = 0
        if fix and detected:
            for row_id, raw in detected:
                encrypted = encrypt_secret_mapping(_mapping_for_legacy_value(field, raw))
                session.execute(
                    text(
                        f"""
                        UPDATE {schema}.{field.table}
                        SET {field.column} = :encrypted
                        WHERE id = :row_id
                        """
                    ),
                    {"encrypted": encrypted, "row_id": row_id},
                )
                fixed += 1
            session.commit()

        results.append(
            {
                "table": field.table,
                "field": field.column,
                "status": "ok",
                "detected": len(detected),
                "fixed": fixed,
            }
        )

    return results
