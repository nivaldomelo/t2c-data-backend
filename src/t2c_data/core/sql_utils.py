from __future__ import annotations

import re


_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def safe_identifier(value: str, *, label: str = "identifier") -> str:
    normalized = (value or "").strip()
    if not normalized or not _IDENTIFIER_RE.match(normalized):
        raise ValueError(f"Invalid {label}: {value!r}")
    return normalized


def safe_relation(schema: str, relation: str, *, label: str = "relation") -> str:
    safe_schema = safe_identifier(schema, label="schema")
    safe_relation_name = safe_identifier(relation, label=label)
    return f"{safe_schema}.{safe_relation_name}"


__all__ = ["safe_identifier", "safe_relation"]
