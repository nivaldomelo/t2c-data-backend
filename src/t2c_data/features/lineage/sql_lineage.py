from __future__ import annotations

import re
from typing import Any

_TABLE_KEYWORD_PATTERN = re.compile(r"\b(?:from|join)\b", re.IGNORECASE)


def _normalize_identifier(value: str) -> str:
    raw = value.strip()
    parts = [part.strip().strip("`").strip('"').strip("'") for part in raw.split(".")]
    return ".".join(part for part in parts if part)


def _extract_sql_identifier(raw: str, start_index: int) -> str | None:
    parts: list[str] = []
    current: list[str] = []
    quote_char: str | None = None
    in_identifier = False
    i = start_index

    def flush_current() -> None:
        nonlocal current, in_identifier
        if current:
            part = "".join(current).strip()
            if part:
                parts.append(part)
            current = []
        in_identifier = False

    while i < len(raw):
        char = raw[i]
        if quote_char is not None:
            if char == quote_char:
                quote_char = None
            else:
                current.append(char)
                in_identifier = True
            i += 1
            continue

        if char in {'"', "'", "`"}:
            quote_char = char
            in_identifier = True
            i += 1
            continue

        if char == ".":
            flush_current()
            i += 1
            continue

        if char.isspace():
            if in_identifier:
                break
            i += 1
            continue

        if char in {",", ";"}:
            break

        if char == "(":
            if not parts and not current:
                return None
            break

        current.append(char)
        in_identifier = True
        i += 1

    flush_current()
    if not parts:
        return None
    return _normalize_identifier(".".join(parts))


def _extract_sql_table_candidates(sql: str) -> list[str]:
    raw = sql or ""
    candidates: list[str] = []
    for match in _TABLE_KEYWORD_PATTERN.finditer(raw):
        candidate = _extract_sql_identifier(raw, match.end())
        if candidate:
            candidates.append(candidate)
    return candidates


def extract_sql_table_lineage(sql: str) -> list[str]:
    raw = (sql or "").strip()
    if not raw:
        return []
    try:  # pragma: no cover - optional dependency path
        import sqlglot

        expression = sqlglot.parse_one(raw)
        tables = []
        for table in expression.find_all(sqlglot.expressions.Table):
            normalized = table.sql(dialect="ansi")
            if normalized:
                tables.append(normalized)
        if tables:
            return list(dict.fromkeys(_normalize_identifier(item) for item in tables if item))
    except Exception:
        pass
    return list(dict.fromkeys(_extract_sql_table_candidates(raw)))


def extract_sql_column_lineage(sql: str, schema: dict[str, Any] | None = None) -> list[dict[str, str]]:
    raw = (sql or "").strip()
    if not raw:
        return []
    try:  # pragma: no cover - optional dependency path
        import sqlglot
        from sqlglot import lineage as sqlglot_lineage

        expression = sqlglot.parse_one(raw)
        columns: list[dict[str, str]] = []
        for select in expression.find_all(sqlglot.expressions.Select):
            for projection in select.expressions:
                alias = projection.alias_or_name
                if not alias:
                    continue
                # sqlglot lineage yields a graph of dependencies for a projected expression.
                try:
                    lineage_graph = sqlglot_lineage.lineage(alias, expression, schema=schema or {})
                except Exception:
                    continue
                for ancestor in getattr(lineage_graph, "walk", lambda: [])():
                    name = getattr(ancestor, "name", None)
                    if name:
                        columns.append({"source_column": str(name), "target_column": str(alias)})
        if columns:
            return columns
    except Exception:
        pass
    return []


__all__ = ["extract_sql_column_lineage", "extract_sql_table_lineage"]
