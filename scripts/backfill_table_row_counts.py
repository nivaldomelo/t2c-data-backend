#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from t2c_data.features.catalog.table_volume import measure_all_active_tables_volume


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backfill table row-count snapshots for catalog tables.")
    parser.add_argument("--datasource-id", type=int, default=None, help="Optional datasource ID filter.")
    parser.add_argument("--schema-id", type=int, default=None, help="Optional schema ID filter.")
    parser.add_argument("--limit", type=int, default=None, help="Optional max number of tables to process.")
    parser.add_argument(
        "--database-url",
        default=os.getenv("DATABASE_URL"),
        help="Target catalog database URL (defaults to DATABASE_URL).",
    )
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    if not args.database_url:
        raise SystemExit("DATABASE_URL is required.")

    engine = create_engine(args.database_url, future=True, pool_pre_ping=True)
    SessionLocal = sessionmaker(bind=engine, future=True)
    try:
        with SessionLocal() as session:
            result = measure_all_active_tables_volume(
                db=session,
                datasource_id=args.datasource_id,
                schema_id=args.schema_id,
                limit=args.limit,
            )
            print(json.dumps(result.model_dump(), default=str, ensure_ascii=False, indent=2))
            return 0
    finally:
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
