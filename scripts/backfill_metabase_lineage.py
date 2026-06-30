#!/usr/bin/env python3
"""Backfill consumption lineage from already-synced Metabase artifacts.

Iterates every Metabase instance and builds table -> dashboard/question
consumption edges via the lineage bridge. Idempotent.

Run from backend/:  python scripts/backfill_metabase_lineage.py
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import select  # noqa: E402

from t2c_data.core.db import SessionLocal  # noqa: E402
from t2c_data.features.lineage.metabase_bridge import sync_metabase_lineage  # noqa: E402
from t2c_data.models.metabase import MetabaseInstance  # noqa: E402


def main() -> None:
    session = SessionLocal()
    try:
        instances = session.scalars(select(MetabaseInstance)).all()
        if not instances:
            print("No Metabase instances found.")
            return
        for instance in instances:
            summary = sync_metabase_lineage(session, instance=instance, commit=True)
            print(
                f"instance={instance.name!r} artifacts={summary['artifacts']} "
                f"edges_total={summary['edges_total']} edges_created={summary['edges_created']}"
            )
    finally:
        session.close()


if __name__ == "__main__":
    main()
