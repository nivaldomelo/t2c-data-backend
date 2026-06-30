from __future__ import annotations

from sqlalchemy import text

from t2c_data.core.db import SessionLocal


def main() -> None:
    with SessionLocal() as session:
        cols = session.execute(
            text(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 't2c_ops'
                  AND table_name = 'incidents'
                  AND column_name IN ('source_type', 'source_ref_id', 'evidence_json', 'occurrences', 'last_seen_at')
                ORDER BY column_name
                """
            )
        ).scalars().all()
        print("incident_columns:", cols)

        counts = session.execute(
            text(
                """
                SELECT status, COUNT(*)::int
                FROM t2c_ops.incidents
                GROUP BY status
                ORDER BY status
                """
            )
        ).all()
        print("status_counts:", [(row[0], row[1]) for row in counts])

        sample = session.execute(
            text(
                """
                SELECT id, title, status, source_type, source_ref_id, occurrences
                FROM t2c_ops.incidents
                ORDER BY detected_at DESC
                LIMIT 5
                """
            )
        ).all()
        print("sample_rows:", [tuple(row) for row in sample])


if __name__ == "__main__":
    main()

