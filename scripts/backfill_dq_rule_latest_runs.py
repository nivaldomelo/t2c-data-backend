from __future__ import annotations

from t2c_data.core.db import SessionLocal
from t2c_data.features.data_quality.latest_runs import backfill_latest_rule_runs


def main() -> None:
    with SessionLocal() as session:
        summary = backfill_latest_rule_runs(session)
        session.commit()
    print(
        "dq_rule_latest_runs backfill completed: "
        f"rules_total={summary['rules_total']} "
        f"created={summary['created']} "
        f"updated={summary['updated']} "
        f"latest_rule_runs={summary['latest_rule_runs']} "
        f"latest_jobs={summary['latest_jobs']} "
        f"errors={summary['errors']}"
    )


if __name__ == "__main__":
    main()
