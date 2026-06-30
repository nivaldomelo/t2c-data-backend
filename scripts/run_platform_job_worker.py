from __future__ import annotations

import argparse

from t2c_data.features.platform.job_worker import process_next_integration_job, run_integration_worker_forever


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the dedicated platform integration job worker.")
    parser.add_argument("--once", action="store_true", help="Process a single queued job and exit.")
    parser.add_argument("--source", default=None, help="Optional job source filter, for example datasource.")
    parser.add_argument("--job-type", dest="job_type", default=None, help="Optional job type filter, for example scan.")
    parser.add_argument("--poll-interval", dest="poll_interval", type=float, default=2.0, help="Polling interval in seconds for long-running mode.")
    args = parser.parse_args()

    if args.once:
        process_next_integration_job(source=args.source, job_type=args.job_type)
        return

    run_integration_worker_forever(source=args.source, job_type=args.job_type, poll_interval_seconds=args.poll_interval)


if __name__ == "__main__":
    main()
