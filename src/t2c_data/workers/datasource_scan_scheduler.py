from __future__ import annotations

import asyncio

from t2c_data.features.datasource.scheduler import run_datasource_scan_scheduler_forever


def main() -> None:
    asyncio.run(run_datasource_scan_scheduler_forever())


if __name__ == "__main__":
    main()
