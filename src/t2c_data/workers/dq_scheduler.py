from __future__ import annotations

import asyncio

from t2c_data.features.data_quality.scheduler import run_dq_scheduler_forever


def main() -> None:
    asyncio.run(run_dq_scheduler_forever())


if __name__ == "__main__":
    main()
