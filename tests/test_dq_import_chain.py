from __future__ import annotations

import os
import unittest


os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")


class DQImportChainTest(unittest.TestCase):
    def test_feature_package_imports_schema_orchestration(self) -> None:
        from t2c_data.features import data_quality

        self.assertTrue(callable(data_quality.execute_schema_profiling_orchestration))

    def test_service_imports_schema_scheduler(self) -> None:
        from t2c_data.services import dq_spark

        self.assertTrue(callable(dq_spark.enqueue_schema_profiling_run))


if __name__ == "__main__":
    unittest.main()
