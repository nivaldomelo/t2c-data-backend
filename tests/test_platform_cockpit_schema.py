from __future__ import annotations

import unittest

from t2c_data.schemas.platform import PlatformCockpitOut


class PlatformCockpitSchemaTest(unittest.TestCase):
    def test_cockpit_schema_does_not_expose_read_models_or_scheduler(self) -> None:
        self.assertNotIn("read_models", PlatformCockpitOut.model_fields)
        self.assertNotIn("scheduler", PlatformCockpitOut.model_fields)


if __name__ == "__main__":
    unittest.main()
