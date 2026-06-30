from __future__ import annotations

import os
import unittest
from types import SimpleNamespace

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from t2c_data.features.lineage.source_configs import serialize_source_config


def _source() -> SimpleNamespace:
    return SimpleNamespace(
        id=1,
        name="OpenLineage",
        source_type="openlineage",
        base_url="http://internal-openlineage:5000",
        default_namespace="prod",
        auth_type="bearer",
        auth_username="svc-openlineage",
        auth_secret="super-secret",
        enabled=True,
        last_sync_at="2026-04-07T10:00:00Z",
        last_sync_status="success",
        last_sync_message="ok",
        created_at="2026-04-07T09:00:00Z",
        updated_at="2026-04-07T09:30:00Z",
    )


class LineageSourceRedactionTests(unittest.TestCase):
    def test_serialize_source_config_masks_sensitive_fields_for_non_admin(self) -> None:
        result = serialize_source_config(
            _source(),
            current_user=SimpleNamespace(roles=[SimpleNamespace(name="viewer")]),
        )

        self.assertEqual(result.name, "Linhagem automática interna")
        self.assertEqual(result.source_type, "internal_openlineage")
        self.assertEqual(result.base_url, "Oculto para seu perfil")
        self.assertEqual(result.default_namespace, "Oculto para seu perfil")
        self.assertEqual(result.auth_username, "Oculto para seu perfil")
        self.assertIsNone(result.auth_secret)
        self.assertTrue(result.configured_auth)

    def test_serialize_source_config_exposes_full_fields_for_admin(self) -> None:
        result = serialize_source_config(
            _source(),
            current_user=SimpleNamespace(roles=[SimpleNamespace(name="admin")]),
        )

        self.assertEqual(result.name, "Linhagem automática interna")
        self.assertEqual(result.source_type, "internal_openlineage")
        self.assertEqual(result.base_url, "http://internal-openlineage:5000")
        self.assertEqual(result.default_namespace, "prod")
        self.assertEqual(result.auth_username, "svc-openlineage")
        self.assertEqual(result.auth_secret, "super-secret")
        self.assertTrue(result.configured_auth)
