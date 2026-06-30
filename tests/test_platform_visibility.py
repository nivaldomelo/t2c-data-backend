from __future__ import annotations

import unittest

from t2c_data.features.platform.visibility import (
    mask_audit_event_payload,
    mask_certification_summary_payload,
    mask_incident_asset_context_payload,
    mask_privacy_summary_payload,
    mask_table_payload,
)
from t2c_data.features.platform.sensitive_data import mask_payload_by_policy


class PlatformVisibilityMaskingTests(unittest.TestCase):
    def test_mask_table_payload_clears_sensitive_catalog_fields(self) -> None:
        payload = {
            "sensitivity_level": "confidential",
            "has_personal_data": True,
            "has_sensitive_personal_data": True,
            "legal_basis": "consent",
            "retention_policy": "5 years",
            "access_scope": "restricted",
            "access_roles": ["admin"],
            "privacy_notes": "internal note",
            "is_masked": False,
        }

        masked = mask_table_payload(payload)

        self.assertIsNone(masked["sensitivity_level"])
        self.assertFalse(masked["has_personal_data"])
        self.assertFalse(masked["has_sensitive_personal_data"])
        self.assertIsNone(masked["legal_basis"])
        self.assertEqual(masked["access_roles"], [])
        self.assertTrue(masked["is_masked"])

    def test_mask_privacy_summary_payload_replaces_context_consistently(self) -> None:
        payload = {
            "sensitivity_level": "restricted",
            "sensitivity_label": "Restrito",
            "legal_basis": "contract",
            "legal_basis_label": "Contrato",
            "retention_policy": "12 months",
            "is_masked": False,
            "access_scope": "internal",
            "access_scope_label": "Interno",
            "access_roles": ["editor"],
            "access_role_labels": ["Editor"],
            "privacy_notes": "sensitive note",
        }

        masked = mask_privacy_summary_payload(payload)

        self.assertIsNone(masked["sensitivity_level"])
        self.assertEqual(masked["sensitivity_label"], "Mascarado para o seu perfil")
        self.assertIsNone(masked["legal_basis"])
        self.assertEqual(masked["access_roles"], [])
        self.assertEqual(masked["access_scope_label"], "Mascarado")

    def test_mask_certification_summary_payload_hides_owner_and_notes(self) -> None:
        payload = {
            "owner": "Maria",
            "owner_email": "maria@example.com",
            "data_owner_id": 2,
            "certification_criticality": "high",
            "certification_badges": ["official_use"],
            "certification_notes": "restricted",
        }

        masked = mask_certification_summary_payload(payload)

        self.assertIsNone(masked["owner"])
        self.assertIsNone(masked["owner_email"])
        self.assertIsNone(masked["data_owner_id"])
        self.assertEqual(masked["certification_badges"], [])
        self.assertIsNone(masked["certification_notes"])

    def test_mask_incident_asset_context_payload_hides_sensitive_context(self) -> None:
        payload = {
            "owner_name": "Paulo",
            "owner_defined": True,
            "data_owner_id": 10,
            "sensitivity_level": "restricted",
            "sensitivity_label": "Restrito",
            "actions": [{"label": "Abrir incidente"}],
        }

        masked = mask_incident_asset_context_payload(payload)

        self.assertEqual(masked["owner_name"], "Visibilidade parcial")
        self.assertFalse(masked["owner_defined"])
        self.assertIsNone(masked["data_owner_id"])
        self.assertIsNone(masked["sensitivity_level"])
        self.assertEqual(masked["actions"], [])

    def test_mask_audit_event_payload_replaces_values_with_notice(self) -> None:
        payload = {
            "actor_name": "Analista",
            "actor_email": "analista@example.com",
            "before_value": "A",
            "after_value": "B",
            "metadata_json": {"foo": "bar"},
        }

        masked = mask_audit_event_payload(payload)

        self.assertIsNone(masked["actor_name"])
        self.assertIsNone(masked["actor_email"])
        self.assertEqual(masked["before_value"], "Mascarado para o seu perfil")
        self.assertEqual(masked["after_value"], "Mascarado para o seu perfil")
        self.assertTrue(masked["metadata_json"]["masked"])

    def test_mask_payload_by_policy_masks_personal_owner_fields(self) -> None:
        payload = {
            "owner": "Maria Silva",
            "owner_email": "maria.silva@example.com",
            "table_name": "clientes",
            "data_owner": {
                "id": 7,
                "name": "Maria Silva",
                "email": "maria.silva@example.com",
                "area": "Operações",
                "is_active": True,
            },
        }

        masked = mask_payload_by_policy(payload, can_view_sensitive=False)

        self.assertEqual(masked["owner"], "[masked]")
        self.assertEqual(masked["owner_email"], "[masked]")
        self.assertEqual(masked["table_name"], "clientes")
        self.assertEqual(masked["data_owner"]["name"], "[masked]")
        self.assertEqual(masked["data_owner"]["email"], "[masked]")
        self.assertEqual(masked["data_owner"]["area"], "Operações")


if __name__ == "__main__":
    unittest.main()
