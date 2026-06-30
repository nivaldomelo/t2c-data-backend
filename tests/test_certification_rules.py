from __future__ import annotations

import os
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch
from datetime import datetime, timedelta, timezone

os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")
os.environ.setdefault("ENV", "test")

from fastapi import HTTPException

from t2c_data.features.certification.api_support import resolve_certification_status, resolve_certification_status_for_profile, validate_certification_patch
from t2c_data.schemas.catalog import TableCertificationPatch


def _table(
    status: str = "not_eligible",
    *,
    review_at=None,
    expires_at=None,
    has_personal_data: bool = False,
    has_sensitive_personal_data: bool = False,
    privacy_reviewed_at=None,
    legal_basis: str | None = None,
    privacy_purpose: str | None = None,
):
    return SimpleNamespace(
        certification_status=status,
        certification_review_at=review_at,
        certification_expires_at=expires_at,
        critical_open_incidents=0,
        active_dq_violation=False,
        active_dq_violation_count=0,
        active_dq_rule_names=[],
        operational_revalidation_required=False,
        readiness_score=0,
        eligible_for_certification=False,
        has_personal_data=has_personal_data,
        has_sensitive_personal_data=has_sensitive_personal_data,
        privacy_reviewed_at=privacy_reviewed_at,
        legal_basis=legal_basis,
        privacy_purpose=privacy_purpose,
    )


class CertificationRuleTests(TestCase):
    def test_thresholds_follow_50_and_80_percent(self) -> None:
        table = _table()
        self.assertEqual(resolve_certification_status(table, readiness_score=49), "not_eligible")
        self.assertEqual(resolve_certification_status(table, readiness_score=50), "eligible")
        self.assertEqual(resolve_certification_status(table, readiness_score=80), "certified")

    def test_high_readiness_stays_certified_even_with_review_metadata(self) -> None:
        table = _table("eligible", review_at=datetime.now(timezone.utc) - timedelta(days=1), expires_at=None)
        table.readiness_score = 88
        table.eligible_for_certification = True
        table.operational_revalidation_required = True
        self.assertEqual(resolve_certification_status_for_profile(table), "certified")

    def test_review_due_does_not_block_automatic_certification(self) -> None:
        table = _table("eligible", review_at=datetime.now(timezone.utc) - timedelta(days=1), expires_at=None)
        table.readiness_score = 88
        table.eligible_for_certification = True
        table.operational_revalidation_required = False
        self.assertEqual(resolve_certification_status_for_profile(table), "certified")

    def test_certified_table_with_active_dq_goes_to_revalidation_pending(self) -> None:
        table = _table("certified")
        table.readiness_score = 90
        table.eligible_for_certification = True
        table.active_dq_violation = True
        table.active_dq_violation_count = 1
        table.active_dq_rule_names = ["dq_rule_orders_not_null"]
        self.assertEqual(resolve_certification_status_for_profile(table), "revalidation_pending")

    def test_certified_table_with_readiness_drop_goes_to_revalidation_pending(self) -> None:
        table = _table("certified")
        table.readiness_score = 68
        table.eligible_for_certification = True
        self.assertEqual(resolve_certification_status_for_profile(table), "revalidation_pending")

    @patch("t2c_data.features.certification.api_support._has_open_critical_incident", return_value=False)
    @patch("t2c_data.features.certification.api_support._active_dq_violation_summary", return_value=(False, 0, []))
    @patch("t2c_data.features.certification.api_support.build_certification_checklist")
    def test_validate_patch_blocks_certified_below_eighty(self, checklist_mock, _dq_mock, _critical_mock) -> None:
        checklist_mock.return_value = (
            [{"key": f"k{i}", "label": f"Check {i}", "passed": True, "detail": "ok"} for i in range(8)],
            6,
            True,
        )
        table = _table()
        payload = TableCertificationPatch(
            certification_status="certified",
            certification_notes="Justificativa formal.",
            certification_review_at=datetime.now(timezone.utc),
            certification_expires_at=datetime.now(timezone.utc) + timedelta(days=30),
        )
        with self.assertRaises(HTTPException) as ctx:
            validate_certification_patch(SimpleNamespace(), table=table, payload=payload)
        self.assertEqual(ctx.exception.status_code, 422)
        self.assertIn("80%", str(ctx.exception.detail))

    @patch("t2c_data.features.certification.api_support._has_open_critical_incident", return_value=False)
    @patch("t2c_data.features.certification.api_support._active_dq_violation_summary", return_value=(True, 1, ["dq_rule_orders_not_null"]))
    @patch("t2c_data.features.certification.api_support.build_certification_checklist")
    def test_validate_patch_blocks_certified_with_active_dq(self, checklist_mock, _dq_mock, _critical_mock) -> None:
        checklist_mock.return_value = (
            [{"key": f"k{i}", "label": f"Check {i}", "passed": True, "detail": "ok"} for i in range(8)],
            8,
            True,
        )
        table = _table()
        payload = TableCertificationPatch(
            certification_status="certified",
            certification_notes="Justificativa formal.",
            certification_review_at=datetime.now(timezone.utc),
            certification_expires_at=datetime.now(timezone.utc) + timedelta(days=30),
        )
        with self.assertRaises(HTTPException) as ctx:
            validate_certification_patch(SimpleNamespace(), table=table, payload=payload)
        self.assertEqual(ctx.exception.status_code, 422)
        self.assertIn("Data Quality", str(ctx.exception.detail))

    @patch("t2c_data.features.certification.api_support._has_open_critical_incident", return_value=False)
    @patch("t2c_data.features.certification.api_support._active_dq_violation_summary", return_value=(False, 0, []))
    @patch("t2c_data.features.certification.api_support.build_certification_checklist")
    def test_validate_patch_blocks_in_review_until_all_gates_pass(self, checklist_mock, _dq_mock, _critical_mock) -> None:
        checklist_mock.return_value = (
            [
                {"key": "owner_defined", "label": "Owner definido", "passed": True, "detail": "ok"},
                {"key": "table_description_complete", "label": "Descrição da tabela", "passed": False, "detail": "ok"},
                {"key": "documentation_coverage", "label": "Colunas documentadas >= 80%", "passed": True, "detail": "ok"},
                {"key": "tags_applied", "label": "Tags aplicadas", "passed": True, "detail": "ok"},
                {"key": "terms_associated", "label": "Termos associados", "passed": True, "detail": "ok"},
                {"key": "privacy_reviewed", "label": "Privacidade revisada quando aplicável", "passed": True, "detail": "ok"},
                {"key": "privacy_context_complete", "label": "Base legal e finalidade registradas quando aplicável", "passed": True, "detail": "ok"},
                {"key": "dq_score", "label": "DQ score >= 90", "passed": True, "detail": "ok"},
                {"key": "no_critical_incidents", "label": "Sem incidente crítico aberto", "passed": True, "detail": "ok"},
                {"key": "review_recent", "label": "Revisão realizada nos últimos 90 dias", "passed": True, "detail": "ok"},
            ],
            9,
            True,
        )
        table = _table()
        payload = TableCertificationPatch(
            certification_status="in_review",
            certification_notes="Submetido para revisão formal.",
        )
        with self.assertRaises(HTTPException) as ctx:
            validate_certification_patch(SimpleNamespace(), table=table, payload=payload)
        self.assertEqual(ctx.exception.status_code, 422)
        self.assertIn("gates obrigatórios", str(ctx.exception.detail).lower())

    @patch("t2c_data.features.certification.api_support._has_open_critical_incident", return_value=False)
    @patch("t2c_data.features.certification.api_support._active_dq_violation_summary", return_value=(False, 0, []))
    @patch("t2c_data.features.certification.api_support.build_certification_checklist")
    def test_validate_patch_requires_formal_notes_for_workflow_statuses(self, checklist_mock, _dq_mock, _critical_mock) -> None:
        checklist_mock.return_value = (
            [{"key": f"k{i}", "label": f"Check {i}", "passed": True, "detail": "ok"} for i in range(10)],
            10,
            True,
        )
        table = _table()
        payload = TableCertificationPatch(certification_status="rejected", certification_notes=None)
        with self.assertRaises(HTTPException) as ctx:
            validate_certification_patch(SimpleNamespace(), table=table, payload=payload)
        self.assertEqual(ctx.exception.status_code, 422)
        self.assertIn("motivo", str(ctx.exception.detail).lower())

    @patch("t2c_data.features.certification.api_support._has_open_critical_incident", return_value=False)
    @patch("t2c_data.features.certification.api_support._active_dq_violation_summary", return_value=(False, 0, []))
    @patch("t2c_data.features.certification.api_support.build_certification_checklist")
    def test_validate_patch_requires_review_and_validity_for_certification(self, checklist_mock, _dq_mock, _critical_mock) -> None:
        checklist_mock.return_value = (
            [{"key": f"k{i}", "label": f"Check {i}", "passed": True, "detail": "ok"} for i in range(10)],
            10,
            True,
        )
        table = _table()
        payload = TableCertificationPatch(certification_status="certified", certification_notes="ok")
        with self.assertRaises(HTTPException) as ctx:
            validate_certification_patch(SimpleNamespace(), table=table, payload=payload)
        self.assertEqual(ctx.exception.status_code, 422)
        self.assertIn("próxima revisão", str(ctx.exception.detail).lower())

    @patch("t2c_data.features.certification.api_support._has_open_critical_incident", return_value=False)
    @patch("t2c_data.features.certification.api_support._active_dq_violation_summary", return_value=(False, 0, []))
    @patch("t2c_data.features.certification.api_support.build_certification_checklist")
    def test_validate_patch_requires_privacy_review_for_personal_data_assets(self, checklist_mock, _dq_mock, _critical_mock) -> None:
        checklist_mock.return_value = (
            [{"key": f"k{i}", "label": f"Check {i}", "passed": True, "detail": "ok"} for i in range(10)],
            10,
            True,
        )
        table = _table(
            has_personal_data=True,
            privacy_reviewed_at=None,
            legal_basis="contract",
            privacy_purpose="Atendimento",
        )
        payload = TableCertificationPatch(
            certification_status="in_review",
            certification_notes="Submetido para revisão formal.",
        )
        with self.assertRaises(HTTPException) as ctx:
            validate_certification_patch(SimpleNamespace(), table=table, payload=payload)
        self.assertEqual(ctx.exception.status_code, 422)
        self.assertIn("privacidade", str(ctx.exception.detail).lower())


if __name__ == "__main__":
    import unittest

    unittest.main()
