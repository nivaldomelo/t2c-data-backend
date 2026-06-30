from __future__ import annotations

from types import SimpleNamespace

from t2c_data.features.data_quality import rules as dq_rules
from t2c_data.models.incident import Incident
from t2c_data.services import dq as dq_service


class FakeSession:
    def __init__(self) -> None:
        self.incidents: list[Incident] = []
        self.runs: list[object] = []
        self._next_incident_id = 1
        self._next_run_id = 1

    def add(self, obj: object) -> None:
        if hasattr(obj, "rule_id") and hasattr(obj, "violations_count"):
            if getattr(obj, "id", None) is None:
                setattr(obj, "id", self._next_run_id)
                self._next_run_id += 1
            self.runs.append(obj)
            return
        if isinstance(obj, Incident):
            if obj.id is None:
                obj.id = self._next_incident_id
                self._next_incident_id += 1
            if obj not in self.incidents:
                self.incidents.append(obj)

    def flush(self) -> None:
        return

    def execute(self, _query: object) -> object:
        return SimpleNamespace(scalar_one=lambda: 5)

    def scalar(self, _query: object) -> Incident | None:
        # run_dq_rule asks for a single open/investigating incident for source dq_rule.
        for incident in reversed(self.incidents):
            if (
                incident.source_type == "dq_rule"
                and incident.source_ref_id == 10
                and incident.status in {"open", "investigating"}
            ):
                return incident
        return None


def _patch_dependencies(monkeypatch):
    monkeypatch.setattr(
        dq_rules,
        "resolve_table_context_by_fqn",
        lambda session, table_fqn: (
            SimpleNamespace(id=500),
            SimpleNamespace(),
            SimpleNamespace(),
            SimpleNamespace(db_type="postgres"),
        ),
    )
def _build_rule():
    return SimpleNamespace(
        id=10,
        name="valor_invalido",
        table_fqn="gold.sales",
        is_active=True,
        severity="high",
    )


def test_upsert_incident_creates_incident_on_first_violation(monkeypatch):
    _patch_dependencies(monkeypatch)
    session = FakeSession()
    rule = _build_rule()

    incident = dq_service.upsert_incident_for_dq_rule(
        session,
        rule,
        violations_count=4,
        preview_rows=[{"id": 1, "coluna": "valor"}],
        run_id=1,
        reporter_user_id=1,
    )

    assert incident is not None
    assert len(session.incidents) == 1
    incident = session.incidents[0]
    assert incident.source_type == "dq_rule"
    assert incident.source_ref_id == rule.id
    assert incident.status == "open"
    assert incident.occurrences == 1
    assert incident.evidence_json is not None
    assert incident.evidence_json["violations_count"] == 4


def test_upsert_incident_reuses_open_incident_and_increments_occurrences(monkeypatch):
    _patch_dependencies(monkeypatch)
    session = FakeSession()
    rule = _build_rule()

    dq_service.upsert_incident_for_dq_rule(
        session,
        rule,
        violations_count=2,
        preview_rows=[{"id": 1, "coluna": "valor"}],
        run_id=1,
        reporter_user_id=1,
    )
    dq_service.upsert_incident_for_dq_rule(
        session,
        rule,
        violations_count=2,
        preview_rows=[{"id": 1, "coluna": "valor"}],
        run_id=2,
        reporter_user_id=1,
    )

    assert len(session.incidents) == 1
    incident = session.incidents[0]
    assert incident.occurrences == 2
    assert incident.last_seen_at is not None
    assert incident.evidence_json is not None
    assert incident.evidence_json["violations_count"] == 2
