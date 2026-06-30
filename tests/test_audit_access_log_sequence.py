from __future__ import annotations

import os
from types import SimpleNamespace

os.environ.setdefault("DATABASE_URL", "postgresql+psycopg://user:pass@localhost/testdb")

from sqlalchemy.exc import IntegrityError

from t2c_data.services import audit as audit_service


class _FakeDuplicateKeyError(Exception):
    def __init__(self, constraint_name: str) -> None:
        super().__init__(f'duplicate key value violates unique constraint "{constraint_name}"')
        self.diag = SimpleNamespace(constraint_name=constraint_name)


class _FakeSession:
    def __init__(self, commit_side_effects: list[object | Exception] | None = None) -> None:
        self.commit_side_effects = list(commit_side_effects or [])
        self.commit_calls = 0
        self.rollback_calls = 0

    def commit(self) -> None:
        self.commit_calls += 1
        if self.commit_side_effects:
            effect = self.commit_side_effects.pop(0)
            if isinstance(effect, Exception):
                raise effect

    def rollback(self) -> None:
        self.rollback_calls += 1


def test_commit_access_log_with_repair_retries_on_sequence_conflict(monkeypatch) -> None:
    session = _FakeSession(
        commit_side_effects=[
            IntegrityError("insert", {}, _FakeDuplicateKeyError("access_log_pkey")),
            None,
            None,
        ]
    )
    writes: list[dict[str, object]] = []
    repairs: list[tuple[str, str]] = []

    monkeypatch.setattr(audit_service, "write_access_log_sync", lambda sess, **kwargs: writes.append(kwargs))
    monkeypatch.setattr(
        audit_service,
        "align_integer_pk_sequence",
        lambda sess, *, schema, table_name, column_name="id", use_advisory_lock=True: repairs.append((schema, table_name))
        or audit_service.SequenceAlignmentResult(
            table_name=table_name,
            column_name=column_name,
            sequence_name=f"{schema}.{table_name}_{column_name}_seq",
            max_value=123,
            created_sequence=False,
        ),
    )

    result = audit_service.commit_access_log_with_repair(session, route="/api/v1/me", api_version="v1")

    assert result is not None
    assert repairs == [("t2c_data", "access_log")]
    assert len(writes) == 2
    assert session.rollback_calls == 1
    assert session.commit_calls == 3


def test_commit_access_log_with_repair_does_not_mask_other_integrity_errors(monkeypatch) -> None:
    session = _FakeSession(
        commit_side_effects=[
            IntegrityError("insert", {}, _FakeDuplicateKeyError("some_other_constraint")),
        ]
    )
    monkeypatch.setattr(audit_service, "write_access_log_sync", lambda sess, **kwargs: None)

    try:
        audit_service.commit_access_log_with_repair(session, route="/api/v1/me", api_version="v1")
    except IntegrityError as exc:
        assert "some_other_constraint" in str(exc.orig)
    else:
        raise AssertionError("IntegrityError deveria ter sido propagado")

    assert session.rollback_calls == 1
    assert session.commit_calls == 1
