from __future__ import annotations

import shlex
import subprocess
from datetime import datetime, timezone
from time import perf_counter
from typing import Any, Callable

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from t2c_data.core.config import settings
from t2c_data.core.json_utils import to_jsonable
from t2c_data.models.operations import BackupExecution


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _default_executor(command: list[str]) -> tuple[int, str]:
    completed = subprocess.run(command, capture_output=True, text=True, check=False)  # noqa: S603, S607
    output = (completed.stdout or "") + (completed.stderr or "")
    return completed.returncode, output.strip()


def latest_backup(session: Session, *, scope: str = "platform") -> BackupExecution | None:
    return session.scalar(
        select(BackupExecution)
        .where(BackupExecution.scope == scope)
        .order_by(BackupExecution.started_at.desc())
        .limit(1)
    )


def list_backups(session: Session, *, scope: str | None = None, offset: int = 0, limit: int = 50) -> list[BackupExecution]:
    stmt = select(BackupExecution)
    if scope:
        stmt = stmt.where(BackupExecution.scope == scope)
    stmt = (
        stmt.order_by(BackupExecution.started_at.desc(), BackupExecution.id.desc())
        .offset(max(int(offset or 0), 0))
        .limit(max(1, min(limit, 200)))
    )
    return session.scalars(stmt).all()


def run_backup(
    session: Session,
    *,
    scope: str = "platform",
    triggered_by_user_id: int | None = None,
    trigger_source: str = "manual",
    execute_command: Callable[[list[str]], tuple[int, str]] | None = None,
) -> BackupExecution:
    started = _now()
    record = BackupExecution(
        scope=scope,
        status="running",
        started_at=started,
        trigger_source=trigger_source,
        triggered_by_user_id=triggered_by_user_id,
        retention_days=int(settings.platform_backup_retention_days or 14),
    )
    session.add(record)
    session.flush()

    if not settings.platform_backup_command:
        record.status = "skipped"
        record.finished_at = _now()
        record.duration_ms = int((record.finished_at - started).total_seconds() * 1000)
        record.error_message = "Backup command not configured."
        session.commit()
        return record

    command = shlex.split(str(settings.platform_backup_command))
    executor = execute_command or _default_executor
    started_timer = perf_counter()
    return_code, output = executor(command)
    finished = _now()
    duration_ms = int((perf_counter() - started_timer) * 1000)

    record.finished_at = finished
    record.duration_ms = duration_ms
    record.metadata_json = to_jsonable({"command": command, "output": output[:2000] if output else None})
    if return_code == 0:
        record.status = "success"
    else:
        record.status = "failed"
        record.error_message = output[:2000] if output else f"Backup command failed with exit code {return_code}"
    session.commit()
    return record


def backup_health_snapshot(session: Session) -> dict[str, object]:
    latest = latest_backup(session)
    total = int(session.scalar(select(func.count(BackupExecution.id))) or 0)
    return {
        "total": total,
        "latest": {
            "id": latest.id if latest else None,
            "status": latest.status if latest else None,
            "started_at": latest.started_at if latest else None,
            "finished_at": latest.finished_at if latest else None,
        },
    }


__all__ = ["backup_health_snapshot", "latest_backup", "list_backups", "run_backup"]
