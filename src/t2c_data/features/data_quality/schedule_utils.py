from __future__ import annotations

from calendar import monthrange
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from typing import Any, Mapping, Literal

from fastapi import HTTPException, status

ScheduleMode = Literal["manual", "interval", "daily", "weekly", "biweekly", "monthly"]

SCHEDULE_MODE_LABELS: dict[ScheduleMode, str] = {
    "manual": "Manual",
    "interval": "Intervalo técnico",
    "daily": "Diário",
    "weekly": "Semanal",
    "biweekly": "Quinzenal",
    "monthly": "Mensal",
}

WEEKDAY_LABELS: list[str] = [
    "segunda-feira",
    "terça-feira",
    "quarta-feira",
    "quinta-feira",
    "sexta-feira",
    "sábado",
    "domingo",
]

DEFAULT_TIME_TEXT = "08:00"
DEFAULT_WEEKDAY = 0
DEFAULT_DAY_OF_MONTH = 1


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _coerce_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return None
        return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)
    return None


def _coerce_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        dt = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        return dt.date()
    if isinstance(value, str):
        try:
            return date.fromisoformat(value[:10])
        except ValueError:
            return None
    return None


def _parse_time_text(value: Any) -> time | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        hour_text, minute_text = text.split(":", 1)
        hour = int(hour_text)
        minute = int(minute_text)
    except Exception:  # noqa: BLE001
        return None
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        return None
    return time(hour=hour, minute=minute, tzinfo=timezone.utc)


def _normalize_time_text(value: Any, *, default: str = DEFAULT_TIME_TEXT) -> str:
    parsed = _parse_time_text(value)
    if parsed is None:
        return default
    return f"{parsed.hour:02d}:{parsed.minute:02d}"


def _schedule_timezone(source: Any) -> timezone | ZoneInfo:
    tz_name = getattr(source, "schedule_timezone", None)
    if tz_name is None:
        return timezone.utc
    tz_text = str(tz_name).strip()
    if not tz_text:
        return timezone.utc
    try:
        return ZoneInfo(tz_text)
    except ZoneInfoNotFoundError:
        return timezone.utc


def _normalize_mode_text(value: Any) -> ScheduleMode | None:
    if value is None:
        return None
    text = str(value).strip().lower()
    if text in SCHEDULE_MODE_LABELS:
        return text  # type: ignore[return-value]
    return None


def infer_schedule_mode(*, schedule_mode: Any, schedule_enabled: Any, schedule_every_minutes: Any) -> ScheduleMode:
    mode = _normalize_mode_text(schedule_mode)
    if schedule_enabled is False:
        return "manual"
    if mode in {"interval", "daily", "weekly", "biweekly", "monthly"}:
        return mode
    if mode == "manual":
        if schedule_every_minutes is not None:
            return "interval"
        return "daily"
    if schedule_every_minutes is not None:
        return "interval"
    return "daily"


def schedule_mode_label(mode: ScheduleMode | None) -> str:
    normalized = _normalize_mode_text(mode) or "manual"
    return SCHEDULE_MODE_LABELS[normalized]


def schedule_weekday_label(weekday: Any) -> str:
    try:
        value = int(weekday)
    except Exception:  # noqa: BLE001
        value = DEFAULT_WEEKDAY
    value = max(0, min(6, value))
    return WEEKDAY_LABELS[value]


def schedule_day_of_month_value(value: Any) -> int:
    try:
        day = int(value)
    except Exception:  # noqa: BLE001
        day = DEFAULT_DAY_OF_MONTH
    return max(1, min(31, day))


def validate_schedule_payload(payload: Mapping[str, Any], *, existing: Mapping[str, Any] | None = None) -> dict[str, Any]:
    existing = existing or {}
    raw_mode = _normalize_mode_text(payload.get("schedule_mode", existing.get("schedule_mode")))
    schedule_enabled = bool(payload.get("schedule_enabled", existing.get("schedule_enabled", raw_mode != "manual")))
    mode = infer_schedule_mode(
        schedule_mode=raw_mode,
        schedule_enabled=schedule_enabled,
        schedule_every_minutes=payload.get("schedule_every_minutes", existing.get("schedule_every_minutes")),
    )

    normalized: dict[str, Any] = {
        "schedule_mode": mode,
        "schedule_enabled": schedule_enabled,
    }

    if schedule_enabled is False:
        normalized["schedule_mode"] = raw_mode or _normalize_mode_text(existing.get("schedule_mode")) or "manual"
        return normalized

    if mode == "interval":
        raw_interval = payload.get("schedule_every_minutes", existing.get("schedule_every_minutes"))
        try:
            interval = int(raw_interval)
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="schedule_every_minutes must be a positive integer",
            ) from exc
        if interval <= 0:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="schedule_every_minutes must be greater than zero",
            )
        normalized["schedule_enabled"] = True
        normalized["schedule_every_minutes"] = interval
        normalized["schedule_time"] = None
        normalized["schedule_day_of_week"] = None
        normalized["schedule_day_of_month"] = None
        normalized["schedule_anchor_date"] = None
        return normalized

    normalized["schedule_enabled"] = True
    normalized["schedule_every_minutes"] = None

    schedule_time = _normalize_time_text(payload.get("schedule_time", existing.get("schedule_time")))
    normalized["schedule_time"] = schedule_time

    if mode == "daily":
        normalized["schedule_day_of_week"] = None
        normalized["schedule_day_of_month"] = None
        normalized["schedule_anchor_date"] = None
        return normalized

    if mode == "weekly":
        raw_weekday = payload.get("schedule_day_of_week", existing.get("schedule_day_of_week"))
        try:
            weekday = int(raw_weekday)
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="schedule_day_of_week must be an integer from 0 to 6",
            ) from exc
        if weekday < 0 or weekday > 6:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="schedule_day_of_week must be between 0 and 6",
            )
        normalized["schedule_day_of_week"] = weekday
        normalized["schedule_day_of_month"] = None
        normalized["schedule_anchor_date"] = None
        return normalized

    if mode == "biweekly":
        anchor_date = _coerce_date(payload.get("schedule_anchor_date", existing.get("schedule_anchor_date")))
        if anchor_date is None:
            anchor_date = _utcnow().date()
        anchor_datetime = datetime.combine(anchor_date, time(0, 0, tzinfo=timezone.utc))
        normalized["schedule_day_of_week"] = None
        normalized["schedule_day_of_month"] = None
        normalized["schedule_anchor_date"] = anchor_datetime
        return normalized

    if mode == "monthly":
        raw_day = payload.get("schedule_day_of_month", existing.get("schedule_day_of_month"))
        try:
            day_of_month = int(raw_day)
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="schedule_day_of_month must be an integer between 1 and 31",
            ) from exc
        if day_of_month < 1 or day_of_month > 31:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="schedule_day_of_month must be between 1 and 31",
            )
        normalized["schedule_day_of_week"] = None
        normalized["schedule_day_of_month"] = day_of_month
        normalized["schedule_anchor_date"] = None
        return normalized

    raise HTTPException(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        detail=f"Unsupported schedule_mode '{mode}'",
    )


def describe_schedule(source: Any) -> str:
    mode = infer_schedule_mode(
        schedule_mode=getattr(source, "schedule_mode", None),
        schedule_enabled=getattr(source, "schedule_enabled", None),
        schedule_every_minutes=getattr(source, "schedule_every_minutes", None),
    )
    if mode == "manual":
        return "Manual"
    if mode == "interval":
        interval = int(getattr(source, "schedule_every_minutes", None) or 0)
        if interval <= 0:
            return "Intervalo técnico não configurado"
        if interval % 60 == 0:
            hours = interval // 60
            return f"A cada {hours} hora(s)" if hours != 1 else "A cada 1 hora"
        return f"A cada {interval} minuto(s)"
    schedule_time = _normalize_time_text(getattr(source, "schedule_time", None))
    if mode == "daily":
        return f"Diário às {schedule_time}"
    if mode == "weekly":
        weekday = schedule_weekday_label(getattr(source, "schedule_day_of_week", None))
        return f"Semanal na {weekday} às {schedule_time}"
    if mode == "biweekly":
        anchor_date = _coerce_date(getattr(source, "schedule_anchor_date", None))
        anchor_label = anchor_date.strftime("%d/%m/%Y") if anchor_date else "data base"
        return f"Quinzenal às {schedule_time} (base {anchor_label})"
    if mode == "monthly":
        day_of_month = schedule_day_of_month_value(getattr(source, "schedule_day_of_month", None))
        return f"Mensal no dia {day_of_month} às {schedule_time}"
    return "Manual"


def compute_next_run_at(source: Any, *, reference: datetime | None = None) -> datetime | None:
    reference = reference or _utcnow()
    if reference.tzinfo is None:
        reference = reference.replace(tzinfo=timezone.utc)
    else:
        reference = reference.astimezone(timezone.utc)
    schedule_tz = _schedule_timezone(source)
    local_reference = reference.astimezone(schedule_tz)

    mode = infer_schedule_mode(
        schedule_mode=getattr(source, "schedule_mode", None),
        schedule_enabled=getattr(source, "schedule_enabled", None),
        schedule_every_minutes=getattr(source, "schedule_every_minutes", None),
    )
    if mode == "manual":
        return None

    if mode == "interval":
        interval = int(getattr(source, "schedule_every_minutes", None) or 0)
        if interval <= 0:
            return None
        last_run_at = _coerce_datetime(getattr(source, "schedule_last_run_at", None))
        base = last_run_at or reference
        candidate = base + timedelta(minutes=interval)
        while candidate <= reference:
            candidate += timedelta(minutes=interval)
        return candidate.astimezone(timezone.utc)

    schedule_time = _parse_time_text(getattr(source, "schedule_time", None)) or time(8, 0, tzinfo=schedule_tz)

    if mode == "daily":
        candidate = datetime.combine(local_reference.date(), schedule_time)
        if candidate.tzinfo is None:
            candidate = candidate.replace(tzinfo=schedule_tz)
        if candidate <= local_reference:
            candidate += timedelta(days=1)
        return candidate.astimezone(timezone.utc)

    if mode == "weekly":
        weekday = max(0, min(6, int(getattr(source, "schedule_day_of_week", None) or 0)))
        days_ahead = (weekday - local_reference.weekday()) % 7
        candidate = datetime.combine(local_reference.date() + timedelta(days=days_ahead), schedule_time)
        if candidate.tzinfo is None:
            candidate = candidate.replace(tzinfo=schedule_tz)
        if candidate <= local_reference:
            candidate += timedelta(days=7)
        return candidate.astimezone(timezone.utc)

    if mode == "biweekly":
        anchor_date = _coerce_date(getattr(source, "schedule_anchor_date", None))
        if anchor_date is None:
            anchor_dt = _coerce_datetime(getattr(source, "created_at", None)) or reference
            anchor_date = anchor_dt.date()
        candidate = datetime.combine(anchor_date, schedule_time)
        if candidate.tzinfo is None:
            candidate = candidate.replace(tzinfo=schedule_tz)
        while candidate <= local_reference:
            candidate += timedelta(days=14)
        return candidate.astimezone(timezone.utc)

    if mode == "monthly":
        day_of_month = schedule_day_of_month_value(getattr(source, "schedule_day_of_month", None))
        year = local_reference.year
        month = local_reference.month
        for _ in range(15):
            last_day = monthrange(year, month)[1]
            day = min(day_of_month, last_day)
            candidate = datetime.combine(date(year, month, day), schedule_time)
            if candidate.tzinfo is None:
                candidate = candidate.replace(tzinfo=schedule_tz)
            if candidate > local_reference:
                return candidate.astimezone(timezone.utc)
            month += 1
            if month > 12:
                month = 1
                year += 1
        return None

    return None
