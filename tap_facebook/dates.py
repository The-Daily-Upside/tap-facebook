"""Date/time helpers using the standard library (no pendulum)."""

from __future__ import annotations

import calendar
from datetime import date, datetime, timezone


def parse_date(value: str) -> date:
    """Parse YYYY-MM-DD or ISO datetime strings (including trailing Z) as a date."""
    normalized = value.strip().replace("Z", "+00:00")
    try:
        return date.fromisoformat(normalized)
    except ValueError:
        return datetime.fromisoformat(normalized).date()


def parse_datetime(value: str) -> datetime:
    """Parse ISO datetime or date strings as a timezone-aware UTC datetime."""
    normalized = value.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        day = date.fromisoformat(normalized)
        parsed = datetime(day.year, day.month, day.day)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def utc_today() -> date:
    """Current calendar date in UTC."""
    return datetime.now(timezone.utc).date()


def subtract_months(value: date, months: int) -> date:
    """Return ``value`` minus ``months`` calendar months (day clamped to month end)."""
    year = value.year
    month = value.month - months
    while month <= 0:
        month += 12
        year -= 1
    last_day = calendar.monthrange(year, month)[1]
    return date(year, month, min(value.day, last_day))
