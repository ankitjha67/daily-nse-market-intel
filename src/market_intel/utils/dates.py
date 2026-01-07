from __future__ import annotations

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def now_local(tz: str) -> datetime:
    return datetime.now(ZoneInfo(tz))


def since_hours(tz: str, hours: int) -> datetime:
    return now_local(tz) - timedelta(hours=hours)
