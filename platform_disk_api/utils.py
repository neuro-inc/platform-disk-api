from datetime import datetime, timedelta, timezone


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def datetime_dump(dt: datetime) -> str:
    return str(dt.timestamp())


def datetime_load(raw: str) -> datetime:
    return datetime.fromtimestamp(float(raw), timezone.utc)


def timedelta_dump(td: timedelta) -> str:
    return str(td.total_seconds())


def timedelta_load(raw: str) -> timedelta:
    return timedelta(seconds=float(raw))
