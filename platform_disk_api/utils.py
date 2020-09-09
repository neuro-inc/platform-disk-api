from datetime import datetime, timezone


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def datetime_dump(dt: datetime) -> str:
    return str(dt.timestamp())


def datetime_load(raw: str) -> datetime:
    return datetime.fromtimestamp(float(raw), timezone.utc)
