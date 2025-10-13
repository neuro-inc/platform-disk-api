from datetime import UTC, datetime, timedelta


SUFFIX_TO_FACTOR = {
    "E": 10**18,
    "P": 10**15,
    "T": 10**12,
    "G": 10**9,
    "M": 10**6,
    "k": 10**3,
    "Ei": 1024**6,
    "Pi": 1024**5,
    "Ti": 1024**4,
    "Gi": 1024**3,
    "Mi": 1024**2,
    "Ki": 1024,
}


def utc_now() -> datetime:
    return datetime.now(UTC)


def datetime_dump(dt: datetime) -> str:
    return str(dt.timestamp())


def datetime_load(raw: str) -> datetime:
    return datetime.fromtimestamp(float(raw), UTC)


def timedelta_dump(td: timedelta) -> str:
    return str(td.total_seconds())


def timedelta_load(raw: str) -> timedelta:
    return timedelta(seconds=float(raw))


def _storage_str_to_int(storage: str) -> int:
    # More about this format:
    # https://github.com/kubernetes/kubernetes/blob/6b963ed9c841619d511d2830719b6100d6ab1431/staging/src/k8s.io/apimachinery/pkg/api/resource/quantity.go#L30
    try:
        return int(float(storage))
    except ValueError:
        for suffix, factor in SUFFIX_TO_FACTOR.items():
            if storage.endswith(suffix):
                return factor * int(storage[: -len(suffix)])
        raise
