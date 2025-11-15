from __future__ import annotations

import re
from typing import Dict, Iterable, Tuple

try:
    import polars as pl  # type: ignore
except Exception:  # pragma: no cover - optional at import time
    pl = None  # type: ignore

EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")
URL_RE = re.compile(r"^(https?://|www\.)", re.IGNORECASE)
UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$", re.IGNORECASE)
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
DATETIME_RE = re.compile(r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(:\d{2})?(?:[+-]\d{2}:?\d{2})?$")
PRICE_RE = re.compile(r"^[\$€£]?\s?\d+[\.,]?\d*$")
PHONE_RE = re.compile(r"^\+?\d{1,3}?[\s-]?\(?\d{2,4}\)?[\s-]?\d{3,4}[\s-]?\d{3,4}$")


def sample_values(series, max_n: int = 200) -> Iterable[str]:
    """Yield up to max_n non-null string representations from a series-like object."""
    count = 0
    try:
        # Polars Series path
        if pl is not None and isinstance(series, pl.Series):  # type: ignore
            for v in series.drop_nulls().head(max_n).to_list():
                yield "" if v is None else str(v)
                count += 1
                if count >= max_n:
                    break
            return
    except Exception:
        pass
    # Fallback: generic iterable
    for v in series:
        if v is None:
            continue
        yield str(v)
        count += 1
        if count >= max_n:
            break


def detect_value_patterns(series) -> Tuple[Dict[str, float], Dict[str, float]]:
    """Detect simple value patterns and return (hints, boosts).

    hints: proportion of samples matching each pattern key.
    boosts: small positive boosts to be added to similarity score, derived from strong hints.
    """
    hints: Dict[str, float] = {}
    boosts: Dict[str, float] = {}

    # short-circuit for non-string-like columns
    try:
        if pl is not None and isinstance(series, pl.Series):  # type: ignore
            dt = series.dtype  # type: ignore
            if dt in (getattr(pl, "Int8", None), getattr(pl, "Int16", None), getattr(pl, "Int32", None), getattr(pl, "Int64", None),
                      getattr(pl, "UInt8", None), getattr(pl, "UInt16", None), getattr(pl, "UInt32", None), getattr(pl, "UInt64", None),
                      getattr(pl, "Float32", None), getattr(pl, "Float64", None)):
                hints["numeric"] = 1.0
                return hints, boosts
    except Exception:
        pass

    counters = {
        "email": 0,
        "url": 0,
        "uuid": 0,
        "date": 0,
        "datetime": 0,
        "price": 0,
        "phone": 0,
    }
    total = 0
    for s in sample_values(series):
        s = s.strip()
        if not s:
            continue
        total += 1
        if EMAIL_RE.match(s):
            counters["email"] += 1
        if URL_RE.match(s):
            counters["url"] += 1
        if UUID_RE.match(s):
            counters["uuid"] += 1
        if DATE_RE.match(s):
            counters["date"] += 1
        if DATETIME_RE.match(s):
            counters["datetime"] += 1
        if PRICE_RE.match(s):
            counters["price"] += 1
        if PHONE_RE.match(s):
            counters["phone"] += 1

    if total == 0:
        return hints, boosts

    for k, v in counters.items():
        ratio = v / total
        hints[k] = ratio
        # define simple boosts
        if ratio >= 0.6:  # strong signal
            boosts[k] = 0.03
        elif ratio >= 0.3:
            boosts[k] = 0.015

    return hints, boosts
