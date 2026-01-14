from __future__ import annotations

import re


_DECIMAL_PAIR_RE = re.compile(
    r"(?P<lat>-?\d{1,2}\.\d+)\s*,\s*(?P<lon>-?\d{1,3}\.\d+)",
    flags=re.UNICODE,
)


def extract_decimal_coords(text: str) -> tuple[float, float] | None:
    match = _DECIMAL_PAIR_RE.search(text)
    if match is None:
        return None
    return (float(match.group("lat")), float(match.group("lon")))
