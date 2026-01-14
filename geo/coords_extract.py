from __future__ import annotations

import re


_DECIMAL_PAIR_RE = re.compile(
    r"(?P<lat>-?\d{1,2}\.\d+)\s*,\s*(?P<lon>-?\d{1,3}\.\d+)",
    flags=re.UNICODE,
)

_DECIMAL_HEM_PAIR_RE = re.compile(
    r"(?P<lat>\d{1,2}(?:\.\d+)?)\s*(?P<lat_hem>[NS])\s*[, ]\s*(?P<lon>\d{1,3}(?:\.\d+)?)\s*(?P<lon_hem>[EW])",
    flags=re.UNICODE | re.IGNORECASE,
)

_DEGMIN_HEM_PAIR_RE = re.compile(
    r"(?P<lat_deg>\d{1,2})[- ](?P<lat_min>\d{1,2}(?:\.\d+)?)\s*(?P<lat_hem>[NS])\s*[, ]\s*(?P<lon_deg>\d{1,3})[- ](?P<lon_min>\d{1,2}(?:\.\d+)?)\s*(?P<lon_hem>[EW])",
    flags=re.UNICODE | re.IGNORECASE,
)


def extract_decimal_coords(text: str) -> tuple[float, float] | None:
    match = _DECIMAL_PAIR_RE.search(text)
    if match is None:
        return None
    return (float(match.group("lat")), float(match.group("lon")))


def extract_coords(text: str) -> list[tuple[float, float]]:
    coords: list[tuple[float, float]] = []

    for match in _DEGMIN_HEM_PAIR_RE.finditer(text):
        lat = float(match.group("lat_deg")) + float(match.group("lat_min")) / 60.0
        if match.group("lat_hem").casefold() == "s":
            lat = -lat

        lon = float(match.group("lon_deg")) + float(match.group("lon_min")) / 60.0
        if match.group("lon_hem").casefold() == "w":
            lon = -lon

        coords.append((lat, lon))

    for match in _DECIMAL_HEM_PAIR_RE.finditer(text):
        lat = float(match.group("lat"))
        if match.group("lat_hem").casefold() == "s":
            lat = -lat

        lon = float(match.group("lon"))
        if match.group("lon_hem").casefold() == "w":
            lon = -lon

        coords.append((lat, lon))

    for match in _DECIMAL_PAIR_RE.finditer(text):
        coords.append((float(match.group("lat")), float(match.group("lon"))))

    return coords


def extract_coords_centroid(text: str) -> tuple[float, float] | None:
    coords = extract_coords(text)
    if not coords:
        return None
    lat = sum(c[0] for c in coords) / len(coords)
    lon = sum(c[1] for c in coords) / len(coords)
    return (lat, lon)
