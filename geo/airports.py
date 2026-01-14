from __future__ import annotations

import csv
from pathlib import Path


def load_airports_by_iata(path: Path) -> dict[str, tuple[float, float, str]]:
    by_iata: dict[str, tuple[float, float, str]] = {}
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            code = (row.get("iata_code") or "").strip().upper()
            if not code:
                continue
            lat = row.get("latitude_deg")
            lon = row.get("longitude_deg")
            if not lat or not lon:
                continue
            by_iata[code] = (float(lat), float(lon), str(row.get("name") or code))
    return by_iata
