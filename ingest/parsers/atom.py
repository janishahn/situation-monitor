from __future__ import annotations

import xml.etree.ElementTree as ET
from datetime import UTC, datetime


_ATOM_NS = "{http://www.w3.org/2005/Atom}"
_GEORSS_NS = "{http://www.georss.org/georss}"


def _to_iso(ts: str | None) -> str | None:
    if not ts:
        return None
    if ts.endswith("Z"):
        return ts
    try:
        dt = datetime.fromisoformat(ts)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(tz=UTC).isoformat().replace("+00:00", "Z")


def parse_atom_feed(data: bytes) -> list[dict]:
    root = ET.fromstring(data)
    records: list[dict] = []
    for entry in root.findall(f".//{_ATOM_NS}entry"):
        link_url = None
        for link in entry.findall(f"{_ATOM_NS}link"):
            href = link.get("href")
            if not href:
                continue
            rel = link.get("rel")
            if rel in (None, "", "alternate"):
                link_url = href
                break

        georss = None
        point = entry.findtext(f"{_GEORSS_NS}point")
        if point:
            lat_str, lon_str = point.split()
            georss = {
                "type": "Point",
                "coordinates": [float(lon_str), float(lat_str)],
            }

        records.append(
            {
                "id": entry.findtext(f"{_ATOM_NS}id") or link_url,
                "link": link_url,
                "title": entry.findtext(f"{_ATOM_NS}title") or "",
                "summary": entry.findtext(f"{_ATOM_NS}summary")
                or entry.findtext(f"{_ATOM_NS}content")
                or "",
                "published": _to_iso(entry.findtext(f"{_ATOM_NS}published")),
                "updated": _to_iso(entry.findtext(f"{_ATOM_NS}updated")),
                "georss": georss,
            }
        )
    return records
