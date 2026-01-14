from __future__ import annotations

import xml.etree.ElementTree as ET
from datetime import UTC, datetime


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


def _parse_polygons(area: ET.Element) -> dict | None:
    polygons: list[list[list[float]]] = []
    for polygon_el in area.findall("{*}polygon"):
        polygon_text = polygon_el.text
        if not polygon_text:
            continue
        coords: list[list[float]] = []
        for pair in polygon_text.split():
            lat_str, lon_str = pair.split(",", maxsplit=1)
            coords.append([float(lon_str), float(lat_str)])
        if coords and coords[0] != coords[-1]:
            coords.append(coords[0])
        if coords:
            polygons.append(coords)

    if not polygons:
        return None
    if len(polygons) == 1:
        return {"type": "Polygon", "coordinates": [polygons[0]]}
    return {"type": "MultiPolygon", "coordinates": [[p] for p in polygons]}


def parse_cap_alerts(data: bytes) -> list[dict]:
    root = ET.fromstring(data)
    alert_els: list[ET.Element] = []
    if root.tag.endswith("alert"):
        alert_els = [root]
    else:
        alert_els = root.findall(".//{*}alert")

    records: list[dict] = []
    for alert in alert_els:
        identifier = alert.findtext("{*}identifier") or ""
        sent = _to_iso(alert.findtext("{*}sent"))
        status = alert.findtext("{*}status")
        msg_type = alert.findtext("{*}msgType")

        info = alert.find("{*}info")
        if info is None:
            continue

        event = info.findtext("{*}event")
        headline = info.findtext("{*}headline")
        description = info.findtext("{*}description") or ""
        area_desc = None
        geom = None
        for area in info.findall("{*}area") or []:
            area_desc = area.findtext("{*}areaDesc") or area_desc
            geom = geom or _parse_polygons(area)

        records.append(
            {
                "identifier": identifier,
                "sent": sent,
                "status": status,
                "msg_type": msg_type,
                "event": event,
                "headline": headline,
                "description": description,
                "area_desc": area_desc,
                "geom": geom,
            }
        )

    return records
