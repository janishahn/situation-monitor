from __future__ import annotations

from datetime import UTC
from email.utils import parsedate_to_datetime

import feedparser


def parse_rss(data: bytes) -> list[dict]:
    parsed = feedparser.parse(data)
    records: list[dict] = []
    for entry in parsed.entries:
        georss = None
        georss_point = entry.get("georss_point")
        if georss_point:
            lat_str, lon_str = str(georss_point).split()
            georss = {
                "type": "Point",
                "coordinates": [float(lon_str), float(lat_str)],
            }
        georss_polygon = entry.get("georss_polygon")
        if georss_polygon:
            nums = [float(x) for x in str(georss_polygon).split()]
            coords = [[nums[i + 1], nums[i]] for i in range(0, len(nums), 2)]
            if coords and coords[0] != coords[-1]:
                coords.append(coords[0])
            georss = {"type": "Polygon", "coordinates": [coords]}

        if georss is None and entry.get("geo_lat") and entry.get("geo_long"):
            georss = {
                "type": "Point",
                "coordinates": [float(entry["geo_long"]), float(entry["geo_lat"])],
            }

        published = None
        if "published" in entry:
            try:
                published = (
                    parsedate_to_datetime(entry["published"])
                    .astimezone(tz=UTC)
                    .isoformat()
                    .replace("+00:00", "Z")
                )
            except (TypeError, ValueError):
                published = None
        updated = None
        if "updated" in entry:
            try:
                updated = (
                    parsedate_to_datetime(entry["updated"])
                    .astimezone(tz=UTC)
                    .isoformat()
                    .replace("+00:00", "Z")
                )
            except (TypeError, ValueError):
                updated = None

        content = None
        if "content" in entry and entry["content"]:
            content = entry["content"][0].get("value")

        records.append(
            {
                "id": entry.get("id") or entry.get("guid") or entry.get("link"),
                "link": entry.get("link"),
                "title": entry.get("title", ""),
                "summary": entry.get("summary", ""),
                "content": content,
                "published": published,
                "updated": updated,
                "georss": georss,
            }
        )
    return records
