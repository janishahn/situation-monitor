from __future__ import annotations

import xml.etree.ElementTree as ET
from datetime import UTC
from email.utils import parsedate_to_datetime


_GEORSS_NS = "{http://www.georss.org/georss}"


def parse_xml_feed(data: bytes) -> list[dict]:
    root = ET.fromstring(data)
    records: list[dict] = []
    for item in root.findall(".//item"):
        published = None
        pub_date = item.findtext("pubDate")
        if pub_date:
            try:
                published = (
                    parsedate_to_datetime(pub_date)
                    .astimezone(tz=UTC)
                    .isoformat()
                    .replace("+00:00", "Z")
                )
            except (TypeError, ValueError):
                published = None

        georss = None
        point = item.findtext(f"{_GEORSS_NS}point")
        if point:
            lat_str, lon_str = point.split()
            georss = {
                "type": "Point",
                "coordinates": [float(lon_str), float(lat_str)],
            }

        polygon = item.findtext(f"{_GEORSS_NS}polygon")
        if polygon:
            nums = [float(x) for x in polygon.split()]
            coords = [[nums[i + 1], nums[i]] for i in range(0, len(nums), 2)]
            if coords and coords[0] != coords[-1]:
                coords.append(coords[0])
            georss = {"type": "Polygon", "coordinates": [coords]}

        links: list[str] = []
        link_text = item.findtext("link")
        if link_text:
            links.append(link_text)
        for enclosure in item.findall("enclosure"):
            url = enclosure.get("url")
            if url:
                links.append(url)

        records.append(
            {
                "guid": item.findtext("guid") or link_text,
                "title": item.findtext("title") or "",
                "link": link_text,
                "description": item.findtext("description") or "",
                "published": published,
                "georss": georss,
                "links": links,
            }
        )
    return records
