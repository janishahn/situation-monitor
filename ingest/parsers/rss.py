from __future__ import annotations

from datetime import UTC
from email.utils import parsedate_to_datetime

import feedparser


def parse_rss(data: bytes) -> list[dict]:
    parsed = feedparser.parse(data)
    records: list[dict] = []
    for entry in parsed.entries:
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
            }
        )
    return records
