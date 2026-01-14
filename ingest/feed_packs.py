from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass(frozen=True)
class FeedPackEntry:
    pack_id: str
    source_id: str
    name: str
    source_type: str
    url: str
    region: str
    tags: list[str]
    poll_seconds: int
    enabled: bool


def load_feed_pack_entries(feeds_dir: Path) -> dict[str, list[FeedPackEntry]]:
    packs: dict[str, list[FeedPackEntry]] = {}
    if not feeds_dir.exists():
        return packs

    for path in sorted(feeds_dir.glob("*.yaml")):
        pack_id = path.stem
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        if raw is None:
            packs[pack_id] = []
            continue
        if not isinstance(raw, list):
            raise ValueError(f"invalid feed pack: {path}")

        entries: list[FeedPackEntry] = []
        for entry in raw:
            if not isinstance(entry, dict):
                raise ValueError(f"invalid feed entry in: {path}")
            entries.append(
                FeedPackEntry(
                    pack_id=pack_id,
                    source_id=str(entry["id"]),
                    name=str(entry["name"]),
                    source_type=str(entry.get("type") or "rss"),
                    url=str(entry["url"]),
                    region=str(entry.get("region") or pack_id),
                    tags=[str(t) for t in (entry.get("tags") or [])],
                    poll_seconds=int(entry.get("poll_seconds") or 180),
                    enabled=bool(entry.get("enabled", True)),
                )
            )

        packs[pack_id] = entries

    return packs
