from __future__ import annotations

import json


def parse_geojson(data: bytes) -> list[dict]:
    doc = json.loads(data)
    if doc.get("type") != "FeatureCollection":
        return []
    features = doc.get("features", [])
    return list(features)
