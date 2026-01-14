from __future__ import annotations

import json


def parse_json_records(data: bytes) -> list[dict]:
    doc = json.loads(data)
    if isinstance(doc, list):
        return doc
    if isinstance(doc, dict):
        for key in (
            "destinations",
            "countries",
            "items",
            "events",
            "vulnerabilities",
            "data",
        ):
            value = doc.get(key)
            if isinstance(value, list):
                return value
    return []
