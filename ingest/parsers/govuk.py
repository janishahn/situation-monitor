from __future__ import annotations

import json


def parse_govuk_travel_advice_index(data: bytes) -> list[dict]:
    doc = json.loads(data)
    links = doc["links"]
    children = links["children"]
    return list(children)
