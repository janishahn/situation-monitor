from __future__ import annotations

import hashlib
import json
import re
import uuid
from datetime import UTC, datetime

from cluster.clusterer import canonicalize_url, normalize_title, simhash64


def _utc_now_iso() -> str:
    return datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")


def _iso_from_epoch_ms(ms: int) -> str:
    return (
        datetime.fromtimestamp(ms / 1000.0, tz=UTC).isoformat().replace("+00:00", "Z")
    )


def _sha256_hex(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _u64_to_i64(value: int) -> int:
    if value >= (1 << 63):
        return value - (1 << 64)
    return value


def _bbox_from_geojson(geom: dict) -> tuple[float, float, float, float] | None:
    geom_type = geom.get("type")
    coords = geom.get("coordinates")
    if geom_type is None or coords is None:
        return None

    points: list[tuple[float, float]] = []
    if geom_type == "Point":
        lon, lat = coords
        points.append((float(lon), float(lat)))
    elif geom_type == "Polygon":
        for ring in coords:
            for lon, lat in ring:
                points.append((float(lon), float(lat)))
    elif geom_type == "MultiPolygon":
        for polygon in coords:
            for ring in polygon:
                for lon, lat in ring:
                    points.append((float(lon), float(lat)))
    else:
        return None

    if not points:
        return None
    min_lon = min(p[0] for p in points)
    min_lat = min(p[1] for p in points)
    max_lon = max(p[0] for p in points)
    max_lat = max(p[1] for p in points)
    return (min_lon, min_lat, max_lon, max_lat)


def _centroid_from_bbox(bbox: tuple[float, float, float, float]) -> tuple[float, float]:
    min_lon, min_lat, max_lon, max_lat = bbox
    return ((min_lat + max_lat) / 2.0, (min_lon + max_lon) / 2.0)


def normalize_usgs_earthquake(*, source_id: str, record: dict, fetched_at: str) -> dict:
    properties = record["properties"]
    geometry = record["geometry"]
    coords = geometry["coordinates"]
    lon = float(coords[0])
    lat = float(coords[1])

    title = str(properties.get("title") or "")
    url = canonicalize_url(str(properties["url"]))
    published_at = _iso_from_epoch_ms(int(properties["time"]))
    updated_at = _iso_from_epoch_ms(int(properties["updated"]))
    mag = properties.get("mag")

    summary = str(properties.get("place") or "")
    raw = {
        "mag": float(mag) if mag is not None else None,
        "place": properties.get("place"),
        "time": properties.get("time"),
        "updated": properties.get("updated"),
        "usgs_url": properties.get("url"),
    }

    tags: list[str] = ["usgs", "earthquake"]
    if mag is not None:
        tags.append(f"mag:{float(mag):.1f}")

    normalized_title = normalize_title(title)
    content_for_hash = f"{normalized_title}\n{summary}".strip()
    sim = simhash64(f"{title} {summary[:280]}")

    return {
        "item_id": str(uuid.uuid4()),
        "source_id": source_id,
        "source_type": "geojson_api",
        "external_id": str(record.get("id") or ""),
        "url": url,
        "title": title,
        "summary": summary,
        "content": None,
        "published_at": published_at,
        "updated_at": updated_at,
        "fetched_at": fetched_at,
        "category": "earthquake",
        "tags": json.dumps(tags, ensure_ascii=False),
        "geom_geojson": json.dumps(geometry, ensure_ascii=False),
        "lat": lat,
        "lon": lon,
        "location_name": summary or None,
        "location_confidence": "A_exact",
        "location_rationale": "USGS GeoJSON coordinates",
        "raw": json.dumps(raw, ensure_ascii=False),
        "hash_title": _sha256_hex(normalized_title),
        "hash_content": _sha256_hex(content_for_hash),
        "simhash": _u64_to_i64(sim),
    }


def normalize_nws_alert(*, source_id: str, record: dict, fetched_at: str) -> dict:
    properties = record["properties"]
    geometry = record.get("geometry")

    title = str(properties.get("headline") or properties.get("event") or "")
    url = canonicalize_url(str(record.get("id") or properties.get("id") or ""))
    external_id = str(record.get("id") or properties.get("id") or url)

    summary = str(properties.get("headline") or properties.get("event") or "")
    description = properties.get("description")
    instruction = properties.get("instruction")
    content = None
    if description and instruction:
        content = f"{description}\n\n{instruction}"
    elif description:
        content = str(description)
    elif instruction:
        content = str(instruction)

    published_at = str(
        properties.get("effective")
        or properties.get("onset")
        or properties.get("sent")
        or fetched_at
    )
    updated_at = str(
        properties.get("sent") or properties.get("effective") or fetched_at
    )

    geom_json = None
    lat = None
    lon = None
    bbox = None
    confidence = "U_unknown"
    rationale = "NWS alert without geometry"

    if geometry is not None:
        geom_json = json.dumps(geometry, ensure_ascii=False)
        bbox = _bbox_from_geojson(geometry)
        if bbox is not None:
            lat, lon = _centroid_from_bbox(bbox)
        confidence = "A_exact"
        rationale = "NWS polygon geometry"

    raw = {
        "event": properties.get("event"),
        "severity": properties.get("severity"),
        "urgency": properties.get("urgency"),
        "certainty": properties.get("certainty"),
        "areaDesc": properties.get("areaDesc"),
        "expires": properties.get("expires"),
        "ends": properties.get("ends"),
        "headline": properties.get("headline"),
    }

    tags = [
        "nws",
        "weather_alert",
        f"severity:{properties.get('severity')}",
        f"urgency:{properties.get('urgency')}",
        f"certainty:{properties.get('certainty')}",
    ]

    normalized_title = normalize_title(title)
    content_for_hash = f"{normalized_title}\n{summary}\n{content or ''}".strip()
    sim = simhash64(f"{title} {summary[:280]}")

    return {
        "item_id": str(uuid.uuid4()),
        "source_id": source_id,
        "source_type": "geojson_api",
        "external_id": external_id,
        "url": url,
        "title": title,
        "summary": summary,
        "content": content,
        "published_at": published_at,
        "updated_at": updated_at,
        "fetched_at": fetched_at,
        "category": "weather_alert",
        "tags": json.dumps(tags, ensure_ascii=False),
        "geom_geojson": geom_json,
        "lat": lat,
        "lon": lon,
        "location_name": str(properties.get("areaDesc") or "") or None,
        "location_confidence": confidence,
        "location_rationale": rationale,
        "raw": json.dumps(raw, ensure_ascii=False),
        "hash_title": _sha256_hex(normalized_title),
        "hash_content": _sha256_hex(content_for_hash),
        "simhash": _u64_to_i64(sim),
    }


def normalize_nhc_item(*, source_id: str, record: dict, fetched_at: str) -> dict:
    title = str(record.get("title") or "")
    url = str(record.get("link") or record.get("guid") or "")
    external_id = str(record.get("guid") or url)
    description = str(record.get("description") or "")

    geom = record.get("georss")
    geom_json = json.dumps(geom, ensure_ascii=False) if geom is not None else None
    lat = None
    lon = None
    bbox = _bbox_from_geojson(geom) if isinstance(geom, dict) else None
    if bbox is not None:
        lat, lon = _centroid_from_bbox(bbox)

    confidence = "A_exact" if geom is not None else "C_source_default"
    rationale = (
        "NHC GIS GeoRSS geometry" if geom is not None else "NHC feed (basin-wide)"
    )

    summary = description.strip()
    if len(summary) > 300:
        summary = summary[:297] + "..."

    links = record.get("links") or []
    raw = {"links": links}

    published_at = str(record.get("published") or fetched_at)

    normalized_title = normalize_title(title)
    content_for_hash = f"{normalized_title}\n{summary}".strip()
    sim = simhash64(f"{title} {summary[:280]}")

    return {
        "item_id": str(uuid.uuid4()),
        "source_id": source_id,
        "source_type": "xml_api",
        "external_id": external_id,
        "url": canonicalize_url(url) if url else canonicalize_url(f"nhc:{external_id}"),
        "title": title,
        "summary": summary,
        "content": None,
        "published_at": published_at,
        "updated_at": None,
        "fetched_at": fetched_at,
        "category": "tropical_cyclone",
        "tags": json.dumps(["nhc", "tropical_cyclone"], ensure_ascii=False),
        "geom_geojson": geom_json,
        "lat": lat,
        "lon": lon,
        "location_name": None,
        "location_confidence": confidence,
        "location_rationale": rationale,
        "raw": json.dumps(raw, ensure_ascii=False),
        "hash_title": _sha256_hex(normalized_title),
        "hash_content": _sha256_hex(content_for_hash),
        "simhash": _u64_to_i64(sim),
    }


def normalize_generic_rss(
    *, source_id: str, record: dict, fetched_at: str, category: str
) -> dict:
    title = str(record.get("title") or "")
    url = canonicalize_url(str(record.get("link") or ""))
    external_id = str(record.get("id") or url)
    summary = str(record.get("summary") or "")
    content = record.get("content")
    published_at = str(record.get("published") or fetched_at)
    updated_at = record.get("updated")

    raw = {"feed_id": record.get("id")}

    normalized_title = normalize_title(title)
    content_for_hash = f"{normalized_title}\n{summary}\n{content or ''}".strip()
    sim = simhash64(f"{title} {summary[:280]}")

    return {
        "item_id": str(uuid.uuid4()),
        "source_id": source_id,
        "source_type": "rss",
        "external_id": external_id,
        "url": url,
        "title": title,
        "summary": summary,
        "content": str(content) if content else None,
        "published_at": published_at,
        "updated_at": str(updated_at) if updated_at else None,
        "fetched_at": fetched_at,
        "category": category,
        "tags": json.dumps(["rss", source_id], ensure_ascii=False),
        "geom_geojson": None,
        "lat": None,
        "lon": None,
        "location_name": None,
        "location_confidence": "U_unknown",
        "location_rationale": "RSS without structured geo",
        "raw": json.dumps(raw, ensure_ascii=False),
        "hash_title": _sha256_hex(normalized_title),
        "hash_content": _sha256_hex(content_for_hash),
        "simhash": _u64_to_i64(sim),
    }


_SMARTRAVELLER_TITLE_PREFIX_RE = re.compile(r"^([A-Za-z .()'-]+)\\s*[-:–—]\\s+")


def normalize_smartraveller_rss(
    *, source_id: str, record: dict, fetched_at: str, advice_level: str
) -> dict:
    title = str(record.get("title") or "")
    url = canonicalize_url(str(record.get("link") or ""))
    external_id = str(record.get("id") or url)
    summary = str(record.get("summary") or "")
    published_at = str(record.get("published") or fetched_at)

    country = None
    match = _SMARTRAVELLER_TITLE_PREFIX_RE.match(title)
    if match is not None:
        country = match.group(1).strip()

    normalized_title = normalize_title(title)
    content_for_hash = f"{normalized_title}\n{summary}".strip()
    sim = simhash64(f"{title} {summary[:280]}")

    raw = {"advice_level": advice_level}

    return {
        "item_id": str(uuid.uuid4()),
        "source_id": source_id,
        "source_type": "rss",
        "external_id": external_id,
        "url": url,
        "title": title,
        "summary": summary,
        "content": None,
        "published_at": published_at,
        "updated_at": None,
        "fetched_at": fetched_at,
        "category": "travel_advisory",
        "tags": json.dumps(
            ["smartraveller", "travel_advisory", advice_level], ensure_ascii=False
        ),
        "geom_geojson": None,
        "lat": None,
        "lon": None,
        "location_name": country,
        "location_confidence": "C_country" if country else "U_unknown",
        "location_rationale": "Smartraveller is country-level"
        if country
        else "No country detected",
        "raw": json.dumps(raw, ensure_ascii=False),
        "hash_title": _sha256_hex(normalized_title),
        "hash_content": _sha256_hex(content_for_hash),
        "simhash": _u64_to_i64(sim),
    }


def normalize_smartraveller_export(
    *, source_id: str, record: dict, fetched_at: str
) -> dict:
    name = str(
        record.get("name")
        or record.get("country")
        or record.get("title")
        or "Smartraveller"
    )
    url = str(record.get("url") or record.get("link") or "")

    summary = str(record.get("advice") or record.get("summary") or "")
    if len(summary) > 300:
        summary = summary[:297] + "..."

    country_code = record.get("iso2") or record.get("countryCode") or record.get("code")

    lat = record.get("lat") or record.get("latitude")
    lon = record.get("lon") or record.get("longitude") or record.get("lng")
    lat_f = float(lat) if lat is not None else None
    lon_f = float(lon) if lon is not None else None

    normalized_title = normalize_title(name)
    content_for_hash = f"{normalized_title}\n{summary}".strip()
    sim = simhash64(f"{name} {summary[:280]}")

    raw = {"country_code": country_code}

    return {
        "item_id": str(uuid.uuid4()),
        "source_id": source_id,
        "source_type": "json_api",
        "external_id": str(country_code or name),
        "url": canonicalize_url(url)
        if url
        else canonicalize_url(f"smartraveller:{country_code or name}"),
        "title": name,
        "summary": summary,
        "content": None,
        "published_at": fetched_at,
        "updated_at": None,
        "fetched_at": fetched_at,
        "category": "travel_advisory",
        "tags": json.dumps(["smartraveller", "travel_advisory"], ensure_ascii=False),
        "geom_geojson": None,
        "lat": lat_f,
        "lon": lon_f,
        "location_name": name,
        "location_confidence": "C_country",
        "location_rationale": "Smartraveller destinations export",
        "raw": json.dumps(raw, ensure_ascii=False),
        "hash_title": _sha256_hex(normalized_title),
        "hash_content": _sha256_hex(content_for_hash),
        "simhash": _u64_to_i64(sim),
    }
