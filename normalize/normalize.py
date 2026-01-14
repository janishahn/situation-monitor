from __future__ import annotations

import html
import hashlib
import json
import re
import uuid
from datetime import UTC, datetime

from cluster.clusterer import canonicalize_url, normalize_title, simhash64
from geo.coords_extract import extract_coords_centroid


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
    *,
    source_id: str,
    record: dict,
    fetched_at: str,
    category: str,
    tags: list[str] | None = None,
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

    tag_values = ["rss", source_id]
    if tags:
        for tag in tags:
            if tag not in tag_values:
                tag_values.append(tag)

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
        "tags": json.dumps(tag_values, ensure_ascii=False),
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


def normalize_gdacs_rss(*, source_id: str, record: dict, fetched_at: str) -> dict:
    title = str(record.get("title") or "")
    url = str(record.get("link") or "")
    external_id = str(record.get("id") or url)
    summary = str(record.get("summary") or "")
    published_at = str(record.get("published") or fetched_at)
    updated_at = record.get("updated")

    text = f"{title} {summary}".casefold()
    if "tsunami" in text:
        category = "tsunami"
    elif "volcano" in text:
        category = "volcano"
    elif "wildfire" in text or "forest fire" in text:
        category = "wildfire"
    elif (
        "cyclone" in text
        or "hurricane" in text
        or "typhoon" in text
        or "tropical storm" in text
    ):
        category = "tropical_cyclone"
    elif "earthquake" in text or re.search(r"\beq\b", text):
        category = "earthquake"
    else:
        category = "disaster"

    geom = record.get("georss")
    geom_json = json.dumps(geom, ensure_ascii=False) if geom is not None else None
    lat = None
    lon = None
    bbox = _bbox_from_geojson(geom) if isinstance(geom, dict) else None
    if bbox is not None:
        lat, lon = _centroid_from_bbox(bbox)

    confidence = "A_exact" if geom is not None else "U_unknown"
    rationale = (
        "GDACS GeoRSS geometry" if geom is not None else "GDACS entry without geometry"
    )

    tags = ["gdacs", category]
    normalized_title = normalize_title(title)
    content_for_hash = f"{normalized_title}\n{summary}".strip()
    sim = simhash64(f"{title} {summary[:280]}")

    raw = {"feed_id": record.get("id")}

    return {
        "item_id": str(uuid.uuid4()),
        "source_id": source_id,
        "source_type": "rss",
        "external_id": external_id,
        "url": canonicalize_url(url)
        if url
        else canonicalize_url(f"gdacs:{external_id}"),
        "title": title,
        "summary": summary,
        "content": None,
        "published_at": published_at,
        "updated_at": str(updated_at) if updated_at else None,
        "fetched_at": fetched_at,
        "category": category,
        "tags": json.dumps(tags, ensure_ascii=False),
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


def normalize_eonet_event(*, source_id: str, record: dict, fetched_at: str) -> dict:
    event_id = str(record.get("id") or "")
    title = str(record.get("title") or "")
    url = str(record.get("link") or "")

    category_title = ""
    categories = record.get("categories") or []
    if categories:
        category_title = str(categories[0].get("title") or "")

    cat_text = category_title.casefold()
    if "wildfire" in cat_text:
        category = "wildfire"
    elif "volcano" in cat_text:
        category = "volcano"
    elif "earthquake" in cat_text:
        category = "earthquake"
    else:
        category = "disaster"

    geometries = record.get("geometry") or []
    geom_json = None
    lat = None
    lon = None
    published_at = fetched_at
    updated_at = None

    if geometries:
        first_date = geometries[0].get("date")
        published_at = str(first_date or fetched_at)

        last = geometries[-1]
        updated_at = str(last.get("date") or published_at)
        geom = {"type": last.get("type"), "coordinates": last.get("coordinates")}
        geom_json = json.dumps(geom, ensure_ascii=False)
        bbox = _bbox_from_geojson(geom) if isinstance(geom, dict) else None
        if bbox is not None:
            lat, lon = _centroid_from_bbox(bbox)

    summary = category_title.strip() or "EONET event"
    raw = {
        "categories": record.get("categories"),
        "sources": record.get("sources"),
        "geometry_count": len(geometries),
    }

    tags = ["eonet", category]
    normalized_title = normalize_title(title)
    content_for_hash = f"{normalized_title}\n{summary}".strip()
    sim = simhash64(f"{title} {summary[:280]}")

    return {
        "item_id": str(uuid.uuid4()),
        "source_id": source_id,
        "source_type": "json_api",
        "external_id": event_id,
        "url": canonicalize_url(url) if url else canonicalize_url(f"eonet:{event_id}"),
        "title": title,
        "summary": summary,
        "content": None,
        "published_at": published_at,
        "updated_at": updated_at,
        "fetched_at": fetched_at,
        "category": category,
        "tags": json.dumps(tags, ensure_ascii=False),
        "geom_geojson": geom_json,
        "lat": lat,
        "lon": lon,
        "location_name": None,
        "location_confidence": "A_exact"
        if lat is not None and lon is not None
        else "U_unknown",
        "location_rationale": "EONET geometry"
        if lat is not None and lon is not None
        else "EONET without geometry",
        "raw": json.dumps(raw, ensure_ascii=False),
        "hash_title": _sha256_hex(normalized_title),
        "hash_content": _sha256_hex(content_for_hash),
        "simhash": _u64_to_i64(sim),
    }


def normalize_hans_elevated_notice(
    *, source_id: str, record: dict, fetched_at: str
) -> dict:
    volcano_name = str(record.get("volcano_name") or "")
    alert_level = str(record.get("alert_level") or "")
    color_code = str(record.get("color_code") or "")
    notice_id = str(record.get("notice_identifier") or "")
    url = str(record.get("notice_url") or "")

    published_at = fetched_at
    sent_utc = record.get("sent_utc")
    if sent_utc:
        try:
            dt = datetime.strptime(str(sent_utc), "%Y-%m-%d %H:%M:%S").replace(
                tzinfo=UTC
            )
            published_at = dt.isoformat().replace("+00:00", "Z")
        except ValueError:
            published_at = fetched_at

    alert_rank = {"normal": 1, "advisory": 2, "watch": 3, "warning": 4}.get(
        alert_level.casefold(), 2
    )
    color_rank = {"green": 1, "yellow": 2, "orange": 3, "red": 4}.get(
        color_code.casefold(), 2
    )
    severity_level = max(alert_rank, color_rank)
    if alert_rank >= 4 and color_rank >= 4:
        severity_level = 5

    title = f"{volcano_name} - {alert_level} / {color_code}".strip(" -/")
    summary = str(record.get("obs_fullname") or "") or "USGS HANS elevated volcano"

    normalized_title = normalize_title(title)
    content_for_hash = f"{normalized_title}\n{summary}".strip()
    sim = simhash64(f"{title} {summary[:280]}")

    raw = {
        "vnum": record.get("vnum"),
        "notice_type": record.get("notice_type_cd"),
        "alert_level": alert_level,
        "aviation_color_code": color_code,
        "severity_level_1_5": severity_level,
        "notice_data": record.get("notice_data"),
    }

    tags = [
        "usgs",
        "hans",
        "volcano",
        f"alert:{alert_level.casefold() or 'unknown'}",
        f"aviation:{color_code.casefold() or 'unknown'}",
    ]

    return {
        "item_id": str(uuid.uuid4()),
        "source_id": source_id,
        "source_type": "json_api",
        "external_id": notice_id,
        "url": canonicalize_url(url) if url else canonicalize_url(f"hans:{notice_id}"),
        "title": title,
        "summary": summary,
        "content": None,
        "published_at": published_at,
        "updated_at": None,
        "fetched_at": fetched_at,
        "category": "volcano",
        "tags": json.dumps(tags, ensure_ascii=False),
        "geom_geojson": None,
        "lat": None,
        "lon": None,
        "location_name": volcano_name or None,
        "location_confidence": "U_unknown",
        "location_rationale": "USGS HANS elevated list",
        "raw": json.dumps(raw, ensure_ascii=False),
        "hash_title": _sha256_hex(normalized_title),
        "hash_content": _sha256_hex(content_for_hash),
        "simhash": _u64_to_i64(sim),
    }


def normalize_hans_volcano_rss_item(
    *,
    source_id: str,
    record: dict,
    fetched_at: str,
    volcano_name: str,
    vnum: str,
) -> dict:
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

    confidence = "A_exact" if geom is not None else "U_unknown"
    rationale = (
        "USGS HANS GeoRSS geometry" if geom is not None else "HANS RSS without geo"
    )

    summary = description.strip()
    if len(summary) > 300:
        summary = summary[:297] + "..."

    published_at = str(record.get("published") or fetched_at)

    normalized_title = normalize_title(title)
    content_for_hash = f"{normalized_title}\n{summary}".strip()
    sim = simhash64(f"{title} {summary[:280]}")

    raw = {"vnum": vnum, "volcano_name": volcano_name, "links": record.get("links")}

    return {
        "item_id": str(uuid.uuid4()),
        "source_id": source_id,
        "source_type": "xml_api",
        "external_id": external_id,
        "url": canonicalize_url(url)
        if url
        else canonicalize_url(f"hans:{external_id}"),
        "title": title,
        "summary": summary,
        "content": None,
        "published_at": published_at,
        "updated_at": None,
        "fetched_at": fetched_at,
        "category": "volcano",
        "tags": json.dumps(["usgs", "hans", "volcano"], ensure_ascii=False),
        "geom_geojson": geom_json,
        "lat": lat,
        "lon": lon,
        "location_name": volcano_name or None,
        "location_confidence": confidence,
        "location_rationale": rationale,
        "raw": json.dumps(raw, ensure_ascii=False),
        "hash_title": _sha256_hex(normalized_title),
        "hash_content": _sha256_hex(content_for_hash),
        "simhash": _u64_to_i64(sim),
    }


def normalize_tsunami_atom(*, source_id: str, record: dict, fetched_at: str) -> dict:
    title = str(record.get("title") or "")
    url = str(record.get("link") or "")
    external_id = str(record.get("id") or url)
    summary = str(record.get("summary") or "")
    published_at = str(record.get("published") or fetched_at)
    updated_at = record.get("updated")

    geom = record.get("georss")
    geom_json = json.dumps(geom, ensure_ascii=False) if geom is not None else None
    lat = None
    lon = None
    bbox = _bbox_from_geojson(geom) if isinstance(geom, dict) else None
    if bbox is not None:
        lat, lon = _centroid_from_bbox(bbox)

    confidence = "A_exact" if geom is not None else "U_unknown"
    rationale = (
        "Tsunami Atom GeoRSS geometry" if geom is not None else "Tsunami Atom feed"
    )

    if geom is None and "ntwc" in source_id:
        lat, lon = 61.0, -150.0
        confidence = "C_source_default"
        rationale = "NTWC feed default region centroid"
    if geom is None and "ptwc" in source_id:
        lat, lon = 21.3, -157.9
        confidence = "C_source_default"
        rationale = "PTWC feed default region centroid"

    normalized_title = normalize_title(title)
    content_for_hash = f"{normalized_title}\n{summary}".strip()
    sim = simhash64(f"{title} {summary[:280]}")

    return {
        "item_id": str(uuid.uuid4()),
        "source_id": source_id,
        "source_type": "xml_api",
        "external_id": external_id,
        "url": canonicalize_url(url)
        if url
        else canonicalize_url(f"tsunami:{external_id}"),
        "title": title,
        "summary": summary,
        "content": None,
        "published_at": published_at,
        "updated_at": str(updated_at) if updated_at else None,
        "fetched_at": fetched_at,
        "category": "tsunami",
        "tags": json.dumps(["tsunami"], ensure_ascii=False),
        "geom_geojson": geom_json,
        "lat": lat,
        "lon": lon,
        "location_name": None,
        "location_confidence": confidence,
        "location_rationale": rationale,
        "raw": json.dumps({"feed_id": record.get("id")}, ensure_ascii=False),
        "hash_title": _sha256_hex(normalized_title),
        "hash_content": _sha256_hex(content_for_hash),
        "simhash": _u64_to_i64(sim),
    }


def normalize_tsunami_cap(*, source_id: str, record: dict, fetched_at: str) -> dict:
    identifier = str(record.get("identifier") or "")
    published_at = str(record.get("sent") or fetched_at)
    event = str(record.get("event") or "")
    headline = str(record.get("headline") or "") or event or "Tsunami alert"
    description = str(record.get("description") or "")
    area_desc = str(record.get("area_desc") or "") or None

    geom = record.get("geom")
    geom_json = json.dumps(geom, ensure_ascii=False) if geom is not None else None
    lat = None
    lon = None
    bbox = _bbox_from_geojson(geom) if isinstance(geom, dict) else None
    if bbox is not None:
        lat, lon = _centroid_from_bbox(bbox)

    confidence = "A_exact" if geom is not None else "U_unknown"
    rationale = "CAP polygon geometry" if geom is not None else "CAP without geometry"

    if geom is None and "ntwc" in source_id:
        lat, lon = 61.0, -150.0
        confidence = "C_source_default"
        rationale = "NTWC CAP default region centroid"
    if geom is None and "ptwc" in source_id:
        lat, lon = 21.3, -157.9
        confidence = "C_source_default"
        rationale = "PTWC CAP default region centroid"

    summary = description.strip()
    if len(summary) > 300:
        summary = summary[:297] + "..."

    normalized_title = normalize_title(headline)
    content_for_hash = f"{normalized_title}\n{summary}".strip()
    sim = simhash64(f"{headline} {summary[:280]}")

    raw = {
        "status": record.get("status"),
        "msg_type": record.get("msg_type"),
        "area_desc": area_desc,
    }

    return {
        "item_id": str(uuid.uuid4()),
        "source_id": source_id,
        "source_type": "xml_api",
        "external_id": identifier,
        "url": canonicalize_url(f"tsunami:{identifier}"),
        "title": headline,
        "summary": summary,
        "content": None,
        "published_at": published_at,
        "updated_at": None,
        "fetched_at": fetched_at,
        "category": "tsunami",
        "tags": json.dumps(["tsunami"], ensure_ascii=False),
        "geom_geojson": geom_json,
        "lat": lat,
        "lon": lon,
        "location_name": area_desc,
        "location_confidence": confidence,
        "location_rationale": rationale,
        "raw": json.dumps(raw, ensure_ascii=False),
        "hash_title": _sha256_hex(normalized_title),
        "hash_content": _sha256_hex(content_for_hash),
        "simhash": _u64_to_i64(sim),
    }


def normalize_firms_hotspot(*, source_id: str, record: dict, fetched_at: str) -> dict:
    lat = float(record["latitude"])
    lon = float(record["longitude"])

    acq_date = str(record.get("acq_date") or "")
    acq_time = str(record.get("acq_time") or "")
    published_at = fetched_at
    if acq_date and acq_time and len(acq_time) >= 3:
        try:
            hh = int(acq_time.zfill(4)[:2])
            mm = int(acq_time.zfill(4)[2:4])
            dt = datetime.fromisoformat(acq_date).replace(
                tzinfo=UTC, hour=hh, minute=mm, second=0, microsecond=0
            )
            published_at = dt.isoformat().replace("+00:00", "Z")
        except ValueError:
            published_at = fetched_at

    external_id = f"{acq_date}T{acq_time}:{lat:.4f}:{lon:.4f}"
    title = "Wildfire hotspot"

    frp = record.get("frp")
    bright = record.get("bright_ti4") or record.get("brightness")
    summary_parts: list[str] = []
    if bright:
        summary_parts.append(f"brightness={bright}")
    if frp:
        summary_parts.append(f"frp={frp}")
    summary = ", ".join(summary_parts) or "NASA FIRMS hotspot"

    normalized_title = normalize_title(title)
    content_for_hash = f"{normalized_title}\n{summary}".strip()
    sim = simhash64(f"{title} {summary[:280]}")

    return {
        "item_id": str(uuid.uuid4()),
        "source_id": source_id,
        "source_type": "csv_api",
        "external_id": external_id,
        "url": canonicalize_url(f"firms:{external_id}"),
        "title": title,
        "summary": summary,
        "content": None,
        "published_at": published_at,
        "updated_at": None,
        "fetched_at": fetched_at,
        "category": "wildfire",
        "tags": json.dumps(["firms", "wildfire"], ensure_ascii=False),
        "geom_geojson": None,
        "lat": lat,
        "lon": lon,
        "location_name": None,
        "location_confidence": "A_exact",
        "location_rationale": "FIRMS hotspot lat/lon",
        "raw": json.dumps(record, ensure_ascii=False),
        "hash_title": _sha256_hex(normalized_title),
        "hash_content": _sha256_hex(content_for_hash),
        "simhash": _u64_to_i64(sim),
    }


def normalize_faa_airport_disruption(
    *,
    source_id: str,
    record: dict,
    fetched_at: str,
    airports_by_iata: dict[str, tuple[float, float, str]],
) -> dict:
    iata = str(record.get("iata") or "").strip().upper()
    name = str(record.get("name") or iata)
    reason = str(record.get("reason") or "")
    program = str(record.get("program") or "")
    delay_type = str(record.get("type") or "")

    title = f"{iata} - {delay_type or 'Disruption'}"
    summary = reason.strip()
    if record.get("avg_delay"):
        summary = f"{summary} (avg {record['avg_delay']})".strip()

    published_at = fetched_at
    update_time = record.get("update_time")
    if update_time:
        try:
            dt = datetime.strptime(
                str(update_time), "%a %b %d %H:%M:%S %Y UTC"
            ).replace(tzinfo=UTC)
            published_at = dt.isoformat().replace("+00:00", "Z")
        except ValueError:
            published_at = fetched_at

    lat = None
    lon = None
    confidence = "U_unknown"
    rationale = "Airport not found in offline dataset"
    if iata and iata in airports_by_iata:
        lat, lon, name_out = airports_by_iata[iata]
        name = name_out
        confidence = "A_exact"
        rationale = "Offline airport dataset (IATA)"

    severity_kind = "delay"
    reason_lc = reason.casefold()
    if "closed" in reason_lc or "closure" in reason_lc:
        severity_kind = "closure"
    elif "ground stop" in reason_lc:
        severity_kind = "ground_stop"
    elif program.casefold() in {"gdp", "ground delay program"}:
        severity_kind = "gdp"

    avg_delay_min = None
    if record.get("avg_delay"):
        m = re.search(r"(\\d+)", str(record["avg_delay"]))
        if m:
            avg_delay_min = int(m.group(1))

    normalized_title = normalize_title(title)
    content_for_hash = f"{normalized_title}\n{summary}".strip()
    sim = simhash64(f"{title} {summary[:280]}")

    raw = {
        "reason": reason or None,
        "program": program or None,
        "type": delay_type or None,
        "trend": record.get("trend"),
        "severity_kind": severity_kind,
        "avg_delay_min": avg_delay_min,
    }

    return {
        "item_id": str(uuid.uuid4()),
        "source_id": source_id,
        "source_type": "xml_api",
        "external_id": f"{iata}:{published_at}",
        "url": canonicalize_url(f"faa:{iata}:{published_at}"),
        "title": title,
        "summary": summary,
        "content": None,
        "published_at": published_at,
        "updated_at": None,
        "fetched_at": fetched_at,
        "category": "aviation_disruption",
        "tags": json.dumps(["faa", "aviation_disruption"], ensure_ascii=False),
        "geom_geojson": None,
        "lat": lat,
        "lon": lon,
        "location_name": name or None,
        "location_confidence": confidence,
        "location_rationale": rationale,
        "raw": json.dumps(raw, ensure_ascii=False),
        "hash_title": _sha256_hex(normalized_title),
        "hash_content": _sha256_hex(content_for_hash),
        "simhash": _u64_to_i64(sim),
    }


def normalize_country_level_rss(
    *, source_id: str, record: dict, fetched_at: str, category: str, tags: list[str]
) -> dict:
    title = str(record.get("title") or "")
    url = canonicalize_url(str(record.get("link") or ""))
    external_id = str(record.get("id") or url)
    summary = str(record.get("summary") or "")
    published_at = str(record.get("published") or fetched_at)
    updated_at = record.get("updated")

    country = None
    if " - " in title:
        candidate = title.rsplit(" - ", maxsplit=1)[1].strip()
        if candidate:
            country = candidate

    normalized_title = normalize_title(title)
    content_for_hash = f"{normalized_title}\n{summary}".strip()
    sim = simhash64(f"{title} {summary[:280]}")

    return {
        "item_id": str(uuid.uuid4()),
        "source_id": source_id,
        "source_type": "rss",
        "external_id": external_id,
        "url": url if url else canonicalize_url(f"{source_id}:{external_id}"),
        "title": title,
        "summary": summary,
        "content": None,
        "published_at": published_at,
        "updated_at": str(updated_at) if updated_at else None,
        "fetched_at": fetched_at,
        "category": category,
        "tags": json.dumps(tags, ensure_ascii=False),
        "geom_geojson": None,
        "lat": None,
        "lon": None,
        "location_name": country,
        "location_confidence": "C_country" if country else "U_unknown",
        "location_rationale": "Country inferred from title"
        if country
        else "No country detected",
        "raw": json.dumps({"feed_id": record.get("id")}, ensure_ascii=False),
        "hash_title": _sha256_hex(normalized_title),
        "hash_content": _sha256_hex(content_for_hash),
        "simhash": _u64_to_i64(sim),
    }


def normalize_nvd_cve(*, source_id: str, record: dict, fetched_at: str) -> dict:
    cve = record["cve"]
    cve_id = str(cve["id"])

    description = ""
    for desc in cve.get("descriptions", []):
        if desc.get("lang") == "en":
            description = str(desc.get("value") or "")
            break

    vendor_product: list[str] = []
    configurations = cve.get("configurations") or {}
    nodes = list(configurations.get("nodes") or [])
    while nodes:
        node = nodes.pop()
        nodes.extend(node.get("children") or [])
        for match in node.get("cpeMatch") or []:
            criteria = str(match.get("criteria") or "")
            if not criteria.startswith("cpe:2.3:"):
                continue
            parts = criteria.split(":")
            if len(parts) >= 5:
                vendor = parts[3]
                product = parts[4]
                if vendor and product:
                    vp = f"{vendor}:{product}"
                    if vp not in vendor_product:
                        vendor_product.append(vp)

    vp_summary = ", ".join(vendor_product[:3])
    title = f"{cve_id} - {vp_summary}" if vp_summary else cve_id
    summary = description.strip()
    if len(summary) > 300:
        summary = summary[:297] + "..."

    published_at = str(cve.get("published") or fetched_at)
    updated_at = str(cve.get("lastModified") or "") or None

    normalized_title = normalize_title(title)
    content_for_hash = f"{normalized_title}\n{summary}".strip()
    sim = simhash64(f"{title} {summary[:280]}")

    raw = {
        "cve": cve_id,
        "vendor_product": vendor_product,
        "metrics": cve.get("metrics"),
    }

    tags = ["nvd", "cve"] + vendor_product[:5]

    return {
        "item_id": str(uuid.uuid4()),
        "source_id": source_id,
        "source_type": "json_api",
        "external_id": cve_id,
        "url": canonicalize_url(f"https://nvd.nist.gov/vuln/detail/{cve_id}"),
        "title": title,
        "summary": summary,
        "content": None,
        "published_at": published_at,
        "updated_at": updated_at,
        "fetched_at": fetched_at,
        "category": "cyber_cve",
        "tags": json.dumps(tags, ensure_ascii=False),
        "geom_geojson": None,
        "lat": None,
        "lon": None,
        "location_name": None,
        "location_confidence": "U_unknown",
        "location_rationale": "Cyber advisory (non-geographic)",
        "raw": json.dumps(raw, ensure_ascii=False),
        "hash_title": _sha256_hex(normalized_title),
        "hash_content": _sha256_hex(content_for_hash),
        "simhash": _u64_to_i64(sim),
    }


def normalize_cisa_kev(*, source_id: str, record: dict, fetched_at: str) -> dict:
    cve_id = str(record["cveID"])
    vendor = str(record.get("vendorProject") or "")
    product = str(record.get("product") or "")

    title = f"KEV: {cve_id} - {vendor} {product}".strip()
    summary = str(
        record.get("vulnerabilityName") or record.get("shortDescription") or ""
    )
    if len(summary) > 300:
        summary = summary[:297] + "..."

    published_at = str(record.get("dateAdded") or fetched_at)

    normalized_title = normalize_title(title)
    content_for_hash = f"{normalized_title}\n{summary}".strip()
    sim = simhash64(f"{title} {summary[:280]}")

    return {
        "item_id": str(uuid.uuid4()),
        "source_id": source_id,
        "source_type": "json_api",
        "external_id": cve_id,
        "url": canonicalize_url(f"cisa-kev:{cve_id}"),
        "title": title,
        "summary": summary,
        "content": None,
        "published_at": published_at,
        "updated_at": None,
        "fetched_at": fetched_at,
        "category": "cyber_kev",
        "tags": json.dumps(["cisa", "kev", vendor, product], ensure_ascii=False),
        "geom_geojson": None,
        "lat": None,
        "lon": None,
        "location_name": None,
        "location_confidence": "U_unknown",
        "location_rationale": "Cyber advisory (non-geographic)",
        "raw": json.dumps(record, ensure_ascii=False),
        "hash_title": _sha256_hex(normalized_title),
        "hash_content": _sha256_hex(content_for_hash),
        "simhash": _u64_to_i64(sim),
    }


def normalize_govuk_travel_advice(
    *, source_id: str, record: dict, fetched_at: str
) -> dict:
    title = str(record.get("title") or "")
    url = str(record.get("web_url") or "")
    external_id = str(record.get("content_id") or record.get("base_path") or title)

    details = record.get("details") or {}
    change = str(details.get("change_description") or "")
    if len(change) > 300:
        change = change[:297] + "..."

    country = details.get("country") or {}
    country_name = str(country.get("name") or "") or None

    published_at = str(record.get("public_updated_at") or fetched_at)

    normalized_title = normalize_title(title)
    content_for_hash = f"{normalized_title}\n{change}".strip()
    sim = simhash64(f"{title} {change[:280]}")

    return {
        "item_id": str(uuid.uuid4()),
        "source_id": source_id,
        "source_type": "json_api",
        "external_id": external_id,
        "url": canonicalize_url(url)
        if url
        else canonicalize_url(f"govuk:{external_id}"),
        "title": title,
        "summary": change,
        "content": None,
        "published_at": published_at,
        "updated_at": None,
        "fetched_at": fetched_at,
        "category": "travel_advisory",
        "tags": json.dumps(["govuk", "travel_advisory"], ensure_ascii=False),
        "geom_geojson": None,
        "lat": None,
        "lon": None,
        "location_name": country_name,
        "location_confidence": "C_country" if country_name else "U_unknown",
        "location_rationale": "GOV.UK is country-level"
        if country_name
        else "No country detected",
        "raw": json.dumps(record, ensure_ascii=False),
        "hash_title": _sha256_hex(normalized_title),
        "hash_content": _sha256_hex(content_for_hash),
        "simhash": _u64_to_i64(sim),
    }


def normalize_reliefweb_report(
    *, source_id: str, record: dict, fetched_at: str
) -> dict:
    report_id = str(record["id"])
    fields = record.get("fields") or {}

    title = str(fields.get("title") or "")
    summary = str(fields.get("headline") or fields.get("body") or "")
    if len(summary) > 300:
        summary = summary[:297] + "..."

    published_at = fetched_at
    dates = fields.get("date")
    if isinstance(dates, dict) and dates.get("created"):
        published_at = str(dates["created"])

    country = None
    primary_country = fields.get("primary_country")
    if isinstance(primary_country, dict) and primary_country.get("name"):
        country = str(primary_country["name"])
    countries = fields.get("country") or []
    if country is None and countries:
        first = countries[0]
        if isinstance(first, dict) and first.get("name"):
            country = str(first["name"])

    normalized_title = normalize_title(title)
    content_for_hash = f"{normalized_title}\n{summary}".strip()
    sim = simhash64(f"{title} {summary[:280]}")

    return {
        "item_id": str(uuid.uuid4()),
        "source_id": source_id,
        "source_type": "json_api",
        "external_id": report_id,
        "url": canonicalize_url(f"reliefweb:report:{report_id}"),
        "title": title,
        "summary": summary,
        "content": None,
        "published_at": published_at,
        "updated_at": None,
        "fetched_at": fetched_at,
        "category": "disaster",
        "tags": json.dumps(["reliefweb", "report"], ensure_ascii=False),
        "geom_geojson": None,
        "lat": None,
        "lon": None,
        "location_name": country,
        "location_confidence": "C_country" if country else "U_unknown",
        "location_rationale": "ReliefWeb report country"
        if country
        else "No country detected",
        "raw": json.dumps(record, ensure_ascii=False),
        "hash_title": _sha256_hex(normalized_title),
        "hash_content": _sha256_hex(content_for_hash),
        "simhash": _u64_to_i64(sim),
    }


def normalize_reliefweb_disaster(
    *, source_id: str, record: dict, fetched_at: str
) -> dict:
    disaster_id = str(record["id"])
    fields = record.get("fields") or {}

    title = str(fields.get("name") or fields.get("title") or "")
    summary = str(fields.get("description") or "")
    if len(summary) > 300:
        summary = summary[:297] + "..."

    published_at = fetched_at
    dates = fields.get("date")
    if isinstance(dates, dict) and dates.get("created"):
        published_at = str(dates["created"])

    country = None
    countries = fields.get("country") or []
    if countries:
        first = countries[0]
        if isinstance(first, dict) and first.get("name"):
            country = str(first["name"])

    normalized_title = normalize_title(title)
    content_for_hash = f"{normalized_title}\n{summary}".strip()
    sim = simhash64(f"{title} {summary[:280]}")

    return {
        "item_id": str(uuid.uuid4()),
        "source_id": source_id,
        "source_type": "json_api",
        "external_id": disaster_id,
        "url": canonicalize_url(f"reliefweb:disaster:{disaster_id}"),
        "title": title,
        "summary": summary,
        "content": None,
        "published_at": published_at,
        "updated_at": None,
        "fetched_at": fetched_at,
        "category": "disaster",
        "tags": json.dumps(["reliefweb", "disaster"], ensure_ascii=False),
        "geom_geojson": None,
        "lat": None,
        "lon": None,
        "location_name": country,
        "location_confidence": "C_country" if country else "U_unknown",
        "location_rationale": "ReliefWeb disaster country"
        if country
        else "No country detected",
        "raw": json.dumps(record, ensure_ascii=False),
        "hash_title": _sha256_hex(normalized_title),
        "hash_content": _sha256_hex(content_for_hash),
        "simhash": _u64_to_i64(sim),
    }


def normalize_msi_broadcast_warning(
    *, source_id: str, record: dict, fetched_at: str
) -> dict:
    nav_area = str(record.get("navArea") or record.get("area") or "").strip()
    msg_number = str(record.get("msgNumber") or record.get("number") or "").strip()
    msg_year = str(record.get("msgYear") or record.get("year") or "").strip()

    issue_date = str(record.get("issueDate") or "").strip().strip(".")
    published_at = fetched_at
    if issue_date:
        try:
            dt = datetime.strptime(issue_date, "%d%H%MZ %b %Y").replace(tzinfo=UTC)
            published_at = dt.isoformat().replace("+00:00", "Z")
        except ValueError:
            published_at = fetched_at

    text = str(record.get("text") or "").strip()
    title = f"NAVAREA {nav_area} {msg_number}/{msg_year}".strip()
    summary = text.replace("\n", " ").strip()
    if len(summary) > 300:
        summary = summary[:297] + "..."

    coords = extract_coords_centroid(text)
    lat = coords[0] if coords is not None else None
    lon = coords[1] if coords is not None else None
    confidence = "B_coords_in_text" if coords is not None else "U_unknown"
    rationale = (
        "Coordinates found in MSI warning text"
        if coords is not None
        else "MSI warning without coordinates"
    )

    raw = {
        "nav_area": nav_area or None,
        "msg_number": msg_number or None,
        "msg_year": msg_year or None,
        "subregion": record.get("subregion"),
        "status": record.get("status"),
        "authority": record.get("authority"),
        "issue_date": record.get("issueDate"),
    }

    tags = ["msi", "maritime_warning"]
    if nav_area:
        tags.append(f"navarea:{nav_area}")
    subregion = str(record.get("subregion") or "").strip()
    if subregion:
        tags.append(f"subregion:{subregion}")

    lower = text.casefold()
    if "distress" in lower or "adrift" in lower or "sinking" in lower:
        tags.append("distress")
        raw["is_distress"] = True
    if "hazard" in lower or "danger" in lower:
        tags.append("hazard")
        raw["is_hazard"] = True

    normalized_title = normalize_title(title)
    content_for_hash = f"{normalized_title}\n{summary}".strip()
    sim = simhash64(f"{title} {summary[:280]}")

    url = canonicalize_url(
        f"https://msi.pub.kubic.nga.mil/api/publications/broadcast-warn?output=json&navArea={nav_area}&msgNumber={msg_number}&msgYear={msg_year}"
    )
    external_id = f"{nav_area}-{msg_number}-{msg_year}".strip("-")

    return {
        "item_id": str(uuid.uuid4()),
        "source_id": source_id,
        "source_type": "json_api",
        "external_id": external_id,
        "url": url,
        "title": title,
        "summary": summary,
        "content": text or None,
        "published_at": published_at,
        "updated_at": None,
        "fetched_at": fetched_at,
        "category": "maritime_warning",
        "tags": json.dumps(tags, ensure_ascii=False),
        "geom_geojson": None,
        "lat": lat,
        "lon": lon,
        "location_name": f"NAVAREA {nav_area}".strip() if nav_area else None,
        "location_confidence": confidence,
        "location_rationale": rationale,
        "raw": json.dumps(raw, ensure_ascii=False),
        "hash_title": _sha256_hex(normalized_title),
        "hash_content": _sha256_hex(content_for_hash),
        "simhash": _u64_to_i64(sim),
    }


_HTML_TAG_RE = re.compile(r"<[^>]+>", flags=re.UNICODE)
_WS_RE = re.compile(r"\s+", flags=re.UNICODE)


def normalize_mastodon_status(
    *, source_id: str, record: dict, fetched_at: str, instance: str, tag: str
) -> dict:
    created_at = str(record.get("created_at") or fetched_at)
    url = str(record.get("url") or record.get("uri") or "")
    external_id = str(record.get("id") or url)

    content_html = str(record.get("content") or "")
    text = html.unescape(_HTML_TAG_RE.sub(" ", content_html))
    text = _WS_RE.sub(" ", text).strip()

    spoiler = str(record.get("spoiler_text") or "").strip()
    title = spoiler or text[:140] or f"Mastodon post on {instance}"
    summary = text[:300] + ("..." if len(text) > 300 else "")

    account = record.get("account") or {}
    acct = str(account.get("acct") or account.get("username") or "")

    raw = {
        "instance": instance,
        "acct": acct or None,
        "tag": tag,
        "visibility": record.get("visibility"),
        "replies_count": record.get("replies_count"),
        "reblogs_count": record.get("reblogs_count"),
        "favourites_count": record.get("favourites_count"),
    }

    tags = ["mastodon", "social", f"instance:{instance}", f"tag:{tag.lstrip('#')}"]
    if acct:
        tags.append(f"acct:{acct}")

    normalized_title = normalize_title(title)
    content_for_hash = f"{normalized_title}\n{summary}".strip()
    sim = simhash64(f"{title} {summary[:280]}")

    return {
        "item_id": str(uuid.uuid4()),
        "source_id": source_id,
        "source_type": "social",
        "external_id": external_id,
        "url": canonicalize_url(url)
        if url
        else canonicalize_url(f"mastodon:{external_id}"),
        "title": title,
        "summary": summary,
        "content": text or None,
        "published_at": created_at,
        "updated_at": None,
        "fetched_at": fetched_at,
        "category": "social",
        "tags": json.dumps(tags, ensure_ascii=False),
        "geom_geojson": None,
        "lat": None,
        "lon": None,
        "location_name": None,
        "location_confidence": "U_unknown",
        "location_rationale": "Mastodon post without structured geo",
        "raw": json.dumps(raw, ensure_ascii=False),
        "hash_title": _sha256_hex(normalized_title),
        "hash_content": _sha256_hex(content_for_hash),
        "simhash": _u64_to_i64(sim),
    }


def normalize_bluesky_post(*, source_id: str, record: dict, fetched_at: str) -> dict:
    uri = str(record.get("uri") or "")
    cid = str(record.get("cid") or "")
    author = record.get("author") or {}
    handle = str(author.get("handle") or "")

    post_record = record.get("record") or {}
    text = str(post_record.get("text") or "").strip()
    created_at = str(post_record.get("createdAt") or fetched_at)

    rkey = ""
    if "/app.bsky.feed.post/" in uri:
        rkey = uri.split("/app.bsky.feed.post/", maxsplit=1)[-1].strip()

    url = ""
    if handle and rkey:
        url = f"https://bsky.app/profile/{handle}/post/{rkey}"

    title = text[:140] or f"Bluesky post by {handle or 'unknown'}"
    summary = text[:300] + ("..." if len(text) > 300 else "")

    raw = {"uri": uri, "cid": cid, "handle": handle or None}

    normalized_title = normalize_title(title)
    content_for_hash = f"{normalized_title}\n{summary}".strip()
    sim = simhash64(f"{title} {summary[:280]}")

    return {
        "item_id": str(uuid.uuid4()),
        "source_id": source_id,
        "source_type": "social",
        "external_id": uri or cid or url,
        "url": canonicalize_url(url)
        if url
        else canonicalize_url(f"bluesky:{uri or cid}"),
        "title": title,
        "summary": summary,
        "content": text or None,
        "published_at": created_at,
        "updated_at": None,
        "fetched_at": fetched_at,
        "category": "social",
        "tags": json.dumps(["bluesky", "social"], ensure_ascii=False),
        "geom_geojson": None,
        "lat": None,
        "lon": None,
        "location_name": None,
        "location_confidence": "U_unknown",
        "location_rationale": "Bluesky post without structured geo",
        "raw": json.dumps(raw, ensure_ascii=False),
        "hash_title": _sha256_hex(normalized_title),
        "hash_content": _sha256_hex(content_for_hash),
        "simhash": _u64_to_i64(sim),
    }
