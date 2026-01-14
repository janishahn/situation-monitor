from __future__ import annotations

import hashlib
import json
import math
import re
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from store.db import Database


_TITLE_WHITESPACE_RE = re.compile(r"\s+")
_TITLE_PUNCT_RE = re.compile(r"[^\w\s]+", flags=re.UNICODE)


def normalize_title(title: str) -> str:
    normalized = title.strip().casefold()
    normalized = _TITLE_PUNCT_RE.sub(" ", normalized)
    normalized = _TITLE_WHITESPACE_RE.sub(" ", normalized).strip()
    return normalized


_TRACKING_PARAM_NAMES = {
    "fbclid",
    "gclid",
    "mc_cid",
    "mc_eid",
    "mkt_tok",
}


def canonicalize_url(url: str) -> str:
    parts = urlsplit(url)
    kept_params: list[tuple[str, str]] = []
    for key, value in parse_qsl(parts.query, keep_blank_values=True):
        key_lower = key.casefold()
        if key_lower.startswith("utm_"):
            continue
        if key_lower in _TRACKING_PARAM_NAMES:
            continue
        kept_params.append((key, value))

    return urlunsplit(
        (
            parts.scheme,
            parts.netloc.casefold(),
            parts.path,
            urlencode(kept_params, doseq=True),
            "",
        )
    )


def simhash64(text: str) -> int:
    tokens = re.findall(r"[a-z0-9]+", text.casefold())
    if not tokens:
        return 0

    weights: dict[str, int] = {}
    for token in tokens:
        weights[token] = weights.get(token, 0) + 1

    vector = [0] * 64
    for token, weight in weights.items():
        digest = hashlib.blake2b(token.encode("utf-8"), digest_size=8).digest()
        token_hash = int.from_bytes(digest, byteorder="big", signed=False)
        for bit in range(64):
            if token_hash & (1 << bit):
                vector[bit] += weight
            else:
                vector[bit] -= weight

    result = 0
    for bit, value in enumerate(vector):
        if value > 0:
            result |= 1 << bit
    return result & ((1 << 64) - 1)


def hamming_distance(a: int, b: int) -> int:
    return (a ^ b).bit_count()


def _u64_to_i64(value: int) -> int:
    if value >= (1 << 63):
        return value - (1 << 64)
    return value


def _i64_to_u64(value: int) -> int:
    return value & ((1 << 64) - 1)


def _utc_now_iso() -> str:
    return datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")


def _parse_iso(ts: str) -> datetime:
    if ts.endswith("Z"):
        return datetime.fromisoformat(ts.removesuffix("Z") + "+00:00")
    return datetime.fromisoformat(ts)


def _location_rank(conf: str) -> int:
    if conf == "A_exact":
        return 30
    if conf.startswith("B_"):
        return 20
    if conf.startswith("C_"):
        return 10
    return 0


def _token_jaccard(a: str, b: str) -> float:
    a_tokens = set(re.findall(r"[a-z0-9]+", a.casefold()))
    b_tokens = set(re.findall(r"[a-z0-9]+", b.casefold()))
    if not a_tokens or not b_tokens:
        return 0.0
    return len(a_tokens & b_tokens) / len(a_tokens | b_tokens)


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)
    a = (
        math.sin(d_phi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2) ** 2
    )
    return 2 * r * math.atan2(math.sqrt(a), math.sqrt(1 - a))


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
    elif geom_type == "LineString":
        for lon, lat in coords:
            points.append((float(lon), float(lat)))
    elif geom_type == "MultiLineString":
        for line in coords:
            for lon, lat in line:
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


def _severity_score(category: str, raw: dict) -> int:
    if category == "earthquake":
        mag = raw.get("mag")
        if isinstance(mag, (int, float)):
            return max(0, min(100, round((float(mag) - 3.0) * 20.0)))
        return 40
    if category == "weather_alert":
        severity = raw.get("severity")
        if severity == "Extreme":
            return 95
        if severity == "Severe":
            return 80
        if severity == "Moderate":
            return 55
        if severity == "Minor":
            return 35
        return 50
    if category == "tropical_cyclone":
        return 75
    if category == "travel_advisory":
        level = raw.get("advice_level")
        if level == "do_not_travel":
            return 85
        if level == "reconsider_your_need_to_travel":
            return 65
        return 50
    if category == "tsunami":
        return 90
    if category == "volcano":
        sev = raw.get("severity_level_1_5")
        if isinstance(sev, int):
            return max(0, min(100, sev * 20))
        if isinstance(sev, str) and sev.isdigit():
            return max(0, min(100, int(sev) * 20))
        return 70
    if category == "wildfire":
        frp = raw.get("frp")
        try:
            frp_f = float(frp) if frp is not None else None
        except ValueError:
            frp_f = None
        if frp_f is not None:
            return max(0, min(100, round(frp_f * 3.0)))
        return 55
    if category == "aviation_disruption":
        kind = str(raw.get("severity_kind") or "")
        if kind == "closure":
            return 90
        if kind == "ground_stop":
            return 80
        if kind == "gdp":
            return 65
        avg = raw.get("avg_delay_min")
        if isinstance(avg, int):
            return max(40, min(80, avg))
        return 50
    if category == "health_advisory":
        return 55
    if category == "cyber_kev":
        return 75
    if category == "cyber_cve":
        return 60
    if category == "disaster":
        return 60
    return 40


def _incident_summary_from_item(
    category: str, item_title: str, item_summary: str
) -> str:
    if category == "earthquake":
        return item_title
    if category == "weather_alert":
        return item_summary or item_title
    if category == "tropical_cyclone":
        return item_title
    if category == "travel_advisory":
        return item_title
    if category in {"cyber_cve", "cyber_kev"}:
        return item_title
    return item_summary or item_title


@dataclass(frozen=True)
class ClusterResult:
    incident_id: str
    event_type: str
    payload: dict


def assign_item_to_incident(db: Database, item_id: str) -> ClusterResult:
    now_iso = _utc_now_iso()
    with db.lock:
        item = db.conn.execute(
            """
            SELECT item_id, source_id, title, summary, category, published_at, updated_at, lat, lon,
                   geom_geojson, location_confidence, location_rationale, raw, simhash
            FROM items
            WHERE item_id = ?;
            """,
            (item_id,),
        ).fetchone()
        if item is None:
            raise ValueError(f"item not found: {item_id}")

        category = str(item["category"])
        item_simhash_u = _i64_to_u64(int(item["simhash"]))
        bucket = (item_simhash_u >> 48) & 0xFFFF
        lookback_hours = 24 if category == "news" else 48
        cutoff_iso = (
            (datetime.now(tz=UTC) - timedelta(hours=lookback_hours))
            .isoformat()
            .replace("+00:00", "Z")
        )

        candidates = db.conn.execute(
            """
            SELECT incident_id, title, summary, incident_simhash, lat, lon, last_seen_at, location_confidence
            FROM incidents
            WHERE category = ?
              AND last_seen_at >= ?
              AND ((incident_simhash >> 48) & 65535) = ?
            ORDER BY last_seen_at DESC
            LIMIT 200;
            """,
            (category, cutoff_iso, bucket),
        ).fetchall()

        best: sqlite3.Row | None = None
        best_distance = 10_000
        for candidate in candidates:
            dist = hamming_distance(
                item_simhash_u, _i64_to_u64(int(candidate["incident_simhash"]))
            )
            if dist < best_distance:
                best = candidate
                best_distance = dist

        if category == "news":
            match_dist = 4
            match_dist_loose = 10
            jaccard_min = 0.6
        elif category in {"earthquake", "volcano", "tsunami"}:
            match_dist = 8
            match_dist_loose = 14
            jaccard_min = 0.4
        else:
            match_dist = 6
            match_dist_loose = 12
            jaccard_min = 0.45

        matched_incident_id: str | None = None
        if best is not None and best_distance <= match_dist:
            matched_incident_id = str(best["incident_id"])
        elif best is not None and match_dist < best_distance <= match_dist_loose:
            sim = _token_jaccard(
                f"{item['title']} {item['summary']}",
                f"{best['title']} {best['summary']}",
            )
            if sim >= jaccard_min:
                matched_incident_id = str(best["incident_id"])

        item_raw = json.loads(item["raw"])
        item_score = _severity_score(category, item_raw)

        geom_geojson = item["geom_geojson"]
        item_bbox: tuple[float, float, float, float] | None = None
        if geom_geojson is not None:
            item_bbox = _bbox_from_geojson(json.loads(geom_geojson))

        if matched_incident_id is None:
            incident_id = str(uuid.uuid4())
            summary = _incident_summary_from_item(
                category, str(item["title"]), str(item["summary"])
            )
            token_sig = (
                " ".join(re.findall(r"[a-z0-9]+", summary.casefold())[:6]) or None
            )

            incident_bbox = None
            incident_lat = item["lat"]
            incident_lon = item["lon"]
            if item_bbox is not None:
                incident_bbox = ",".join(str(v) for v in item_bbox)
                incident_lat, incident_lon = _centroid_from_bbox(item_bbox)

            db.conn.execute(
                """
                INSERT INTO incidents(
                  incident_id, title, summary, category, first_seen_at, last_seen_at, last_item_at,
                  status, severity_score, geom_geojson, lat, lon, bbox,
                  location_confidence, location_rationale, incident_simhash, token_signature,
                  item_count, source_count
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 1);
                """,
                (
                    incident_id,
                    str(item["title"]),
                    summary,
                    category,
                    now_iso,
                    now_iso,
                    str(item["published_at"]),
                    "active",
                    item_score,
                    geom_geojson,
                    incident_lat,
                    incident_lon,
                    incident_bbox,
                    str(item["location_confidence"]),
                    str(item["location_rationale"]),
                    int(item["simhash"]),
                    token_sig,
                ),
            )
            db.conn.execute(
                "INSERT INTO incident_items(incident_id, item_id) VALUES(?, ?);",
                (incident_id, item_id),
            )
            db.conn.commit()

            payload = {
                "type": "incident.created",
                "incident_id": incident_id,
                "title": str(item["title"]),
                "summary": summary,
                "last_seen_at": now_iso,
                "category": category,
                "lat": incident_lat,
                "lon": incident_lon,
                "severity_score": item_score,
                "source_count": 1,
                "item_count": 1,
            }
            return ClusterResult(
                incident_id=incident_id, event_type="incident.created", payload=payload
            )

        incident_id = matched_incident_id
        db.conn.execute(
            "INSERT OR IGNORE INTO incident_items(incident_id, item_id) VALUES(?, ?);",
            (incident_id, item_id),
        )

        incident = db.conn.execute(
            """
            SELECT incident_id, title, summary, last_item_at, severity_score,
                   geom_geojson, lat, lon, bbox, location_confidence, location_rationale
            FROM incidents
            WHERE incident_id = ?;
            """,
            (incident_id,),
        ).fetchone()
        if incident is None:
            raise ValueError(f"incident not found after match: {incident_id}")

        summary = _incident_summary_from_item(
            category, str(item["title"]), str(item["summary"])
        )
        token_sig = " ".join(re.findall(r"[a-z0-9]+", summary.casefold())[:6]) or None
        incident_simhash = simhash64(f"{incident['title']} {summary}")

        existing_last_item_at = _parse_iso(str(incident["last_item_at"]))
        item_time = _parse_iso(str(item["published_at"]))
        last_item_at = (
            max(existing_last_item_at, item_time).isoformat().replace("+00:00", "Z")
        )

        geom_out = incident["geom_geojson"]
        lat_out = incident["lat"]
        lon_out = incident["lon"]
        bbox_out = incident["bbox"]
        conf_out = str(incident["location_confidence"])
        rationale_out = str(incident["location_rationale"])

        if _location_rank(str(item["location_confidence"])) > _location_rank(conf_out):
            geom_out = geom_geojson
            conf_out = str(item["location_confidence"])
            rationale_out = str(item["location_rationale"])
            lat_out = item["lat"]
            lon_out = item["lon"]
            if item_bbox is not None:
                bbox_out = ",".join(str(v) for v in item_bbox)
                lat_out, lon_out = _centroid_from_bbox(item_bbox)

        if item_bbox is not None and bbox_out is not None:
            current = tuple(float(x) for x in str(bbox_out).split(","))
            merged = (
                min(current[0], item_bbox[0]),
                min(current[1], item_bbox[1]),
                max(current[2], item_bbox[2]),
                max(current[3], item_bbox[3]),
            )
            bbox_out = ",".join(str(v) for v in merged)
            lat_out, lon_out = _centroid_from_bbox(merged)

        severity_out = max(int(incident["severity_score"]), item_score)

        db.conn.execute(
            """
            UPDATE incidents
            SET summary = ?,
                last_seen_at = ?,
                last_item_at = ?,
                severity_score = ?,
                geom_geojson = ?,
                lat = ?,
                lon = ?,
                bbox = ?,
                location_confidence = ?,
                location_rationale = ?,
                incident_simhash = ?,
                token_signature = ?
            WHERE incident_id = ?;
            """,
            (
                summary,
                now_iso,
                last_item_at,
                severity_out,
                geom_out,
                lat_out,
                lon_out,
                bbox_out,
                conf_out,
                rationale_out,
                _u64_to_i64(incident_simhash),
                token_sig,
                incident_id,
            ),
        )

        counts = db.conn.execute(
            """
            SELECT
              COUNT(*) AS item_count,
              COUNT(DISTINCT i.source_id) AS source_count
            FROM incident_items ii
            JOIN items i ON i.item_id = ii.item_id
            WHERE ii.incident_id = ?;
            """,
            (incident_id,),
        ).fetchone()
        db.conn.execute(
            "UPDATE incidents SET item_count = ?, source_count = ? WHERE incident_id = ?;",
            (int(counts["item_count"]), int(counts["source_count"]), incident_id),
        )

        if category == "wildfire":
            density_bonus = min(20, int(counts["item_count"]) // 10)
            severity_out = min(100, severity_out + density_bonus)
            db.conn.execute(
                "UPDATE incidents SET severity_score = ? WHERE incident_id = ?;",
                (severity_out, incident_id),
            )

        _maybe_merge_incidents(db, incident_id)
        db.conn.commit()

        payload = {
            "type": "incident.updated",
            "incident_id": incident_id,
            "title": str(incident["title"]),
            "summary": summary,
            "last_seen_at": now_iso,
            "category": category,
            "lat": lat_out,
            "lon": lon_out,
            "severity_score": severity_out,
            "source_count": int(counts["source_count"]),
            "item_count": int(counts["item_count"]),
        }
        return ClusterResult(
            incident_id=incident_id, event_type="incident.updated", payload=payload
        )


def _maybe_merge_incidents(db: Database, incident_id: str) -> None:
    incident = db.conn.execute(
        """
        SELECT incident_id, category, incident_simhash, lat, lon, last_seen_at
        FROM incidents
        WHERE incident_id = ?;
        """,
        (incident_id,),
    ).fetchone()
    if incident is None:
        return
    if incident["lat"] is None or incident["lon"] is None:
        return

    category = str(incident["category"])
    if category == "news":
        max_km = 40.0
        max_dist = 2
        lookback_hours = 24
    elif category in {"earthquake", "volcano"}:
        max_km = 120.0
        max_dist = 4
        lookback_hours = 72
    elif category == "wildfire":
        max_km = 50.0
        max_dist = 3
        lookback_hours = 48
    elif category == "tsunami":
        max_km = 2500.0
        max_dist = 4
        lookback_hours = 72
    elif category == "aviation_disruption":
        max_km = 30.0
        max_dist = 3
        lookback_hours = 24
    elif category == "weather_alert":
        max_km = 120.0
        max_dist = 3
        lookback_hours = 48
    elif category == "tropical_cyclone":
        max_km = 500.0
        max_dist = 3
        lookback_hours = 72
    else:
        max_km = 150.0
        max_dist = 3
        lookback_hours = 48

    cutoff_iso = (
        (datetime.now(tz=UTC) - timedelta(hours=lookback_hours))
        .isoformat()
        .replace("+00:00", "Z")
    )
    sim_u = _i64_to_u64(int(incident["incident_simhash"]))
    bucket = (sim_u >> 48) & 0xFFFF

    others = db.conn.execute(
        """
        SELECT incident_id, incident_simhash, lat, lon
        FROM incidents
        WHERE category = ?
          AND incident_id <> ?
          AND last_seen_at >= ?
          AND ((incident_simhash >> 48) & 65535) = ?
        LIMIT 50;
        """,
        (category, incident_id, cutoff_iso, bucket),
    ).fetchall()

    for other in others:
        if other["lat"] is None or other["lon"] is None:
            continue
        if (
            _haversine_km(
                float(incident["lat"]),
                float(incident["lon"]),
                float(other["lat"]),
                float(other["lon"]),
            )
            > max_km
        ):
            continue
        dist = hamming_distance(sim_u, _i64_to_u64(int(other["incident_simhash"])))
        if dist > max_dist:
            continue

        other_id = str(other["incident_id"])
        rows = db.conn.execute(
            "SELECT item_id FROM incident_items WHERE incident_id = ?;",
            (other_id,),
        ).fetchall()
        for row in rows:
            db.conn.execute(
                "INSERT OR IGNORE INTO incident_items(incident_id, item_id) VALUES(?, ?);",
                (incident_id, str(row["item_id"])),
            )
        db.conn.execute("DELETE FROM incidents WHERE incident_id = ?;", (other_id,))

        counts = db.conn.execute(
            """
            SELECT
              COUNT(*) AS item_count,
              COUNT(DISTINCT i.source_id) AS source_count
            FROM incident_items ii
            JOIN items i ON i.item_id = ii.item_id
            WHERE ii.incident_id = ?;
            """,
            (incident_id,),
        ).fetchone()
        db.conn.execute(
            "UPDATE incidents SET item_count = ?, source_count = ? WHERE incident_id = ?;",
            (int(counts["item_count"]), int(counts["source_count"]), incident_id),
        )
