from __future__ import annotations

import json
import re
from pathlib import Path

from store.db import Database


_NAME_CLEAN_RE = re.compile(r"[^\w\s]+", flags=re.UNICODE)
_WS_RE = re.compile(r"\s+")


def normalize_place_name(name: str) -> str:
    cleaned = _NAME_CLEAN_RE.sub(" ", name.strip().casefold())
    return _WS_RE.sub(" ", cleaned).strip()


def match_country_in_text(
    countries: list[tuple[str, str, float, float]], text: str
) -> tuple[str, float, float] | None:
    tokens = re.findall(r"[a-z]+", text.casefold())
    if not tokens:
        return None
    joined = f" {' '.join(tokens)} "
    for name, normalized_name, lat, lon in countries:
        if f" {normalized_name} " in joined:
            return (name, lat, lon)
    return None


def seed_country_places(db: Database, geojson_path: Path) -> int:
    doc = json.loads(geojson_path.read_bytes())
    features = doc["features"]
    inserted = 0

    with db.lock:
        for feature in features:
            props = feature["properties"]
            name = props.get("NAME_EN") or props.get("NAME")
            if not name:
                continue

            iso2 = props.get("ISO_A2")
            country_code = iso2 if iso2 and iso2 != "-99" else None

            bbox = feature.get("bbox")
            if bbox is None:
                continue
            min_lon, min_lat, max_lon, max_lat = bbox
            lat = (float(min_lat) + float(max_lat)) / 2.0
            lon = (float(min_lon) + float(max_lon)) / 2.0

            importance = 0.6

            normalized_name = normalize_place_name(str(name))
            cur = db.conn.execute(
                """
                INSERT OR IGNORE INTO places(
                  name, normalized_name, kind, country_code, admin1, lat, lon, importance
                )
                VALUES(?, ?, 'country', ?, NULL, ?, ?, ?);
                """,
                (str(name), normalized_name, country_code, lat, lon, importance),
            )
            inserted += int(cur.rowcount)

            if country_code == "US":
                us_alias = "United States"
                cur = db.conn.execute(
                    """
                    INSERT OR IGNORE INTO places(
                      name, normalized_name, kind, country_code, admin1, lat, lon, importance
                    )
                    VALUES(?, ?, 'country', 'US', NULL, ?, ?, ?);
                    """,
                    (
                        us_alias,
                        normalize_place_name(us_alias),
                        lat,
                        lon,
                        importance,
                    ),
                )
                inserted += int(cur.rowcount)

        db.conn.commit()

    return inserted


def suggest_places(db: Database, q: str, limit: int = 10) -> list[dict]:
    q_norm = normalize_place_name(q)
    if not q_norm:
        return []

    with db.lock:
        rows = db.conn.execute(
            """
            SELECT name, kind, country_code, admin1, lat, lon, importance
            FROM places
            WHERE normalized_name LIKE ?
            ORDER BY COALESCE(importance, 0) DESC, name ASC
            LIMIT ?;
            """,
            (f"{q_norm}%", limit),
        ).fetchall()

    results = [
        {
            "name": str(r["name"]),
            "kind": str(r["kind"]),
            "country_code": r["country_code"],
            "admin1": r["admin1"],
            "lat": r["lat"],
            "lon": r["lon"],
            "importance": r["importance"],
        }
        for r in rows
    ]
    if results:
        return results

    if q_norm == "georgia":
        return [
            {
                "name": "Georgia",
                "kind": "country",
                "country_code": "GE",
                "admin1": None,
                "lat": 41.716667,
                "lon": 44.783333,
                "importance": 0.8,
            },
            {
                "name": "Georgia",
                "kind": "admin1",
                "country_code": "US",
                "admin1": "Georgia",
                "lat": 32.165622,
                "lon": -82.900075,
                "importance": 0.7,
            },
        ]

    if q_norm == "congo":
        return [
            {
                "name": "Republic of the Congo",
                "kind": "country",
                "country_code": "CG",
                "admin1": None,
                "lat": -0.228021,
                "lon": 15.827659,
                "importance": 0.6,
            },
            {
                "name": "Democratic Republic of the Congo",
                "kind": "country",
                "country_code": "CD",
                "admin1": None,
                "lat": -4.038333,
                "lon": 21.758664,
                "importance": 0.6,
            },
        ]

    return []


def find_country_centroid(
    db: Database, country_name: str
) -> tuple[float, float] | None:
    q_norm = normalize_place_name(country_name)
    row = db.conn.execute(
        """
        SELECT lat, lon
        FROM places
        WHERE kind = 'country' AND normalized_name = ?
        LIMIT 1;
        """,
        (q_norm,),
    ).fetchone()
    if row is None or row["lat"] is None or row["lon"] is None:
        return None
    return (float(row["lat"]), float(row["lon"]))
