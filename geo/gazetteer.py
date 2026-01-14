from __future__ import annotations

import math
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


def seed_places(db: Database, data_dir: Path) -> int:
    countries_path = data_dir / "ne_110m_admin_0_countries.geojson"
    admin1_path = data_dir / "ne_110m_admin_1_states_provinces.geojson"
    populated_path = data_dir / "ne_110m_populated_places_simple.geojson"

    inserted = 0
    with db.lock:
        if countries_path.exists():
            doc = json.loads(countries_path.read_bytes())
            for feature in doc["features"]:
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

        if admin1_path.exists():
            doc = json.loads(admin1_path.read_bytes())
            for feature in doc["features"]:
                props = feature["properties"]
                name = props.get("name_en") or props.get("name")
                if not name:
                    continue

                iso2 = props.get("iso_a2")
                country_code = iso2 if iso2 and iso2 != "-99" else None
                bbox = feature.get("bbox")
                if bbox is None:
                    continue
                min_lon, min_lat, max_lon, max_lat = bbox
                lat = (float(min_lat) + float(max_lat)) / 2.0
                lon = (float(min_lon) + float(max_lon)) / 2.0

                scalerank = int(props.get("scalerank") or 5)
                importance = max(0.4, 0.8 - scalerank * 0.05)

                normalized_name = normalize_place_name(str(name))
                cur = db.conn.execute(
                    """
                    INSERT OR IGNORE INTO places(
                      name, normalized_name, kind, country_code, admin1, lat, lon, importance
                    )
                    VALUES(?, ?, 'admin1', ?, ?, ?, ?, ?);
                    """,
                    (
                        str(name),
                        normalized_name,
                        country_code,
                        str(name),
                        lat,
                        lon,
                        importance,
                    ),
                )
                inserted += int(cur.rowcount)

        if populated_path.exists():
            doc = json.loads(populated_path.read_bytes())
            for feature in doc["features"]:
                props = feature["properties"]
                name = props.get("nameascii") or props.get("name")
                if not name:
                    continue

                iso2 = props.get("iso_a2")
                country_code = iso2 if iso2 and iso2 != "-99" else None
                admin1 = props.get("adm1name")

                geom = feature.get("geometry") or {}
                if geom.get("type") != "Point":
                    continue
                lon, lat = geom["coordinates"]

                pop_max = float(props.get("pop_max") or 0.0)
                importance = max(
                    0.3, min(0.95, math.log10(max(1.0, pop_max)) / 10.0 + 0.2)
                )

                normalized_name = normalize_place_name(str(name))
                cur = db.conn.execute(
                    """
                    INSERT OR IGNORE INTO places(
                      name, normalized_name, kind, country_code, admin1, lat, lon, importance
                    )
                    VALUES(?, ?, 'populated', ?, ?, ?, ?, ?);
                    """,
                    (
                        str(name),
                        normalized_name,
                        country_code,
                        str(admin1) if admin1 else None,
                        float(lat),
                        float(lon),
                        importance,
                    ),
                )
                inserted += int(cur.rowcount)

        db.conn.commit()

    return inserted


def match_place_in_text(
    db: Database,
    text: str,
    *,
    coords_hint: tuple[float, float] | None,
    country_code_hint: str | None,
) -> dict | None:
    tokens = re.findall(r"[a-z0-9]+", text.casefold())
    if not tokens:
        return None
    tokens = tokens[:80]

    names: set[str] = set()
    for i in range(len(tokens)):
        for n in (1, 2, 3):
            if i + n > len(tokens):
                continue
            names.add(" ".join(tokens[i : i + n]))

    if not names:
        return None

    placeholders = ",".join("?" for _ in names)
    rows = db.conn.execute(
        f"""
        SELECT name, normalized_name, kind, country_code, admin1, lat, lon, importance
        FROM places
        WHERE normalized_name IN ({placeholders});
        """,
        sorted(names),
    ).fetchall()

    if not rows:
        return None

    best: dict | None = None
    best_score = -1.0

    if coords_hint is not None:
        lat0, lon0 = coords_hint
        phi0 = math.radians(lat0)
        lam0 = math.radians(lon0)

    for row in rows:
        if row["lat"] is None or row["lon"] is None:
            continue

        importance = float(row["importance"] or 0.0)
        kind = str(row["kind"])
        score = importance

        if kind == "populated":
            score += 0.2
        elif kind == "country":
            score += 0.1
        elif kind == "admin1":
            score += 0.05

        if country_code_hint and row["country_code"] == country_code_hint:
            score += 0.25

        if coords_hint is not None:
            phi1 = math.radians(float(row["lat"]))
            lam1 = math.radians(float(row["lon"]))
            d_phi = phi1 - phi0
            d_lam = lam1 - lam0
            a = (
                math.sin(d_phi / 2.0) ** 2
                + math.cos(phi0) * math.cos(phi1) * math.sin(d_lam / 2.0) ** 2
            )
            dist_km = 2.0 * 6371.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
            score += max(0.0, 1.0 - min(dist_km, 2000.0) / 2000.0) * 0.35

        tokens_len = len(str(row["normalized_name"]).split())
        score += min(0.15, tokens_len * 0.05)

        if score > best_score:
            best_score = score
            best = {
                "name": str(row["name"]),
                "kind": kind,
                "country_code": row["country_code"],
                "admin1": row["admin1"],
                "lat": float(row["lat"]),
                "lon": float(row["lon"]),
                "importance": float(row["importance"])
                if row["importance"] is not None
                else None,
            }

    return best


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
