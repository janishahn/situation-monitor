from __future__ import annotations

import asyncio
import json
import sqlite3
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from urllib.parse import urlsplit

import httpx

from app.settings import Settings
from cluster.clusterer import ClusterResult, assign_item_to_incident
from geo.gazetteer import (
    find_country_centroid,
    match_country_in_text,
    normalize_place_name,
)
from health.health import record_fetch_error, record_fetch_success
from ingest.fetch import cache_control_max_age_seconds, fetch
from ingest.parsers.geojson import parse_geojson
from ingest.parsers.json import parse_json_records
from ingest.parsers.rss import parse_rss
from ingest.parsers.xml import parse_xml_feed
from normalize.normalize import (
    normalize_generic_rss,
    normalize_nhc_item,
    normalize_nws_alert,
    normalize_smartraveller_export,
    normalize_smartraveller_rss,
    normalize_usgs_earthquake,
)
from realtime.bus import Event, EventBus
from store.db import Database


ParseFn = Callable[[bytes], list[dict]]
NormalizeFn = Callable[[dict, str], dict]


@dataclass(frozen=True)
class SourcePlugin:
    source_id: str
    name: str
    url: str
    source_type: str
    poll_interval_seconds: int
    parse: ParseFn
    normalize: NormalizeFn


def _utc_now_iso() -> str:
    return datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")


def phase1_sources() -> list[SourcePlugin]:
    sources: list[SourcePlugin] = [
        SourcePlugin(
            source_id="usgs_all_hour",
            name="USGS Earthquakes (All, Past Hour)",
            url="https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson",
            source_type="geojson_api",
            poll_interval_seconds=60,
            parse=parse_geojson,
            normalize=lambda r, fetched_at: normalize_usgs_earthquake(
                source_id="usgs_all_hour", record=r, fetched_at=fetched_at
            ),
        ),
        SourcePlugin(
            source_id="usgs_all_day",
            name="USGS Earthquakes (All, Past Day)",
            url="https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson",
            source_type="geojson_api",
            poll_interval_seconds=600,
            parse=parse_geojson,
            normalize=lambda r, fetched_at: normalize_usgs_earthquake(
                source_id="usgs_all_day", record=r, fetched_at=fetched_at
            ),
        ),
        SourcePlugin(
            source_id="usgs_45_hour",
            name="USGS Earthquakes (M4.5+, Past Hour)",
            url="https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/4.5_hour.geojson",
            source_type="geojson_api",
            poll_interval_seconds=60,
            parse=parse_geojson,
            normalize=lambda r, fetched_at: normalize_usgs_earthquake(
                source_id="usgs_45_hour", record=r, fetched_at=fetched_at
            ),
        ),
        SourcePlugin(
            source_id="nws_alerts_active",
            name="NWS Alerts (Active)",
            url="https://api.weather.gov/alerts/active",
            source_type="geojson_api",
            poll_interval_seconds=60,
            parse=parse_geojson,
            normalize=lambda r, fetched_at: normalize_nws_alert(
                source_id="nws_alerts_active", record=r, fetched_at=fetched_at
            ),
        ),
        SourcePlugin(
            source_id="nws_alerts_actual",
            name="NWS Alerts (Actual)",
            url="https://api.weather.gov/alerts/active?status=actual",
            source_type="geojson_api",
            poll_interval_seconds=60,
            parse=parse_geojson,
            normalize=lambda r, fetched_at: normalize_nws_alert(
                source_id="nws_alerts_actual", record=r, fetched_at=fetched_at
            ),
        ),
        SourcePlugin(
            source_id="nws_alerts_severe",
            name="NWS Alerts (Severe)",
            url="https://api.weather.gov/alerts/active?severity=Severe",
            source_type="geojson_api",
            poll_interval_seconds=60,
            parse=parse_geojson,
            normalize=lambda r, fetched_at: normalize_nws_alert(
                source_id="nws_alerts_severe", record=r, fetched_at=fetched_at
            ),
        ),
        SourcePlugin(
            source_id="nhc_gtwo",
            name="NHC Graphical Tropical Weather Outlooks",
            url="https://www.nhc.noaa.gov/gtwo.xml",
            source_type="xml_api",
            poll_interval_seconds=300,
            parse=parse_xml_feed,
            normalize=lambda r, fetched_at: normalize_nhc_item(
                source_id="nhc_gtwo", record=r, fetched_at=fetched_at
            ),
        ),
        SourcePlugin(
            source_id="nhc_index_at",
            name="NHC Atlantic Tropical Cyclones",
            url="https://www.nhc.noaa.gov/index-at.xml",
            source_type="xml_api",
            poll_interval_seconds=300,
            parse=parse_xml_feed,
            normalize=lambda r, fetched_at: normalize_nhc_item(
                source_id="nhc_index_at", record=r, fetched_at=fetched_at
            ),
        ),
        SourcePlugin(
            source_id="nhc_index_ep",
            name="NHC Eastern Pacific Tropical Cyclones",
            url="https://www.nhc.noaa.gov/index-ep.xml",
            source_type="xml_api",
            poll_interval_seconds=300,
            parse=parse_xml_feed,
            normalize=lambda r, fetched_at: normalize_nhc_item(
                source_id="nhc_index_ep", record=r, fetched_at=fetched_at
            ),
        ),
        SourcePlugin(
            source_id="nhc_index_cp",
            name="NHC Central Pacific Tropical Cyclones",
            url="https://www.nhc.noaa.gov/index-cp.xml",
            source_type="xml_api",
            poll_interval_seconds=300,
            parse=parse_xml_feed,
            normalize=lambda r, fetched_at: normalize_nhc_item(
                source_id="nhc_index_cp", record=r, fetched_at=fetched_at
            ),
        ),
        SourcePlugin(
            source_id="nhc_gis_at",
            name="NHC Atlantic GIS",
            url="https://www.nhc.noaa.gov/gis-at.xml",
            source_type="xml_api",
            poll_interval_seconds=300,
            parse=parse_xml_feed,
            normalize=lambda r, fetched_at: normalize_nhc_item(
                source_id="nhc_gis_at", record=r, fetched_at=fetched_at
            ),
        ),
        SourcePlugin(
            source_id="nhc_gis_ep",
            name="NHC Eastern Pacific GIS",
            url="https://www.nhc.noaa.gov/gis-ep.xml",
            source_type="xml_api",
            poll_interval_seconds=300,
            parse=parse_xml_feed,
            normalize=lambda r, fetched_at: normalize_nhc_item(
                source_id="nhc_gis_ep", record=r, fetched_at=fetched_at
            ),
        ),
        SourcePlugin(
            source_id="nhc_gis_cp",
            name="NHC Central Pacific GIS",
            url="https://www.nhc.noaa.gov/gis-cp.xml",
            source_type="xml_api",
            poll_interval_seconds=300,
            parse=parse_xml_feed,
            normalize=lambda r, fetched_at: normalize_nhc_item(
                source_id="nhc_gis_cp", record=r, fetched_at=fetched_at
            ),
        ),
        SourcePlugin(
            source_id="smartraveller_documents",
            name="Smartraveller Documents",
            url="https://www.smartraveller.gov.au/countries/documents/index.rss",
            source_type="rss",
            poll_interval_seconds=3600,
            parse=parse_rss,
            normalize=lambda r, fetched_at: normalize_smartraveller_rss(
                source_id="smartraveller_documents",
                record=r,
                fetched_at=fetched_at,
                advice_level="all",
            ),
        ),
        SourcePlugin(
            source_id="smartraveller_do_not_travel",
            name="Smartraveller Do Not Travel",
            url="https://www.smartraveller.gov.au/countries/documents/do-not-travel.rss",
            source_type="rss",
            poll_interval_seconds=3600,
            parse=parse_rss,
            normalize=lambda r, fetched_at: normalize_smartraveller_rss(
                source_id="smartraveller_do_not_travel",
                record=r,
                fetched_at=fetched_at,
                advice_level="do_not_travel",
            ),
        ),
        SourcePlugin(
            source_id="smartraveller_reconsider",
            name="Smartraveller Reconsider Your Need to Travel",
            url="https://www.smartraveller.gov.au/countries/documents/reconsider-your-need-to-travel.rss",
            source_type="rss",
            poll_interval_seconds=3600,
            parse=parse_rss,
            normalize=lambda r, fetched_at: normalize_smartraveller_rss(
                source_id="smartraveller_reconsider",
                record=r,
                fetched_at=fetched_at,
                advice_level="reconsider_your_need_to_travel",
            ),
        ),
        SourcePlugin(
            source_id="smartraveller_export",
            name="Smartraveller Destinations Export",
            url="https://www.smartraveller.gov.au/destinations-export",
            source_type="json_api",
            poll_interval_seconds=21600,
            parse=parse_json_records,
            normalize=lambda r, fetched_at: normalize_smartraveller_export(
                source_id="smartraveller_export", record=r, fetched_at=fetched_at
            ),
        ),
        SourcePlugin(
            source_id="bbc_front_page",
            name="BBC Front Page",
            url="http://newsrss.bbc.co.uk/rss/newsonline_uk_edition/front_page/rss.xml",
            source_type="rss",
            poll_interval_seconds=240,
            parse=parse_rss,
            normalize=lambda r, fetched_at: normalize_generic_rss(
                source_id="bbc_front_page",
                record=r,
                fetched_at=fetched_at,
                category="news",
            ),
        ),
        SourcePlugin(
            source_id="bbc_world",
            name="BBC World",
            url="http://newsrss.bbc.co.uk/rss/newsonline_uk_edition/world/rss.xml",
            source_type="rss",
            poll_interval_seconds=240,
            parse=parse_rss,
            normalize=lambda r, fetched_at: normalize_generic_rss(
                source_id="bbc_world", record=r, fetched_at=fetched_at, category="news"
            ),
        ),
        SourcePlugin(
            source_id="dw_top",
            name="DW Top News",
            url="https://rss.dw.com/rdf/rss-en-top",
            source_type="rss",
            poll_interval_seconds=240,
            parse=parse_rss,
            normalize=lambda r, fetched_at: normalize_generic_rss(
                source_id="dw_top", record=r, fetched_at=fetched_at, category="news"
            ),
        ),
        SourcePlugin(
            source_id="aljazeera_all",
            name="Al Jazeera All",
            url="https://www.aljazeera.com/xml/rss/all.xml",
            source_type="rss",
            poll_interval_seconds=240,
            parse=parse_rss,
            normalize=lambda r, fetched_at: normalize_generic_rss(
                source_id="aljazeera_all",
                record=r,
                fetched_at=fetched_at,
                category="news",
            ),
        ),
    ]
    return sources


def ensure_sources(db: Database, plugins: list[SourcePlugin]) -> None:
    now_iso = _utc_now_iso()
    with db.lock:
        for plugin in plugins:
            db.conn.execute(
                """
                INSERT OR IGNORE INTO sources(
                  source_id, name, source_type, url, poll_interval_seconds, enabled, next_fetch_at
                )
                VALUES(?, ?, ?, ?, ?, 1, ?);
                """,
                (
                    plugin.source_id,
                    plugin.name,
                    plugin.source_type,
                    plugin.url,
                    plugin.poll_interval_seconds,
                    now_iso,
                ),
            )
            db.conn.execute(
                """
                UPDATE sources
                SET name = ?,
                    source_type = ?,
                    url = ?,
                    poll_interval_seconds = ?
                WHERE source_id = ?;
                """,
                (
                    plugin.name,
                    plugin.source_type,
                    plugin.url,
                    plugin.poll_interval_seconds,
                    plugin.source_id,
                ),
            )
        db.conn.commit()


async def run_scheduler(*, settings: Settings, db: Database, bus: EventBus) -> None:
    plugins = phase1_sources()
    plugin_by_id = {p.source_id: p for p in plugins}
    ensure_sources(db, plugins)

    global_sem = asyncio.Semaphore(4)
    host_sems: dict[str, asyncio.Semaphore] = {}
    next_cleanup_at = datetime.now(tz=UTC) + timedelta(minutes=10)

    async with httpx.AsyncClient(follow_redirects=True) as client:
        while True:
            now_iso = _utc_now_iso()
            with db.lock:
                due = db.conn.execute(
                    """
                    SELECT source_id, url, etag, last_modified, poll_interval_seconds
                    FROM sources
                    WHERE enabled = 1
                      AND (next_fetch_at IS NULL OR next_fetch_at <= ?)
                    ORDER BY COALESCE(next_fetch_at, '') ASC
                    LIMIT 12;
                    """,
                    (now_iso,),
                ).fetchall()

            if not due:
                await asyncio.sleep(0.5)
            else:
                tasks = []
                for row in due:
                    source_id = str(row["source_id"])
                    plugin = plugin_by_id.get(source_id)
                    if plugin is None:
                        continue
                    host = urlsplit(plugin.url).netloc
                    host_sem = host_sems.setdefault(host, asyncio.Semaphore(1))
                    tasks.append(
                        asyncio.create_task(
                            _run_one(
                                client,
                                plugin,
                                db,
                                bus,
                                settings.user_agent,
                                global_sem,
                                host_sem,
                                str(row["etag"]) if row["etag"] is not None else None,
                                str(row["last_modified"])
                                if row["last_modified"] is not None
                                else None,
                                int(row["poll_interval_seconds"]),
                            )
                        )
                    )
                if tasks:
                    await asyncio.gather(*tasks)

            if datetime.now(tz=UTC) >= next_cleanup_at:
                _run_retention(db, settings)
                next_cleanup_at = datetime.now(tz=UTC) + timedelta(hours=1)


async def _run_one(
    client: httpx.AsyncClient,
    plugin: SourcePlugin,
    db: Database,
    bus: EventBus,
    user_agent: str,
    global_sem: asyncio.Semaphore,
    host_sem: asyncio.Semaphore,
    etag: str | None,
    last_modified: str | None,
    poll_interval_seconds: int,
) -> None:
    async with global_sem, host_sem:
        fetched_at = _utc_now_iso()
        try:
            status_code, content, headers, elapsed_ms = await fetch(
                client,
                url=plugin.url,
                user_agent=user_agent,
                etag=etag,
                last_modified=last_modified,
            )
        except httpx.TimeoutException:
            backoff = record_fetch_error(
                db,
                source_id=plugin.source_id,
                status_code=None,
                fetch_ms=None,
                error="timeout",
            )
            await bus.publish(
                Event(
                    type="source.health",
                    data={"source_id": plugin.source_id, "backoff": backoff},
                )
            )
            return
        except httpx.RequestError as e:
            backoff = record_fetch_error(
                db,
                source_id=plugin.source_id,
                status_code=None,
                fetch_ms=None,
                error=f"request_error:{e.__class__.__name__}",
            )
            await bus.publish(
                Event(
                    type="source.health",
                    data={"source_id": plugin.source_id, "backoff": backoff},
                )
            )
            return

        etag_out = headers.get("ETag")
        last_modified_out = headers.get("Last-Modified")
        cache_age = cache_control_max_age_seconds(headers.get("Cache-Control"))
        next_seconds = cache_age if cache_age is not None else poll_interval_seconds

        if status_code == 304:
            record_fetch_success(
                db,
                source_id=plugin.source_id,
                status_code=status_code,
                fetch_ms=elapsed_ms,
                etag=etag_out or etag,
                last_modified=last_modified_out or last_modified,
                next_fetch_in_seconds=next_seconds,
            )
            await bus.publish(
                Event(
                    type="source.health",
                    data={"source_id": plugin.source_id, "status": 304},
                )
            )
            return

        if status_code != 200 or content is None:
            backoff = record_fetch_error(
                db,
                source_id=plugin.source_id,
                status_code=status_code,
                fetch_ms=elapsed_ms,
                error=f"http_{status_code}",
            )
            await bus.publish(
                Event(
                    type="source.health",
                    data={
                        "source_id": plugin.source_id,
                        "status": status_code,
                        "backoff": backoff,
                    },
                )
            )
            return

        try:
            records = plugin.parse(content)
        except (ValueError, json.JSONDecodeError):
            backoff = record_fetch_error(
                db,
                source_id=plugin.source_id,
                status_code=status_code,
                fetch_ms=elapsed_ms,
                error="parse_error",
            )
            await bus.publish(
                Event(
                    type="source.health",
                    data={
                        "source_id": plugin.source_id,
                        "status": status_code,
                        "backoff": backoff,
                    },
                )
            )
            return

        record_fetch_success(
            db,
            source_id=plugin.source_id,
            status_code=status_code,
            fetch_ms=elapsed_ms,
            etag=etag_out,
            last_modified=last_modified_out,
            next_fetch_in_seconds=next_seconds,
        )

        inserted: list[str] = []
        title_cutoff = (
            (datetime.now(tz=UTC) - timedelta(hours=24))
            .isoformat()
            .replace("+00:00", "Z")
        )

        with db.lock:
            countries: list[tuple[str, str, float, float]] = []
            if plugin.source_type == "rss":
                country_rows = db.conn.execute(
                    """
                    SELECT name, normalized_name, lat, lon
                    FROM places
                    WHERE kind = 'country' AND lat IS NOT NULL AND lon IS NOT NULL;
                    """
                ).fetchall()
                countries = [
                    (
                        str(r["name"]),
                        str(r["normalized_name"]),
                        float(r["lat"]),
                        float(r["lon"]),
                    )
                    for r in country_rows
                ]

            for record in records:
                item = plugin.normalize(record, fetched_at)

                external_id = str(item.get("external_id") or "").strip() or None
                item["external_id"] = external_id

                if (
                    item["category"] == "news"
                    and item["location_confidence"] == "U_unknown"
                    and countries
                ):
                    match = match_country_in_text(
                        countries,
                        f"{item['title']} {item['summary']}",
                    )
                    if match is not None:
                        name, lat, lon = match
                        item["location_name"] = name
                        item["location_confidence"] = "C_country"
                        item["location_rationale"] = "Country mentioned in RSS text"
                        item["lat"] = lat
                        item["lon"] = lon

                if (
                    item["source_id"] == "smartraveller_export"
                    and item.get("location_name")
                    and item.get("lat") is not None
                    and item.get("lon") is not None
                ):
                    raw = json.loads(str(item["raw"]))
                    country_code = raw.get("country_code")
                    normalized_name = normalize_place_name(str(item["location_name"]))
                    existing = db.conn.execute(
                        """
                        SELECT place_id
                        FROM places
                        WHERE kind = 'country' AND normalized_name = ?
                        LIMIT 1;
                        """,
                        (normalized_name,),
                    ).fetchone()
                    if existing is None:
                        db.conn.execute(
                            """
                            INSERT INTO places(
                              name, normalized_name, kind, country_code, admin1, lat, lon, importance
                            )
                            VALUES(?, ?, 'country', ?, NULL, ?, ?, 0.6);
                            """,
                            (
                                str(item["location_name"]),
                                normalized_name,
                                country_code,
                                float(item["lat"]),
                                float(item["lon"]),
                            ),
                        )
                    else:
                        db.conn.execute(
                            """
                            UPDATE places
                            SET name = ?, country_code = COALESCE(?, country_code), lat = ?, lon = ?
                            WHERE place_id = ?;
                            """,
                            (
                                str(item["location_name"]),
                                country_code,
                                float(item["lat"]),
                                float(item["lon"]),
                                int(existing["place_id"]),
                            ),
                        )

                if (
                    item.get("location_confidence") == "C_country"
                    and item.get("lat") is None
                    and item.get("location_name")
                ):
                    centroid = find_country_centroid(db, str(item["location_name"]))
                    if centroid is not None:
                        item["lat"], item["lon"] = centroid

                exists = db.conn.execute(
                    """
                    SELECT 1
                    FROM items
                    WHERE source_id = ?
                      AND hash_title = ?
                      AND published_at >= ?
                    LIMIT 1;
                    """,
                    (item["source_id"], item["hash_title"], title_cutoff),
                ).fetchone()
                if exists is not None:
                    continue

                try:
                    db.conn.execute(
                        """
                        INSERT INTO items(
                          item_id, source_id, source_type, external_id, url, title, summary, content,
                          published_at, updated_at, fetched_at, category, tags,
                          geom_geojson, lat, lon, location_name, location_confidence, location_rationale,
                          raw, hash_title, hash_content, simhash
                        )
                        VALUES(
                          :item_id, :source_id, :source_type, :external_id, :url, :title, :summary, :content,
                          :published_at, :updated_at, :fetched_at, :category, :tags,
                          :geom_geojson, :lat, :lon, :location_name, :location_confidence, :location_rationale,
                          :raw, :hash_title, :hash_content, :simhash
                        );
                        """,
                        item,
                    )
                except sqlite3.IntegrityError:
                    continue
                inserted.append(str(item["item_id"]))

            db.conn.commit()

        for item_id in inserted:
            result: ClusterResult = assign_item_to_incident(db, item_id)
            await bus.publish(Event(type=result.event_type, data=result.payload))


def _run_retention(db: Database, settings: Settings) -> None:
    now = datetime.now(tz=UTC)
    items_cutoff = (
        (now - timedelta(days=settings.items_retention_days))
        .isoformat()
        .replace("+00:00", "Z")
    )
    incidents_cutoff = (
        (now - timedelta(days=settings.incidents_retention_days))
        .isoformat()
        .replace("+00:00", "Z")
    )
    cooling_cutoff = (now - timedelta(hours=24)).isoformat().replace("+00:00", "Z")
    resolved_cutoff = (now - timedelta(hours=72)).isoformat().replace("+00:00", "Z")

    with db.lock:
        db.conn.execute(
            "UPDATE incidents SET status = 'cooling' WHERE status = 'active' AND last_seen_at < ?;",
            (cooling_cutoff,),
        )
        db.conn.execute(
            "UPDATE incidents SET status = 'resolved' WHERE status <> 'resolved' AND last_seen_at < ?;",
            (resolved_cutoff,),
        )
        db.conn.execute(
            """
            DELETE FROM items
            WHERE published_at < ?
              AND item_id NOT IN (
                SELECT ii.item_id
                FROM incident_items ii
                JOIN incidents inc ON inc.incident_id = ii.incident_id
                WHERE inc.status IN ('active', 'cooling')
              );
            """,
            (items_cutoff,),
        )
        db.conn.execute(
            """
            DELETE FROM incidents
            WHERE status = 'resolved'
              AND last_seen_at < ?;
            """,
            (incidents_cutoff,),
        )
        db.conn.commit()
