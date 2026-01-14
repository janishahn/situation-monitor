from __future__ import annotations

import asyncio
import json
import os
import sqlite3
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import urlencode, urlsplit

import httpx

from app.settings import Settings
from cluster.clusterer import ClusterResult, assign_item_to_incident
from geo.gazetteer import (
    find_country_centroid,
    match_country_in_text,
    match_place_in_text,
    normalize_place_name,
)
from geo.coords_extract import extract_coords_centroid
from geo.airports import load_airports_by_iata
from health.health import record_fetch_error, record_fetch_success
from ingest.fetch import cache_control_max_age_seconds, fetch
from ingest.feed_packs import load_feed_pack_entries
from ingest.parsers.geojson import parse_geojson
from ingest.parsers.govuk import parse_govuk_travel_advice_index
from ingest.parsers.json import parse_json_records
from ingest.parsers.csv import parse_csv_records
from ingest.parsers.atom import parse_atom_feed
from ingest.parsers.cap import parse_cap_alerts
from ingest.parsers.faa import parse_faa_airport_status
from ingest.parsers.rss import parse_rss
from ingest.parsers.xml import parse_xml_feed
from normalize.normalize import (
    normalize_cisa_kev,
    normalize_country_level_rss,
    normalize_eonet_event,
    normalize_faa_airport_disruption,
    normalize_firms_hotspot,
    normalize_gdacs_rss,
    normalize_govuk_travel_advice,
    normalize_hans_elevated_notice,
    normalize_hans_volcano_rss_item,
    normalize_mastodon_status,
    normalize_msi_broadcast_warning,
    normalize_generic_rss,
    normalize_nhc_item,
    normalize_nvd_cve,
    normalize_nws_alert,
    normalize_bluesky_post,
    normalize_reliefweb_report,
    normalize_reliefweb_disaster,
    normalize_smartraveller_export,
    normalize_smartraveller_rss,
    normalize_tsunami_atom,
    normalize_tsunami_cap,
    normalize_usgs_earthquake,
)
from realtime.bus import Event, EventBus
from store.db import Database


ParseFn = Callable[[bytes], list[dict]]
NormalizeFn = Callable[[dict, str], dict]
BuildUrlFn = Callable[[Database, str], str]


@dataclass(frozen=True)
class SourcePlugin:
    source_id: str
    name: str
    url: str
    source_type: str
    poll_interval_seconds: int
    parse: ParseFn
    normalize: NormalizeFn
    default_enabled: bool = True
    headers: dict[str, str] | None = None
    build_url: BuildUrlFn | None = None


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
    ]
    return sources


def phase2_sources(settings: Settings) -> list[SourcePlugin]:
    airports_path = (
        Path(__file__).resolve().parents[1] / "geo" / "data" / "airports.csv"
    )
    airports_by_iata = (
        load_airports_by_iata(airports_path) if airports_path.exists() else {}
    )

    nvd_headers = {"apiKey": settings.nvd_api_key} if settings.nvd_api_key else None
    nvd_base = "https://services.nvd.nist.gov/rest/json/cves/2.0"

    def nvd_build_url(db: Database, fetched_at: str) -> str:
        now = datetime.now(tz=UTC)
        start = now - timedelta(hours=1)
        with db.lock:
            row = db.conn.execute(
                "SELECT last_success_at FROM sources WHERE source_id = ?;",
                ("nvd_cves",),
            ).fetchone()
        if row is not None and row["last_success_at"]:
            ts = str(row["last_success_at"])
            if ts.endswith("Z"):
                start = datetime.fromisoformat(
                    ts.removesuffix("Z") + "+00:00"
                ).astimezone(tz=UTC)
            else:
                start = datetime.fromisoformat(ts).astimezone(tz=UTC)
            start = start - timedelta(minutes=15)

        params = {
            "lastModStartDate": start.isoformat(timespec="milliseconds").replace(
                "+00:00", "Z"
            ),
            "lastModEndDate": now.isoformat(timespec="milliseconds").replace(
                "+00:00", "Z"
            ),
            "resultsPerPage": "2000",
        }
        return f"{nvd_base}?{urlencode(params)}"

    firms_key = (settings.firms_api_key or "").strip()
    firms_enabled = bool(firms_key)
    firms_base = "https://firms.modaps.eosdis.nasa.gov/api/area/csv/"

    def firms_build_url(db: Database, fetched_at: str) -> str:
        return f"{firms_base}{firms_key}/VIIRS_SNPP_NRT/world/1"

    return [
        SourcePlugin(
            source_id="gdacs_rss",
            name="GDACS (Global Disaster Alerts)",
            url="https://www.gdacs.org/xml/rss.xml",
            source_type="rss",
            poll_interval_seconds=300,
            parse=parse_rss,
            normalize=lambda r, fetched_at: normalize_gdacs_rss(
                source_id="gdacs_rss", record=r, fetched_at=fetched_at
            ),
        ),
        SourcePlugin(
            source_id="eonet_open_events",
            name="NASA EONET (Open Events)",
            url="https://eonet.gsfc.nasa.gov/api/v3/events?status=open",
            source_type="json_api",
            poll_interval_seconds=900,
            parse=parse_json_records,
            normalize=lambda r, fetched_at: normalize_eonet_event(
                source_id="eonet_open_events", record=r, fetched_at=fetched_at
            ),
        ),
        SourcePlugin(
            source_id="hans_elevated_volcanoes",
            name="USGS HANS (Elevated Volcanoes)",
            url="https://volcanoes.usgs.gov/hans-public/api/volcano/getElevatedVolcanoes",
            source_type="json_api",
            poll_interval_seconds=300,
            parse=parse_json_records,
            normalize=lambda r, fetched_at: normalize_hans_elevated_notice(
                source_id="hans_elevated_volcanoes", record=r, fetched_at=fetched_at
            ),
        ),
        SourcePlugin(
            source_id="tsunami_ntwc_atom",
            name="Tsunami.gov NTWC (Atom)",
            url="https://tsunami.gov/events/xml/PAAQAtom.xml",
            source_type="xml_api",
            poll_interval_seconds=300,
            parse=parse_atom_feed,
            normalize=lambda r, fetched_at: normalize_tsunami_atom(
                source_id="tsunami_ntwc_atom", record=r, fetched_at=fetched_at
            ),
        ),
        SourcePlugin(
            source_id="tsunami_ntwc_cap",
            name="Tsunami.gov NTWC (CAP)",
            url="https://tsunami.gov/events/xml/PAAQCAP.xml",
            source_type="xml_api",
            poll_interval_seconds=300,
            parse=parse_cap_alerts,
            normalize=lambda r, fetched_at: normalize_tsunami_cap(
                source_id="tsunami_ntwc_cap", record=r, fetched_at=fetched_at
            ),
        ),
        SourcePlugin(
            source_id="tsunami_ptwc_atom",
            name="Tsunami.gov PTWC (Atom)",
            url="https://tsunami.gov/events/xml/PHEBAtom.xml",
            source_type="xml_api",
            poll_interval_seconds=300,
            parse=parse_atom_feed,
            normalize=lambda r, fetched_at: normalize_tsunami_atom(
                source_id="tsunami_ptwc_atom", record=r, fetched_at=fetched_at
            ),
        ),
        SourcePlugin(
            source_id="tsunami_ptwc_cap",
            name="Tsunami.gov PTWC (CAP)",
            url="https://tsunami.gov/events/xml/PHEBCAP.xml",
            source_type="xml_api",
            poll_interval_seconds=300,
            parse=parse_cap_alerts,
            normalize=lambda r, fetched_at: normalize_tsunami_cap(
                source_id="tsunami_ptwc_cap", record=r, fetched_at=fetched_at
            ),
        ),
        SourcePlugin(
            source_id="firms_hotspots",
            name="NASA FIRMS (Wildfire Hotspots)",
            url=f"{firms_base}{{FIRMS_API_KEY}}/VIIRS_SNPP_NRT/world/1",
            source_type="csv_api",
            poll_interval_seconds=900,
            default_enabled=firms_enabled,
            build_url=firms_build_url,
            parse=parse_csv_records,
            normalize=lambda r, fetched_at: normalize_firms_hotspot(
                source_id="firms_hotspots", record=r, fetched_at=fetched_at
            ),
        ),
        SourcePlugin(
            source_id="faa_airport_status",
            name="FAA NAS Status (Airport Status)",
            url="https://nasstatus.faa.gov/api/airport-status-information",
            source_type="xml_api",
            poll_interval_seconds=180,
            parse=parse_faa_airport_status,
            normalize=lambda r, fetched_at: normalize_faa_airport_disruption(
                source_id="faa_airport_status",
                record=r,
                fetched_at=fetched_at,
                airports_by_iata=airports_by_iata,
            ),
        ),
        SourcePlugin(
            source_id="cdc_travel_notices",
            name="CDC Travel Health Notices",
            url="https://wwwnc.cdc.gov/travel/rss/notices.xml",
            source_type="rss",
            poll_interval_seconds=3600,
            parse=parse_rss,
            normalize=lambda r, fetched_at: normalize_country_level_rss(
                source_id="cdc_travel_notices",
                record=r,
                fetched_at=fetched_at,
                category="health_advisory",
                tags=["cdc", "health_advisory"],
            ),
        ),
        SourcePlugin(
            source_id="who_afro_emergencies",
            name="WHO AFRO Emergencies",
            url="https://www.afro.who.int/rss/emergencies.xml",
            source_type="rss",
            poll_interval_seconds=3600,
            parse=parse_rss,
            normalize=lambda r, fetched_at: normalize_country_level_rss(
                source_id="who_afro_emergencies",
                record=r,
                fetched_at=fetched_at,
                category="health_advisory",
                tags=["who", "health_advisory"],
            ),
        ),
        SourcePlugin(
            source_id="nvd_cves",
            name="NVD CVE API (Recent Changes)",
            url=nvd_base,
            source_type="json_api",
            poll_interval_seconds=900,
            headers=nvd_headers,
            build_url=nvd_build_url,
            parse=parse_json_records,
            normalize=lambda r, fetched_at: normalize_nvd_cve(
                source_id="nvd_cves", record=r, fetched_at=fetched_at
            ),
        ),
        SourcePlugin(
            source_id="cisa_kev",
            name="CISA Known Exploited Vulnerabilities (KEV)",
            url="https://www.cisa.gov/sites/default/files/feeds/known_exploited_vulnerabilities.json",
            source_type="json_api",
            poll_interval_seconds=21600,
            parse=parse_json_records,
            normalize=lambda r, fetched_at: normalize_cisa_kev(
                source_id="cisa_kev", record=r, fetched_at=fetched_at
            ),
        ),
        SourcePlugin(
            source_id="travel_canada_updates",
            name="Canada Travel Updates",
            url="https://travel.gc.ca/feeds/rss/eng/travel-updates-24.aspx",
            source_type="rss",
            poll_interval_seconds=3600,
            parse=parse_rss,
            normalize=lambda r, fetched_at: normalize_country_level_rss(
                source_id="travel_canada_updates",
                record=r,
                fetched_at=fetched_at,
                category="travel_advisory",
                tags=["canada", "travel_advisory"],
            ),
        ),
        SourcePlugin(
            source_id="travel_us_state",
            name="US State Dept Travel Advisories",
            url="https://travel.state.gov/_res/rss/TAs.xml",
            source_type="rss",
            poll_interval_seconds=3600,
            parse=parse_rss,
            normalize=lambda r, fetched_at: normalize_country_level_rss(
                source_id="travel_us_state",
                record=r,
                fetched_at=fetched_at,
                category="travel_advisory",
                tags=["us_state", "travel_advisory"],
            ),
        ),
        SourcePlugin(
            source_id="govuk_travel_advice",
            name="GOV.UK Foreign Travel Advice (Index)",
            url="https://www.gov.uk/api/content/foreign-travel-advice",
            source_type="json_api",
            poll_interval_seconds=14400,
            parse=parse_govuk_travel_advice_index,
            normalize=lambda r, fetched_at: normalize_govuk_travel_advice(
                source_id="govuk_travel_advice", record=r, fetched_at=fetched_at
            ),
        ),
        SourcePlugin(
            source_id="reliefweb_reports",
            name="ReliefWeb Reports",
            url="https://api.reliefweb.int/v1/reports?appname=situation-monitor&limit=50&preset=latest",
            source_type="json_api",
            poll_interval_seconds=1800,
            parse=parse_json_records,
            normalize=lambda r, fetched_at: normalize_reliefweb_report(
                source_id="reliefweb_reports", record=r, fetched_at=fetched_at
            ),
        ),
        SourcePlugin(
            source_id="reliefweb_disasters",
            name="ReliefWeb Disasters",
            url="https://api.reliefweb.int/v1/disasters?appname=situation-monitor&limit=50&preset=latest",
            source_type="json_api",
            poll_interval_seconds=1800,
            parse=parse_json_records,
            normalize=lambda r, fetched_at: normalize_reliefweb_disaster(
                source_id="reliefweb_disasters", record=r, fetched_at=fetched_at
            ),
        ),
    ]


def phase3_sources(settings: Settings) -> list[SourcePlugin]:
    def msi_build_url(db: Database, fetched_at: str) -> str:
        base_url = "https://msi.pub.kubic.nga.mil"
        with db.lock:
            row = db.conn.execute(
                "SELECT value FROM app_config WHERE key = 'msi_api_base_url' LIMIT 1;"
            ).fetchone()
        if row is not None:
            base_url = str(row["value"]).rstrip("/")
        return f"{base_url}/api/publications/broadcast-warn?output=json&status=current"

    sources: list[SourcePlugin] = [
        SourcePlugin(
            source_id="msi_navwarn_current",
            name="NGA MSI Broadcast Warnings (Current)",
            url="https://msi.pub.kubic.nga.mil/api/publications/broadcast-warn?output=json&status=current",
            source_type="json_api",
            poll_interval_seconds=900,
            build_url=msi_build_url,
            parse=parse_json_records,
            normalize=lambda r, fetched_at: normalize_msi_broadcast_warning(
                source_id="msi_navwarn_current", record=r, fetched_at=fetched_at
            ),
        )
    ]

    for subreddit in (
        "worldnews",
        "geopolitics",
        "Cybersecurity",
        "osint",
        "news",
    ):
        sources.append(
            SourcePlugin(
                source_id=f"reddit_{subreddit.casefold()}",
                name=f"Reddit RSS /r/{subreddit}",
                url=f"https://www.reddit.com/r/{subreddit}/.rss",
                source_type="rss",
                poll_interval_seconds=240,
                headers={"User-Agent": f"{settings.user_agent} (reddit rss)"},
                parse=parse_rss,
                normalize=lambda r,
                fetched_at,
                subreddit=subreddit: normalize_generic_rss(
                    source_id=f"reddit_{subreddit.casefold()}",
                    record=r,
                    fetched_at=fetched_at,
                    category="social",
                    tags=["reddit", f"r:{subreddit.casefold()}"],
                ),
            )
        )

    instances = [p.strip() for p in settings.mastodon_instances.split(",") if p.strip()]
    tags = [t.strip() for t in settings.mastodon_tags.split(",") if t.strip()]
    for instance in instances:
        token_key = "MASTODON_TOKEN_" + instance.upper().replace(".", "_").replace(
            "-", "_"
        ).replace(":", "_")
        token = os.environ.get(token_key)
        headers = {"Authorization": f"Bearer {token}"} if token else None

        for tag in tags:
            tag_slug = tag.lstrip("#").casefold()
            source_id = f"mastodon_{instance.casefold().replace('.', '_').replace('-', '_')}_{tag_slug}"

            def build_url(
                db: Database,
                fetched_at: str,
                *,
                instance=instance,
                tag_slug=tag_slug,
                source_id=source_id,
            ) -> str:
                base = f"https://{instance}/api/v1/timelines/tag/{tag_slug}"
                params: dict[str, str] = {"limit": "20"}
                with db.lock:
                    row = db.conn.execute(
                        "SELECT cursor FROM sources WHERE source_id = ? LIMIT 1;",
                        (source_id,),
                    ).fetchone()
                if row is not None and row["cursor"]:
                    params["since_id"] = str(row["cursor"])
                return f"{base}?{urlencode(params)}"

            sources.append(
                SourcePlugin(
                    source_id=source_id,
                    name=f"Mastodon #{tag_slug} ({instance})",
                    url=f"https://{instance}/api/v1/timelines/tag/{tag_slug}?limit=20",
                    source_type="social",
                    poll_interval_seconds=180,
                    headers=headers,
                    build_url=build_url,
                    parse=parse_json_records,
                    normalize=lambda r,
                    fetched_at,
                    instance=instance,
                    tag=tag,
                    source_id=source_id: normalize_mastodon_status(
                        source_id=source_id,
                        record=r,
                        fetched_at=fetched_at,
                        instance=instance,
                        tag=tag,
                    ),
                    default_enabled=False,
                )
            )

    if settings.bluesky_handle and settings.bluesky_app_password:
        sources.append(
            SourcePlugin(
                source_id="bluesky_search_breaking",
                name="Bluesky Search (breaking)",
                url="https://bsky.social/xrpc/app.bsky.feed.searchPosts?q=breaking&limit=30",
                source_type="social",
                poll_interval_seconds=300,
                parse=parse_json_records,
                normalize=lambda r, fetched_at: normalize_bluesky_post(
                    source_id="bluesky_search_breaking", record=r, fetched_at=fetched_at
                ),
                default_enabled=False,
            )
        )

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
                VALUES(?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    plugin.source_id,
                    plugin.name,
                    plugin.source_type,
                    plugin.url,
                    plugin.poll_interval_seconds,
                    1 if plugin.default_enabled else 0,
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


def feed_pack_sources(feeds_dir: Path) -> list[SourcePlugin]:
    packs = load_feed_pack_entries(feeds_dir)
    sources: list[SourcePlugin] = []
    for pack in packs.values():
        for entry in pack:
            if entry.source_type != "rss":
                continue
            sources.append(
                SourcePlugin(
                    source_id=entry.source_id,
                    name=entry.name,
                    url=entry.url,
                    source_type="rss",
                    poll_interval_seconds=entry.poll_seconds,
                    default_enabled=entry.enabled,
                    parse=parse_rss,
                    normalize=lambda r, fetched_at, entry=entry: normalize_generic_rss(
                        source_id=entry.source_id,
                        record=r,
                        fetched_at=fetched_at,
                        category="news",
                        tags=entry.tags,
                    ),
                )
            )
    return sources


async def _ensure_msi_openapi(
    client: httpx.AsyncClient, db: Database, user_agent: str
) -> None:
    with db.lock:
        row = db.conn.execute(
            "SELECT value FROM app_config WHERE key = 'msi_openapi_url' LIMIT 1;"
        ).fetchone()
        if row is not None:
            return

    candidates = [
        "https://msi.nga.mil/v2/api-docs",
        "https://msi.nga.mil/v3/api-docs",
        "https://msi.nga.mil/openapi.json",
        "https://msi.pub.kubic.nga.mil/v2/api-docs",
        "https://msi.pub.kubic.nga.mil/v3/api-docs",
        "https://msi.pub.kubic.nga.mil/openapi.json",
    ]

    for url in candidates:
        try:
            res = await client.get(
                url,
                headers={"User-Agent": user_agent, "Accept": "application/json"},
                timeout=httpx.Timeout(connect=5.0, read=15.0, write=5.0, pool=5.0),
            )
        except httpx.TimeoutException:
            continue
        except httpx.RequestError:
            continue

        if res.status_code != 200:
            continue

        try:
            spec = res.json()
        except json.JSONDecodeError:
            continue

        base_url = None
        if spec.get("swagger") == "2.0":
            host = str(spec.get("host") or "").strip()
            base_path = str(spec.get("basePath") or "").strip()
            if host:
                base_url = f"https://{host}{base_path}".rstrip("/")
        elif spec.get("openapi"):
            servers = spec.get("servers") or []
            if servers and isinstance(servers, list) and isinstance(servers[0], dict):
                base_url = str(servers[0].get("url") or "").rstrip("/")

        if not base_url:
            continue

        now_iso = _utc_now_iso()
        with db.lock:
            db.conn.execute(
                """
                INSERT INTO app_config(key, value)
                VALUES('msi_openapi_url', ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value;
                """,
                (url,),
            )
            db.conn.execute(
                """
                INSERT INTO app_config(key, value)
                VALUES('msi_api_base_url', ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value;
                """,
                (base_url,),
            )
            db.conn.execute(
                """
                INSERT INTO app_config(key, value)
                VALUES('msi_openapi_fetched_at', ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value;
                """,
                (now_iso,),
            )
            db.conn.commit()
        return


async def run_scheduler(*, settings: Settings, db: Database, bus: EventBus) -> None:
    feeds_dir = Path(__file__).resolve().parents[1] / "feeds"
    plugins = (
        phase1_sources()
        + phase2_sources(settings)
        + phase3_sources(settings)
        + feed_pack_sources(feeds_dir)
    )
    plugin_by_id = {p.source_id: p for p in plugins}
    ensure_sources(db, plugins)

    global_sem = asyncio.Semaphore(4)
    host_sems: dict[str, asyncio.Semaphore] = {}
    plugins_lock = asyncio.Lock()
    next_cleanup_at = datetime.now(tz=UTC) + timedelta(minutes=10)

    async with httpx.AsyncClient(follow_redirects=True) as client:
        await _ensure_msi_openapi(client, db, settings.user_agent)
        while True:
            now_iso = _utc_now_iso()
            polling_enabled = True
            with db.lock:
                row = db.conn.execute(
                    "SELECT value FROM app_config WHERE key = 'polling_enabled' LIMIT 1;"
                ).fetchone()
                if row is not None and str(row["value"]) == "0":
                    polling_enabled = False

            due = []
            if polling_enabled:
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
                                plugin_by_id,
                                plugins_lock,
                                settings,
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
    plugin_by_id: dict[str, SourcePlugin],
    plugins_lock: asyncio.Lock,
    settings: Settings,
    global_sem: asyncio.Semaphore,
    host_sem: asyncio.Semaphore,
    etag: str | None,
    last_modified: str | None,
    poll_interval_seconds: int,
) -> None:
    async with global_sem, host_sem:
        fetched_at = _utc_now_iso()
        url = plugin.build_url(db, fetched_at) if plugin.build_url else plugin.url
        user_agent = settings.user_agent
        extra_headers = plugin.headers

        if (
            plugin.source_id.startswith("bluesky_")
            and settings.bluesky_handle
            and settings.bluesky_app_password
        ):
            try:
                res = await client.post(
                    "https://bsky.social/xrpc/com.atproto.server.createSession",
                    json={
                        "identifier": settings.bluesky_handle,
                        "password": settings.bluesky_app_password,
                    },
                    headers={"User-Agent": user_agent, "Accept": "application/json"},
                    timeout=httpx.Timeout(connect=5.0, read=15.0, write=5.0, pool=5.0),
                )
            except httpx.TimeoutException:
                backoff = record_fetch_error(
                    db,
                    source_id=plugin.source_id,
                    status_code=None,
                    fetch_ms=None,
                    error="bluesky_auth_timeout",
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
                    error=f"bluesky_auth_error:{e.__class__.__name__}",
                )
                await bus.publish(
                    Event(
                        type="source.health",
                        data={"source_id": plugin.source_id, "backoff": backoff},
                    )
                )
                return

            if res.status_code != 200:
                backoff = record_fetch_error(
                    db,
                    source_id=plugin.source_id,
                    status_code=res.status_code,
                    fetch_ms=None,
                    error=f"bluesky_auth_http_{res.status_code}",
                )
                await bus.publish(
                    Event(
                        type="source.health",
                        data={
                            "source_id": plugin.source_id,
                            "status": res.status_code,
                            "backoff": backoff,
                        },
                    )
                )
                return

            try:
                session = res.json()
            except json.JSONDecodeError:
                backoff = record_fetch_error(
                    db,
                    source_id=plugin.source_id,
                    status_code=res.status_code,
                    fetch_ms=None,
                    error="bluesky_auth_parse_error",
                )
                await bus.publish(
                    Event(
                        type="source.health",
                        data={
                            "source_id": plugin.source_id,
                            "status": res.status_code,
                            "backoff": backoff,
                        },
                    )
                )
                return

            token = str(session.get("accessJwt") or "").strip()
            if token:
                extra_headers = {
                    **(plugin.headers or {}),
                    "Authorization": f"Bearer {token}",
                }
        try:
            status_code, content, headers, elapsed_ms = await fetch(
                client,
                url=url,
                user_agent=user_agent,
                etag=etag,
                last_modified=last_modified,
                extra_headers=extra_headers,
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
            if status_code == 429:
                backoff = record_fetch_error(
                    db,
                    source_id=plugin.source_id,
                    status_code=status_code,
                    fetch_ms=elapsed_ms,
                    error="http_429",
                )
                retry_after = headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    retry_seconds = int(retry_after)
                    if retry_seconds > backoff:
                        next_iso = (
                            (datetime.now(tz=UTC) + timedelta(seconds=retry_seconds))
                            .isoformat()
                            .replace("+00:00", "Z")
                        )
                        with db.lock:
                            db.conn.execute(
                                "UPDATE sources SET next_fetch_at = ? WHERE source_id = ?;",
                                (next_iso, plugin.source_id),
                            )
                            db.conn.commit()
                        backoff = retry_seconds

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

        mastodon_cursor_out = None
        if plugin.source_id.startswith("mastodon_") and records:
            mastodon_cursor_out = str(max(int(r["id"]) for r in records))

        if plugin.source_id == "hans_elevated_volcanoes":
            volcanoes: dict[str, str] = {}
            for record in records:
                vnum = str(record.get("vnum") or "").strip()
                if not vnum:
                    continue
                volcanoes[vnum] = (
                    str(record.get("volcano_name") or vnum).strip() or vnum
                )

            if volcanoes:
                new_plugins: list[SourcePlugin] = []
                current_ids = {f"hans_volcano_{vnum}" for vnum in volcanoes}
                async with plugins_lock:
                    for vnum, name in volcanoes.items():
                        source_id = f"hans_volcano_{vnum}"
                        if source_id in plugin_by_id:
                            continue
                        new_plugins.append(
                            SourcePlugin(
                                source_id=source_id,
                                name=f"USGS HANS Volcano ({name})",
                                url=f"https://volcanoes.usgs.gov/hans-public/rss/cap/volcano/{vnum}",
                                source_type="xml_api",
                                poll_interval_seconds=600,
                                parse=parse_xml_feed,
                                normalize=lambda r,
                                fetched_at,
                                vnum=vnum,
                                name=name,
                                source_id=source_id: normalize_hans_volcano_rss_item(
                                    source_id=source_id,
                                    record=r,
                                    fetched_at=fetched_at,
                                    volcano_name=name,
                                    vnum=vnum,
                                ),
                            )
                        )
                    for added in new_plugins:
                        plugin_by_id[added.source_id] = added

                ensure_sources(db, new_plugins)

                with db.lock:
                    rows = db.conn.execute(
                        """
                        SELECT source_id
                        FROM sources
                        WHERE source_id LIKE 'hans_volcano_%';
                        """
                    ).fetchall()
                    existing_ids = {str(r["source_id"]) for r in rows}
                    to_disable = sorted(existing_ids - current_ids)
                    if to_disable:
                        placeholders = ",".join("?" for _ in to_disable)
                        db.conn.execute(
                            f"UPDATE sources SET enabled = 0 WHERE source_id IN ({placeholders});",
                            to_disable,
                        )
                    to_enable = sorted(current_ids)
                    if to_enable:
                        placeholders = ",".join("?" for _ in to_enable)
                        db.conn.execute(
                            f"UPDATE sources SET enabled = 1 WHERE source_id IN ({placeholders});",
                            to_enable,
                        )
                    db.conn.commit()
            else:
                with db.lock:
                    db.conn.execute(
                        "UPDATE sources SET enabled = 0 WHERE source_id LIKE 'hans_volcano_%';"
                    )
                    db.conn.commit()

        if plugin.source_id.startswith("tsunami_"):
            next_seconds = 90 if records else 300

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
                    item["category"] in {"news", "social", "maritime_warning"}
                    and item.get("geom_geojson") is None
                    and item.get("lat") is None
                    and item.get("lon") is None
                ):
                    text_for_geo = f"{item['title']} {item['summary']} {item.get('content') or ''}".strip()
                    coords_hint = extract_coords_centroid(text_for_geo)
                    country_match = (
                        match_country_in_text(countries, text_for_geo)
                        if countries
                        else None
                    )
                    country_code_hint = None
                    if country_match is not None:
                        country_norm = normalize_place_name(country_match[0])
                        row = db.conn.execute(
                            """
                            SELECT country_code
                            FROM places
                            WHERE kind = 'country' AND normalized_name = ?
                            LIMIT 1;
                            """,
                            (country_norm,),
                        ).fetchone()
                        if row is not None and row["country_code"]:
                            country_code_hint = str(row["country_code"])

                    place = match_place_in_text(
                        db,
                        text_for_geo,
                        coords_hint=coords_hint,
                        country_code_hint=country_code_hint,
                    )

                    conf = str(item.get("location_confidence") or "U_unknown")
                    if conf == "U_unknown" or conf.startswith("C_"):
                        if coords_hint is not None:
                            item["lat"], item["lon"] = coords_hint
                            item["location_confidence"] = "B_coords_in_text"
                            item["location_rationale"] = "Coordinates found in text"
                            if place is not None and not item.get("location_name"):
                                item["location_name"] = str(place["name"])
                        elif place is not None:
                            item["lat"] = float(place["lat"])
                            item["lon"] = float(place["lon"])
                            item["location_name"] = str(place["name"])
                            item["location_confidence"] = "B_place_match"
                            item["location_rationale"] = (
                                f"Gazetteer match: {place['name']}"
                            )
                        elif country_match is not None and conf == "U_unknown":
                            name, lat, lon = country_match
                            item["location_name"] = name
                            item["location_confidence"] = "C_country"
                            item["location_rationale"] = "Country detected in text"
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

                exists = None
                if item["category"] == "news" and item.get("external_id"):
                    exists = db.conn.execute(
                        """
                        SELECT 1
                        FROM items
                        WHERE source_id = ?
                          AND external_id = ?
                        LIMIT 1;
                        """,
                        (item["source_id"], item["external_id"]),
                    ).fetchone()
                else:
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

            if mastodon_cursor_out is not None:
                db.conn.execute(
                    "UPDATE sources SET cursor = ? WHERE source_id = ?;",
                    (mastodon_cursor_out, plugin.source_id),
                )

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
