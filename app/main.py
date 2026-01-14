from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager, suppress
from datetime import UTC, datetime, timedelta
from pathlib import Path

from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.settings import Settings
from geo.gazetteer import seed_country_places, suggest_places
from ingest.scheduler import run_scheduler
from realtime.bus import EventBus
from realtime.sse import router as sse_router
from store.db import Database, open_database


def _parse_iso(ts: str) -> datetime:
    if ts.endswith("Z"):
        return datetime.fromisoformat(ts.removesuffix("Z") + "+00:00")
    return datetime.fromisoformat(ts)


def _utc_now() -> datetime:
    return datetime.now(tz=UTC)


def _utc_now_iso() -> str:
    return _utc_now().isoformat().replace("+00:00", "Z")


def _time_window_to_since(window: str, until: datetime) -> datetime:
    if window == "1h":
        return until - timedelta(hours=1)
    if window == "6h":
        return until - timedelta(hours=6)
    if window == "24h":
        return until - timedelta(hours=24)
    if window == "7d":
        return until - timedelta(days=7)
    return until - timedelta(hours=24)


def _split_csv(value: str | None) -> list[str]:
    if not value:
        return []
    return [part.strip() for part in value.split(",") if part.strip()]


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = Settings()
    db = open_database(settings.db_path)
    bus = EventBus()
    app.state.settings = settings
    app.state.db = db
    app.state.bus = bus
    seed_country_places(
        db,
        Path(__file__).resolve().parents[1]
        / "geo"
        / "data"
        / "ne_110m_admin_0_countries.geojson",
    )

    scheduler_task = asyncio.create_task(
        run_scheduler(settings=settings, db=db, bus=bus)
    )
    try:
        yield
    finally:
        scheduler_task.cancel()
        with suppress(asyncio.CancelledError):
            await scheduler_task
        with db.lock:
            db.conn.close()


app = FastAPI(lifespan=lifespan)
app.include_router(sse_router)

BASE_DIR = Path(__file__).resolve().parent
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


def _format_eu_datetime(value: str) -> str:
    dt = _parse_iso(value).astimezone(tz=UTC)
    return dt.strftime("%d.%m.%Y %H:%M")


templates.env.filters["eu_datetime"] = _format_eu_datetime


@app.get("/", response_class=HTMLResponse)
def index(request: Request) -> HTMLResponse:
    settings: Settings = request.app.state.settings
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={
            "map_tile_url": settings.map_tile_url,
            "now_iso": _utc_now_iso(),
        },
    )


def _query_incidents(
    db: Database,
    *,
    since_iso: str,
    until_iso: str,
    categories: list[str],
    bbox: tuple[float, float, float, float] | None,
    q: str | None,
    min_severity: int | None,
    limit: int = 300,
) -> list[dict]:
    where = ["last_seen_at >= ?", "last_seen_at <= ?"]
    params: list[object] = [since_iso, until_iso]

    if categories:
        where.append(f"category IN ({','.join('?' for _ in categories)})")
        params.extend(categories)

    if min_severity is not None:
        where.append("severity_score >= ?")
        params.append(min_severity)

    if bbox is not None:
        minlon, minlat, maxlon, maxlat = bbox
        where.append("lon IS NOT NULL AND lat IS NOT NULL")
        where.append("lon >= ? AND lon <= ? AND lat >= ? AND lat <= ?")
        params.extend([minlon, maxlon, minlat, maxlat])

    joins = ""
    if q:
        joins = "JOIN incidents_fts fts ON fts.rowid = incidents.rowid"
        where.append("fts MATCH ?")
        params.append(q)

    sql = f"""
        SELECT incident_id, title, summary, category, first_seen_at, last_seen_at, last_item_at,
               status, severity_score, lat, lon, bbox, location_confidence, location_rationale,
               source_count, item_count, geom_geojson
        FROM incidents
        {joins}
        WHERE {" AND ".join(where)}
        ORDER BY last_seen_at DESC, source_count DESC, severity_score DESC
        LIMIT ?;
    """
    params.append(limit)

    with db.lock:
        rows = db.conn.execute(sql, params).fetchall()

    return [
        {
            "incident_id": str(r["incident_id"]),
            "title": str(r["title"]),
            "summary": str(r["summary"]),
            "category": str(r["category"]),
            "first_seen_at": str(r["first_seen_at"]),
            "last_seen_at": str(r["last_seen_at"]),
            "last_item_at": str(r["last_item_at"]),
            "status": str(r["status"]),
            "severity_score": int(r["severity_score"]),
            "lat": r["lat"],
            "lon": r["lon"],
            "bbox": r["bbox"],
            "location_confidence": str(r["location_confidence"]),
            "location_rationale": str(r["location_rationale"]),
            "source_count": int(r["source_count"]),
            "item_count": int(r["item_count"]),
            "geom_geojson": r["geom_geojson"],
        }
        for r in rows
    ]


@app.get("/api/incidents")
def api_incidents(
    request: Request,
    since: str | None = None,
    until: str | None = None,
    window: str = Query(default="24h", pattern="^(1h|6h|24h|7d)$"),
    categories: str | None = None,
    bbox: str | None = None,
    q: str | None = None,
    min_severity: str | None = None,
) -> JSONResponse:
    db: Database = request.app.state.db
    until_dt = _parse_iso(until) if until else _utc_now()
    since_dt = _parse_iso(since) if since else _time_window_to_since(window, until_dt)

    bbox_tuple = None
    if bbox:
        parts = [float(p.strip()) for p in bbox.split(",") if p.strip()]
        if len(parts) == 4:
            bbox_tuple = (parts[0], parts[1], parts[2], parts[3])

    min_sev = int(min_severity) if min_severity and min_severity.strip() else None

    incidents = _query_incidents(
        db,
        since_iso=since_dt.isoformat().replace("+00:00", "Z"),
        until_iso=until_dt.isoformat().replace("+00:00", "Z"),
        categories=_split_csv(categories),
        bbox=bbox_tuple,
        q=q,
        min_severity=min_sev,
    )
    return JSONResponse(incidents)


@app.get("/api/incidents/{incident_id}")
def api_incident(request: Request, incident_id: str) -> JSONResponse:
    db: Database = request.app.state.db
    with db.lock:
        row = db.conn.execute(
            """
            SELECT *
            FROM incidents
            WHERE incident_id = ?;
            """,
            (incident_id,),
        ).fetchone()
    if row is None:
        return JSONResponse({"error": "not_found"}, status_code=404)
    return JSONResponse({k: row[k] for k in row.keys()})


@app.get("/api/incidents/{incident_id}/items")
def api_incident_items(request: Request, incident_id: str) -> JSONResponse:
    db: Database = request.app.state.db
    with db.lock:
        rows = db.conn.execute(
            """
            SELECT i.*
            FROM incident_items ii
            JOIN items i ON i.item_id = ii.item_id
            WHERE ii.incident_id = ?
            ORDER BY i.published_at DESC
            LIMIT 200;
            """,
            (incident_id,),
        ).fetchall()
    return JSONResponse([{k: r[k] for k in r.keys()} for r in rows])


@app.get("/api/items")
def api_items(request: Request, limit: int = 100) -> JSONResponse:
    db: Database = request.app.state.db
    with db.lock:
        rows = db.conn.execute(
            """
            SELECT item_id, source_id, title, category, published_at, url
            FROM items
            ORDER BY published_at DESC
            LIMIT ?;
            """,
            (limit,),
        ).fetchall()
    return JSONResponse([{k: r[k] for k in r.keys()} for r in rows])


@app.get("/api/sources")
def api_sources(request: Request) -> JSONResponse:
    db: Database = request.app.state.db
    with db.lock:
        rows = db.conn.execute(
            """
            SELECT source_id, name, source_type, url, poll_interval_seconds, enabled,
                   next_fetch_at, last_fetch_at, last_success_at, last_error_at,
                   consecutive_failures, last_status_code, last_fetch_ms, last_error
            FROM sources
            ORDER BY name ASC;
            """
        ).fetchall()
    return JSONResponse([{k: r[k] for k in r.keys()} for r in rows])


@app.get("/api/stats")
def api_stats(
    request: Request,
    window: str = Query(default="24h", pattern="^(1h|6h|24h|7d)$"),
) -> JSONResponse:
    db: Database = request.app.state.db
    until_dt = _utc_now()
    since_dt = _time_window_to_since(window, until_dt)
    since_iso = since_dt.isoformat().replace("+00:00", "Z")
    until_iso = until_dt.isoformat().replace("+00:00", "Z")

    with db.lock:
        by_category = db.conn.execute(
            """
            SELECT category, COUNT(*) AS n
            FROM incidents
            WHERE last_seen_at >= ? AND last_seen_at <= ?
            GROUP BY category
            ORDER BY n DESC;
            """,
            (since_iso, until_iso),
        ).fetchall()
    return JSONResponse(
        {
            "window": window,
            "by_category": [
                {"category": r["category"], "count": r["n"]} for r in by_category
            ],
        }
    )


@app.get("/api/places/suggest")
def api_places_suggest(request: Request, q: str) -> JSONResponse:
    db: Database = request.app.state.db
    results = suggest_places(db, q, limit=10)
    return JSONResponse(results)


@app.get("/partials/incidents", response_class=HTMLResponse)
def partial_incidents(
    request: Request,
    since: str | None = None,
    until: str | None = None,
    window: str = Query(default="24h", pattern="^(1h|6h|24h|7d)$"),
    categories: str | None = None,
    bbox: str | None = None,
    q: str | None = None,
    min_severity: str | None = None,
) -> HTMLResponse:
    db: Database = request.app.state.db
    until_dt = _parse_iso(until) if until else _utc_now()
    since_dt = _parse_iso(since) if since else _time_window_to_since(window, until_dt)

    bbox_tuple = None
    if bbox:
        parts = [float(p.strip()) for p in bbox.split(",") if p.strip()]
        if len(parts) == 4:
            bbox_tuple = (parts[0], parts[1], parts[2], parts[3])

    min_sev = int(min_severity) if min_severity and min_severity.strip() else None

    incidents = _query_incidents(
        db,
        since_iso=since_dt.isoformat().replace("+00:00", "Z"),
        until_iso=until_dt.isoformat().replace("+00:00", "Z"),
        categories=_split_csv(categories),
        bbox=bbox_tuple,
        q=q,
        min_severity=min_sev,
        limit=200,
    )
    return templates.TemplateResponse(
        request=request,
        name="partials/incidents.html",
        context={"incidents": incidents},
    )


@app.get("/partials/incident/{incident_id}", response_class=HTMLResponse)
def partial_incident_detail(request: Request, incident_id: str) -> HTMLResponse:
    db: Database = request.app.state.db
    with db.lock:
        incident = db.conn.execute(
            """
            SELECT *
            FROM incidents
            WHERE incident_id = ?;
            """,
            (incident_id,),
        ).fetchone()
        items = db.conn.execute(
            """
            SELECT i.*
            FROM incident_items ii
            JOIN items i ON i.item_id = ii.item_id
            WHERE ii.incident_id = ?
            ORDER BY i.published_at DESC
            LIMIT 100;
            """,
            (incident_id,),
        ).fetchall()

    if incident is None:
        return templates.TemplateResponse(
            request=request,
            name="partials/incident_detail.html",
            context={"incident": None, "items": []},
            status_code=404,
        )

    items_out = [{k: r[k] for k in r.keys()} for r in items]
    return templates.TemplateResponse(
        request=request,
        name="partials/incident_detail.html",
        context={
            "incident": {k: incident[k] for k in incident.keys()},
            "items": items_out,
        },
    )


@app.get("/partials/source-health", response_class=HTMLResponse)
def partial_source_health(request: Request) -> HTMLResponse:
    db: Database = request.app.state.db
    with db.lock:
        sources = db.conn.execute(
            """
            SELECT source_id, name, source_type, url, poll_interval_seconds,
                   last_success_at, last_error_at, consecutive_failures,
                   last_status_code, last_fetch_ms, last_error
            FROM sources
            ORDER BY name ASC;
            """
        ).fetchall()
    return templates.TemplateResponse(
        request=request,
        name="partials/source_health.html",
        context={"sources": [{k: r[k] for k in r.keys()} for r in sources]},
    )


@app.get("/partials/timeline", response_class=HTMLResponse)
def partial_timeline(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request=request,
        name="partials/timeline.html",
        context={},
    )
