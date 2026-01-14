# Phase 1: Foundation and MVP

Before you start
- Thoroughly explore the repo and its current status. Read README.md and SPEC.md, list files, inspect any existing code, and note any migrations or schemas already present.
- Run the existing test suite if it exists and note failures or gaps.
 - Follow repo AGENTS.md rules: avoid micro-helpers/defensive typing, and run Ruff formatting/fixing if available.

Context (short)
- This app is a map-first, awareness-first global incident dashboard designed to answer "What is happening globally right now?" in under 30 seconds.
- The experience is scan-first, with a large world map and a live incident list. Incidents are clusters of related items (dedup + similarity).
- The system must be Pi-friendly (Raspberry Pi 4B, ~2GB RAM): low CPU/RAM, minimal JS, no heavyweight ML/LLM runtime, and ingestion via ToS-safe RSS/official APIs.
- Run the entire web app as a single FastAPI process via `uvicorn app.main:app` serving both API and HTML/HTMX UI on one port (no separate frontend/backend servers).

Goals (must align with spec)
- Awareness-first UX for fast scanning.
- Event-centric aggregation into incidents.
- Map-first layout with fast filters and timeline.
- Raspberry Pi 4B compatibility (2GB RAM) with minimal JS.
- Free + ToS-safe ingestion; prefer RSS/Atom and official APIs; avoid scraping.
- Provenance and confidence: every incident shows sources and location confidence Tier A/B/C.

Non-goals (do not implement)
- Deep investigation platform, entity graphs, full-text crawling, paywalled sources.
- Predictive analytics.
- High-frequency social firehose ingestion.
- Paid APIs.

Scope of Phase 1
- Build the core backend architecture, SQLite schema, and ingestion pipeline.
- Implement MVP data sources and normalization.
- Implement dedup + incident clustering basics.
- Implement real-time updates via SSE.
- Build the main UI: map + incident list + detail drawer, HTMX partials, and SSE-driven updates.
- Add source health monitoring and basic UI.

Out of scope in Phase 1 (later phases)
- Advanced Tier B geo (gazetteer + coords-in-text), saved views, MSI maritime, OSINT sources, and hardening/deployment details.

---

## 1) Backend architecture and module layout

Implement the following structure (names from spec):
- `app/main.py`: FastAPI app + routers.
- `app/settings.py`: Pydantic settings from env.
- `ingest/scheduler.py`: async scheduler loop (per-source next-run times).
- `ingest/fetch.py`: HTTP client (httpx), conditional GET, caching headers.
- `ingest/parsers/`: `rss.py` (feedparser), `geojson.py`, `xml.py`, `json.py`.
- `normalize/normalize.py`: source-specific raw -> Item schema.
- `cluster/clusterer.py`: dedup + incident assignment.
- `geo/gazetteer.py`: placeholder to be extended later.
- `geo/coords_extract.py`: placeholder to be extended later.
- `store/db.py`: SQLite connection + migrations.
- `realtime/bus.py`: in-process pubsub.
- `realtime/sse.py`: SSE endpoint + fanout.
- `health/health.py`: per-source health metrics.

Implementation detail that should guide the ingestion pipeline:
- Treat everything as a source plugin with this uniform interface:
  - `fetch() -> bytes`
  - `parse(bytes) -> list[RawRecord]`
  - `normalize(RawRecord) -> Item`
- Keep the pipeline uniform whether the source is GeoJSON (USGS), XML (NHC), JSON exports (Smartraveller), or RSS.

---

## 2) Data model and SQLite schema (Phase 1 must implement fully)

### 2.1 Normalized Item fields (logical schema)
- `item_id` (UUID)
- `source_id`
- `source_type` (`rss`, `geojson_api`, `json_api`, `xml_api`, `social`)
- `external_id` (feed GUID, feature.id, CVE ID, etc.)
- `url` (canonical)
- `title`
- `summary` (short)
- `content` (optional; avoid full scraping)
- `published_at`, `updated_at`, `fetched_at`
- `category` (enum-like string)
- `tags` (JSON array)
- Geo:
  - `geom_geojson` (nullable)
  - `lat`, `lon` (nullable; computed from geom centroid if polygon)
  - `location_name` (nullable)
  - `location_confidence` (`A_exact`, `B_place_match`, `B_coords_in_text`, `C_country`, `C_source_default`, `U_unknown`)
  - `location_rationale` (short text)
- `raw` (JSON blob for provenance/debug)
- `hash_title`, `hash_content` (for dedup)
- `simhash` (64-bit int for clustering)

### 2.2 Incident fields (logical schema)
- `incident_id` (UUID)
- `title` (representative)
- `category` (dominant)
- `first_seen_at`, `last_seen_at`
- `last_item_at` (latest item timestamp)
- `status` (`active`, `cooling`, `resolved`)
- `severity_score` (0-100 heuristic)
- Geo:
  - `geom_geojson` (best available)
  - `lat`, `lon` (centroid)
  - `bbox` (minlon,minlat,maxlon,maxlat)
  - `location_confidence` + rationale
- Clustering:
  - `incident_simhash` (rolling)
  - `token_signature` (optional short string)
- Counts:
  - `item_count`, `source_count`

### 2.3 SQLite tables
- `sources`
- `items`
- `incidents`
- `incident_items` (many-to-many)
- `source_health` (or fields on `sources`)
- `saved_views` (create table now even if unused in Phase 1)
- `places` (gazetteer stub table; populate later)

FTS (create in Phase 1):
- `items_fts` (FTS5 over `title`, `summary`, `content`)
- `incidents_fts` (FTS5 over `title`, `summary`)

Indexes:
- `items(published_at)`, `items(category)`, `items(source_id)`
- `incidents(last_seen_at)`, `incidents(category)`
- `items(hash_title)`, `items(url)` with unique-ish constraints where possible

Retention policies (implement as scheduled cleanup task):
- Keep raw `items`: 30 days (configurable)
- Keep `incidents`: 90 days
- Keep active/cooling incidents longer regardless of age until resolved

SQLite settings:
- WAL mode
- `synchronous=NORMAL`
- periodic `VACUUM` off-hours

---

## 3) Ingestion pipeline and scheduling

### 3.1 Scheduling constraints
- Global concurrency cap: 4.
- Per-host cap: 1.
- HTTP timeouts: connect 5s, read 15s.
- Conditional GET support: honor `ETag` and `Last-Modified`.
- Respect `Cache-Control` where present.

Central loop (per spec):
1. Find sources due
2. Fetch with timeouts
3. Parse/normalize
4. Store
5. Cluster + update incidents
6. Publish SSE events

### 3.2 Source selection principles (implement in code comments or docs)
- Score sources by latency, authority, structured geo, global coverage, ToS safety, and operational stability.
- Two lanes:
  1) Structured authoritative lane (earthquakes, alerts, cyclone, etc.) with highest map signal and clean geo.
  2) News/social lane (RSS + optional OSINT later) for context and early chatter, but lower geo confidence and higher noise.

### 3.3 Phase 1 sources (implement now)

#### A) USGS Earthquakes (GeoJSON)
Endpoints:
- `https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson`
- `https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson`
- `https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/4.5_hour.geojson`

Auth: none
Caching: honor `ETag` / `Last-Modified`
Polling:
- `all_hour`: every 60s (or per Cache-Control)
- `all_day`: every 5-10 min

Expected fields:
- `features[].properties`: `mag`, `place`, `time`, `updated`, `url`, `title`
- `features[].geometry.coordinates`: `[lon, lat, depth]`

Normalization:
- `category = "earthquake"`
- `lat/lon = geometry.coordinates[1/0]`
- `severity = mag`
- `external_id = feature.id`
- `published_at = properties.time`, `updated_at = properties.updated`
- `location_confidence = A_exact`

#### B) NOAA/NWS Alerts API (GeoJSON)
Endpoints:
- `https://api.weather.gov/alerts/active`
- `https://api.weather.gov/alerts/active?status=actual`
- `https://api.weather.gov/alerts/active?severity=Severe`

Auth: none
Notes:
- NWS is strict about User-Agent; send a descriptive UA.
- Honor caching headers.
Polling: 30-60s (respect Cache-Control)

Expected fields:
- GeoJSON `features[].properties`: `event`, `headline`, `description`, `instruction`, `severity`, `urgency`, `certainty`, `effective`, `onset`, `expires`, `ends`, `areaDesc`, `geocode`
- `features[].geometry` may be polygon/multipolygon

Normalization:
- `category = "weather_alert"`
- Store geometry as GeoJSON; compute centroid for marker.
- `location_confidence = A_exact`

#### C) NOAA National Hurricane Center (RSS/XML)
Core feeds:
- `https://www.nhc.noaa.gov/gtwo.xml`
- `https://www.nhc.noaa.gov/index-at.xml`
- `https://www.nhc.noaa.gov/index-ep.xml`
- `https://www.nhc.noaa.gov/index-cp.xml`
- `https://www.nhc.noaa.gov/gis-at.xml`
- `https://www.nhc.noaa.gov/gis-ep.xml`
- `https://www.nhc.noaa.gov/gis-cp.xml`

Auth: none
Polling: 2-5 min

Normalization:
- `category = "tropical_cyclone"`
- Prefer GIS feed items that include downloadable shapefile/KML/GeoJSON links. Store these as overlay resources.
- Use best available geo for Tier A.

#### D) Australia Smartraveller (RSS + JSON export)
RSS:
- `https://www.smartraveller.gov.au/countries/documents/index.rss`
- `https://www.smartraveller.gov.au/countries/documents/do-not-travel.rss`
- `https://www.smartraveller.gov.au/countries/documents/reconsider-your-need-to-travel.rss`

JSON export:
- `https://www.smartraveller.gov.au/destinations-export`

Polling:
- RSS: 30-60 min
- JSON export: 6-12h (large)

Geo:
- Country-level by design -> Tier C (country centroid)

#### E) RSS backbone news (context + corroboration)
- `http://newsrss.bbc.co.uk/rss/newsonline_uk_edition/front_page/rss.xml`
- `http://newsrss.bbc.co.uk/rss/newsonline_uk_edition/world/rss.xml`
- `https://rss.dw.com/rdf/rss-en-top`
- `https://www.aljazeera.com/xml/rss/all.xml`

Polling: 1-5 min per feed with conditional GET

Feed parser requirement:
- Use a robust RSS/Atom parser (feedparser) and keep fields minimal.

---

## 4) Normalization, dedup, and clustering

### 4.1 Hard dedup rules
- Canonical URL match (strip tracking params like `utm_*`, `fbclid`).
- External ID match (`guid`, `feature.id`, CVE ID, etc.).
- Normalized title exact match within same source within 24h.

### 4.2 Similarity clustering
- Compute `simhash64(title + short_summary)` for each item.
- Candidate incidents: same category + within last 48h + simhash prefix bucket match.
- Hamming distance thresholds:
  - If distance <= 6 -> same incident
  - If 7-12 -> require secondary check (token Jaccard or RapidFuzz ratio)
  - Else -> new incident

### 4.3 Merge/split rules
- Merge incidents if simhashes converge and centroids are within X km (category configurable).
- Split only manually later or for large geo divergence (e.g., separate continents).

---

## 5) Geo handling (Phase 1 baseline)

Tier A (structured geo) must be implemented now:
- USGS quakes, NWS polygons, NHC GIS, etc. -> `location_confidence = A_exact`.

Tier C fallback (minimal for Phase 1):
- For country-level sources (Smartraveller) or unstructured RSS, map to country centroid if country is detected; otherwise mark as unknown.
- Always populate `location_rationale` (short human-readable reason).

Tier B (coords-in-text + gazetteer match) will be fully implemented in Phase 3.

Gazetteer data:
- Create the `places` table now but postpone full population. Later use Natural Earth (countries + admin1 + populated places) with optional GeoNames cities15000.

---

## 6) API contract (implement full endpoints in Phase 1)

### 6.1 JSON API
- `GET /api/incidents`
  - Query params: `since`, `until`, `window` (`1h|6h|24h|7d`), `categories=...`, `bbox=minlon,minlat,maxlon,maxlat`, `q`, `min_severity`
- `GET /api/incidents/{incident_id}`
- `GET /api/incidents/{incident_id}/items`
- `GET /api/items` (debug/admin)
- `GET /api/sources` (health + freshness)
- `GET /api/stats` (counts by category/time)
- `GET /api/places/suggest?q=...` (optional helper; can be stubbed until Phase 3)

### 6.2 HTMX partial routes
- `GET /partials/incidents` (left panel list)
- `GET /partials/incident/{id}` (detail drawer)
- `GET /partials/source-health` (health overlay)
- `GET /partials/timeline` (timeline histogram; can return placeholder until Phase 2)

### 6.3 SSE endpoint
- `GET /sse`

Events:
- `incident.created`
- `incident.updated`
- `source.health`
- `heartbeat`

Payload example:
```
{
  "type": "incident.updated",
  "incident_id": "uuid",
  "last_seen_at": "2026-01-13T12:34:56Z",
  "category": "earthquake",
  "lat": 35.7,
  "lon": 139.7,
  "severity_score": 72,
  "source_count": 5,
  "item_count": 12
}
```

---

## 7) Frontend (Jinja2 + HTMX + Alpine + Tailwind)

### 7.1 Layout
- Center (~70%): world map (Leaflet + markercluster)
- Left (~20%): incident list + filters
- Right (~30% overlay/drawer): incident details
- Top bar: time window (Last 1h / 6h / 24h), category toggles, quick search
- Overlays: source health modal; settings modal (map tiles) can be stubbed
- The map is the primary index; other UI elements support fast scanning (filters, timeline, incident cards).

Scan mode behavior:
- Incident list sorted by freshness + corroboration.
- Hover/click marker -> popover with title, category, last update, source count.
- Clicking incident card opens the right-side detail drawer.

Detail drawer contents (drill-down):
- "What happened" summary (auto-generated heuristic from normalized fields)
- Updates list of items (reverse chronological)
- Sources grouped by type (official feeds vs media vs social)
- Location explanation (Tier A/B/C) + confidence rationale

### 7.2 HTMX patterns
- Filters form:
  - `<form hx-get="/partials/incidents" hx-target="#incident-list" hx-push-url="true">`
- Incident click:
  - `<a hx-get="/partials/incident/{{id}}" hx-target="#incident-detail" hx-swap="innerHTML">`
- Source health:
  - `hx-get="/partials/source-health"` into modal container

### 7.3 Map
- Leaflet with raster tiles; marker clustering by zoom.
- Minimal JS responsibilities:
  - Initialize map
  - Maintain marker layer keyed by `incident_id`
  - Subscribe to SSE and apply incremental updates

Optional (later): client-side heat layer for dense categories.

Map tile URL is configurable via `MAP_TILE_URL` (default to OSM-compatible). Respect provider usage policies.

---

## 8) Performance and reliability (Phase 1 defaults)

Budgets:
- Max concurrent fetches: 4
- Per-domain concurrency: 1
- HTTP timeouts: connect 5s, read 15s
- Batch inserts per fetch cycle
- Avoid huge raw blobs for very large feeds (truncate as needed)

Polling cadence (Phase 1 sources):
- USGS earthquakes: 60s (hour feed), 5-10 min (day feed)
- NWS alerts: 30-60s (respect cache headers)
- NHC: 2-5 min
- RSS news: 180-300s per feed
- Smartraveller RSS: 30-60 min
- Smartraveller JSON export: 6-12h

Failure handling:
- Exponential backoff per source on errors/timeouts
- Surface failures in source health panel

---

## 9) Testing (Phase 1)

Unit tests:
- Title normalization
- URL canonicalization
- Simhash distance checks

Unit tests for geo:
- Gazetteer matching edge cases (prepare cases now; full logic in Phase 3)
- Example: "Georgia" ambiguity

Integration tests:
- Feed parser fixtures for each Phase 1 source type (GeoJSON, XML, RSS)

---

## 10) Spec milestone reference (for alignment)

MVP (1-2 weeks) from spec:
- SQLite schema + migrations
- Source registry + poller with ETag/If-Modified-Since
- Ingest: USGS earthquakes, NWS alerts, NHC cyclones, Smartraveller RSS + export, RSS backbone (BBC + DW + Al Jazeera)
- Normalize + basic dedup + incident creation
- Leaflet map + incident list + detail drawer
- SSE updates
- Source health panel

Tests (from spec):
- Unit: title normalization, URL canonicalization, simhash distance checks
- Unit: gazetteer matching edge cases ("Georgia", "Congo", etc.)
- Integration: feed parser fixtures for each source type

Usable beta (next):
- GDACS + EONET ingestion
- Volcano + Tsunami
- FAA disruptions
- Cyber panel (NVD + KEV)
- Playback timeline + histogram
- Better clustering rules per category
- Configurable feed packs + UI toggles

"Done":
- Robust geotagging Tier B (gazetteer + coords-in-text)
- Incident merging + cooling/resolution heuristics
- MSI maritime ingestion via discovered OpenAPI
- Saved views
- Hardening: backoff, health UX, backup/restore, metrics endpoint

## 11) Deliverables checklist
- SQLite schema + migrations with tables + FTS + indexes
- Source registry and scheduler with ETag/If-Modified-Since
- Parsers for RSS, GeoJSON, XML, JSON
- Normalization + basic dedup + incident creation
- SSE updates
- Leaflet map + incident list + detail drawer
- Source health panel
