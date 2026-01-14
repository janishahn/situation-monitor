## 1. Product Goals & Non-Goals

### Goals

* **Awareness-first UX**: answer “**What’s happening globally right now?**” in <30 seconds of scanning.
* **Event-centric aggregation**: convert many incoming items into a smaller number of evolving **incidents** (dedup + clustering).
* **Map-first**: a large central world map is the primary index; everything else supports fast scanning (filters, timeline, incident cards).
* **Pi-friendly**: Raspberry Pi 4B (≈2GB RAM), low CPU, low I/O contention, minimal JS, no heavyweight ML/LLM runtime.
* **Free + ToS-safe ingestion**: prefer official RSS/Atom and documented APIs; avoid fragile scraping and ToS-violating extraction.
* **Provenance & confidence**: every incident shows sources; every location shows confidence + how it was derived (Tier A/B/C).

### Non-goals

* **Deep investigation platform** (thread reconstruction, entity graphs, full-text article crawling, paywalled sources).
* **Predictive analytics** (forecasting conflict, disease spread, etc.).
* **High-frequency social firehose ingestion** (e.g., full Bluesky firehose) on a Pi.
* **Paid APIs** (FlightAware, commercial geocoders, premium news APIs).

---

## 2. User Workflows (scan mode, drill-down, playback, saved views)

### A) Scan mode (default)

1. User opens `/` and sees:

   * Large world map with clustered markers.
   * Left panel: “Active incidents” sorted by **freshness + corroboration**.
   * Top bar: time window (Last 1h / 6h / 24h), category toggles, quick search.
2. As new items arrive, the incident list and map update in near real-time (SSE).

**Fast-scan interactions**

* Hover/Click marker → small popover: title, category, last update, source count.
* Click incident card → opens right-side detail drawer with:

  * Summary
  * Timeline of updates
  * Source list (links out)
  * Location confidence + rationale

### B) Drill-down (incident detail)

* User clicks an incident:

  * Right panel loads `/incidents/{id}` (HTMX swap) with:

    * “What happened” (auto-generated heuristic summary from normalized fields)
    * “Updates” list of items (reverse chronological)
    * “Sources” grouped by type (official feeds vs media vs social)
    * “Location” explanation (Tier A/B/C) + confidence

### C) Playback (what just happened?)

* Timeline slider selects a **time cursor** within last N hours.
* Map and incident list switch to **playback view**:

  * Show incidents as-of cursor time
  * Optional “step” playback (e.g., 5-minute increments)

### D) Saved views (future-friendly, but spec now)

* Save filters + map viewport + time window + source sets as named presets:

  * “Crisis mode” (only official alerts + disasters)
  * “Cyber watch” (NVD/CISA/CERT)
  * “Regional focus: East Asia”

Implementation: store JSON blobs in SQLite (`saved_views` table), load via dropdown.

---

## 3. Data Sources & Research (largest section)

### 3.0 Source selection principles (OSINT-aware)

Each source is scored on:

* **Latency** (minutes matter)
* **Authority** (official bulletin > repost)
* **Structured geo** (lat/lon preferred)
* **Global coverage** (or clearly regional)
* **ToS safety** (documented endpoints, RSS/Atom, published APIs)
* **Operational stability** (ETag/Last-Modified, predictable uptime)

You’ll run **two parallel “lanes”**:

1. **Structured authoritative lane** (earthquakes, alerts, volcano, cyclone, tsunami, travel advisories, cyber advisories) → highest map-signal, easiest geo.
2. **News/social lane** (RSS + Mastodon/Bluesky/Reddit) → adds context + early chatter, but lower geo confidence and higher noise.

---

### 3.1 Structured event/alert feeds (map-friendly, real-time)

#### A) Earthquakes (global, authoritative)

**USGS Earthquake GeoJSON Feeds** (multiple severities + time windows)

Use at least:

```text
https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson
https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson
https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/4.5_hour.geojson
```

**Auth**: none
**Caching**: honor `ETag` / `Last-Modified` (conditional GET)
**Recommended poll**:

* `all_hour`: every 60s (or per Cache-Control if present)
* `all_day`: every 5–10 min

**Expected fields (key)**:

* `features[].properties`: `mag`, `place`, `time`, `updated`, `url`, `title`
* `features[].geometry.coordinates`: `[lon, lat, depth]`

**Normalization mapping**

* `category = "earthquake"`
* `lat/lon = geometry.coordinates[1/0]`
* `severity = mag`
* `external_id = feature.id`
* `published_at = properties.time`, `updated_at = properties.updated`

---

#### B) Severe weather alerts (official, CAP-like)

**NOAA/NWS Alerts API (US & territories)**

```text
https://api.weather.gov/alerts/active
https://api.weather.gov/alerts/active?status=actual
https://api.weather.gov/alerts/active?severity=Severe
```

**Auth**: none
**Caching**: NWS is strict about User-Agent; send a descriptive UA. Honor caching headers.
**Recommended poll**: 30–60s (but respect Cache-Control)

**Expected fields**

* GeoJSON FeatureCollection with `features[].properties`:

  * `event`, `headline`, `description`, `instruction`
  * `severity`, `urgency`, `certainty`
  * `effective`, `onset`, `expires`, `ends`
  * `areaDesc`, `geocode`
* `features[].geometry` may be polygon/multipolygon.

**Normalization**

* `category = "weather_alert"`
* Geometry stored as GeoJSON; centroid computed for marker.
* Confidence Tier A (official geometry)

---

#### C) Tropical cyclones / storm tracking (high signal)

**NOAA National Hurricane Center RSS/XML feeds**
These are **excellent** for map overlays and “active storm” tracking.

Core “dynamic feeds”:

```text
https://www.nhc.noaa.gov/gtwo.xml          (Graphical Tropical Weather Outlooks)
https://www.nhc.noaa.gov/index-at.xml      (Atlantic Basin Tropical Cyclones)
https://www.nhc.noaa.gov/index-ep.xml      (Eastern Pacific Basin Tropical Cyclones)
https://www.nhc.noaa.gov/index-cp.xml      (Central Pacific Basin Tropical Cyclones)
https://www.nhc.noaa.gov/gis-at.xml        (Atlantic Basin GIS Data feed)
https://www.nhc.noaa.gov/gis-ep.xml
https://www.nhc.noaa.gov/gis-cp.xml
```

**Auth**: none
**Recommended poll**: 2–5 min (NHC updates are not sub-minute; respect server load)

**Normalization**

* `category = "tropical_cyclone"`
* Prefer GIS feed items that include downloadable shapefiles/KML/GeoJSON links (store as “overlay resources”)

---

#### D) Volcano alerts (authoritative)

**USGS HANS (volcano status + CAP/RSS per volcano)**

Key endpoints:

```text
https://volcanoes.usgs.gov/hans-public/api/volcano/getElevatedVolcanoes
https://volcanoes.usgs.gov/hans-public/rss/cap/volcano/332010   (example: Kilauea)
```

**Auth**: none
**Recommended poll**:

* Elevated list API: every 5 min
* Per-volcano CAP/RSS: every 5–10 min for volcanoes currently elevated

**Expected fields (API)**
From the elevated volcanoes JSON, you’ll typically get:

* Volcano name, alert level, aviation color code, latitude/longitude, status strings, timestamps.

**Normalization**

* `category = "volcano"`
* Tier A geo from API lat/lon
* Severity heuristic: map alert level + aviation color to numeric 1–5

---

#### E) Tsunami warnings (official)

**tsunami.gov “Product Retrieval” Atom/CAP feeds**

Feeds:

```text
https://tsunami.gov/events/xml/PAAQAtom.xml   (National Tsunami Warning Center Atom)
https://tsunami.gov/events/xml/PAAQCAP.xml    (NTWC CAP)
https://tsunami.gov/events/xml/PHEBAtom.xml   (Pacific Tsunami Warning Center Atom)
https://tsunami.gov/events/xml/PHEBCAP.xml    (PTWC CAP)
```

**Auth**: none
**Recommended poll**: every 60–120s during active events; otherwise 5 min
**Normalization**

* `category = "tsunami"`
* CAP geometry if available → Tier A; otherwise infer region centroid (Tier C)

---

#### F) Wildfires (global, structured; requires free key)

**NASA FIRMS (Fire Information for Resource Management System) API**

FIRMS provides multiple ways; common pattern is area/time queries returning CSV/JSON.
You will likely need a **free FIRMS MAP_KEY**.

Example endpoint pattern (you’ll configure per product/dataset):

```text
https://firms.modaps.eosdis.nasa.gov/api/
```

**Auth**: free API key
**Recommended poll**: 10–15 min (satellite refresh cadence; avoid hammering)

**Normalization**

* Each hotspot becomes an item:

  * `category="wildfire"`
  * `lat/lon` direct
  * Severity heuristic: brightness / FRP if provided, plus cluster density

---

#### G) Multi-hazard global aggregation (good “situational” layer)

**GDACS (Global Disaster Alert and Coordination System)**
Use GDACS as a *meta-layer* for earthquakes, floods, cyclones, volcano, etc.

Common RSS entrypoint (verify availability in deployment):

```text
https://www.gdacs.org/xml/rss.xml
```

**Auth**: none
**Recommended poll**: 5 min
**Normalization**

* GDACS often includes event type + coordinates; treat as Tier A or Tier B depending on feed fields.

---

#### H) “Natural events” general-purpose feed (nice-to-have)

**NASA EONET v3**

Open events endpoint:

```text
https://eonet.gsfc.nasa.gov/api/v3/events?status=open
```

**Auth**: none
**Recommended poll**: 10–15 min
**Use case**: broad awareness overlay (wildfires, volcanoes, storms), but may lag behind ultra-real-time sources.

---

### 3.2 Aviation incidents / disruptions (free, official-ish)

**FAA NAS Status – Airport Status Information API (XML)**

Endpoint:

```text
https://nasstatus.faa.gov/api/airport-status-information
```

**Auth**: none
**Recommended poll**: 2–5 min
**Expected**: XML listing ground stops, delays, closures, etc.

**Normalization**

* `category="aviation_disruption"`
* Geo: map airport code → lat/lon via offline airport dataset (OpenFlights or OurAirports CSV, bundled at install time)
* Severity heuristic: closure > ground stop > GDP > delay minutes

**ToS note**: This is an FAA-hosted endpoint used by the NAS status site; still treat gently and cache responses.

---

### 3.3 Maritime safety incidents / navigational warnings (free, official)

**NGA Maritime Safety Information (MSI)**

Primary site + API discovery:

```text
https://msi.nga.mil/
https://msi.nga.mil/api/swagger-ui.html
```

Swagger indicates a REST API with base URL:

```text
https://msi.pub.kubic.nga.mil/
```

**Auth**: typically none for public endpoints
**Recommended poll**: 10–30 min depending on endpoint (NAVWARN updates aren’t sub-minute)

**Implementation approach**

* At install time (or first run), fetch MSI OpenAPI JSON (paths vary by server; you’ll implement a small “discover OpenAPI” routine trying common locations like `/v3/api-docs`, `/openapi.json`, etc.).
* Once discovered, ingest NAVWARN + “persons in distress / hazards” categories into:

  * `category="maritime_warning"`
  * Geo: many warnings contain coordinates in text; apply coordinate regex extraction (Tier B) if API doesn’t supply geometry.

---

### 3.4 Public health advisories (free, official)

**CDC Travel Health Notices RSS**

```text
https://wwwnc.cdc.gov/travel/rss/notices.xml
```

**Auth**: none
**Recommended poll**: 30–60 min (not minute-to-minute)
**Geo**: destination country/region in title; map to country centroid (Tier C), optionally refine with gazetteer match (Tier B).

**WHO AFRO Emergencies RSS** (regional, but valuable)

```text
https://www.afro.who.int/rss/emergencies.xml
```

**Auth**: none
**Recommended poll**: 30–60 min

---

### 3.5 Cybersecurity advisories (structured, high-signal)

#### A) NVD (NIST) CVE API (free; API key recommended)

NVD API base + docs

```text
https://services.nvd.nist.gov/rest/json/cves/2.0
```

**Auth**: free API key recommended (rate limits improve); implement env var `NVD_API_KEY`
**Recommended poll**:

* Every 15 min for “recent changes” window (use `lastModStartDate`/`lastModEndDate`)
* Backfill nightly if needed

**Normalization**

* `category="cyber_cve"`
* Geo: none (these are non-geographic). Display in a “Cyber” layer/panel, not on map by default.
* Incident clustering: group by CVE ID and by vendor/product keywords.

#### B) CISA Known Exploited Vulnerabilities (KEV) catalog (JSON)

Official feed URL (may be protected by WAF in some environments; still standard to use)

```text
https://www.cisa.gov/sites/default/files/feeds/known_exploited_vulnerabilities.json
```

**Recommended poll**: 6h (KEV updates are not minute-level)
**Normalization**: `category="cyber_kev"` (non-geo)

---

### 3.6 Conflict/security-related official advisories (high-signal)

#### A) Australia Smartraveller (RSS + JSON API)

Smartraveller provides both RSS feeds and a **public JSON API export**

RSS:

```text
https://www.smartraveller.gov.au/countries/documents/index.rss
https://www.smartraveller.gov.au/countries/documents/do-not-travel.rss
https://www.smartraveller.gov.au/countries/documents/reconsider-your-need-to-travel.rss
```

JSON export:

```text
https://www.smartraveller.gov.au/destinations-export
```

**Recommended poll**:

* RSS: 30–60 min
* JSON export: 6–12h (it’s large)

**Geo strategy**

* Country-level by design → Tier C (country centroid)
* Great for “rising risk” scanning overlays.

#### B) Canada “Assistance to travellers” RSS

Canada provides RSS feeds including travel updates

```text
https://travel.gc.ca/feeds/rss/eng/travel-updates-24.aspx
```

**Recommended poll**: 30–60 min
**Geo**: often country/region; Tier C.

#### C) US State Department Travel Advisories RSS (legacy but widely used)

The State Department historically published RSS at:

```text
https://travel.state.gov/_res/rss/TAs.xml
```

**Recommended poll**: 30–60 min
**Geo**: country-level.

#### D) UK GOV.UK travel advice (structured via GOV.UK Content API)

GOV.UK Content API base is documented

```text
https://www.gov.uk/api/content/foreign-travel-advice
https://www.gov.uk/api/content/foreign-travel-advice/{country-slug}
```

**Recommended poll**: 1–6h (content API is not designed for sub-minute)
**Geo**: country-level.

---

### 3.7 Humanitarian / official relief reporting (API, not scraping)

**ReliefWeb API v1** (free, documented)

Base:

```text
https://api.reliefweb.int/v1/
```

Useful endpoints:

```text
https://api.reliefweb.int/v1/reports
https://api.reliefweb.int/v1/disasters
```

**Recommended poll**: 10–30 min
**Geo**: ReliefWeb reports often include countries/regions; map to country centroid (Tier C), optionally refine via gazetteer (Tier B).

---

### 3.8 News RSS/Atom (curated starter set)

**Design intent**: RSS is *context + corroboration*, not the primary incident “truth.” You’ll still cluster/dedup aggressively.

#### A) “Global backbone” news feeds

**BBC RSS (high-quality feed directory)**

```text
http://newsrss.bbc.co.uk/rss/newsonline_uk_edition/front_page/rss.xml
http://newsrss.bbc.co.uk/rss/newsonline_uk_edition/world/rss.xml
```

**Deutsche Welle (Top Stories RSS)**

```text
https://rss.dw.com/rdf/rss-en-top
```

**Al Jazeera (commonly used RSS entrypoint)**

```text
https://www.aljazeera.com/xml/rss/all.xml
```

**Polling**: 1–5 min per feed (but obey ETag/Last-Modified; back off on errors)

#### B) Regional expansion pattern (implementation-ready)

Instead of hardcoding 200 feeds in code, implement **feed packs** as YAML files (deploy-time editable), grouped by region/topic:

* `feeds/global.yaml`
* `feeds/americas.yaml`
* `feeds/europe.yaml`
* `feeds/mena.yaml`
* `feeds/africa.yaml`
* `feeds/apac.yaml`
* `feeds/cyber.yaml`
* `feeds/disasters.yaml`

Each entry includes:

* `name`, `url`, `region`, `topic_tags`, `default_geo` (optional), `poll_min`, `enabled`

Example feed pack entry:

```yaml
- id: bbc_world
  name: "BBC News - World"
  type: rss
  url: "http://newsrss.bbc.co.uk/rss/newsonline_uk_edition/world/rss.xml"
  region: global
  tags: ["news", "world"]
  poll_seconds: 180
```

---

### 3.9 OSINT sources (Mastodon, Bluesky, Reddit) — free, ToS-respecting

#### A) Mastodon (serious OSINT signal, API-first)

**Access method**: standard Mastodon REST API; do not scrape HTML.

**Core endpoints (polling model, Pi-safe)**

```text
GET https://{instance}/api/v1/timelines/tag/{tag}?limit=20&since_id={since_id}
GET https://{instance}/api/v1/timelines/public?local=false&limit=20&since_id={since_id}
GET https://{instance}/api/v1/accounts/{id}/statuses?limit=20&since_id={since_id}
GET https://{instance}/api/v2/search?q={query}&type=accounts|statuses&resolve=true
```

**Auth**:

* Many instances allow limited unauthenticated reads, but for reliability implement optional token:

  * `MASTODON_TOKEN_{INSTANCE}` env vars
* Respect instance rate limits; implement per-instance concurrency = 1.

**OSINT “starter” configuration**

* Prefer **hashtags** for disasters + breaking:

  * `#earthquake`, `#wildfire`, `#flood`, `#tsunami`, `#storm`, `#breaking`, `#OSINT`
* Prefer **official agency accounts** where available (varies by instance) — you’ll support adding accounts by handle in config:

```yaml
- id: mastodon_tag_earthquake
  type: mastodon_tag
  instance: "mastodon.social"
  tag: "earthquake"
  poll_seconds: 60
```

**Normalization**

* `category="social"`
* Geo: usually Tier B (gazetteer + coordinate extraction); rarely Tier A.
* Store post URL + author handle + instance as provenance.

#### B) Bluesky (feasible, but keep scope controlled)

Use AT Protocol XRPC endpoints (read-only queries). Keep it **optional** in MVP due to rate limiting and evolving API expectations.

Typical endpoints:

```text
GET https://bsky.social/xrpc/app.bsky.feed.getAuthorFeed?actor={handle}&limit=30
GET https://bsky.social/xrpc/app.bsky.feed.searchPosts?q={query}&limit=30
```

Auth:

* Some endpoints may require auth for consistent access; implement optional `BLUESKY_HANDLE/BLUESKY_APP_PASSWORD`.

#### C) Reddit (permitted via RSS or API)

**RSS method (no auth, simplest):**

```text
https://www.reddit.com/r/worldnews/.rss
https://www.reddit.com/r/geopolitics/.rss
https://www.reddit.com/r/Cybersecurity/.rss
https://www.reddit.com/r/osint/.rss
https://www.reddit.com/r/news/.rss
```

**ToS/rate notes**

* Send a descriptive User-Agent.
* Cache aggressively; poll 2–5 min, back off on 429.

---

## 4. Normalized Data Model

### 4.1 Common schema

#### Normalized “Item” (single incoming record)

Fields (logical):

* `item_id` (UUID)
* `source_id`
* `source_type` (`rss`, `geojson_api`, `json_api`, `xml_api`, `social`)
* `external_id` (feed GUID, feature.id, etc.)
* `url` (canonical)
* `title`
* `summary` (short)
* `content` (optional; avoid full scraping)
* `published_at`, `updated_at`, `fetched_at`
* `category` (enum-ish string)
* `tags` (JSON array)
* Geo:

  * `geom_geojson` (nullable)
  * `lat`, `lon` (nullable; computed from geom centroid if polygon)
  * `location_name` (nullable)
  * `location_confidence` (`A_exact`, `B_place_match`, `B_coords_in_text`, `C_country`, `C_source_default`, `U_unknown`)
  * `location_rationale` (short text)
* `raw` (JSON blob for provenance/debug)
* `hash_title`, `hash_content` (for dedup)
* `simhash` (64-bit int for clustering)

#### “Incident” (cluster of related items)

Fields:

* `incident_id` (UUID)
* `title` (representative)
* `category` (dominant)
* `first_seen_at`, `last_seen_at`
* `last_item_at` (latest item timestamp)
* `status` (`active`, `cooling`, `resolved`)
* `severity_score` (0–100 heuristic)
* Geo:

  * `geom_geojson` (best available)
  * `lat`, `lon` (centroid)
  * `bbox` (minlon,minlat,maxlon,maxlat)
  * `location_confidence` + rationale
* Clustering:

  * `incident_simhash` (rolling)
  * `token_signature` (optional short string)
* Counts:

  * `item_count`, `source_count`

### 4.2 SQLite tables (implementation-oriented)

Recommended SQLite settings:

* WAL mode, `synchronous=NORMAL`, proper indexes, periodic `VACUUM` off-hours.

Tables:

* `sources`
* `items`
* `incidents`
* `incident_items` (many-to-many)
* `source_health` (or fields on `sources`)
* `saved_views`
* `places` (gazetteer)

FTS:

* `items_fts` (FTS5 over `title`, `summary`, `content`)
* `incidents_fts` (FTS5 over `title`, `summary`)

Indexes:

* `items(published_at)`, `items(category)`, `items(source_id)`
* `incidents(last_seen_at)`, `incidents(category)`
* `items(hash_title)`, `items(url)` unique-ish constraints where possible

Retention:

* Keep raw `items`: 30 days (configurable)
* Keep `incidents`: 90 days
* Keep “active/cooling” incidents longer regardless of age until resolved

---

## 5. Backend Architecture (FastAPI)

### 5.1 Components/modules

* `app/main.py`: FastAPI app + routers
* `app/settings.py`: Pydantic settings (env-driven)
* `ingest/scheduler.py`: async scheduler loop (per-source next-run times)
* `ingest/fetch.py`: HTTP client (httpx) + conditional GET support
* `ingest/parsers/`:

  * `rss.py` (feedparser)
  * `geojson.py`
  * `xml.py` (FAA, NHC)
  * `json.py` (NVD, Smartraveller, USGS volcano)
* `normalize/normalize.py`: source-specific → Item schema
* `cluster/clusterer.py`: dedup + similarity + incident assignment
* `geo/gazetteer.py`: offline lookup + matching
* `geo/coords_extract.py`: regex coordinate extraction
* `store/db.py`: SQLite connection + migrations
* `realtime/bus.py`: in-process pubsub for updates
* `realtime/sse.py`: SSE endpoint + fanout
* `health/health.py`: per-source health metrics

### 5.2 Background scheduling (Pi-safe)

Do **not** run Celery/Redis. Use:

* A single async task per source **only while fetching** (bounded concurrency).
* Central loop:

  1. Find sources due
  2. Fetch with timeouts
  3. Parse/normalize
  4. Store
  5. Cluster + update incidents
  6. Publish SSE events

Constraints:

* Global concurrency cap: 4
* Per-host cap: 1 (prevents hammering single domains)

### 5.3 Deduplication & clustering (cheap but effective)

#### Step 1: Hard dedup

* Canonical URL match (strip tracking params like `utm_*`, `fbclid`, etc.)
* External ID match (`guid`, `feature.id`, CVE ID)
* Normalized title exact match within same source within 24h

#### Step 2: Similarity clustering (incident assignment)

Pi-friendly approach:

* Compute `simhash64(title + short_summary)` for each item.
* Candidate incidents = same category + within time window (e.g., last 48h) AND simhash prefix bucket match.
* Compute Hamming distance:

  * If distance ≤ 6 → same incident
  * If 7–12 → require secondary check (token Jaccard or RapidFuzz ratio)
  * Else → new incident

Merge/split rules:

* Merge incidents if their representative simhashes converge and their centroids are within X km (configurable per category).
* Split only manually (later) or via large geo divergence (e.g., 2+ separate continents).

### 5.4 Geotagging tiers A/B/C (no ML)

#### Tier A (best): structured geo

* USGS quakes, NWS polygons, volcano API lat/lon, FIRMS hotspots, NHC GIS.
* Set `location_confidence = A_exact`.

#### Tier B: heuristic extraction

1. **Coordinate regex** in title/summary (e.g., `12.34N 56.78E`, `-33.9, 151.2`)
2. **Gazetteer match**:

   * Offline `places` table (countries + major cities + admin1).
   * N-gram scan (1–3 tokens) with normalization (casefold, strip punctuation).
   * Score by place importance (population/importance) and proximity to any extracted coords.
3. If multiple matches, prefer:

   * Matches near other hints (country mention)
   * Higher importance if ambiguous (“Georgia” problem handled by context tokens like “Tbilisi” vs “Atlanta”)

Set `location_confidence = B_place_match` or `B_coords_in_text`.

#### Tier C: fallback

* Source region default (e.g., Smartraveller/Canada advisory is country-based)
* Country centroid from gazetteer if country detected
* Else unknown

Always store `location_rationale` (short human-readable string).

#### Gazetteer dataset choice (offline footprint)

* **Natural Earth** (countries + admin1 + populated places) as a small base (good Pi footprint).
* Optional: GeoNames “cities15000” (bigger, better coverage) as an install-time optional download.

---

## 6. API Contract

### 6.1 JSON API (for map + client JS)

* `GET /api/incidents`

  * Query params:

    * `since` (ISO datetime), `until`
    * `window` (`1h|6h|24h|7d`)
    * `categories=earthquake,weather_alert,...`
    * `bbox=minlon,minlat,maxlon,maxlat`
    * `q` (search)
    * `min_severity`
* `GET /api/incidents/{incident_id}`
* `GET /api/incidents/{incident_id}/items`
* `GET /api/items` (debug/admin)
* `GET /api/sources` (health + freshness)
* `GET /api/stats` (counts by category/time)
* `GET /api/places/suggest?q=...` (optional helper for search)

### 6.2 HTMX partial routes (server-rendered)

* `GET /partials/incidents` (left panel list)
* `GET /partials/incident/{id}` (detail drawer)
* `GET /partials/source-health` (health overlay)
* `GET /partials/timeline` (timeline histogram)

### 6.3 SSE (real-time updates)

* `GET /sse`

  * Events:

    * `incident.created`
    * `incident.updated`
    * `source.health`
    * `heartbeat`

Payload (example):

```json
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

## 7. Frontend Architecture (Jinja2 + HTMX + Alpine + Tailwind)

### 7.1 Page layout (single-page “dashboard”)

* **Center (70%)**: world map
* **Left (20%)**: incident list + filters
* **Right (30% overlay / drawer)**: incident details
* **Top bar**: time window, search, category chips, “layers”
* **Overlays**:

  * Timeline/playback drawer
  * Source health modal
  * Settings modal (feeds enabled, polling, map tiles)

### 7.2 HTMX interaction patterns

* Filter changes:

  * `<form hx-get="/partials/incidents" hx-target="#incident-list" hx-push-url="true">`
* Clicking an incident:

  * `<a hx-get="/partials/incident/{{id}}" hx-target="#incident-detail" hx-swap="innerHTML">`
* Source health button:

  * `hx-get="/partials/source-health"` into modal container

### 7.3 Map choice + minimal JS

**Leaflet** (raster tiles) + **Leaflet.markercluster**:

* Lowest complexity, best Pi/browser performance.
* Keep JS limited to:

  * Initialize map
  * Maintain marker layer keyed by `incident_id`
  * Subscribe to SSE and apply incremental updates

Tile URL is configurable (`MAP_TILE_URL`), defaulting to an OSM-compatible provider (user-configurable to respect usage policies).

Decluttering:

* Marker clustering by zoom
* Category layers (toggle earthquakes/weather/etc.)
* Optional heat layer (client-side) for dense categories

### 7.4 Timeline playback

* Alpine state: `window="6h"`, `cursorTs=...`, `playing=false`
* When cursor changes:

  * HTMX fetch `/partials/incidents?asof=...`
  * JS requests `/api/incidents?asof=...` to redraw markers

### 7.5 Twitter/X embed panel (manual scanning, not ingestion)

* Right-side tab “X Scan”
* Contains official embed widgets (lists/search pages you configure manually)
* Performance/privacy notes:

  * Load embed script only when panel opens
  * Warn user that X embeds may track; provide toggle to disable entirely

---

## 8. Performance & Reliability Plan (Pi constraints)

### 8.1 Budgets & guardrails

* Max concurrent fetches: 4
* Per-domain concurrency: 1
* HTTP timeouts: connect 5s, read 15s
* SQLite:

  * WAL mode
  * Batch inserts per fetch cycle
  * Avoid writing huge raw blobs for very large feeds (truncate)

### 8.2 Polling cadence (starting defaults)

* USGS earthquakes: 60s (hour feed)
* NWS alerts: 30–60s (respect caching)
* Tsunami: 60–120s when active
* Volcano elevated: 5 min
* NHC: 2–5 min
* FIRMS: 10–15 min
* RSS news: 180–300s per feed (but conditional GET)
* Cyber (NVD): 15 min
* Travel advisories: 30–60 min (RSS), 6–12h (bulk JSON)

### 8.3 Failure modes & mitigations

* **Feed errors/timeouts**: exponential backoff per source, show in health panel.
* **Duplicate floods**: tighten clustering thresholds per category; use URL canonicalization.
* **Geo ambiguity**: show Tier C clearly; never pretend city-level certainty.
* **Map overload**: default to clustering + category toggles; cap markers drawn (e.g., top 200 by score).

---

## 9. Security & Deployment

### 9.1 Exposure model

* Default bind: `127.0.0.1:8000` (local only)
* Optional LAN: `0.0.0.0` behind:

  * Tailscale, or
  * Nginx reverse proxy + basic auth

### 9.2 Security headers

* CSP (tight, but allow map tiles + optional X embeds only when enabled)
* HSTS if TLS enabled
* Disable framing except where needed for embed panel

### 9.3 systemd deployment (Pi)

Two services:

1. `geodash.service` (FastAPI/uvicorn)
2. `geodash-ingest.service` (optional split; but simplest is one process)

Example `geodash.service`:

```ini
[Unit]
Description=Geo Awareness Dashboard
After=network-online.target

[Service]
User=pi
WorkingDirectory=/opt/geodash
EnvironmentFile=/opt/geodash/.env
ExecStart=/opt/geodash/venv/bin/uvicorn app.main:app --host 127.0.0.1 --port 8000
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
```

Env vars:

* `DATABASE_PATH=/opt/geodash/data/geodash.sqlite`
* `MAP_TILE_URL=...`
* `NVD_API_KEY=...` (optional)
* `FIRMS_API_KEY=...` (optional)
* `MASTODON_TOKEN_mastodon_social=...` (optional)

Logging:

* journald + rotating file logs for ingest errors

---

## 10. Milestones

### MVP (1–2 weeks of focused build)

* SQLite schema + migrations
* Source registry + poller with ETag/If-Modified-Since
* Ingest:

  * USGS earthquakes
  * NWS alerts
  * NHC cyclones
  * Smartraveller RSS + export
  * RSS backbone (BBC + DW + Al Jazeera)
* Normalize + basic dedup + incident creation
* Leaflet map + incident list + detail drawer
* SSE updates
* Source health panel

**Tests**

* Unit: title normalization, URL canonicalization, simhash distance checks
* Unit: gazetteer matching edge cases (“Georgia”, “Congo”, etc.)
* Integration: feed parser fixtures for each source type

### Usable beta (next)

* GDACS + EONET ingestion
* Volcano + Tsunami
* FAA disruptions
* Cyber panel (NVD + KEV)
* Playback timeline + histogram
* Better clustering rules per category
* Configurable feed packs + UI toggles

### “Done”

* Robust geotagging Tier B (gazetteer + coords-in-text)
* Incident merging + cooling/resolution heuristics
* MSI maritime ingestion via discovered OpenAPI
* Saved views
* Hardening: backoff, health UX, backup/restore, metrics endpoint

---

### One implementation detail that will save you pain

Treat **everything** as a “source plugin” with:

* `fetch() -> bytes`
* `parse(bytes) -> list[RawRecord]`
* `normalize(RawRecord) -> Item`

That keeps the ingestion pipeline uniform whether it’s GeoJSON (USGS), XML (FAA/NHC), JSON exports (Smartraveller), or RSS.