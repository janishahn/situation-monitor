# Phase 2: Usable Beta Expansion

Before you start
- Thoroughly explore the repo and its current status. Read README.md and SPEC.md again, inspect Phase 1 implementation, and understand existing pipelines, schema, and UI.
- Run the existing test suite; fix regressions before adding new features.
 - Follow repo AGENTS.md rules: avoid micro-helpers/defensive typing, and run Ruff formatting/fixing if available.

Context (short)
- Phase 1 delivered the core ingestion pipeline, SQLite schema, map-first UI, and basic incident clustering.
- Phase 2 expands coverage with additional authoritative sources, adds playback/timeline, introduces feed packs, and adds a cyber-specific panel.
- The app is a single FastAPI service served via `uvicorn app.main:app` on one port (API + UI together).

Scope of Phase 2
- Add structured and official sources beyond Phase 1 (GDACS, EONET, volcano, tsunami, wildfire, FAA, health advisories, ReliefWeb, travel advisories, cyber).
- Implement feed packs as YAML-configured sources with UI toggles.
- Add playback timeline and histogram.
- Improve clustering rules per category.

Out of scope (Phase 3)
- Maritime MSI, OSINT social sources, advanced Tier B geotagging, saved views, deployment hardening, X embed panel.

---

## 1) New data sources to implement

### 1.1 GDACS (Global Disaster Alert and Coordination System)
- RSS endpoint:
  - `https://www.gdacs.org/xml/rss.xml`
- Poll every 5 minutes.
- Normalization:
  - Map to the appropriate hazard category if present; otherwise treat as a general "disaster" category.
  - GDACS entries often include coordinates; use as Tier A if available, Tier B if only implied.

### 1.2 NASA EONET v3 (natural events)
- Endpoint:
  - `https://eonet.gsfc.nasa.gov/api/v3/events?status=open`
- Poll every 10-15 minutes.
- Use case: broad awareness overlay for wildfires, volcanoes, storms, etc. Expect some lag vs ultra-real-time sources.

### 1.3 USGS HANS (volcano alerts)
- Elevated volcanoes list:
  - `https://volcanoes.usgs.gov/hans-public/api/volcano/getElevatedVolcanoes`
- Per-volcano CAP/RSS (example):
  - `https://volcanoes.usgs.gov/hans-public/rss/cap/volcano/332010`
- Polling:
  - Elevated list: every 5 min
  - Per-volcano CAP/RSS: every 5-10 min for elevated volcanoes
- Normalization:
  - `category = "volcano"`
  - Tier A geo from API lat/lon
  - Severity heuristic: map alert level + aviation color code to numeric 1-5

### 1.4 Tsunami warnings (tsunami.gov)
- NTWC Atom + CAP:
  - `https://tsunami.gov/events/xml/PAAQAtom.xml`
  - `https://tsunami.gov/events/xml/PAAQCAP.xml`
- PTWC Atom + CAP:
  - `https://tsunami.gov/events/xml/PHEBAtom.xml`
  - `https://tsunami.gov/events/xml/PHEBCAP.xml`
- Polling:
  - 60-120s when active; otherwise 5 min
- Normalization:
  - `category = "tsunami"`
  - Use CAP geometry if available (Tier A). If not, infer region centroid (Tier C).

### 1.5 NASA FIRMS wildfire hotspots (requires free API key)
- Base endpoint pattern:
  - `https://firms.modaps.eosdis.nasa.gov/api/`
- Auth: use `FIRMS_API_KEY` env var
- Poll every 10-15 min
- Normalization:
  - Each hotspot -> `category = "wildfire"`
  - Use direct lat/lon
  - Severity heuristic: brightness/FRP if provided + cluster density

### 1.6 FAA NAS Status (airport disruptions)
- Endpoint:
  - `https://nasstatus.faa.gov/api/airport-status-information`
- Poll every 2-5 min
- Expected: XML listing ground stops, delays, closures
- Normalization:
  - `category = "aviation_disruption"`
  - Map airport code -> lat/lon using offline dataset (OpenFlights or OurAirports CSV; bundled at install time)
  - Severity heuristic: closure > ground stop > GDP > delay minutes

### 1.7 Public health advisories
- CDC Travel Health Notices RSS:
  - `https://wwwnc.cdc.gov/travel/rss/notices.xml`
- WHO AFRO Emergencies RSS:
  - `https://www.afro.who.int/rss/emergencies.xml`
- Poll every 30-60 min
- Geo: use country/region in title -> country centroid (Tier C) or gazetteer match (Tier B later)

### 1.8 Cybersecurity advisories

#### NVD CVE API
- Base endpoint:
  - `https://services.nvd.nist.gov/rest/json/cves/2.0`
- Auth: optional `NVD_API_KEY` env var for higher rate limits
- Poll every 15 min for recent changes using `lastModStartDate`/`lastModEndDate`
- Nightly backfill if needed
- Normalization:
  - `category = "cyber_cve"`
  - Non-geographic: do not show on map by default
  - Cluster by CVE ID and vendor/product keywords

#### CISA KEV catalog
- JSON feed:
  - `https://www.cisa.gov/sites/default/files/feeds/known_exploited_vulnerabilities.json`
- Poll every 6 hours
- Note: may be protected by WAF in some environments; still standard to use.
- Normalization:
  - `category = "cyber_kev"` (non-geo)

### 1.9 Travel advisories (conflict/security)
- Canada Travel Updates RSS:
  - `https://travel.gc.ca/feeds/rss/eng/travel-updates-24.aspx`
- US State Dept Travel Advisories RSS:
  - `https://travel.state.gov/_res/rss/TAs.xml`
- UK GOV.UK Content API:
  - `https://www.gov.uk/api/content/foreign-travel-advice`
  - `https://www.gov.uk/api/content/foreign-travel-advice/{country-slug}`
- Poll 30-60 min for RSS, 1-6 hours for GOV.UK API
- Geo: country-level -> Tier C

### 1.10 ReliefWeb (humanitarian reports)
- Base:
  - `https://api.reliefweb.int/v1/`
- Endpoints:
  - `https://api.reliefweb.int/v1/reports`
  - `https://api.reliefweb.int/v1/disasters`
- Poll every 10-30 min
- Geo: country/region -> country centroid (Tier C), optional gazetteer refine later

---

## 2) Feed packs (YAML-driven sources)

Implement feed packs for configurable RSS sources instead of hardcoding in code:
- `feeds/global.yaml`
- `feeds/americas.yaml`
- `feeds/europe.yaml`
- `feeds/mena.yaml`
- `feeds/africa.yaml`
- `feeds/apac.yaml`
- `feeds/cyber.yaml`
- `feeds/disasters.yaml`

Each entry includes:
- `id`, `name`, `type`, `url`, `region`, `tags`, `poll_seconds`, `enabled`

Example entry:
```
- id: bbc_world
  name: "BBC News - World"
  type: rss
  url: "http://newsrss.bbc.co.uk/rss/newsonline_uk_edition/world/rss.xml"
  region: global
  tags: ["news", "world"]
  poll_seconds: 180
```

Add UI toggles to enable/disable feed packs and persist settings (can be stored in config for now; saved views come later).

---

## 3) Timeline playback and histogram

Implement the playback workflow:
- UI slider selects a time cursor within the last N hours.
- Map and incident list switch to playback view showing incidents as-of cursor time.
- Optional step playback (e.g., 5-minute increments).

Implementation specifics:
- Alpine state: `window="6h"`, `cursorTs=...`, `playing=false`.
- When cursor changes:
  - HTMX fetch `GET /partials/incidents?asof=...`
  - JS requests `GET /api/incidents?asof=...` to redraw markers
- Implement `GET /partials/timeline` to render a histogram of incident counts by time bucket.

---

## 4) Clustering improvements (category-aware)

Improve clustering rules per category:
- Tighten thresholds for noisy categories (news/social).
- Allow broader merging for structured feeds with consistent geo (earthquake/volcano).
- Keep rules cheap: simhash + optional token Jaccard/RapidFuzz.

---

## 5) UI additions

- Add a Cyber panel/layer that lists `cyber_cve` and `cyber_kev` incidents.
- Ensure cyber items are not drawn on the map by default.
- Add toggles for category layers and feed packs.
- Add/extend a Settings modal that includes feeds enabled, polling controls, and map tile configuration.

---

## 6) Performance and polling updates

Add polling defaults for new sources:
- GDACS: 5 min
- EONET: 10-15 min
- Volcano elevated: 5 min; per-volcano CAP/RSS: 5-10 min
- Tsunami: 60-120s when active, otherwise 5 min
- FIRMS: 10-15 min
- FAA: 2-5 min
- CDC/WHO advisories: 30-60 min
- NVD: 15 min
- KEV: 6h
- GOV.UK API: 1-6h
- ReliefWeb: 10-30 min

Continue honoring ETag/Last-Modified and Cache-Control where available.

---

## 7) Deliverables checklist
- New source plugins for GDACS, EONET, Volcano, Tsunami, FIRMS, FAA, CDC/WHO, NVD, KEV, travel advisories, ReliefWeb
- Feed pack loader + UI toggles
- Playback timeline slider + histogram
- Cyber panel
- Category-aware clustering adjustments
