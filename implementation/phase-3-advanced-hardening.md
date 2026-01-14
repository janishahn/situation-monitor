# Phase 3: Advanced Geotagging, OSINT, Maritime, and Hardening

Before you start
- Thoroughly explore the repo and its current status. Read README.md and SPEC.md again, and review Phase 1 and Phase 2 implementations.
- Run the test suite and confirm ingestion stability before adding new sources and hardening features.
 - Follow repo AGENTS.md rules: avoid micro-helpers/defensive typing, and run Ruff formatting/fixing if available.

Context (short)
- Phase 3 completes the spec with advanced geotagging, additional sources (OSINT + maritime), saved views, deployment/security, and reliability hardening.
- Focus on improving confidence tiers, incident lifecycle, and operational resilience.
- The app remains a single FastAPI service served via `uvicorn app.main:app` on one port (API + UI together).

Scope of Phase 3
- Implement Tier B geotagging (coords extraction + gazetteer matching) and robust Tier C fallback logic.
- Add MSI maritime ingestion with OpenAPI discovery.
- Add OSINT sources (Mastodon, optional Bluesky, Reddit RSS).
- Implement saved views.
- Add incident lifecycle heuristics (cooling/resolution).
- Add X embed panel (manual scan, not ingestion).
- Add security headers, deployment guidance, and hardening features (backoff, health UX, backup/restore, metrics endpoint).

---

## 1) Advanced geotagging (Tier B + Tier C)

### 1.1 Tier B heuristics (no ML)
1) Coordinate regex extraction
- Extract coordinates in text such as `12.34N 56.78E`, `-33.9, 151.2`.
- If coords extracted, set `location_confidence = B_coords_in_text` and store rationale.

2) Gazetteer match
- Use offline `places` table populated from:
  - Natural Earth (countries + admin1 + populated places) as base
  - Optional GeoNames cities15000 for better coverage
- N-gram scan (1-3 tokens) with normalization (casefold, strip punctuation).
- Score by place importance (population/importance) and proximity to any extracted coords.
- If multiple matches, prefer:
  - Matches near other hints (country mention)
  - Higher importance if ambiguous (e.g., "Georgia" context: Tbilisi vs Atlanta)
- Set `location_confidence = B_place_match` and a rationale string.

### 1.2 Tier C fallback
- Use source defaults (country-level or region-level sources).
- Use country centroid from gazetteer if country is detected.
- Else `U_unknown`.

Always write a short `location_rationale`.

---

## 2) Maritime safety (NGA MSI)

Primary site + API discovery:
- `https://msi.nga.mil/`
- Swagger UI: `https://msi.nga.mil/api/swagger-ui.html`
- Expected base URL: `https://msi.pub.kubic.nga.mil/`

Implementation approach:
- On install/first run, fetch MSI OpenAPI JSON.
- Implement discovery: try common locations such as `/v3/api-docs`, `/openapi.json`, or other discovered Swagger paths.
- Ingest NAVWARN and "persons in distress / hazards" categories.

Normalization:
- `category = "maritime_warning"`
- Geo: if API provides coordinates, use Tier A. Otherwise apply coordinate regex extraction (Tier B).

---

## 3) OSINT sources (ToS-safe, optional)

### 3.1 Mastodon (API-first)
Core endpoints (polling model):
- `GET https://{instance}/api/v1/timelines/tag/{tag}?limit=20&since_id={since_id}`
- `GET https://{instance}/api/v1/timelines/public?local=false&limit=20&since_id={since_id}`
- `GET https://{instance}/api/v1/accounts/{id}/statuses?limit=20&since_id={since_id}`
- `GET https://{instance}/api/v2/search?q={query}&type=accounts|statuses&resolve=true`

Auth:
- Support optional per-instance token: `MASTODON_TOKEN_{INSTANCE}` env vars.
- Respect instance rate limits; per-instance concurrency = 1.

Starter tags:
- `#earthquake`, `#wildfire`, `#flood`, `#tsunami`, `#storm`, `#breaking`, `#OSINT`

Normalization:
- `category = "social"`
- Geo: usually Tier B (gazetteer + coordinate extraction), rarely Tier A.
- Store post URL + author handle + instance in provenance.

### 3.2 Bluesky (optional, AT Protocol)
Endpoints:
- `GET https://bsky.social/xrpc/app.bsky.feed.getAuthorFeed?actor={handle}&limit=30`
- `GET https://bsky.social/xrpc/app.bsky.feed.searchPosts?q={query}&limit=30`

Auth:
- Optional: `BLUESKY_HANDLE` and `BLUESKY_APP_PASSWORD` for reliable access.
- Keep it optional due to rate limits and evolving API.

### 3.3 Reddit (RSS)
RSS endpoints:
- `https://www.reddit.com/r/worldnews/.rss`
- `https://www.reddit.com/r/geopolitics/.rss`
- `https://www.reddit.com/r/Cybersecurity/.rss`
- `https://www.reddit.com/r/osint/.rss`
- `https://www.reddit.com/r/news/.rss`

Notes:
- Send descriptive User-Agent.
- Poll every 2-5 minutes, back off on 429.

---

## 4) Saved views (presets)

Implement saved views as JSON blobs in SQLite `saved_views` table.
- Preset includes: filters, map viewport, time window, source sets.
- Example presets: "Crisis mode" (official alerts + disasters), "Cyber watch" (NVD/CISA/CERT), "Regional focus: East Asia".
- UI: dropdown to load and apply; include create/update/delete flows.

---

## 5) Incident lifecycle and quality

- Implement cooling/resolution heuristics:
  - `active` -> `cooling` after inactivity threshold
  - `resolved` after longer inactivity
- Keep active/cooling incidents beyond retention window.
- Continue incident merging based on simhash convergence + geo proximity.

---

## 6) X (Twitter) embed panel (manual scanning)

- Right-side tab "X Scan" with official embed widgets for manually configured lists/searches.
- Load embed script only when panel opens.
- Warn users about tracking and provide toggle to disable.

---

## 7) Security and exposure

### 7.1 Exposure model
- Default bind: `127.0.0.1:8000`.
- Optional LAN: `0.0.0.0`, recommend Tailscale or Nginx reverse proxy + basic auth.

### 7.2 Security headers
- CSP tight by default; allow map tiles and optional X embeds only when enabled.
- HSTS if TLS enabled.
- Disable framing except where needed for embeds.

---

## 8) Reliability and hardening

Failure modes and mitigations:
- Feed errors/timeouts: exponential backoff per source, show in health panel.
- Duplicate floods: tighten clustering thresholds per category; use URL canonicalization.
- Geo ambiguity: show Tier C clearly; never pretend city-level certainty.
- Map overload: default to clustering + category toggles; cap markers drawn (e.g., top 200 by score).

Additional hardening tasks:
- Backup/restore for SQLite.
- Metrics endpoint for basic operational stats.
- Health UX improvements (surface per-source status, last success, error rate).

---

## 9) Deliverables checklist
- Tier B geotagging implementation + populated gazetteer
- Maritime MSI ingestion with OpenAPI discovery
- Mastodon + Bluesky (optional) + Reddit RSS ingestion
- Saved views
- Incident cooling/resolution heuristics
- X embed panel
- Security headers and deployment docs
- Backup/restore and metrics endpoint
