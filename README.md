# Situation Monitor

Single-process FastAPI app (API + HTMX UI) for scan-first global incident awareness.

## Run

```bash
uv run uvicorn app.main:app --host 127.0.0.1 --port 8000
```

Default bind is `127.0.0.1:8000`. For LAN access, prefer Tailscale or an Nginx reverse proxy with basic auth.

## Key endpoints

- UI: `/`
- SSE: `/sse`
- Incidents API: `/api/incidents`
- Sources API: `/api/sources`
- Saved views API: `/api/saved-views`
- Metrics: `/metrics` (Prometheus-style text)

## Configuration

Environment variables (see `app/settings.py`):

- `DB_PATH` (default `data/situation-monitor.db`)
- `USER_AGENT` (default `situation-monitor/0.1`)
- `MAP_TILE_URL` (default OSM tiles)
- `FIRMS_API_KEY`, `NVD_API_KEY`

### OSINT sources

Mastodon (disabled unless configured):

- `MASTODON_INSTANCES` (comma-separated, e.g. `mastodon.social,infosec.exchange`)
- `MASTODON_TAGS` (comma-separated, defaults include `#earthquake,#wildfire,#flood,#tsunami,#storm,#breaking,#OSINT`)
- Optional per-instance token: `MASTODON_TOKEN_{INSTANCE}` where `{INSTANCE}` is the instance host uppercased with `.`, `-`, `:` replaced by `_` (example: `MASTODON_TOKEN_MASTODON_SOCIAL`)

Bluesky (optional, disabled unless creds are set):

- `BLUESKY_HANDLE`
- `BLUESKY_APP_PASSWORD`

Reddit RSS is included by default (ToS-safe RSS polling).

### X Scan (manual embeds, not ingestion)

In the Settings modal:

- Enable/disable X embeds (tracking risk)
- Configure embed URLs (one per line)

The embed script loads only when the **X Scan** tab opens.

## Gazetteer (Tier B geotagging)

`places` is seeded on startup from Natural Earth GeoJSON under `geo/data/`:

- `ne_110m_admin_0_countries.geojson`
- `ne_110m_admin_1_states_provinces.geojson`
- `ne_110m_populated_places_simple.geojson`

## Backup / restore

```bash
uv run python scripts/db_backup.py
uv run python scripts/db_restore.py path/to/backup.db
```

Run restore with the app stopped.
