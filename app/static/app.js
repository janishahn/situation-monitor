(() => {
  const state = {
    map: null,
    markers: null,
    markerById: new Map(),
    tileUrl: null,
    savedViews: [],
  };

  const categoryStyles = {
    earthquake: { fill: "#ef4444", label: "EQ" },
    weather_alert: { fill: "#f59e0b", label: "WX" },
    tropical_cyclone: { fill: "#06b6d4", label: "TC" },
    tsunami: { fill: "#0ea5e9", label: "TS" },
    volcano: { fill: "#b45309", label: "VO" },
    wildfire: { fill: "#f97316", label: "WF" },
    aviation_disruption: { fill: "#64748b", label: "AV" },
    health_advisory: { fill: "#10b981", label: "HA" },
    travel_advisory: { fill: "#14b8a6", label: "TA" },
    maritime_warning: { fill: "#1e3a8a", label: "MW" },
    disaster: { fill: "#7f1d1d", label: "DS" },
    news: { fill: "#94a3b8", label: "NW" },
    social: { fill: "#ec4899", label: "SO" },
    cyber_cve: { fill: "#84cc16", label: "CV" },
    cyber_kev: { fill: "#4d7c0f", label: "KE" },
  };
  const defaultStyle = { fill: "#3b82f6", label: "?" };

  function qs() {
    return window.location.search || "";
  }

  function isPlayback() {
    const params = new URLSearchParams(window.location.search);
    const asof = params.get("asof");
    if (!asof) return false;
    const t = Date.parse(asof);
    if (!Number.isFinite(t)) return false;
    return Date.now() - t > 60 * 1000;
  }

  async function fetchIncidents() {
    const res = await fetch("/api/incidents" + qs(), { headers: { Accept: "application/json" } });
    if (!res.ok) return [];
    return await res.json();
  }

  function markerPopupHtml(incident) {
    const lastRaw = incident.last_seen_at || "";
    const last = lastRaw ? formatEuDateTimeShort(new Date(lastRaw)) : "";
    const sources = incident.source_count ?? "";
    const items = incident.item_count ?? "";
    return `
      <div class="text-sm">
        <div class="font-semibold">${escapeHtml(incident.title || "")}</div>
        <div class="opacity-80">${escapeHtml(incident.category || "")}</div>
        <div class="mt-1 opacity-80">Last: ${escapeHtml(last)}</div>
        <div class="opacity-80">Sources: ${sources} • Items: ${items}</div>
      </div>
    `;
  }

  function escapeHtml(str) {
    return String(str)
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");
  }

  function ensureMap() {
    if (state.map) return;

    const el = document.getElementById("map");
    if (!el) return;
    state.tileUrl = el.dataset.tileUrl;

    state.map = L.map("map", { zoomControl: true, worldCopyJump: true }).setView([20, 0], 2);
    L.tileLayer(state.tileUrl, {
      maxZoom: 19,
      attribution: "&copy; OpenStreetMap contributors",
    }).addTo(state.map);

    state.markers = L.markerClusterGroup({ showCoverageOnHover: false, maxClusterRadius: 50 });
    state.map.addLayer(state.markers);
  }

  function markerIconFor(incident) {
    const style = categoryStyles[incident.category] || defaultStyle;
    const isStaticVolcano =
      incident.category === "volcano" &&
      incident.location_rationale === "USGS HANS elevated list";
    const shape = isStaticVolcano ? "circle" : "pin";
    return L.divIcon({
      className: "sm-marker",
      html: markerSvg({ fill: style.fill, label: style.label, shape }),
      iconSize: shape === "circle" ? [26, 26] : [28, 40],
      iconAnchor: shape === "circle" ? [13, 13] : [14, 40],
      popupAnchor: shape === "circle" ? [0, -12] : [0, -34],
    });
  }

  function markerSvg({ fill, label, shape }) {
    if (shape === "circle") {
      return `
        <svg viewBox="0 0 26 26" width="26" height="26" aria-hidden="true">
          <circle cx="13" cy="13" r="12" fill="${fill}" stroke="rgba(0,0,0,0.35)" stroke-width="2"></circle>
          <circle cx="13" cy="13" r="7" fill="rgba(255,255,255,0.18)"></circle>
          <text class="sm-marker-label" x="13" y="13">${label}</text>
        </svg>
      `;
    }

    return `
      <svg viewBox="0 0 28 40" width="28" height="40" aria-hidden="true">
        <path class="sm-marker-shadow" d="M14 1C8.2 1 3.5 5.7 3.5 11.5c0 8.2 9.4 22.2 9.9 23a1 1 0 0 0 1.2 0c0.5-0.8 9.9-14.8 9.9-23C24.5 5.7 19.8 1 14 1z" fill="${fill}" stroke="rgba(0,0,0,0.35)" stroke-width="1.5"></path>
        <circle cx="14" cy="12" r="6.5" fill="rgba(255,255,255,0.2)"></circle>
        <text class="sm-marker-label" x="14" y="12">${label}</text>
      </svg>
    `;
  }

  function upsertMarker(incident) {
    if (incident.lat == null || incident.lon == null) return;
    ensureMap();

    const id = incident.incident_id;
    const latlng = [incident.lat, incident.lon];

    const existing = state.markerById.get(id);
    if (existing) {
      existing.setLatLng(latlng);
      existing.setPopupContent(markerPopupHtml(incident));
      existing.setIcon(markerIconFor(incident));
      return;
    }

    const marker = L.marker(latlng, { icon: markerIconFor(incident) });
    marker.bindPopup(markerPopupHtml(incident));
    marker.on("click", () => {
      if (window.htmx) {
        window.dispatchEvent(new CustomEvent("sm:set-right-tab", { detail: { tab: "incident" } }));
        const detail = document.getElementById("incident-detail-content");
        if (detail) detail.dataset.incidentId = id;
        window.htmx.ajax("GET", `/partials/incident/${id}`, {
          target: "#incident-detail-content",
          swap: "innerHTML",
        });
      }
    });
    state.markers.addLayer(marker);
    state.markerById.set(id, marker);
  }

  function replaceMarkers(incidents) {
    ensureMap();
    state.markers.clearLayers();
    state.markerById.clear();
    const ranked = [...incidents].sort((a, b) => (b.severity_score ?? 0) - (a.severity_score ?? 0)).slice(0, 200);
    for (const incident of ranked) upsertMarker(incident);
  }

  async function refreshMarkers() {
    const incidents = await fetchIncidents();
    replaceMarkers(incidents);
  }

  function refreshIncidentsPanel() {
    if (!window.htmx) return;
    window.htmx.ajax("GET", "/partials/incidents" + qs(), { target: "#incident-list", swap: "innerHTML" });
  }

  function pad2(value) {
    return String(value).padStart(2, "0");
  }

  function formatEuDateTime(date) {
    const dd = pad2(date.getDate());
    const mm = pad2(date.getMonth() + 1);
    const yyyy = date.getFullYear();
    const hh = pad2(date.getHours());
    const min = pad2(date.getMinutes());
    const ss = pad2(date.getSeconds());
    return `${dd}.${mm}.${yyyy} ${hh}:${min}:${ss}`;
  }

  function formatEuDateTimeShort(date) {
    const dd = pad2(date.getDate());
    const mm = pad2(date.getMonth() + 1);
    const yyyy = date.getFullYear();
    const hh = pad2(date.getHours());
    const min = pad2(date.getMinutes());
    return `${dd}.${mm}.${yyyy} ${hh}:${min}`;
  }

  function getTimezoneAbbr() {
    return Intl.DateTimeFormat().resolvedOptions().timeZone;
  }

  async function initAtomicClock() {
    const timeEl = document.getElementById("now-time");
    const sourceEl = document.getElementById("now-source");
    if (!timeEl || !sourceEl) return;

    let offsetMs = 0;
    let hasAtomic = false;

    async function syncTime() {
      const t0 = Date.now();
      try {
        const res = await fetch("https://time.now/developer/api/timezone/UTC", {
          headers: { Accept: "application/json" },
        });
        if (!res.ok) throw new Error("time_api_error");
        const data = await res.json();
        const t1 = Date.now();
        const dt = Date.parse(data.utc_datetime || data.datetime);
        const mid = t0 + (t1 - t0) / 2;
        offsetMs = dt - mid;
        hasAtomic = true;
        sourceEl.textContent = `Atomic time (${getTimezoneAbbr()})`;
      } catch (err) {
        hasAtomic = false;
        sourceEl.textContent = `Local time (${getTimezoneAbbr()})`;
      }
    }

    function tick() {
      const now = new Date(Date.now() + (hasAtomic ? offsetMs : 0));
      timeEl.textContent = formatEuDateTime(now);
    }

    await syncTime();
    tick();
    setInterval(tick, 1000);
    setInterval(syncTime, 10 * 60 * 1000);
  }

  function initSse() {
    const es = new EventSource("/sse");
    for (const type of ["incident.created", "incident.updated"]) {
      es.addEventListener(type, (ev) => {
        if (isPlayback()) return;
        const data = JSON.parse(ev.data);
        upsertMarker(data);
        refreshIncidentsPanel();
        const activeId = document.getElementById("incident-detail-content")?.dataset?.incidentId;
        if (activeId && activeId === data.incident_id) {
          window.htmx.ajax("GET", `/partials/incident/${activeId}`, {
            target: "#incident-detail-content",
            swap: "innerHTML",
          });
        }
      });
    }
  }

  async function loadSavedViews() {
    const select = document.getElementById("saved-view-select");
    if (!select) return;

    const res = await fetch("/api/saved-views", { headers: { Accept: "application/json" } });
    if (!res.ok) return;
    const views = await res.json();
    state.savedViews = views;

    select.innerHTML = '<option value="">Saved view…</option>';
    for (const view of views) {
      const opt = document.createElement("option");
      opt.value = view.view_id;
      opt.textContent = view.name;
      select.appendChild(opt);
    }
  }

  function currentMapView() {
    ensureMap();
    if (!state.map) return null;
    const center = state.map.getCenter();
    return { center: [center.lat, center.lng], zoom: state.map.getZoom() };
  }

  async function currentEnabledSources() {
    const res = await fetch("/api/sources", { headers: { Accept: "application/json" } });
    if (!res.ok) return [];
    const sources = await res.json();
    return sources.filter((s) => s.enabled).map((s) => s.source_id);
  }

  async function currentViewConfig() {
    const form = document.getElementById("filters-form");
    if (!form) return null;

    const windowValue = form.querySelector('select[name="window"]')?.value || "6h";
    const q = form.querySelector('input[name="q"]')?.value || "";
    const minSeverity = form.querySelector('input[name="min_severity"]')?.value || "";
    const categoriesCsv = form.querySelector('input[name="categories"]')?.value || "";
    const categories = categoriesCsv.split(",").map((s) => s.trim()).filter(Boolean);
    const map = currentMapView();
    const enabledSourceIds = await currentEnabledSources();

    return {
      window: windowValue,
      categories,
      q,
      min_severity: minSeverity,
      map,
      enabled_source_ids: enabledSourceIds,
    };
  }

  function applyViewConfig(config) {
    if (!config) return;

    if (config.map && state.map) {
      const c = config.map.center;
      const z = config.map.zoom;
      if (Array.isArray(c) && c.length === 2 && Number.isFinite(c[0]) && Number.isFinite(c[1]) && Number.isFinite(z)) {
        state.map.setView([c[0], c[1]], z);
      }
    }

    window.dispatchEvent(
      new CustomEvent("sm:set-filters", {
        detail: {
          window: config.window,
          cats: config.categories,
          q: config.q,
          minSeverity: config.min_severity,
        },
      }),
    );
  }

  function initSavedViewsUi() {
    const select = document.getElementById("saved-view-select");
    const loadBtn = document.getElementById("saved-view-load");
    const saveBtn = document.getElementById("saved-view-save");
    const updateBtn = document.getElementById("saved-view-update");
    const deleteBtn = document.getElementById("saved-view-delete");
    if (!select || !loadBtn || !saveBtn || !updateBtn || !deleteBtn) return;

    loadBtn.addEventListener("click", async () => {
      const id = select.value;
      if (!id) return;
      const view = state.savedViews.find((v) => v.view_id === id);
      if (!view) return;
      await fetch(`/api/saved-views/${id}/apply`, { method: "POST" });
      applyViewConfig(view.config);
    });

    saveBtn.addEventListener("click", async () => {
      const name = prompt("Saved view name");
      if (!name) return;
      const config = await currentViewConfig();
      if (!config) return;
      const res = await fetch("/api/saved-views", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name, config }),
      });
      if (res.ok) await loadSavedViews();
    });

    updateBtn.addEventListener("click", async () => {
      const id = select.value;
      if (!id) return;
      const config = await currentViewConfig();
      if (!config) return;
      const res = await fetch(`/api/saved-views/${id}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ config }),
      });
      if (res.ok) await loadSavedViews();
    });

    deleteBtn.addEventListener("click", async () => {
      const id = select.value;
      if (!id) return;
      if (!confirm("Delete saved view?")) return;
      const res = await fetch(`/api/saved-views/${id}`, { method: "DELETE" });
      if (res.ok) await loadSavedViews();
    });
  }

  let xScriptPromise = null;
  function openXScan() {
    const panel = document.getElementById("x-scan-panel");
    if (!panel) return;
    if (panel.dataset.xEnabled !== "1") return;

    if (!xScriptPromise) {
      xScriptPromise = new Promise((resolve, reject) => {
        const script = document.createElement("script");
        script.src = "https://platform.twitter.com/widgets.js";
        script.async = true;
        script.onload = resolve;
        script.onerror = reject;
        document.head.appendChild(script);
      });
    }

    xScriptPromise.then(() => {
      if (window.twttr?.widgets?.load) window.twttr.widgets.load(panel);
    });
  }

  document.addEventListener("DOMContentLoaded", () => {
    ensureMap();
    refreshMarkers();
    initSse();
    initAtomicClock();
    loadSavedViews().then(initSavedViewsUi);

    document.body.addEventListener("htmx:afterSwap", (ev) => {
      if (ev.target && ev.target.id === "incident-list") refreshMarkers();
    });

    setInterval(() => {
      if (isPlayback()) return;
      refreshIncidentsPanel();
    }, 30_000);
  });

  window.SM = { refreshMarkers, openXScan, formatEuDateTime, formatEuDateTimeShort };
})();
