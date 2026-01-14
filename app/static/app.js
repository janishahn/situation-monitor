(() => {
  const state = {
    map: null,
    markers: null,
    markerById: new Map(),
    tileUrl: null,
  };

  function qs() {
    return window.location.search || "";
  }

  async function fetchIncidents() {
    const res = await fetch("/api/incidents" + qs(), { headers: { Accept: "application/json" } });
    if (!res.ok) return [];
    return await res.json();
  }

  function markerPopupHtml(incident) {
    const last = incident.last_seen_at || "";
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

  function upsertMarker(incident) {
    if (incident.lat == null || incident.lon == null) return;
    ensureMap();

    const id = incident.incident_id;
    const latlng = [incident.lat, incident.lon];

    const existing = state.markerById.get(id);
    if (existing) {
      existing.setLatLng(latlng);
      existing.setPopupContent(markerPopupHtml(incident));
      return;
    }

    const marker = L.marker(latlng);
    marker.bindPopup(markerPopupHtml(incident));
    marker.on("click", () => {
      if (window.htmx) {
        const detail = document.getElementById("incident-detail");
        if (detail) detail.dataset.incidentId = id;
        window.htmx.ajax("GET", `/partials/incident/${id}`, { target: "#incident-detail", swap: "innerHTML" });
      }
    });
    state.markers.addLayer(marker);
    state.markerById.set(id, marker);
  }

  function replaceMarkers(incidents) {
    ensureMap();
    state.markers.clearLayers();
    state.markerById.clear();
    for (const incident of incidents) upsertMarker(incident);
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
    const dd = pad2(date.getUTCDate());
    const mm = pad2(date.getUTCMonth() + 1);
    const yyyy = date.getUTCFullYear();
    const hh = pad2(date.getUTCHours());
    const min = pad2(date.getUTCMinutes());
    const ss = pad2(date.getUTCSeconds());
    return `${dd}.${mm}.${yyyy} ${hh}:${min}:${ss}`;
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
        sourceEl.textContent = "Atomic time (Time.now UTC)";
      } catch (err) {
        hasAtomic = false;
        sourceEl.textContent = "Local time (fallback)";
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
        const data = JSON.parse(ev.data);
        upsertMarker(data);
        refreshIncidentsPanel();
        const activeId = document.getElementById("incident-detail")?.dataset?.incidentId;
        if (activeId && activeId === data.incident_id) {
          window.htmx.ajax("GET", `/partials/incident/${activeId}`, { target: "#incident-detail", swap: "innerHTML" });
        }
      });
    }
  }

  document.addEventListener("DOMContentLoaded", () => {
    ensureMap();
    refreshMarkers();
    initSse();
    initAtomicClock();

    document.body.addEventListener("htmx:afterSwap", (ev) => {
      if (ev.target && ev.target.id === "incident-list") refreshMarkers();
    });
  });

  window.SM = { refreshMarkers };
})();
