"use strict";
/* Telemetry: service health, stage latency, endpoint stats, status codes, live event stream, raw calls, callbacks, DB. */

const telemetryState = { kind: "all" };

async function loadTelemetry() {
  const [t, callbacks, calls, sys] = await Promise.all([
    api(`/api/telemetry?kind=${telemetryState.kind}&limit=80`),
    api("/api/callbacks").catch(() => []),
    api("/api/api-calls?limit=40"),
    api("/api/system-jobs"),
  ]);
  const svc = Object.fromEntries(t.services.map(x => [x.name, x]));
  const cons = svc["ConsumerApplication"].extra || {};
  const sched = t.db.scheduler_last_run;
  const schedOk = sched && (Date.now() - new Date(sched).getTime()) < 5 * 60 * 1000;
  const card = (name, ok, value, sub) => h("div", { class: "kpi" },
    h("div", { class: "kpi-label" }, name), h("div", { style: { margin: "8px 0 4px" } }, badge(ok ? "ok" : "down")),
    h("div", { class: "kpi-sub", text: value }), h("div", { class: "kpi-sub", text: sub || "\u00a0" }));
  const lat = n => svc[n].latency_ms !== undefined ? `health check ${fmtMs(svc[n].latency_ms)}` : (svc[n].detail?.error || "unreachable");
  replace("#t-services",
    card("Integration Layer", svc["Integration Layer"].ok, lat("Integration Layer"), "il-api:8000"),
    card("ConsumerApplication", svc["ConsumerApplication"].ok, cons.paused ? "consumption PAUSED" : `${cons.inflight ?? 0}/${cons.max_concurrency ?? 0} slots busy`,
      `claimed ${fmtNum(cons.counters?.claimed || 0)} · completed ${fmtNum(cons.counters?.completed || 0)}`),
    card("WWMG_Source", svc["WWMG_Source"].ok, lat("WWMG_Source"), "source app · :8002"),
    card("MyCareHub", svc["MyCareHub"].ok, lat("MyCareHub"), "member portal · :8003"),
    card("BigQuery", svc["BigQuery"].ok, svc["BigQuery"].latency_ms !== undefined ? `query ${fmtMs(svc["BigQuery"].latency_ms)}` : "", svc["BigQuery"].detail?.target || svc["BigQuery"].detail?.error || ""),
    card("PostgreSQL", true, `${fmtBytes(t.db.db_bytes)} · ${t.db.connections} connections`, `${t.db.active} active now`),
    card("Scheduler", schedOk, sched ? `last job ${fmtRelative(sched)}` : "no job run yet", `${sys.jobs.filter(j => !j.is_paused).length} system jobs active`));

  const st = t.stages;
  const a = st.all;
  replace("#t-stages",
    kpi("Queue wait", fmtMs(a.queue.p50), `p95 ${fmtMs(a.queue.p95)} · received to picked up`),
    kpi("Consumer processing", fmtMs(a.processing.p50), `p95 ${fmtMs(a.processing.p95)}`),
    kpi("Callback delivery", fmtMs(a.callback.p50), `p95 ${fmtMs(a.callback.p95)} · completed to delivered`),
    kpi("End-to-end", fmtMs(a.e2e.p50), `p95 ${fmtMs(a.e2e.p95)} · ${fmtNum(a.requests)} requests`));
  const pair = x => `${fmtMs(x.p50)} / ${fmtMs(x.p95)}`;
  replace("#t-stages-backend", table([
    { label: "Backend", render: r => backendTag(r.name) },
    { label: "Requests", num: true, render: r => fmtNum(r.v.requests) },
    { label: "Queue wait", num: true, render: r => pair(r.v.queue) },
    { label: "Processing", num: true, render: r => pair(r.v.processing) },
    { label: "Callback", num: true, render: r => pair(r.v.callback) },
    { label: "End-to-end", num: true, render: r => pair(r.v.e2e) },
  ], [{ name: "postgres", v: st.postgres }, { name: "bigquery", v: st.bigquery }]));

  replace("#t-endpoints", table([
    { label: "Dir", render: e => h("span", { class: "kind http", text: e.direction === "INBOUND" ? "in" : "out" }) },
    { label: "Endpoint", render: e => h("span", { class: "mono", text: `${e.http_method} ${e.endpoint}` }) },
    { label: "Calls", num: true, render: e => fmtNum(e.calls) },
    { label: "p50", num: true, render: e => fmtMs(e.p50_ms) },
    { label: "p95", num: true, render: e => fmtMs(e.p95_ms) },
    { label: "Errors", num: true, render: e => Number(e.errors) ? h("span", { class: "red", text: fmtNum(e.errors) }) : "0" },
    { label: "Last call", render: e => fmtRelative(e.last_call) },
  ], t.endpoints, { empty: "No API calls in the last 24 hours." }));

  const codeClass = c => c === "network error" ? "5xx" : `${String(c)[0]}xx`;
  const codeColor = { "2xx": "var(--good)", "4xx": "var(--warning)", "5xx": "var(--critical)" };
  hbarList($("#t-codes"), t.codes.map(c => ({ label: h("span", { class: `badge s-${codeClass(c.code)}`, text: c.code }), title: `HTTP ${c.code}`,
    value: Number(c.count), color: codeColor[codeClass(c.code)] || "var(--neutral)" })), { empty: "No API calls yet." });

  replace("#t-stream", table([
    { label: "Time", render: e => h("span", { class: "nowrap mono", text: fmtTime(e.ts) }) },
    { label: "Type", render: e => h("span", { class: `kind ${e.kind === "http" ? "http" : ""}`, text: e.kind }) },
    { label: "Event", render: e => e.kind === "state" ? badge(e.title) : h("span", { class: "mono", text: e.title }) },
    { label: "Detail", cls: "trunc", render: e => h("span", { title: e.detail, text: e.detail }) },
    { label: "Source", render: e => h("span", { class: "muted", text: e.source || "" }) },
    { label: "Code", render: e => e.kind === "http" ? httpBadge(e.code === "ERR" ? null : Number(e.code))
        : (e.code === "OK" || e.code === "ERROR" ? badge(e.code) : (e.code ? h("span", { class: "mono red", text: e.code }) : null)) },
    { label: "Latency", num: true, render: e => e.latency_ms === null ? "" : fmtMs(e.latency_ms) },
    { label: "Request", render: e => e.kind !== "job" && e.ref ? h("span", { class: "mono", text: shortId(e.ref) }) : "" },
  ], t.stream, { onRow: e => { if (e.kind !== "job" && e.ref) openRequest(e.ref); }, empty: "No events yet." }));

  replace("#t-calls", table([
    { label: "Time", render: c => h("span", { class: "nowrap", text: fmtTime(c.called_at) }) },
    { label: "Call", cls: "trunc", render: c => h("span", { class: "mono", title: c.api_url, text: `${c.http_method} ${c.endpoint || c.api_url}` }) },
    { label: "Status", render: c => httpBadge(c.response_status_code) },
    { label: "Latency", num: true, render: c => fmtMs(c.latency_ms) },
    { label: "Response", render: c => c.response_body ? jsonToggle("body", c.response_body) : h("span", { class: "muted", text: "not stored" }) },
  ], calls, { onRow: c => c.request_id && openRequest(c.request_id), empty: "No API calls yet." }));

  replace("#t-callbacks", table([
    { label: "Received", render: c => h("span", { class: "nowrap", text: fmtTime(c._received_at) }) },
    { label: "By", render: c => c.receiver },
    { label: "Request", render: c => h("span", { class: "mono", text: shortId(c.request_id) }) },
    { label: "Event", render: c => h("span", { class: "mono", text: c.event }) },
    { label: "Status", render: c => badge(c.status) },
    { label: "Outcome", render: c => c.workflow_status },
    { label: "Payload", render: c => jsonToggle("body", c) },
  ], callbacks.slice(0, 40), { onRow: c => openRequest(c.request_id), empty: "No callbacks received yet." }));

  replace("#t-db", kv([
    ["Server", (t.db.version || "").split(" on ")[0]], ["Database size", fmtBytes(t.db.db_bytes)],
    ["Connections", `${t.db.connections} (${t.db.active} active)`], ["Scheduler sessions", t.db.scheduler_sessions],
    ["request_metadata rows (PostgreSQL)", fmtNum(t.db.requests)], ["BigQuery requests", fmtNum(t.db.bigquery_rows)],
    ["api_call_log rows", fmtNum(t.db.api_calls)],
    ["request_event_log rows", fmtNum(t.db.events)], ["job_submissions rows", fmtNum(t.db.job_submissions)],
  ]));
}

function initTelemetry() {
  $$("#t-kind button").forEach(b => b.addEventListener("click", () => {
    telemetryState.kind = b.dataset.kind;
    $$("#t-kind button").forEach(x => x.setAttribute("aria-pressed", String(x === b)));
    loadTelemetry().catch(console.error);
  }));
}
