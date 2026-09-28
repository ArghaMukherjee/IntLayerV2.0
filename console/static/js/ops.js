"use strict";
/* Dashboard, request list and the request telemetry panel. */

const STATUS_COLOR = { COMPLETED: "var(--good)", FAILED: "var(--critical)", PENDING: "var(--warning)",
  IN_PROGRESS: "var(--info)", CANCELLED: "var(--neutral)" };

const REQUEST_COLUMNS = [
  { label: "Received", render: r => h("span", { class: "nowrap", text: fmtTime(r.received_at) }) },
  { label: "Request", render: r => h("span", { class: "mono", title: r.request_id, text: shortId(r.request_id) }) },
  { label: "Source", render: r => h("span", { class: "nowrap", text: r.source_app }) },
  { label: "Workflow", render: r => r.workflow_name },
  { label: "Stored in", render: r => backendTag(r.storage_backend) },
  { label: "Status", render: r => badge(r.status) },
  { label: "Workflow status", render: r => r.workflow_status },
  { label: "Retries", num: true, render: r => fmtNum(r.retry_count) },
  { label: "End-to-end", num: true, render: r => TERMINAL.has(r.status) ? fmtMs(r.end_to_end_ms) : "running" },
  { label: "Callback", render: r => r.callback_status ? badge(r.callback_status) : h("span", { class: "muted", text: "polling" }) },
  { label: "Error", cls: "trunc", render: r => r.error_code ? h("span", { class: "mono red", text: r.error_code }) : null },
];

/* ---------------------------------------------------------------- dashboard */
function flowNode(title, ok, meta) {
  return h("div", { class: "flow-node" },
    h("div", { class: "flow-title" }, h("span", { text: title }), badge(ok ? "ok" : "down")),
    meta.map(t => h("div", { class: "flow-meta", text: t, title: t })));
}

async function loadDashboard() {
  const [o, recent] = await Promise.all([api("/api/overview"), api("/api/requests?limit=8")]);
  const k = o.kpis, q = o.queue;
  const counts = Object.fromEntries(o.statuses.map(x => [x.status, Number(x.count)]));
  const svc = Object.fromEntries(o.services.map(x => [x.name, x]));
  const stats = svc["ConsumerApplication"].extra || {};
  const pb = o.per_backend || { postgres: {}, bigquery: {} };
  const b24 = o.backend_24h || {};
  const bqDetail = svc["BigQuery"].detail || {};
  replace("#flow",
    flowNode("Sources", svc["WWMG_Source"].ok && svc["MyCareHub"].ok,
      [`WWMG_Source ${svc["WWMG_Source"].ok ? "up" : "DOWN"} · MyCareHub ${svc["MyCareHub"].ok ? "up" : "DOWN"}`, `${fmtNum(q.callbacks_delivered)} callbacks delivered`]),
    flowNode("Integration Layer", svc["Integration Layer"].ok, ["Validates, routes, delivers", `${fmtNum(k.total)} requests in 24 h`]),
    flowNode("PostgreSQL", true, [`${fmtNum(b24.postgres || 0)} requests in 24 h`, `${fmtNum(pb.postgres.PENDING || 0)} pending, ${fmtNum(pb.postgres.IN_PROGRESS || 0)} in progress`]),
    flowNode("BigQuery", svc["BigQuery"].ok, [`${fmtNum(b24.bigquery || 0)} requests in 24 h`, bqDetail.target || bqDetail.error || ""]),
    flowNode("ConsumerApplication", svc["ConsumerApplication"].ok, [stats.paused ? "Workers PAUSED" : (stats.running ? "Workers running" : "Workers stopped"),
      `${fmtNum(stats.inflight ?? 0)} of ${fmtNum(stats.max_concurrency ?? 0)} slots busy`]));
  const finished = Number(k.completed) + Number(k.failed) + Number(k.cancelled);
  replace("#kpis",
    kpi("Requests, 24 h", fmtNum(k.total), `${fmtNum(b24.postgres || 0)} PostgreSQL · ${fmtNum(b24.bigquery || 0)} BigQuery`),
    kpi("Success rate", pct(Number(k.completed), finished), `of ${fmtNum(finished)} finished`),
    kpi("In flight", fmtNum(k.in_flight), q.oldest_pending_s ? `oldest pending ${fmtDuration(q.oldest_pending_s)}` : "queue is empty"),
    kpi("Failed, 24 h", fmtNum(k.failed), `${fmtNum(k.cancelled)} cancelled`),
    kpi("Median end-to-end", fmtMs(k.p50_ms), `p95 ${fmtMs(k.p95_ms)}`),
    kpi("Callbacks delivered", fmtNum(q.callbacks_delivered), `${fmtNum(q.callbacks_pending)} pending, ${fmtNum(q.callbacks_failed)} failed`));
  const hourLabel = t => new Date(t).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
  barChart($("#hourly-chart"), o.hourly.map(d => ({
    label: `${hourLabel(d.hour)} - ${hourLabel(new Date(d.hour).getTime() + 3600e3)}`, value: Number(d.received),
    tip: [["Received", fmtNum(d.received)], ["PostgreSQL", fmtNum(d.postgres)], ["BigQuery", fmtNum(d.bigquery)],
          ["Completed", fmtNum(d.completed)], ["Failed", fmtNum(d.failed)]] })),
    { xLabel: (i, d) => i === 23 ? "now" : (i % 6 === 0 ? d.label.split(" - ")[0] : null), aria: "Requests received per hour, last 24 hours" });
  const order = ["COMPLETED", "IN_PROGRESS", "PENDING", "FAILED", "CANCELLED"];
  hbarList($("#status-chart"), order.map(st => ({ label: badge(st), title: BADGES[st][1], value: counts[st] || 0, color: STATUS_COLOR[st] })));
  replace("#recent-table", table(REQUEST_COLUMNS, recent, { onRow: r => openRequest(r.request_id),
    empty: "No requests yet. Send one from “Payloads & send” or start a job." }));
}

/* ---------------------------------------------------------------- requests tab */
async function loadRequests() {
  const params = new URLSearchParams({ limit: 200 });
  const st = $("#r-status").value, wf = $("#r-workflow").value, q = $("#r-search").value.trim(), be = $("#r-backend").value;
  if (st) params.set("status", st);
  if (be) params.set("backend", be);
  if (wf) params.set("workflow", wf);
  if (q) params.set("q", q);
  const rows = await api(`/api/requests?${params}`);
  $("#r-count").textContent = `${rows.length} shown`;
  replace("#requests-table", table(REQUEST_COLUMNS, rows, { onRow: r => openRequest(r.request_id), empty: "No requests match the filters." }));
}

/* ---------------------------------------------------------------- request telemetry panel */
async function requestAction(id, action) {
  const res = await api(`/api/requests/${id}/${action}`, { method: "POST" });
  toast(res.http_status < 300 ? (action === "cancel" ? "Request cancelled" : "Request re-queued")
    : (res.response?.error?.message || `HTTP ${res.http_status}`));
  refreshPanel();
}

function openRequest(id) {
  return openPanel("Request telemetry", id, async () => {
    const d = await api(`/api/requests/${id}`);
    const r = d.request;
    const stageMs = (a, b) => (a && b) ? fmtMs(new Date(b) - new Date(a)) : "–";
    const actions = [];
    if (r.status === "PENDING") actions.push(h("button", { class: "btn sm danger", onclick: () => requestAction(id, "cancel"), text: "Cancel request" }));
    if (r.status === "FAILED") actions.push(h("button", { class: "btn sm primary", onclick: () => requestAction(id, "replay"), text: "Replay (admin)" }));
    return [
      h("div", { class: "card accent" },
        h("div", { class: "card-head" }, h("div", { class: "result-head" }, badge(r.status),
          r.workflow_status ? h("span", { class: "muted", text: `workflow status: ${r.workflow_status}` }) : null,
          backendTag(r.storage_backend),
          d.job ? h("span", { class: "kind", text: `job: ${d.job.job_name}` }) : null),
          actions.length ? h("div", { class: "actions" }, actions) : null),
        progressSteps(r)),
      h("div", { class: "kpis" },
        kpi("Queue wait", stageMs(r.received_at, r.picked_at), "received to picked up"),
        kpi("Processing", fmtMs(r.processing_duration_ms), "inside ConsumerApplication"),
        kpi("End-to-end", stageMs(r.received_at, r.workflow_completed_at), "received to completed"),
        kpi("Callback", stageMs(r.workflow_completed_at, r.callback_delivered_at), r.callback_url ? `${r.callback_attempts} attempt(s)` : "not requested")),
      h("div", { class: "card" }, h("h3", { text: "Metadata" }), kv([
        ["Workflow", r.workflow_name], ["Success flag", r.success_flag === null ? "in flight" : String(r.success_flag)],
        ["Source → target", `${r.source_app} → ${r.target_app}`], ["Retries", `${r.retry_count} of ${r.max_retries}`],
        ["Correlation id", h("span", { class: "mono", text: r.correlation_id })], ["Idempotency key", r.idempotency_key],
        ["Received", fmtTime(r.received_at, true)], ["Picked up", fmtTime(r.picked_at, true)],
        ["Completed", fmtTime(r.workflow_completed_at, true)], ["Updated", fmtTime(r.updated_at, true)],
        ["API call", `${r.http_method} ${r.api_url}`], ["Client IP", r.client_ip],
        ["IL server", `${r.il_server_hostname ?? "–"} (${r.il_server_ip ?? "–"})`], ["IL instance", r.il_instance_id],
        ["Consumer worker", r.consumer_worker_id ? `${r.consumer_worker_id} on ${r.consumer_worker_host}` : null], ["Stored in", h("span", {}, backendTag(r.storage_backend), " ", r.db_server_addr || "")],
        ["Schema version", r.input_schema_version], ["Priority", r.priority],
        ["Error", r.error_code ? `${r.error_code}: ${r.error_message ?? ""}` : null],
        ["Callback", r.callback_url ? `${r.callback_status ?? "waiting"} → ${r.callback_url}` : "not requested (polling)"],
      ])),
      h("div", { class: "grid-2" },
        h("div", { class: "card" }, h("h3", { text: `Input JSON (${r.source_app})` }), jsonView(r.input_json)),
        h("div", { class: "card" }, h("h3", { text: "Output JSON (ConsumerApplication)" }), jsonView(r.output_json))),
      h("div", { class: "card" }, h("h3", { text: "HTTP calls with headers and responses (api_call_log)" }),
        table([
          { label: "Time", render: c => fmtTime(c.called_at) },
          { label: "Dir", render: c => h("span", { class: "kind http", text: c.direction === "INBOUND" ? "in" : "out" }) },
          { label: "Call", cls: "trunc", render: c => h("span", { class: "mono", title: c.api_url, text: `${c.http_method} ${c.endpoint || c.api_url}` }) },
          { label: "Status", render: c => httpBadge(c.response_status_code) },
          { label: "Latency", num: true, render: c => fmtMs(c.latency_ms) },
          { label: "Details", render: c => h("div", { style: { display: "grid", gap: "4px" } },
              c.response_body ? jsonToggle("response", c.response_body) : null,
              c.request_headers ? jsonToggle("headers", c.request_headers) : null) },
        ], d.api_calls, { empty: "No API calls logged yet." })),
      h("div", { class: "card" }, h("h3", { text: "Status history (request_event_log)" }),
        h("ul", { class: "timeline" }, d.events.map(e => h("li", {},
          h("span", { class: "muted small nowrap", text: fmtTime(e.event_at) }),
          h("span", {}, badge(e.new_status), " ", h("span", { class: "muted small",
            text: `by ${e.actor}` + (e.retry_count ? ` · retry ${e.retry_count}` : "") + (e.error_code ? ` · ${e.error_code}` : "") })))))),
    ];
  });
}
