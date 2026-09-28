"use strict";
/* Analytics dashboard: reads the data warehouse (dwh.fact_request + dimensions), which is loaded
   from both PostgreSQL and BigQuery. */

const analyticsState = { range: "24h", workflow: "", backend: "" };

function bucketLabel(iso, range) {
  const d = new Date(iso);
  if (range === "30d") return d.toLocaleDateString([], { month: "short", day: "numeric" });
  if (range === "7d") return `${d.toLocaleDateString([], { weekday: "short" })} ${d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}`;
  return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
}
function tickEvery(n) { return n <= 12 ? 2 : n <= 24 ? 4 : n <= 30 ? 5 : 7; }

async function loadAnalytics() {
  const tz = Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC";
  const params = new URLSearchParams({ range: analyticsState.range, tz, offset: -new Date().getTimezoneOffset() });
  if (analyticsState.workflow) params.set("workflow", analyticsState.workflow);
  if (analyticsState.backend) params.set("backend", analyticsState.backend);
  const a = await api(`/api/analytics?${params}`);
  const k = a.kpis, finished = Number(k.completed) + Number(k.failed) + Number(k.cancelled);

  const wms = a.warehouse || [];
  $("#a-warehouse").textContent = wms.map(w => `${w.job_name === "fact_request" ? "PostgreSQL" : "BigQuery"} load ${w.last_run_at ? fmtRelative(w.last_run_at) : "pending"}${w.is_paused ? " (paused)" : ""}${w.run_requested ? " (queued)" : ""}`).join(" · ");
  $("#a-bucket").textContent = `per ${a.bucket}`;

  replace("#a-kpis",
    kpi("Requests", fmtNum(k.total), `${fmtNum(k.in_flight)} still in flight`),
    kpi("Success rate", pct(Number(k.completed), finished), `${fmtNum(k.completed)} of ${fmtNum(finished)} finished`),
    kpi("Business rejections", fmtNum(k.rejected), "workflow_status REJECTED"),
    kpi("Failed", fmtNum(k.failed), `${fmtNum(k.cancelled)} cancelled`),
    kpi("Retried", fmtNum(k.retried), `${pct(Number(k.retried), Number(k.total))} of requests`),
    kpi("Median end-to-end", fmtMs(k.p50_e2e), `p95 ${fmtMs(k.p95_e2e)}`),
    kpi("p95 queue wait", fmtMs(k.p95_queue), "received to picked up"),
    kpi("p95 processing", fmtMs(k.p95_processing), `${fmtBytes(k.input_bytes)} in, ${fmtBytes(k.output_bytes)} out`));

  const n = a.series.length, every = tickEvery(n);
  const xl = (i, d) => (i % every === 0) ? d.short : null;
  const series = a.series.map(b => ({ ...b, full: bucketLabel(b.bucket, a.range), short: bucketLabel(b.bucket, a.range === "7d" ? "24h" : a.range) }));
  barChart($("#a-throughput"), series.map(b => ({ label: b.full, short: b.short, value: Number(b.received),
    tip: [["Received", fmtNum(b.received)], ["PostgreSQL", fmtNum(b.postgres)], ["BigQuery", fmtNum(b.bigquery)],
          ["Completed", fmtNum(b.completed)], ["Failed", fmtNum(b.failed)]] })),
    { xLabel: xl, aria: "Requests received per time bucket" });
  lineChart($("#a-success"), series.map(b => ({ label: b.full, short: b.short,
    value: Number(b.finished) ? (Number(b.completed) / Number(b.finished)) * 100 : null,
    tip: [["Success rate", Number(b.finished) ? pct(Number(b.completed), Number(b.finished)) : "no finished requests"],
          ["Completed", fmtNum(b.completed)], ["Finished", fmtNum(b.finished)]] })),
    { max: 100, fmt: v => `${Math.round(v)}%`, xLabel: xl, aria: "Success rate over time" });
  lineChart($("#a-latency-trend"), series.map(b => ({ label: b.full, short: b.short, value: b.p95_e2e === null ? null : Number(b.p95_e2e),
    tip: [["p95 end-to-end", b.p95_e2e === null ? "no data" : fmtMs(b.p95_e2e)], ["Completed", fmtNum(b.completed)]] })),
    { fmt: fmtMs, xLabel: xl, aria: "p95 end-to-end latency over time" });
  barChart($("#a-latency-hist"), a.latency.map(b => ({ label: b.label, value: Number(b.count),
    tip: [["Requests", fmtNum(b.count)], ["Share", pct(Number(b.count), Number(k.completed))]] })),
    { aria: "Distribution of end-to-end latency", xLabel: (i, d) => i % 2 === 0 ? d.label : null });

  hbarList($("#a-outcomes"), a.outcomes.map(o => ({ label: o.outcome, value: Number(o.count),
    tip: [["Requests", fmtNum(o.count)], ["Status", o.status_code], ["Share", pct(Number(o.count), Number(k.total))]] })),
    { total: Number(k.total), empty: "No requests in this range" });
  hbarList($("#a-errors"), a.errors.map(e => ({ label: h("span", { class: "mono", text: e.error_code }), title: e.error_code, value: Number(e.count) })),
    { empty: "No errors in this range" });
  const retries = [0, 1, 2, 3, 4, 5].map(r => ({ r, count: Number((a.retries.find(x => x.retries === r) || {}).count || 0) }));
  barChart($("#a-retries"), retries.map(x => ({ label: x.r === 5 ? "5+ retries" : `${x.r} ${x.r === 1 ? "retry" : "retries"}`, value: x.count,
    tip: [["Requests", fmtNum(x.count)], ["Share", pct(x.count, Number(k.total))]] })),
    { height: 180, xLabel: (i) => i === 5 ? "5+" : String(i), aria: "Requests by number of retries" });
  heatmap($("#a-heatmap"), a.heatmap);
  replace("#a-by-workflow", table([
    { label: "Workflow", render: w => h("b", { text: w.workflow_name }) },
    { label: "Requests", num: true, render: w => fmtNum(w.total) },
    { label: "Completed", num: true, render: w => fmtNum(w.completed) },
    { label: "Rejected", num: true, render: w => fmtNum(w.rejected) },
    { label: "Failed", num: true, render: w => fmtNum(w.failed) },
    { label: "Success rate", num: true, render: w => pct(Number(w.completed), Number(w.completed) + Number(w.failed)) },
    { label: "p50", num: true, render: w => fmtMs(w.p50_e2e) },
    { label: "p95", num: true, render: w => fmtMs(w.p95_e2e) },
    { label: "Avg retries", num: true, render: w => w.avg_retries },
  ], a.by_workflow, { empty: "No requests in this range" }));
  replace("#a-by-backend", table([
    { label: "Backend", render: b => backendTag(b.storage_backend) },
    { label: "Requests", num: true, render: b => fmtNum(b.total) },
    { label: "Completed", num: true, render: b => fmtNum(b.completed) },
    { label: "Failed", num: true, render: b => fmtNum(b.failed) },
    { label: "p50", num: true, render: b => fmtMs(b.p50_e2e) },
    { label: "p95", num: true, render: b => fmtMs(b.p95_e2e) },
    { label: "p95 queue wait", num: true, render: b => fmtMs(b.p95_queue) },
  ], a.by_backend, { empty: "No requests in this range" }));
}

function initAnalytics() {
  $$("#a-range button").forEach(b => b.addEventListener("click", () => {
    analyticsState.range = b.dataset.range;
    $$("#a-range button").forEach(x => x.setAttribute("aria-pressed", String(x === b)));
    loadAnalytics().catch(console.error);
  }));
  $("#a-workflow").addEventListener("change", e => { analyticsState.workflow = e.target.value; loadAnalytics().catch(console.error); });
  $("#a-backend").addEventListener("change", e => { analyticsState.backend = e.target.value; loadAnalytics().catch(console.error); });
  $("#a-refresh-dwh").addEventListener("click", async () => {
    await act(Promise.all(["dwh_load_incremental", "dwh_load_bigquery"].map(j => api(`/api/system-jobs/${j}/run`, { method: "POST" }))),
      "Warehouse refresh queued for PostgreSQL and BigQuery; charts update in a few seconds");
    setTimeout(() => loadAnalytics().catch(console.error), 3500);
  });
}
