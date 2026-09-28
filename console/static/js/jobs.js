"use strict";
/* Jobs: pipeline control (ConsumerApplication workers), traffic jobs, system (maintenance) jobs. */

const jobBadge = st => st === "COMPLETED" ? badge("COMPLETED_JOB") : badge(st);
const schedule = j => j.interval_seconds ? `every ${fmtDuration(j.interval_seconds)} × ${j.requests_per_run}` : `manual × ${j.requests_per_run}`;

async function loadJobs() {
  const [jobs, sys, overview] = await Promise.all([api("/api/jobs"), api("/api/system-jobs"), api("/api/overview")]);
  renderPipeline(overview, jobs);
  renderJobs(jobs);
  renderSystemJobs(sys);
}

/* ---------------------------------------------------------------- pipeline */
function renderPipeline(o, jobs) {
  const svc = o.services.find(s => s.name === "ConsumerApplication") || {};
  const st = svc.extra || {};
  const backends = st.backends || {};
  const pb = o.per_backend || { postgres: {}, bigquery: {} };
  const workerCard = (name, label) => {
    const w = backends[name];
    const c = (w && w.counters) || {};
    return h("div", { class: "kpi" },
      h("div", { class: "kpi-label" }, label),
      h("div", { style: { margin: "8px 0" } }, !svc.ok || !w ? badge("down", w ? "Down" : "Not running") : (w.paused ? badge("PAUSED", "Paused") : badge("ACTIVE", "Consuming"))),
      h("div", { class: "kpi-sub", text: `${fmtNum(w?.inflight ?? 0)} busy · ${fmtNum(pb[name]?.PENDING || 0)} pending · ${fmtNum(pb[name]?.IN_PROGRESS || 0)} in progress` }),
      h("div", { class: "kpi-sub", text: `completed ${fmtNum(c.completed || 0)} · retried ${fmtNum(c.retried || 0)} · failed ${fmtNum(c.failed || 0)}` }),
      w ? h("div", { class: "actions", style: { marginTop: "10px" } }, w.paused
        ? h("button", { class: "btn sm primary", onclick: () => consumerControl("resume", name) }, icon("play"), "Resume")
        : h("button", { class: "btn sm", onclick: () => consumerControl("pause", name) }, icon("pause"), "Pause")) : null);
  };
  replace("#pipeline",
    h("div", { class: "card accent" },
      h("div", { class: "card-head" },
        h("div", {}, h("h2", { text: "ConsumerApplication" }), h("div", { class: "sub", text: st.worker_id ? `worker ${st.worker_id} · one queue worker per backend` : "unreachable" })),
        h("div", { class: "actions" },
          h("button", { class: "btn sm", onclick: () => consumerControl("pause"), disabled: !svc.ok }, icon("pause"), "Pause all"),
          h("button", { class: "btn sm primary", onclick: () => consumerControl("resume"), disabled: !svc.ok }, icon("play"), "Resume all"))),
      h("div", { class: "kpis" }, workerCard("postgres", "PostgreSQL queue"), workerCard("bigquery", "BigQuery queue")),
      h("p", { class: "muted small", style: { marginBottom: 0 }, text: "Pausing lets requests queue up as PENDING; in-flight work still finishes." })),
    h("div", { class: "card" },
      h("div", { class: "card-head" }, h("h2", { text: "Queues" }), h("span", { class: "sub", text: "both backends, live" })),
      h("div", { class: "kpis" },
        kpi("Pending", fmtNum((pb.postgres.PENDING || 0) + (pb.bigquery.PENDING || 0)), o.queue.oldest_pending_s ? `oldest ${fmtDuration(o.queue.oldest_pending_s)}` : "nothing waiting"),
        kpi("In progress", fmtNum((pb.postgres.IN_PROGRESS || 0) + (pb.bigquery.IN_PROGRESS || 0)), "claimed by ConsumerApplication"),
        kpi("Active jobs", fmtNum(jobs.filter(j => j.status === "ACTIVE").length), `${fmtNum(jobs.length)} jobs defined`))));
}
async function consumerControl(action, backend) {
  const r = await api(`/api/consumer/${action}${backend ? `?backend=${backend}` : ""}`, { method: "POST" });
  toast(r.http_status < 300 ? `ConsumerApplication ${backend ? backend + " worker" : "workers"} ${action === "pause" ? "paused" : "resumed"}` : `HTTP ${r.http_status}`);
  refreshCurrent();
}

/* ---------------------------------------------------------------- traffic jobs */
async function jobAction(job, action) {
  if (action === "delete" && !confirm(`Delete job “${job.name}” and its run history?`)) return;
  const method = action === "delete" ? "DELETE" : "POST";
  const path = action === "delete" ? `/api/jobs/${job.job_id}` : `/api/jobs/${job.job_id}/${action}`;
  const msg = { run: "Run queued; il-scheduler picks it up within 2 s", pause: "Job paused", resume: "Job resumed", delete: "Job deleted" }[action];
  await act(api(path, { method }), msg);
  if (action === "delete") closePanel();
  refreshCurrent(); refreshPanel();
}
function jobButtons(j) {
  return h("div", { class: "row-actions" },
    h("button", { class: "btn sm primary", title: "Run once now", onclick: () => jobAction(j, "run") }, icon("play"), "Run now"),
    j.status === "ACTIVE"
      ? h("button", { class: "btn sm", onclick: () => jobAction(j, "pause") }, icon("pause"), "Pause")
      : h("button", { class: "btn sm", disabled: !j.interval_seconds, title: j.interval_seconds ? "Resume the schedule" : "Manual job: no schedule",
          onclick: () => jobAction(j, "resume") }, icon("play"), "Resume"),
    h("button", { class: "btn sm", onclick: () => openJobEditor(j), text: "Edit" }),
    h("button", { class: "btn sm danger", onclick: () => jobAction(j, "delete"), text: "Delete" }));
}
function renderJobs(jobs) {
  replace("#jobs-table", table([
    { label: "Job", render: j => h("div", {}, h("b", { text: j.name }), h("div", { class: "muted small trunc", text: j.description || "" })) },
    { label: "Source", render: j => j.source_app },
    { label: "Workflow", render: j => j.workflow_name },
    { label: "Payload", render: j => j.payload_template ? h("span", { class: "kind", text: "template" }) : (j.sample_name || "–") },
    { label: "Schedule", render: j => h("span", { class: "nowrap", text: schedule(j) }) },
    { label: "Status", render: j => jobBadge(j.status) },
    { label: "Runs", num: true, render: j => h("span", { class: "nowrap", text: `${fmtNum(j.run_count)}${j.max_runs ? ` / ${fmtNum(j.max_runs)}` : ""}` }) },
    { label: "Last run", render: j => j.last_run_at ? h("div", {}, h("div", { class: "nowrap", text: fmtRelative(j.last_run_at) }),
        h("div", { class: "muted small nowrap", text: `${j.last_accepted} ok · ${j.last_rejected} rejected${j.last_errors ? ` · ${j.last_errors} errors` : ""}` }))
        : h("span", { class: "muted", text: j.run_requested ? "queued" : "never" }) },
    { label: "Next run", render: j => j.run_requested ? h("span", { class: "red", text: "queued" }) : (j.status === "ACTIVE" ? fmtRelative(j.next_run_at) : "–") },
    { label: "", render: jobButtons },
  ], jobs, { onRow: j => openJob(j.job_id), empty: "No jobs yet. Create one with “New job”." }));
}

function openJob(jobId) {
  const expanded = new Set();
  return openPanel("Traffic job", `Job #${jobId}`, async () => {
    const { job: j, runs } = await api(`/api/jobs/${jobId}`);
    $("#drawer-title").textContent = j.name;
    const subsByRun = {};
    await Promise.all([...expanded].map(async id => { subsByRun[id] = await api(`/api/job-runs/${id}`); }));
    const runTable = h("div", { class: "table-wrap" }, h("table", {},
      h("thead", {}, h("tr", {}, ["Run", "Trigger", "Started", "Duration", "Accepted", "Rejected", "Errors", ""].map((t, i) => h("th", { class: i >= 4 && i <= 6 ? "num" : null, text: t })))),
      h("tbody", {}, runs.flatMap(r => {
        const open = expanded.has(r.run_id);
        const row = h("tr", { class: "clickable" + (open ? " selected" : ""), onclick: () => { open ? expanded.delete(r.run_id) : expanded.add(r.run_id); refreshPanel(); } },
          h("td", { class: "mono", text: `#${r.run_id}` }), h("td", { text: r.trigger }), h("td", { class: "nowrap", text: fmtTime(r.started_at) }),
          h("td", { text: r.finished_at ? fmtMs(new Date(r.finished_at) - new Date(r.started_at)) : "running" }),
          h("td", { class: "num", text: fmtNum(r.accepted) }), h("td", { class: "num", text: fmtNum(r.rejected) }), h("td", { class: "num", text: fmtNum(r.errors) }),
          h("td", { class: "muted small", text: open ? "hide ▲" : "submissions ▼" }));
        if (!open) return [row];
        const subs = subsByRun[r.run_id] || [];
        const detail = h("tr", {}, h("td", { colspan: 8, style: { background: "var(--surface-2)" } },
          r.error ? h("div", { class: "field-error", text: r.error }) : null,
          table([
            { label: "#", render: x => x.seq },
            { label: "HTTP", render: x => httpBadge(x.http_status) },
            { label: "Latency", num: true, render: x => fmtMs(x.latency_ms) },
            { label: "Request", render: x => x.request_id ? h("button", { class: "link-btn mono", onclick: () => openRequest(x.request_id), text: shortId(x.request_id) }) : "–" },
            { label: "Now", render: x => x.request_status ? badge(x.request_status) : "–" },
            { label: "Outcome", render: x => x.workflow_status },
            { label: "Payload / response", render: x => h("div", { style: { display: "grid", gap: "4px" } }, jsonToggle("payload sent", x.payload), jsonToggle("response body", x.response_body)) },
          ], subs, { empty: "No submissions recorded." })));
        return [row, detail];
      }))));
    return [
      h("div", { class: "card accent" },
        h("div", { class: "card-head" }, h("div", { class: "result-head" }, jobBadge(j.status), h("span", { class: "muted", text: schedule(j) })), jobButtons(j)),
        kv([["Source app", j.source_app], ["Workflow", j.workflow_name], ["Payload", j.payload_template ? "custom template" : j.sample_name],
          ["Runs", `${j.run_count}${j.max_runs ? ` of ${j.max_runs}` : ""}`], ["Requests sent", `${fmtNum(j.submissions)} (${fmtNum(j.accepted_total)} accepted)`],
          ["Callback", j.use_callback ? "yes" : "no (polling)"], ["Next run", j.status === "ACTIVE" ? fmtRelative(j.next_run_at) : "–"],
          ["Last run", fmtTime(j.last_run_at, true)], ["Created", fmtTime(j.created_at, true)]]),
        j.description ? h("p", { class: "muted", text: j.description }) : null),
      h("div", { class: "card" }, h("h3", { text: j.payload_template ? "Payload template" : `Payload (sample “${j.sample_name}”)` }),
        jsonView(j.payload_template ?? j.sample_payload)),
      h("div", { class: "card" }, h("h3", { text: "Runs (click a run to see each request's HTTP status and response)" }),
        runs.length ? runTable : h("div", { class: "empty", text: "Not run yet. Use Run now." })),
    ];
  });
}

async function openJobEditor(job) {
  await loadWorkflowCache();
  cache.samples = await api("/api/samples");
  const isNew = !job;
  job = job || { name: "", description: "", workflow_name: cache.workflows[0]?.workflow_name, source_app: "WWMG_Source", sample_id: null, payload_template: null,
    interval_seconds: 30, requests_per_run: 1, max_runs: null, use_callback: true, status: "PAUSED" };
  const useTemplate = job.payload_template !== null && job.payload_template !== undefined;
  const f = {
    name: h("input", { type: "text", value: job.name, maxlength: 100, placeholder: "e.g. Nightly order burst" }),
    description: h("input", { type: "text", value: job.description || "" }),
    workflow: workflowSelect(job.workflow_name),
    source: h("select", {}, ["WWMG_Source", "MyCareHub"].map(a => h("option", { value: a, text: a, selected: a === job.source_app }))),
    sample: h("select"),
    src: [h("input", { type: "radio", name: "src", value: "sample", checked: !useTemplate }), h("input", { type: "radio", name: "src", value: "template", checked: useTemplate })],
    template: jsonEditor({ value: job.payload_template, rows: 12 }),
    interval: h("input", { type: "number", min: 5, max: 86400, value: job.interval_seconds ?? "", placeholder: "blank = manual only" }),
    perRun: h("input", { type: "number", min: 1, max: 100, value: job.requests_per_run }),
    maxRuns: h("input", { type: "number", min: 1, value: job.max_runs ?? "", placeholder: "blank = unlimited" }),
    callback: h("input", { type: "checkbox", checked: job.use_callback }),
    active: h("input", { type: "checkbox", checked: job.status === "ACTIVE" }),
  };
  const fillSamples = () => {
    const list = cache.samples.filter(x => x.workflow_name === f.workflow.value);
    replace(f.sample, list.map(x => h("option", { value: x.sample_id, text: x.name, selected: x.sample_id === job.sample_id })));
  };
  fillSamples();
  f.workflow.addEventListener("change", () => {
    fillSamples();
    if (isNew) f.source.value = f.workflow.value === "care_request" ? "MyCareHub" : "WWMG_Source";
  });
  const templateBox = h("div", { class: "form-row full" }, h("span", { class: "label", text: "Payload template (JSON, placeholders allowed)" }), f.template.el,
    h("div", { class: "actions" },
      h("button", { class: "btn sm", type: "button", text: "Copy selected sample", onclick: () => {
        const smp = cache.samples.find(x => String(x.sample_id) === f.sample.value); if (smp) f.template.set(smp.payload); } }),
      h("button", { class: "btn sm", type: "button", text: "Format", onclick: () => f.template.format() })));
  const sourceChanged = () => { templateBox.hidden = !f.src[1].checked; };
  f.src.forEach(r => r.addEventListener("change", sourceChanged));
  sourceChanged();
  const preview = h("div");
  const result = h("div");

  const collect = () => {
    const tpl = f.src[1].checked ? f.template.parse() : null;
    const interval = f.interval.value ? Number(f.interval.value) : null;
    return { name: f.name.value.trim(), description: f.description.value.trim() || null, workflow_name: f.workflow.value,
      source_app: f.source.value,
      sample_id: f.sample.value ? Number(f.sample.value) : null, payload_template: tpl, interval_seconds: interval,
      requests_per_run: Number(f.perRun.value) || 1, max_runs: f.maxRuns.value ? Number(f.maxRuns.value) : null,
      use_callback: f.callback.checked, status: f.active.checked && interval ? "ACTIVE" : "PAUSED" };
  };
  const save = async () => {
    let body; try { body = collect(); } catch { return; }
    if (!body.name) { toast("Give the job a name"); f.name.focus(); return; }
    if (!f.src[1].checked && !body.sample_id) { toast("Pick a sample or switch to a custom template"); return; }
    try {
      const saved = await api(isNew ? "/api/jobs" : `/api/jobs/${job.job_id}`, { method: isNew ? "POST" : "PUT", body });
      toast(isNew ? "Job created" : "Job saved");
      refreshCurrent();
      openJob(saved.job_id);
    } catch (e) {
      replace(result, h("div", { class: "field-error", text: e.message }));
    }
  };
  const doPreview = async () => {
    let tpl;
    try { tpl = f.src[1].checked ? f.template.parse() : (cache.samples.find(x => String(x.sample_id) === f.sample.value) || {}).payload; } catch { return; }
    const r = await api("/api/templates/preview", { method: "POST", body: { template: tpl } });
    replace(preview, h("h3", { text: "Preview: three rendered payloads" }), r.rendered.map(p => jsonView(p, { maxHeight: "180px" })));
  };
  const row = (label, input, hint, full) => h("div", { class: `form-row${full ? " full" : ""}` }, h("label", { text: label }), input, hint ? h("div", { class: "hint", text: hint }) : null);
  openStaticPanel(isNew ? "New traffic job" : "Edit traffic job", isNew ? "New job" : job.name, h("div", { class: "card accent" },
    h("div", { class: "form-grid" },
      row("Name", f.name), row("Workflow", f.workflow),
      row("Send as", f.source, "The source application whose API key and callback URL the job uses"),
      row("Description", f.description),
      h("div", { class: "form-row full" }, h("span", { class: "label", text: "Payload source" }),
        h("div", { class: "actions" }, h("label", { class: "check" }, f.src[0], "Sample from the JSON library"), f.sample,
          h("label", { class: "check" }, f.src[1], "Custom template"))),
      templateBox,
      row("Interval (seconds)", f.interval, "How often the job runs while active. Leave blank for a manual-only job."),
      row("Requests per run", f.perRun, "1 to 100"),
      row("Max runs", f.maxRuns, "The job finishes after this many runs."),
      h("div", { class: "form-row" }, h("span", { class: "label", text: "Options" }),
        h("label", { class: "check" }, f.callback, "Deliver results by callback"),
        h("label", { class: "check" }, f.active, "Active (start the schedule now)"))),
    h("div", { class: "actions" },
      h("button", { class: "btn primary", type: "button", onclick: save, text: isNew ? "Create job" : "Save changes" }),
      h("button", { class: "btn", type: "button", onclick: doPreview, text: "Preview payloads" })),
    result, preview,
    h("details", { class: "json-toggle", style: { marginTop: "12px" } }, h("summary", { text: "Placeholder reference" }), placeholderHelp())));
}

/* ---------------------------------------------------------------- system jobs */
async function systemAction(name, action, body) {
  const msg = { run: "Queued; runs within 2 s", pause: "Paused", resume: "Resumed", interval: "Interval saved" }[action];
  await act(api(`/api/system-jobs/${name}${action === "interval" ? "" : "/" + action}`, { method: action === "interval" ? "PUT" : "POST", body }), msg);
  refreshCurrent();
}
function renderSystemJobs({ jobs, runs }) {
  replace("#system-jobs-table", table([
    { label: "Job", render: j => h("div", {}, h("b", { class: "mono", text: j.job_name }), h("div", { class: "muted small", text: j.description })) },
    { label: "Interval (s)", render: j => {
        const input = h("input", { type: "number", min: 10, max: 604800, value: j.interval_seconds, style: { width: "96px" } });
        return h("div", { class: "actions", style: { flexWrap: "nowrap" } }, input,
          h("button", { class: "btn sm", onclick: () => systemAction(j.job_name, "interval", { interval_seconds: Number(input.value) }), text: "Set" }));
      } },
    { label: "Status", render: j => j.is_paused ? badge("PAUSED") : badge("ACTIVE") },
    { label: "Last run", render: j => j.last_status ? h("div", {}, h("div", { class: "actions", style: { flexWrap: "nowrap" } }, badge(j.last_status), h("span", { class: "nowrap", text: fmtRelative(j.last_finished_at) })),
        h("div", { class: "muted small trunc", title: j.last_result, text: `${j.last_result || ""} · ${fmtMs(j.last_duration_ms)}` })) : "never" },
    { label: "Next run", render: j => j.run_requested ? h("span", { class: "red", text: "queued" }) : (j.is_paused ? "–" : fmtRelative(j.next_run_at)) },
    { label: "Runs", num: true, render: j => fmtNum(j.run_count) },
    { label: "", render: j => h("div", { class: "row-actions" },
        h("button", { class: "btn sm primary", onclick: () => systemAction(j.job_name, "run") }, icon("play"), "Run now"),
        j.is_paused ? h("button", { class: "btn sm", onclick: () => systemAction(j.job_name, "resume") }, icon("play"), "Resume")
                    : h("button", { class: "btn sm", onclick: () => systemAction(j.job_name, "pause") }, icon("pause"), "Pause")) },
  ], jobs));
  replace("#system-runs-table", table([
    { label: "Time", render: r => fmtTime(r.started_at) },
    { label: "Job", render: r => h("span", { class: "mono", text: r.job_name }) },
    { label: "Trigger", render: r => r.trigger },
    { label: "Status", render: r => badge(r.status) },
    { label: "Duration", num: true, render: r => fmtMs(r.duration_ms) },
    { label: "Result", cls: "trunc", render: r => h("span", { title: r.result, text: r.result }) },
  ], runs.slice(0, 12), { empty: "No runs yet." }));
}

function initJobs() {
  $("#new-job").addEventListener("click", () => openJobEditor(null));
}
