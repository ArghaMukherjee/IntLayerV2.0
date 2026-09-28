"use strict";
/* Workflows editor, tab routing and the refresh loop. */

/* ---------------------------------------------------------------- workflows */
async function loadWorkflows() {
  await loadWorkflowCache();
  replace("#workflows", cache.workflows.map(w => h("div", { class: "card" },
    h("div", { class: "card-head" },
      h("div", { class: "result-head" }, h("h2", { class: "mono", text: w.workflow_name }), w.is_active ? badge("ACTIVE") : badge("PAUSED", "Inactive"),
        backendTag(w.storage_backend),
        h("span", { class: "kind", text: `schema v${w.schema_version}` })),
      h("button", { class: "btn sm", onclick: () => openWorkflowEditor(w), text: "Edit schemas" })),
    w.description ? h("p", { class: "muted", text: w.description }) : null,
    kv([["Target app", w.target_app], ["Storage backend", w.storage_backend === "bigquery" ? "BigQuery" : "PostgreSQL"],
      ["Max retries", w.max_retries], ["Requests", `${fmtNum(w.request_count)} (${fmtNum(w.postgres_requests)} PostgreSQL, ${fmtNum(w.bigquery_requests)} BigQuery)`],
      ["Updated", fmtTime(w.updated_at, true)]]),
    h("div", { class: "grid-2", style: { marginTop: "12px" } },
      h("div", {}, h("h3", { text: "Input schema (source app payload)" }), jsonView(w.input_schema)),
      h("div", {}, h("h3", { text: "Output schema (ConsumerApplication result)" }), jsonView(w.output_schema))))));
}

function openWorkflowEditor(w) {
  const isNew = !w;
  w = w || { workflow_name: "", target_app: "ConsumerApplication", description: "", max_retries: 3, is_active: true, storage_backend: "postgres",
    input_schema: { "$schema": "https://json-schema.org/draft/2020-12/schema", type: "object", required: ["id"], properties: { id: { type: "string" } } },
    output_schema: null };
  const f = {
    name: h("input", { type: "text", value: w.workflow_name, disabled: !isNew, placeholder: "lowercase_with_underscores" }),
    target: h("input", { type: "text", value: w.target_app }),
    description: h("input", { type: "text", value: w.description || "" }),
    retries: h("input", { type: "number", min: 0, max: 20, value: w.max_retries }),
    active: h("input", { type: "checkbox", checked: w.is_active }),
    backend: h("select", {}, [["postgres", "PostgreSQL"], ["bigquery", "BigQuery"]].map(([v, t]) => h("option", { value: v, text: t, selected: v === w.storage_backend }))),
    input: jsonEditor({ value: w.input_schema, rows: 16 }),
    output: jsonEditor({ value: w.output_schema, rows: 12, optional: true }),
  };
  const out = h("div");
  const save = async () => {
    let input, output;
    try { input = f.input.parse(); output = f.output.parse(); } catch { return; }
    const name = f.name.value.trim();
    if (!/^[a-z][a-z0-9_]{1,62}$/.test(name)) { toast("Name: lowercase letters, digits and underscores, starting with a letter"); return; }
    const res = await api(`/api/workflows/${name}`, { method: "PUT", body: {
      target_app: f.target.value.trim(), description: f.description.value.trim() || null, input_schema: input,
      output_schema: output, max_retries: Number(f.retries.value), is_active: f.active.checked,
      storage_backend: f.backend.value } });
    replace(out, responseView(res, { title: `PUT /v1/admin/workflows/${name}` }));
    if (res.http_status < 300) { toast(`Saved: schema version ${res.response.schema_version}`); loadWorkflows(); }
  };
  const row = (label, input, hint) => h("div", { class: "form-row" }, h("label", { text: label }), input, hint ? h("div", { class: "hint", text: hint }) : null);
  openStaticPanel(isNew ? "New workflow" : "Edit workflow", isNew ? "New workflow" : w.workflow_name, h("div", { class: "card accent" },
    h("div", { class: "form-grid" },
      row("Workflow name", f.name, isNew ? "Cannot be changed later" : null), row("Target app", f.target, "The consumer that processes it"),
      row("Description", f.description), row("Max retries", f.retries),
      row("Storage backend", f.backend, "Where new requests of this workflow are stored and queued. Existing requests stay where they are."),
      h("div", { class: "form-row full" }, h("label", { class: "check" }, f.active, "Active: accepts new requests"))),
    h("div", { class: "form-row" }, h("label", { text: "Input schema: JSON Schema that the source app's payload must match" }), f.input.el,
      h("div", { class: "actions" }, h("button", { class: "btn sm", type: "button", onclick: () => f.input.format(), text: "Format" }))),
    h("div", { class: "form-row" }, h("label", { text: "Output schema: ConsumerApplication's result must match it (optional)" }), f.output.el,
      h("div", { class: "actions" }, h("button", { class: "btn sm", type: "button", onclick: () => f.output.format(), text: "Format" }))),
    h("div", { class: "actions" }, h("button", { class: "btn primary", type: "button", onclick: save, text: isNew ? "Create workflow" : "Save (bumps version if schemas changed)" })),
    h("p", { class: "muted small", text: "ConsumerApplication needs a handler for a new workflow; until it has one, requests fail with NO_HANDLER." }),
    out));
}

/* ---------------------------------------------------------------- theme: black & red <-> light */
function currentTheme() { return document.documentElement.dataset.theme === "light" ? "light" : "dark"; }
function initTheme() {
  const label = () => { $("#theme-label").textContent = currentTheme() === "light" ? "Dark" : "Light"; };
  label();
  $("#theme-toggle").addEventListener("click", () => {
    const next = currentTheme() === "light" ? "dark" : "light";
    document.documentElement.dataset.theme = next;
    try { localStorage.setItem("il-theme", next); } catch { /* storage unavailable */ }
    label();
    if (["dashboard", "analytics"].includes(currentTab)) refreshCurrent();   // re-draw charts in the new colours
  });
}

/* ---------------------------------------------------------------- tabs & refresh */
const LOADERS = { dashboard: loadDashboard, analytics: loadAnalytics, jobs: loadJobs, payloads: loadPayloads,
  requests: loadRequests, telemetry: loadTelemetry, workflows: loadWorkflows };
const REFRESH_MS = { dashboard: 3000, analytics: 10000, jobs: 2000, requests: 3000, telemetry: 3000 };
let currentTab = "dashboard";
let lastRefresh = 0;

async function refreshCurrent() {
  const state = $("#refresh-state");
  try {
    await LOADERS[currentTab]();
    lastRefresh = Date.now();
    state.classList.remove("stale");
    $("#refresh-text").textContent = REFRESH_MS[currentTab] ? `Live · ${new Date().toLocaleTimeString()}` : "Up to date";
  } catch (e) {
    state.classList.add("stale");
    $("#refresh-text").textContent = "Connection problem";
    console.error(e);
  }
}

function selectTab(name) {
  if (!LOADERS[name]) name = "dashboard";
  currentTab = name;
  $$(".tabs button").forEach(b => b.setAttribute("aria-selected", String(b.dataset.tab === name)));
  $$("main > .tab-panel").forEach(p => { p.hidden = p.id !== `tab-${name}`; });
  history.replaceState(null, "", `#${name}`);
  refreshCurrent();
}

function userIsEditing() {
  const el = document.activeElement;
  return el && el.matches("input, textarea, select") && $(`#tab-${currentTab}`).contains(el);
}

function init() {
  $$(".tabs button").forEach(b => b.addEventListener("click", () => selectTab(b.dataset.tab)));
  $$("[data-goto]").forEach(b => b.addEventListener("click", () => selectTab(b.dataset.goto)));
  $("#drawer-close").addEventListener("click", closePanel);
  $("#drawer-backdrop").addEventListener("click", closePanel);
  document.addEventListener("keydown", e => { if (e.key === "Escape") closePanel(); });
  $("#new-workflow").addEventListener("click", () => openWorkflowEditor(null));
  let searchTimer;
  $("#r-search").addEventListener("input", () => { clearTimeout(searchTimer); searchTimer = setTimeout(loadRequests, 300); });
  $("#r-status").addEventListener("change", loadRequests);
  $("#r-workflow").addEventListener("change", loadRequests);
  $("#r-backend").addEventListener("change", loadRequests);
  initTheme();
  initAnalytics(); initJobs(); initPayloads(); initTelemetry();
  let resizeTimer;
  window.addEventListener("resize", () => { clearTimeout(resizeTimer); resizeTimer = setTimeout(() => {
    if (["dashboard", "analytics"].includes(currentTab)) refreshCurrent(); }, 200); });

  loadWorkflowCache().catch(console.error);
  api("/api/samples").then(x => { cache.samples = x; }).catch(console.error);
  selectTab(location.hash.slice(1) || "dashboard");

  setInterval(() => {
    if (document.hidden) return;
    const every = REFRESH_MS[currentTab];
    if (every && Date.now() - lastRefresh >= every - 250 && !userIsEditing()) refreshCurrent();
    if (panel.refresh) refreshPanel();
  }, 1000);
}

init();
