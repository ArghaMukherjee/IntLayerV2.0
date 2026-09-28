"use strict";
/* Shared helpers: DOM, API, formatting, badges, JSON, tables, side panel, responses. */

const $ = (sel, root = document) => root.querySelector(sel);
const $$ = (sel, root = document) => [...root.querySelectorAll(sel)];
const SVG_NS = "http://www.w3.org/2000/svg";
const TERMINAL = new Set(["COMPLETED", "FAILED", "CANCELLED"]);

function h(tag, attrs = {}, ...children) {
  const el = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs || {})) {
    if (v === null || v === undefined || v === false) continue;
    if (k === "class") el.className = v;
    else if (k === "text") el.textContent = v;
    else if (k === "style" && typeof v === "object") Object.assign(el.style, v);
    else if (k.startsWith("on")) el.addEventListener(k.slice(2), v);
    else if (k === "dataset") Object.assign(el.dataset, v);
    else if (k === "value") el.value = v;
    else if (k === "checked") el.checked = !!v;
    else el.setAttribute(k, v === true ? "" : v);
  }
  return append(el, children);
}
function append(el, children) {
  for (const c of children.flat(Infinity)) {
    if (c === null || c === undefined || c === false) continue;
    el.append(c instanceof Node ? c : document.createTextNode(String(c)));
  }
  return el;
}
function s(tag, attrs = {}) {
  const el = document.createElementNS(SVG_NS, tag);
  for (const [k, v] of Object.entries(attrs)) el.setAttribute(k, v);
  return el;
}
function replace(target, ...children) {
  const el = typeof target === "string" ? $(target) : target;
  if (!el) return;
  el.replaceChildren();
  append(el, children);
}

class ApiError extends Error {
  constructor(status, body) {
    super(typeof body?.detail === "string" ? body.detail : (body?.detail?.[0]?.msg || `HTTP ${status}`));
    this.status = status;
    this.body = body;
  }
}
async function api(path, { method = "GET", body, allowError = false } = {}) {
  const res = await fetch(path, { method, headers: { "Content-Type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body) });
  let data = null;
  try { data = await res.json(); } catch { /* empty */ }
  if (!res.ok && !allowError) throw new ApiError(res.status, data);
  return data;
}

/* ---------------------------------------------------------------- formatting */
const fmtNum = n => (n === null || n === undefined) ? "–" : Number(n).toLocaleString();
function fmtMs(ms) {
  if (ms === null || ms === undefined) return "–";
  ms = Number(ms);
  if (ms < 1000) return `${Math.round(ms)} ms`;
  if (ms < 60000) return `${(ms / 1000).toFixed(ms < 10000 ? 2 : 1)} s`;
  if (ms < 3600000) return `${(ms / 60000).toFixed(1)} min`;
  return `${(ms / 3600000).toFixed(1)} h`;
}
function fmtBytes(b) {
  if (b === null || b === undefined) return "–";
  const u = ["B", "KB", "MB", "GB"]; let i = 0; b = Number(b);
  while (b >= 1024 && i < u.length - 1) { b /= 1024; i++; }
  return `${b.toFixed(i ? 1 : 0)} ${u[i]}`;
}
function fmtTime(iso, withDate) {
  if (!iso) return "–";
  const d = new Date(iso);
  const today = new Date().toDateString() === d.toDateString();
  const time = d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
  return (withDate || !today) ? `${d.toLocaleDateString([], { month: "short", day: "numeric" })} ${time}` : time;
}
function fmtDuration(seconds) {
  seconds = Math.abs(Number(seconds));
  if (seconds < 60) return `${Math.round(seconds)} s`;
  if (seconds < 3600) return `${Math.round(seconds / 60)} min`;
  if (seconds < 86400) return `${(seconds / 3600).toFixed(1)} h`;
  return `${(seconds / 86400).toFixed(1)} d`;
}
function fmtRelative(iso) {
  if (!iso) return "–";
  const diff = (new Date(iso).getTime() - Date.now()) / 1000;
  if (Math.abs(diff) < 1.5) return "now";
  return diff > 0 ? `in ${fmtDuration(diff)}` : `${fmtDuration(diff)} ago`;
}
const shortId = id => id ? String(id).slice(0, 8) : "–";
const pct = (a, b) => b ? `${((a / b) * 100).toFixed(1)}%` : "–";

/* ---------------------------------------------------------------- badges */
const ICONS = {
  check: "M3.5 8.5l3 3 6-6.5",
  cross: "M4.5 4.5l7 7M11.5 4.5l-7 7",
  clock: "M8 2.5a5.5 5.5 0 1 0 0 11a5.5 5.5 0 1 0 0-11M8 5v3.3l2.2 1.4",
  spin: "M13 8a5 5 0 1 1-1.6-3.7M13 2.8v3h-3",
  stop: "M8 2.5a5.5 5.5 0 1 0 0 11a5.5 5.5 0 1 0 0-11M4.2 4.2l7.6 7.6",
  pause: "M6 4v8M10 4v8",
  play: "M5.5 3.5v9l7-4.5z",
  dash: "M4 8h8",
  warn: "M8 3v6M8 12v.5",
  flag: "M4 13V3h7l-1.5 2.5L11 8H4",
};
const BADGES = {
  COMPLETED: ["check", "Completed"], DELIVERED: ["check", "Delivered"], ok: ["check", "Healthy"], OK: ["check", "OK"],
  FAILED: ["cross", "Failed"], down: ["cross", "Down"], ERROR: ["cross", "Error"],
  PENDING: ["clock", "Pending"], IN_PROGRESS: ["spin", "In progress"], CANCELLED: ["stop", "Cancelled"],
  ACTIVE: ["play", "Active"], PAUSED: ["pause", "Paused"], COMPLETED_JOB: ["flag", "Finished"], none: ["dash", "None"],
};
function icon(name, cls) {
  const svg = s("svg", { viewBox: "0 0 16 16", "aria-hidden": "true", class: cls || "" });
  const filled = name === "play";
  svg.append(s("path", { d: ICONS[name] || ICONS.dash, fill: filled ? "currentColor" : "none", stroke: "currentColor",
    "stroke-width": "2", "stroke-linecap": "round", "stroke-linejoin": "round" }));
  return svg;
}
function badge(key, label) {
  const [ic, text] = BADGES[key] || ["dash", key];
  return h("span", { class: `badge s-${key}` }, icon(ic), label || text);
}
function httpBadge(code) {
  if (code === null || code === undefined) return h("span", { class: "http err" }, "network error");
  const cls = code < 300 ? "ok" : code < 500 ? "warn" : "err";
  const txt = { 200: "OK", 201: "Created", 202: "Accepted", 401: "Unauthorized", 404: "Not Found", 409: "Conflict",
    413: "Too Large", 422: "Unprocessable", 500: "Server Error", 502: "Bad Gateway" }[code] || "";
  return h("span", { class: `http ${cls}`, title: `HTTP ${code} ${txt}` }, `${code}${txt ? " " + txt : ""}`);
}

/* ---------------------------------------------------------------- JSON */
function jsonView(value, { maxHeight } = {}) {
  const pre = h("pre", { class: "json" });
  if (maxHeight) pre.style.maxHeight = maxHeight;
  if (value === null || value === undefined) { pre.textContent = "null"; return pre; }
  const text = typeof value === "string" ? value : JSON.stringify(value, null, 2);
  const re = /("(?:\\.|[^"\\])*")(\s*:)?|\b(true|false|null)\b|(-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)/g;
  let last = 0, m;
  while ((m = re.exec(text))) {
    if (m.index > last) pre.append(text.slice(last, m.index));
    if (m[1]) { pre.append(h("span", { class: m[2] ? "j-key" : "j-str", text: m[1] })); if (m[2]) pre.append(m[2]); }
    else if (m[3]) pre.append(h("span", { class: "j-lit", text: m[3] }));
    else pre.append(h("span", { class: "j-num", text: m[4] }));
    last = re.lastIndex;
  }
  pre.append(text.slice(last));
  return pre;
}
function jsonToggle(label, value) {
  return h("details", { class: "json-toggle" }, h("summary", { text: label }), jsonView(value, { maxHeight: "260px" }));
}
/** Textarea JSON editor with inline parse errors. */
function jsonEditor({ value, rows = 14, id, optional = false }) {
  const area = h("textarea", { rows, spellcheck: "false", id });
  area.value = value === undefined || value === null ? "" : (typeof value === "string" ? value : JSON.stringify(value, null, 2));
  const err = h("div", { class: "field-error", hidden: true });
  const wrap = h("div", { style: { display: "grid", gap: "6px" } }, area, err);
  const editor = {
    el: wrap, area,
    parse() {
      const text = area.value.trim();
      if (!text && optional) { err.hidden = true; return null; }
      try { const v = JSON.parse(text); err.hidden = true; return v; }
      catch (e) { err.textContent = `Not valid JSON: ${e.message}`; err.hidden = false; area.focus(); throw e; }
    },
    format() { try { const v = editor.parse(); if (v !== null) area.value = JSON.stringify(v, null, 2); } catch { /* shown */ } },
    set(v) { area.value = v === null || v === undefined ? "" : JSON.stringify(v, null, 2); err.hidden = true; },
  };
  area.addEventListener("keydown", e => {
    if (e.key === "Tab") { e.preventDefault(); const p = area.selectionStart; area.setRangeText("  ", p, area.selectionEnd, "end"); }
  });
  return editor;
}

/* ---------------------------------------------------------------- tooltip & toast */
const tip = () => $("#tooltip");
function showTip(evt, title, rows) {
  const t = tip();
  replace(t, h("div", { class: "t-title", text: title }),
    rows.map(([k, v]) => h("div", { class: "t-row" }, h("span", { text: k }), h("b", { text: v }))));
  t.hidden = false;
  const pad = 14, w = t.offsetWidth, ht = t.offsetHeight;
  let x = evt.clientX + pad, y = evt.clientY + pad;
  if (x + w > window.innerWidth - 8) x = evt.clientX - w - pad;
  if (y + ht > window.innerHeight - 8) y = evt.clientY - ht - pad;
  t.style.left = `${x}px`; t.style.top = `${y}px`;
}
const hideTip = () => { tip().hidden = true; };
let toastTimer;
function toast(msg) {
  const t = $("#toast");
  t.textContent = msg; t.hidden = false;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => { t.hidden = true; }, 3500);
}
async function act(promise, okMsg) {
  try { const r = await promise; if (okMsg) toast(okMsg); return r; }
  catch (e) { toast(e.message || "Action failed"); throw e; }
}

/* ---------------------------------------------------------------- tables */
function table(columns, rows, { onRow, empty = "Nothing here yet.", rowClass } = {}) {
  if (!rows.length) return h("div", { class: "empty", text: empty });
  const thead = h("thead", {}, h("tr", {}, columns.map(c => h("th", { class: c.num ? "num" : null, text: c.label }))));
  const tbody = h("tbody", {}, rows.map(r => h("tr", {
      class: [onRow ? "clickable" : "", rowClass ? rowClass(r) : ""].join(" ").trim() || null,
      onclick: onRow ? (e) => { if (!e.target.closest("button, a, details, input, select")) onRow(r); } : null,
    }, columns.map(c => h("td", { class: [c.num ? "num" : "", c.cls || ""].join(" ").trim() || null }, c.render(r) ?? "–")))));
  return h("div", { class: "table-wrap" }, h("table", {}, thead, tbody));
}
/** Storage backend tag (PG / BQ) with its full name as label text. */
function backendTag(b) {
  if (!b) return null;
  return h("span", { class: `tag ${b === "bigquery" ? "bq" : "pg"}`, title: b === "bigquery" ? "Stored in BigQuery" : "Stored in PostgreSQL",
    text: b === "bigquery" ? "BigQuery" : "PostgreSQL" });
}
const kv = pairs => h("dl", { class: "kv" }, pairs.map(([k, v]) => h("div", {}, h("dt", { text: k }), h("dd", {}, v ?? "–"))));
function kpi(label, value, sub) {
  return h("div", { class: "kpi" }, h("div", { class: "kpi-label", text: label }),
    h("div", { class: "kpi-value", text: value }), h("div", { class: "kpi-sub", text: sub || "\u00a0" }));
}

/* ---------------------------------------------------------------- side panel */
const panel = { refresh: null, busy: false, last: 0 };
function openPanel(kicker, title, build) {
  panel.refresh = build;
  $("#drawer-kicker").textContent = kicker;
  $("#drawer-title").textContent = title;
  $("#drawer").classList.add("open");
  $("#drawer").setAttribute("aria-hidden", "false");
  $("#drawer-backdrop").hidden = false;
  replace("#drawer-body", h("div", { class: "muted", text: "Loading… (BigQuery reads can take a few seconds)" }));
  return refreshPanel(true);
}
async function refreshPanel(first) {
  if (!panel.refresh || (!first && (panel.busy || Date.now() - panel.last < 3000))) return;   // BigQuery reads can be slow
  const fn = panel.refresh;
  panel.busy = true;
  const body = $("#drawer-body");
  if (!first && body.contains(document.activeElement) && document.activeElement.matches("input, textarea, select")) return;
  try {
    const content = await fn(first);
    if (panel.refresh !== fn || content === undefined) return;  // closed/replaced meanwhile, or static
    const scroll = body.scrollTop;
    replace(body, content);
    body.scrollTop = scroll;
  } catch (e) {
    replace(body, h("div", { class: "empty", text: `Could not load (${e.message}).` }));
  } finally {
    panel.busy = false;
    panel.last = Date.now();
  }
}
/** Static panel content (editors): rendered once, not refreshed. */
function openStaticPanel(kicker, title, content) {
  openPanel(kicker, title, async (first) => first ? content : undefined);
}
function closePanel() {
  panel.refresh = null;
  $("#drawer").classList.remove("open");
  $("#drawer").setAttribute("aria-hidden", "true");
  $("#drawer-backdrop").hidden = true;
}

/* ---------------------------------------------------------------- HTTP response view */
function responseView(result, { title = "Response" } = {}) {
  const r = result.response || {};
  const err = r.error;
  const errs = err?.errors || [];
  return h("div", { class: "response" },
    h("div", { class: "result-head" }, h("b", { text: title }), httpBadge(result.http_status),
      h("span", { class: "muted small", text: `${fmtMs(result.latency_ms)} · ${result.request?.method || ""} ${result.request?.url || ""}` })),
    err ? h("div", {}, h("div", {}, "Error ", h("code", { class: "red", text: err.code || "" }), " ", err.message || ""),
      errs.length ? h("ul", { class: "err-list", style: { marginTop: "8px" } },
        errs.map(e => h("li", {}, h("code", { text: e.path }), " ", e.message))) : null) : null,
    result.request?.body ? jsonToggle("Request body sent", result.request.body) : null,
    h("div", {}, h("div", { class: "muted small", style: { marginBottom: "4px" }, text: "Response body" }), jsonView(r, { maxHeight: "260px" })));
}

/* ---------------------------------------------------------------- request progress */
function progressSteps(r) {
  const failed = r.status === "FAILED" || r.status === "CANCELLED";
  const steps = [
    { title: "Received by Integration Layer", sub: `validated against schema v${r.input_schema_version ?? "?"}`, time: r.received_at, done: true },
    { title: "Picked up by ConsumerApplication",
      sub: r.consumer_worker_id ? `worker ${r.consumer_worker_id}` + (r.retry_count ? `, attempt ${r.retry_count + 1}` : "")
        : (r.status === "CANCELLED" ? "cancelled before pickup" : "waiting in queue"),
      time: r.picked_at, done: !!r.picked_at, active: r.status === "PENDING" && !r.picked_at },
    { title: failed ? (r.status === "CANCELLED" ? "Cancelled" : "Failed") : "Workflow completed",
      sub: r.status === "PENDING" && r.error_code ? `retry ${r.retry_count} of ${r.max_retries} scheduled after ${r.error_code}`
        : (r.workflow_status || (r.status === "IN_PROGRESS" ? "processing" : "")),
      time: r.workflow_completed_at, done: r.status === "COMPLETED", fail: failed,
      active: r.status === "IN_PROGRESS" || (r.status === "PENDING" && !!r.picked_at) },
  ];
  if (r.callback_url) {
    steps.push({ title: `Callback delivered to ${r.source_app || "the source app"}`,
      sub: r.callback_status ? `${r.callback_status.toLowerCase()}, ${r.callback_attempts} attempt(s)` + (r.callback_last_error ? `: ${r.callback_last_error}` : "") : "after completion",
      time: r.callback_delivered_at, done: r.callback_status === "DELIVERED", fail: r.callback_status === "FAILED",
      active: r.callback_status === "PENDING" });
  } else {
    steps.push({ title: "Result available by polling", sub: "GET /v1/requests/{id}", time: r.workflow_completed_at, done: TERMINAL.has(r.status) });
  }
  return h("ol", { class: "steps" }, steps.map(st => h("li", { class: `step ${st.done ? "done" : ""} ${st.fail ? "fail" : ""} ${st.active ? "active" : ""}` },
    h("span", { class: "step-dot" }, st.done ? icon("check") : st.fail ? icon("cross") : null),
    h("div", {}, h("div", { class: "step-title", text: st.title }), h("div", { class: "step-sub", text: st.sub })),
    h("div", { class: "step-time", text: st.time ? fmtTime(st.time) : "" }))));
}

/** Follows a request until it is finished (and its callback settled), rendering into `target`. */
function trackRequest(target, requestId) {
  let timer = null;
  const started = Date.now();
  const poll = async () => {
    if (!target.isConnected) { clearInterval(timer); return; }
    try {
      const d = await api(`/api/requests/${requestId}`);
      const r = d.request;
      replace(target, progressSteps(r),
        TERMINAL.has(r.status) ? h("div", {}, h("div", { class: "muted small", style: { marginBottom: "4px" }, text: "Output JSON (from ConsumerApplication)" }), jsonView(r.output_json, { maxHeight: "220px" })) : null,
        h("button", { class: "btn sm", type: "button", onclick: () => openRequest(requestId), text: "Open full telemetry" }));
      const cbDone = !r.callback_url || ["DELIVERED", "FAILED"].includes(r.callback_status);
      if ((TERMINAL.has(r.status) && cbDone) || Date.now() - started > 120000) clearInterval(timer);
    } catch { /* keep polling */ }
  };
  poll();
  timer = setInterval(poll, 1000);
}

/* ---------------------------------------------------------------- shared caches */
const cache = { workflows: [], samples: [] };
async function loadWorkflowCache() {
  cache.workflows = await api("/api/workflows");
  for (const sel of $$("select[data-workflow-filter]")) {
    const cur = sel.value;
    replace(sel, h("option", { value: "", text: "All workflows" }), cache.workflows.map(w => h("option", { value: w.workflow_name, text: w.workflow_name })));
    sel.value = cur;
  }
  return cache.workflows;
}
function workflowSelect(value, { id, activeOnly = true } = {}) {
  return h("select", { id }, cache.workflows.filter(w => !activeOnly || w.is_active || w.workflow_name === value)
    .map(w => h("option", { value: w.workflow_name, text: w.workflow_name, selected: w.workflow_name === value })));
}
