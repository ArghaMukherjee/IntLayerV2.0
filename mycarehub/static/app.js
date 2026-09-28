"use strict";
/* MyCareHub member portal. All data comes from the MyCareHub backend, which talks to the Integration Layer. */

const $ = s => document.querySelector(s);
const TYPE = { appointment: "Appointment", prescription_refill: "Prescription refill", referral: "Referral", lab_results: "Lab results" };
const state = { members: [], member: null, seenNotes: 0 };

function h(tag, attrs = {}, ...kids) {
  const el = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (v == null || v === false) continue;
    if (k === "class") el.className = v; else if (k === "text") el.textContent = v;
    else if (k.startsWith("on")) el.addEventListener(k.slice(2), v); else el.setAttribute(k, v);
  }
  for (const c of kids.flat()) if (c != null && c !== false) el.append(c instanceof Node ? c : document.createTextNode(String(c)));
  return el;
}
async function api(path, opts = {}) {
  const res = await fetch(path, { headers: { "Content-Type": "application/json" }, ...opts });
  const body = await res.json().catch(() => null);
  if (!res.ok) throw Object.assign(new Error(body?.detail?.message || body?.detail || `HTTP ${res.status}`), { body });
  return body;
}
const when = iso => iso ? new Date(iso).toLocaleString([], { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" }) : "";

function statusPill(r) {
  if (r.status === "COMPLETED") return h("span", { class: `pill ${r.decision === "ESCALATED" ? "bad" : "done"}`, text: (r.decision || "Completed").replace("_", " ").toLowerCase().replace(/^./, c => c.toUpperCase()) });
  if (r.status === "FAILED") return h("span", { class: "pill bad", text: "Needs attention" });
  if (r.status === "CANCELLED") return h("span", { class: "pill info", text: "Cancelled" });
  if (r.status === "IN_PROGRESS") return h("span", { class: "pill info", text: "In review" });
  return h("span", { class: "pill wait", text: r.retry_count ? "Retrying" : "Received" });
}
function stage(r) { return r.status === "PENDING" ? 1 : r.status === "IN_PROGRESS" ? 2 : 3; }

function requestCard(r) {
  const s = stage(r);
  const msg = r.message || (r.status === "FAILED" ? "We couldn't complete this request automatically. Our team has been notified; please call the care line."
    : r.status === "CANCELLED" ? "You cancelled this request." : r.retry_count ? "Our systems are busy; we're retrying automatically." : "We've received your request and are reviewing it.");
  return h("article", { class: "req" },
    h("div", { class: "req-head" }, h("span", { class: "req-title", text: TYPE[r.request_type] || r.request_type }), statusPill(r)),
    h("div", { class: "track", "aria-hidden": "true" }, [1, 2, 3].map(i => h("span", { class: i <= s ? "on" : "" }))),
    h("div", { class: "req-msg", text: msg }),
    h("div", { class: "req-meta" },
      h("span", { text: `Sent ${when(r.received_at)}` }),
      r.urgency ? h("span", { text: `Urgency: ${r.urgency}` }) : null,
      r.reference ? h("span", { text: `Ref ${r.reference}` }) : null,
      h("span", { text: `Stored in ${r.storage_backend === "bigquery" ? "BigQuery" : "PostgreSQL"}` })),
    r.status === "PENDING" ? h("div", {}, h("button", { class: "btn sm", type: "button", text: "Cancel request",
      onclick: async () => { try { await api(`/api/care-requests/${r.request_id}/cancel`, { method: "POST" }); refresh(); } catch (e) { alert(e.message); } } })) : null);
}

async function refresh() {
  if (!state.member) return;
  try {
    const [reqs, notes] = await Promise.all([
      api(`/api/care-requests?member_id=${state.member.member_id}`),
      api(`/api/notifications?member_id=${state.member.member_id}`)]);
    $("#requests").replaceChildren(...(reqs.length ? reqs.map(requestCard) : [h("div", { class: "empty", text: "No requests yet. Use the form to ask for care." })]));
    const open = reqs.filter(r => r.status === "PENDING" || r.status === "IN_PROGRESS").length;
    $("#stats").replaceChildren(
      h("div", { class: "stat" }, h("b", { text: open }), h("span", { text: "open requests" })),
      h("div", { class: "stat" }, h("b", { text: reqs.filter(r => r.status === "COMPLETED").length }), h("span", { text: "completed" })));
    renderNotes(notes);
    $("#live").textContent = `updated ${new Date().toLocaleTimeString()}`;
  } catch (e) { $("#live").textContent = "connection problem"; }
}

function renderNotes(notes) {
  const unseen = Math.max(0, notes.length - state.seenNotes);
  $("#bell-count").hidden = !unseen || $("#panel").classList.contains("open");
  $("#bell-count").textContent = unseen;
  $("#notes-list").replaceChildren(...(notes.length ? notes.map(n => h("div", { class: "note" },
    h("b", { text: `${TYPE[(n.output_json || {}).request_type] || "Care request"}: ${n.status === "COMPLETED" ? (n.workflow_status || "done") : n.status.toLowerCase()}` }),
    h("span", { text: (n.output_json || {}).message || (n.error ? "We couldn't complete this automatically; please call the care line." : "") }),
    h("time", { text: when(n.received_at) }))) : [h("div", { class: "empty", text: "No notifications yet." })]));
  if ($("#panel").classList.contains("open")) state.seenNotes = notes.length;
}

async function submit(e) {
  e.preventDefault();
  const type = document.querySelector("input[name=type]:checked").value;
  const body = { member_id: state.member.member_id, request_type: type,
    urgency: document.querySelector("input[name=urgency]:checked").value,
    preferred_date: type === "appointment" && $("#date").value ? $("#date").value : null,
    notes: $("#notes").value.trim() || null };
  $("#submit").disabled = true;
  try {
    const r = await api("/api/care-requests", { method: "POST", body: JSON.stringify(body) });
    const ok = r.http_status < 300;
    const err = r.response.error || {};
    $("#result").replaceChildren(h("div", { class: `alert ${ok ? "ok" : "err"}` },
      h("span", { class: "http", text: r.http_status }),
      ok ? `Request received. We'll notify you here when it's done (id ${r.response.request_id.slice(0, 8)}).` : (err.message || "Request rejected"),
      !ok && err.errors ? h("ul", {}, err.errors.map(x => h("li", { text: `${x.path}: ${x.message}` }))) : null));
    if (ok) { $("#notes").value = ""; refresh(); }
  } catch (e2) {
    $("#result").replaceChildren(h("div", { class: "alert err", text: e2.message }));
  } finally { $("#submit").disabled = false; }
}

function selectMember(id) {
  state.member = state.members.find(m => m.member_id === id) || state.members[0];
  state.seenNotes = 0;
  $("#greeting").textContent = `Hello, ${state.member.name.split(" ")[0]}`;
  $("#plan").textContent = `Member ${state.member.member_id} · ${state.member.plan}`;
  try { localStorage.setItem("mch-member", state.member.member_id); } catch { /* ignore */ }
  refresh();
}

async function init() {
  state.members = await api("/api/members");
  $("#member").replaceChildren(...state.members.map(m => h("option", { value: m.member_id, text: `${m.name} (${m.member_id})` })));
  let saved = null; try { saved = localStorage.getItem("mch-member"); } catch { /* ignore */ }
  if (saved) $("#member").value = saved;
  $("#member").addEventListener("change", e => selectMember(e.target.value));
  $("#form").addEventListener("submit", submit);
  document.querySelectorAll("input[name=type]").forEach(r => r.addEventListener("change", () => { $("#date-row").hidden = r.value !== "appointment" || !r.checked; }));
  const tomorrow = new Date(Date.now() + 864e5).toISOString().slice(0, 10);
  $("#date").min = tomorrow; $("#date").value = tomorrow;
  $("#bell").addEventListener("click", () => { $("#panel").classList.add("open"); $("#panel").setAttribute("aria-hidden", "false"); refresh(); });
  $("#panel-close").addEventListener("click", () => { $("#panel").classList.remove("open"); $("#panel").setAttribute("aria-hidden", "true"); });
  $("#theme").addEventListener("click", () => {
    const next = document.documentElement.dataset.theme === "dark" ? "light" : "dark";
    document.documentElement.dataset.theme = next;
    try { localStorage.setItem("mch-theme", next); } catch { /* ignore */ }
  });
  selectMember($("#member").value);
  setInterval(() => { if (!document.hidden) refresh(); }, 3000);
}
init();
