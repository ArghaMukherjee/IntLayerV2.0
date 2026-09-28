"use strict";
/* JSON payload library: view, add, update, delete, validate against the schema, send and see the response. */

const payloadState = { selected: null, filter: "" };

function placeholderHelp() {
  const rows = [
    ["{{seq}}", "sequence number (per job, or 1)"], ["{{short_id}}", "6 random hex characters"], ["{{uuid}}", "random UUID"],
    ["{{rand_int:1:40}}", "random integer (becomes a number)"], ["{{rand_float:5:3000}}", "random number, 2 decimals"],
    ["{{choice:EUR|USD|GBP}}", "one of the options"], ["{{date}} / {{now}}", "today / current timestamp (UTC)"], ["{{job}}", "job name"],
  ];
  return h("div", { class: "placeholder-help" }, rows.flatMap(([c, d]) => [h("code", { text: c }), h("span", { text: d })]));
}

async function loadPayloads() {
  await loadWorkflowCache();
  cache.samples = await api("/api/samples");
  renderSampleList();
  if (payloadState.selected === null && cache.samples.length) payloadState.selected = cache.samples[0].sample_id;
  const current = cache.samples.find(x => x.sample_id === payloadState.selected);
  if (!$("#sample-editor").dataset.loaded || (current && $("#sample-editor").dataset.loaded !== String(current.sample_id))) {
    renderSampleEditor(current || null);
  }
}

function renderSampleList() {
  const list = cache.samples.filter(x => !payloadState.filter || x.workflow_name === payloadState.filter);
  const groups = {};
  list.forEach(x => (groups[x.workflow_name] ||= []).push(x));
  replace("#sample-list", Object.keys(groups).length ? Object.entries(groups).map(([wf, items]) => [
    h("div", { class: "lib-group", text: wf }),
    items.map(x => h("button", { type: "button", class: `lib-item${x.sample_id === payloadState.selected ? " active" : ""}`,
      onclick: () => { payloadState.selected = x.sample_id; renderSampleList(); renderSampleEditor(x); } },
      h("span", { class: "t", text: x.name }), h("span", { class: "d", text: x.description || "" }),
      x.job_count ? h("span", { class: "d red", text: `used by ${x.job_count} job(s)` }) : null)),
  ]) : h("div", { class: "empty", text: "No samples yet." }));
}

function renderSampleEditor(sample) {
  const isNew = !sample || !sample.sample_id;
  sample = sample || { workflow_name: payloadState.filter || cache.workflows[0]?.workflow_name, name: "", description: "", payload: {} };
  $("#sample-editor").dataset.loaded = isNew ? "new" : String(sample.sample_id);
  const f = {
    workflow: workflowSelect(sample.workflow_name),
    name: h("input", { type: "text", value: sample.name, maxlength: 100, placeholder: "e.g. Large EUR order" }),
    description: h("input", { type: "text", value: sample.description || "" }),
    json: jsonEditor({ value: sample.payload, rows: 18 }),
    callback: h("input", { type: "checkbox", checked: true }),
    idem: h("input", { type: "text", maxlength: 200, placeholder: "optional" }),
    priority: h("select", {}, [0, 1, 5, 9].map(p => h("option", { value: p, text: p }))),
    source: h("select", {}, ["WWMG_Source", "MyCareHub"].map(a => h("option", { value: a, text: a,
      selected: a === (sample.workflow_name === "care_request" ? "MyCareHub" : "WWMG_Source") }))),
  };
  const body = () => ({ workflow_name: f.workflow.value, name: f.name.value.trim(), description: f.description.value.trim() || null, payload: f.json.parse() });
  const out = $("#sample-result");

  const save = async () => {
    let b; try { b = body(); } catch { return; }
    if (!b.name) { toast("Give the sample a name"); f.name.focus(); return; }
    try {
      const saved = await api(isNew ? "/api/samples" : `/api/samples/${sample.sample_id}`, { method: isNew ? "POST" : "PUT", body: b });
      toast(isNew ? "Sample created" : "Sample updated");
      payloadState.selected = saved.sample_id;
      $("#sample-editor").dataset.loaded = "";
      await loadPayloads();
    } catch (e) { toast(e.message); }
  };
  const validate = async () => {
    let b; try { b = body(); } catch { return; }
    const r = await api("/api/samples/validate", { method: "POST", body: { workflow_name: b.workflow_name, payload: b.payload } });
    replace(out, h("div", { class: "card-head" }, h("h2", { text: "Schema validation" }), h("span", { class: "sub", text: `workflow ${b.workflow_name}` })),
      r.valid ? h("div", { class: "ok-note" }, icon("check"), " Valid: the Integration Layer will accept this payload (HTTP 202)")
        : h("div", {}, h("p", { class: "field-error", text: `${r.errors.length} problem(s): the Integration Layer will reject it with HTTP 422` }),
            h("ul", { class: "err-list" }, r.errors.map(e => h("li", {}, h("code", { text: e.path }), " ", e.message)))),
      r.has_placeholders ? h("div", {}, h("h3", { text: "Rendered example (placeholders filled)" }), jsonView(r.rendered)) : null);
  };
  const send = async () => {
    let b; try { b = body(); } catch { return; }
    const res = await api("/api/requests", { method: "POST", body: { workflow_name: b.workflow_name, payload: b.payload,
      source_app: f.source.value,
      use_callback: f.callback.checked, idempotency_key: f.idem.value.trim() || null, priority: Number(f.priority.value) } });
    const tracker = h("div");
    const wf = cache.workflows.find(w => w.workflow_name === b.workflow_name) || {};
    replace(out, h("div", { class: "card-head" }, h("h2", { text: "Response from the Integration Layer" }),
        h("span", { class: "sub" }, `as ${f.source.value} → `, backendTag(res.response.storage_backend || wf.storage_backend))),
      responseView(res, { title: "POST /v1/requests" }),
      res.http_status < 300 ? h("div", {}, h("h3", { text: `Pipeline progress · request ${shortId(res.response.request_id)}` }), tracker) : null);
    if (res.http_status < 300) trackRequest(tracker, res.response.request_id);
  };
  const remove = async () => {
    if (!confirm(`Delete sample “${sample.name}”?`)) return;
    try { await api(`/api/samples/${sample.sample_id}`, { method: "DELETE" }); toast("Sample deleted"); payloadState.selected = null; $("#sample-editor").dataset.loaded = ""; loadPayloads(); }
    catch (e) { toast(e.message); }
  };
  const duplicate = () => {
    let payload; try { payload = f.json.parse(); } catch { return; }
    payloadState.selected = null;
    renderSampleList();
    renderSampleEditor({ workflow_name: f.workflow.value, name: `${f.name.value} (copy)`, description: f.description.value, payload });
  };

  replace("#sample-editor",
    h("div", { class: "card-head" }, h("h2", { text: isNew ? "New JSON sample" : "Edit JSON sample" }),
      isNew ? h("span", { class: "kind", text: "unsaved" }) : h("span", { class: "sub", text: `updated ${fmtRelative(sample.updated_at)}` })),
    h("div", { class: "form-grid" },
      h("div", { class: "form-row" }, h("label", { text: "Workflow" }), f.workflow),
      h("div", { class: "form-row" }, h("label", { text: "Name" }), f.name),
      h("div", { class: "form-row full" }, h("label", { text: "Description" }), f.description),
      h("div", { class: "form-row full" }, h("label", { text: "Payload JSON" }), f.json.el)),
    h("div", { class: "actions" },
      h("button", { class: "btn primary", type: "button", onclick: save, text: isNew ? "Save new sample" : "Save changes" }),
      h("button", { class: "btn", type: "button", onclick: validate, text: "Validate" }),
      h("button", { class: "btn", type: "button", onclick: () => f.json.format(), text: "Format" }),
      !isNew ? h("button", { class: "btn", type: "button", onclick: duplicate, text: "Duplicate" }) : null,
      !isNew ? h("button", { class: "btn danger", type: "button", onclick: remove, text: "Delete" }) : null),
    h("h3", { style: { marginTop: "20px" }, text: "Send to the Integration Layer" }),
    h("div", { class: "form-grid" },
      h("div", { class: "form-row" }, h("label", { text: "Send as" }), f.source),
      h("div", { class: "form-row" }, h("label", { text: "Priority" }), f.priority),
      h("div", { class: "form-row full" }, h("label", { text: "Idempotency key" }), f.idem)),
    h("div", { class: "actions" },
      h("button", { class: "btn primary", type: "button", onclick: send }, icon("play"), "Send now"),
      h("label", { class: "check" }, f.callback, "Deliver the result by callback")),
    h("details", { class: "json-toggle", style: { marginTop: "14px" } }, h("summary", { text: "Placeholders: filled in on every send" }), placeholderHelp()));
}

function initPayloads() {
  $("#new-sample").addEventListener("click", () => { payloadState.selected = null; renderSampleList(); renderSampleEditor(null); });
  $("#p-filter").addEventListener("change", e => { payloadState.filter = e.target.value; renderSampleList(); });
}
