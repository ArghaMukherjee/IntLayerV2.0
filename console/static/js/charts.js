"use strict";
/* Small SVG chart kit: single-series bars and lines, horizontal bar lists, heatmap. All with hover tooltips. */

function niceMax(v) {
  if (!v || v <= 4) return 4;
  const pow = 10 ** Math.floor(Math.log10(v));
  const n = v / pow;
  return (n <= 1 ? 1 : n <= 2 ? 2 : n <= 5 ? 5 : 10) * pow;
}
function roundedTopBar(x, y, w, ht, r) {
  r = Math.min(r, w / 2, ht);
  return `M${x},${y + ht}V${y + r}Q${x},${y} ${x + r},${y}H${x + w - r}Q${x + w},${y} ${x + w},${y + r}V${y + ht}Z`;
}
function axisText(svg, x, y, text, anchor = "middle") {
  const t = s("text", { x, y, "text-anchor": anchor, class: "axis-text" });
  t.textContent = text;
  svg.append(t);
}
function frame(container, height) {
  const W = Math.max(container.clientWidth || 0, 300);
  return { W, H: height, m: { l: 42, r: 10, t: 12, b: 26 } };
}
function yAxis(svg, f, max, fmt) {
  const { W, m } = f, ih = f.H - m.t - m.b;
  for (let i = 0; i <= 4; i++) {
    const v = (max / 4) * i, y = m.t + ih - (v / max) * ih;
    if (i > 0) svg.append(s("line", { x1: m.l, x2: W - m.r, y1: y, y2: y, class: "gridline" }));
    axisText(svg, m.l - 6, y + 4, fmt(v), "end");
  }
}

/**
 * data: [{label, value, tip: [[k, v], ...]}]; xLabel(i, d) returns tick text or null.
 */
function barChart(container, data, { height = 220, fmt = fmtNum, xLabel, aria = "Bar chart" } = {}) {
  if (!data.length) return replace(container, h("div", { class: "empty-chart", text: "No data" }));
  const f = frame(container, height), { W, H, m } = f;
  const iw = W - m.l - m.r, ih = H - m.t - m.b;
  const max = niceMax(Math.max(...data.map(d => d.value || 0)));
  const band = iw / data.length;
  const bw = Math.max(2, Math.min(40, band - 2));   // 2px surface gap between neighbours
  const svg = s("svg", { viewBox: `0 0 ${W} ${H}`, role: "img", "aria-label": aria });
  yAxis(svg, f, max, fmt);
  const hits = [];
  data.forEach((d, i) => {
    const x = m.l + i * band + (band - bw) / 2;
    const bh = ((d.value || 0) / max) * ih;
    let bar = null;
    if (d.value > 0) { bar = s("path", { d: roundedTopBar(x, m.t + ih - bh, bw, bh, 4), class: "bar" }); svg.append(bar); }
    const hit = s("rect", { x: m.l + i * band, y: m.t, width: band, height: ih, class: "hit" });
    hit.addEventListener("mousemove", e => { bar && bar.classList.add("active"); showTip(e, d.label, d.tip || [["Value", fmt(d.value)]]); });
    hit.addEventListener("mouseleave", () => { bar && bar.classList.remove("active"); hideTip(); });
    hits.push(hit);
    const lab = xLabel ? xLabel(i, d) : d.label;
    if (lab) axisText(svg, m.l + i * band + band / 2, H - 8, lab);
  });
  svg.append(s("line", { x1: m.l, x2: W - m.r, y1: m.t + ih, y2: m.t + ih, class: "baseline" }));
  hits.forEach(r => svg.append(r));
  replace(container, svg);
}

/**
 * data: [{label, value|null, tip}]; null values break the line. Crosshair + tooltip on hover.
 */
function lineChart(container, data, { height = 220, fmt = fmtNum, max: fixedMax, xLabel, aria = "Line chart" } = {}) {
  if (!data.length) return replace(container, h("div", { class: "empty-chart", text: "No data" }));
  const f = frame(container, height), { W, H, m } = f;
  const iw = W - m.l - m.r, ih = H - m.t - m.b;
  const values = data.map(d => d.value).filter(v => v !== null && v !== undefined);
  if (!values.length) return replace(container, h("div", { class: "empty-chart", text: "No completed requests in this range" }));
  const max = fixedMax || niceMax(Math.max(...values));
  const step = data.length > 1 ? iw / (data.length - 1) : 0;
  const X = i => m.l + (data.length > 1 ? i * step : iw / 2);
  const Y = v => m.t + ih - (v / max) * ih;
  const svg = s("svg", { viewBox: `0 0 ${W} ${H}`, role: "img", "aria-label": aria });
  const defs = s("defs");
  const grad = s("linearGradient", { id: "redfade", x1: 0, x2: 0, y1: 0, y2: 1 });
  grad.append(s("stop", { offset: "0%", "stop-color": "#e5202e", "stop-opacity": "0.28" }),
              s("stop", { offset: "100%", "stop-color": "#e5202e", "stop-opacity": "0" }));
  defs.append(grad); svg.append(defs);
  yAxis(svg, f, max, fmt);
  // split into continuous segments
  let seg = [];
  const segments = [];
  data.forEach((d, i) => {
    if (d.value === null || d.value === undefined) { if (seg.length) segments.push(seg); seg = []; }
    else seg.push([X(i), Y(d.value)]);
  });
  if (seg.length) segments.push(seg);
  for (const pts of segments) {
    if (pts.length === 1) { svg.append(s("circle", { cx: pts[0][0], cy: pts[0][1], r: 3, class: "dot" })); continue; }
    const line = pts.map((p, i) => `${i ? "L" : "M"}${p[0]},${p[1]}`).join("");
    svg.append(s("path", { d: `${line}L${pts[pts.length - 1][0]},${m.t + ih}L${pts[0][0]},${m.t + ih}Z`, class: "area" }));
    svg.append(s("path", { d: line, class: "line" }));
  }
  data.forEach((d, i) => { const lab = xLabel ? xLabel(i, d) : null; if (lab) axisText(svg, X(i), H - 8, lab); });
  svg.append(s("line", { x1: m.l, x2: W - m.r, y1: m.t + ih, y2: m.t + ih, class: "baseline" }));
  const cross = s("line", { y1: m.t, y2: m.t + ih, class: "crosshair", visibility: "hidden" });
  const dot = s("circle", { r: 5, class: "dot", visibility: "hidden" });
  svg.append(cross, dot);
  const overlay = s("rect", { x: m.l, y: m.t, width: iw, height: ih, class: "hit" });
  overlay.addEventListener("mousemove", e => {
    const box = svg.getBoundingClientRect();
    const px = (e.clientX - box.left) * (W / box.width);
    const i = Math.max(0, Math.min(data.length - 1, Math.round((px - m.l) / (step || 1))));
    const d = data[i];
    cross.setAttribute("x1", X(i)); cross.setAttribute("x2", X(i)); cross.setAttribute("visibility", "visible");
    if (d.value !== null && d.value !== undefined) {
      dot.setAttribute("cx", X(i)); dot.setAttribute("cy", Y(d.value)); dot.setAttribute("visibility", "visible");
    } else dot.setAttribute("visibility", "hidden");
    showTip(e, d.label, d.tip || [["Value", d.value === null ? "no data" : fmt(d.value)]]);
  });
  overlay.addEventListener("mouseleave", () => { cross.setAttribute("visibility", "hidden"); dot.setAttribute("visibility", "hidden"); hideTip(); });
  svg.append(overlay);
  replace(container, svg);
}

/** rows: [{label (Node|string), value, color?, tip?}] - magnitude by length, identity by label. */
function hbarList(container, rows, { fmt = fmtNum, empty = "No data", total } = {}) {
  if (!rows.length) return replace(container, h("div", { class: "empty-chart", text: empty }));
  const max = Math.max(...rows.map(r => r.value), 1);
  const sum = total ?? rows.reduce((a, r) => a + Number(r.value), 0);
  replace(container, h("div", { class: "hbars" }, rows.map(r => {
    const fill = h("div", { class: "hbar-fill" });
    fill.style.width = `${(r.value / max) * 100}%`;
    if (r.color) fill.style.background = r.color;
    const row = h("div", { class: "hbar" }, h("div", { class: "hbar-label", title: typeof r.label === "string" ? r.label : null }, r.label),
      h("div", { class: "hbar-track" }, fill), h("div", { class: "hbar-value", text: fmt(r.value) }));
    row.addEventListener("mousemove", e => showTip(e, r.title || (typeof r.label === "string" ? r.label : ""),
      r.tip || [["Count", fmt(r.value)], ["Share", pct(Number(r.value), sum)]]));
    row.addEventListener("mouseleave", hideTip);
    return row;
  })));
}

const RAMP = ["var(--seq-1)", "var(--seq-2)", "var(--seq-3)", "var(--seq-4)", "var(--seq-5)", "var(--seq-6)", "var(--seq-7)"];
/** cells: [{dow 1-7, hour 0-23, count}] */
function heatmap(container, cells) {
  const grid = Array.from({ length: 7 }, () => Array(24).fill(0));
  cells.forEach(c => { grid[c.dow - 1][c.hour] = Number(c.count); });
  const max = Math.max(...grid.flat(), 0);
  const days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
  const color = v => v === 0 ? "var(--seq-0)" : RAMP[Math.min(RAMP.length - 1, Math.floor((v / max) * RAMP.length - 1e-9))];
  const el = h("div", { class: "heat", role: "img", "aria-label": `Requests by weekday and hour, peak ${max}` });
  el.append(h("div"));
  for (let hr = 0; hr < 24; hr++) el.append(h("div", { class: "collab", text: hr % 3 === 0 ? String(hr).padStart(2, "0") : "" }));
  grid.forEach((row, d) => {
    el.append(h("div", { class: "rowlab", text: days[d] }));
    row.forEach((v, hr) => {
      const c = h("div", { class: "cell" });
      c.style.background = color(v);
      c.addEventListener("mousemove", e => showTip(e, `${days[d]} ${String(hr).padStart(2, "0")}:00-${String((hr + 1) % 24).padStart(2, "0")}:00`, [["Requests", fmtNum(v)]]));
      c.addEventListener("mouseleave", hideTip);
      el.append(c);
    });
  });
  const legend = h("div", { class: "heat-legend" }, h("span", { text: "0" }),
    ["var(--seq-0)", ...RAMP].map(c => { const sw = h("span", { class: "sw" }); sw.style.background = c; return sw; }),
    h("span", { text: fmtNum(max) }), h("span", { class: "muted", text: " requests per hour" }));
  replace(container, el, legend);
}
