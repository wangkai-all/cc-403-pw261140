/* Pointwise console.
 *
 * All numbers on this page come from the API.  Nothing here computes funnel
 * results, and a null conversion_rate is rendered as "not yet computable" —
 * never as 0%.
 */
"use strict";

const $ = (sel) => document.querySelector(sel);
const els = {
  funnel: $("#funnel-id"),
  health: $("#health"),
  stateBadge: $("#state-badge"),
  stateMessage: $("#state-message"),
  headline: $("#headline"),
  provenance: $("#provenance"),
  steps: $("#steps"),
  windowDays: $("#window-days"),
  useWindow: $("#use-window"),
  rulesHash: $("#rules-hash"),
  diff: $("#diff"),
  canvas: $("#funnel-canvas"),
  ingestText: $("#ingest-text"),
  ingestOut: $("#ingest-out"),
  eventCount: $("#event-count"),
  rbStatus: $("#rb-status"),
  rbPasses: $("#rb-passes"),
  rbReused: $("#rb-reused"),
  rbRecomputed: $("#rb-recomputed"),
  affected: $("#affected"),
  progress: $("#progress-fill"),
  userId: $("#user-id"),
  userSummary: $("#user-summary"),
  userPath: $("#user-path"),
  stopBtn: $("#stop-btn"),
  resumeBtn: $("#resume-btn"),
};

const DAY_MS = 24 * 3600 * 1000;
const state = {
  steps: [],
  windowDays: 7,
  funnelId: "checkout",
  lastPreview: null,
  jobId: null,
  poll: null,
  frozen: null,
};

/* ---------- tiny helpers ---------- */

async function api(method, path, body, ctype) {
  const opts = { method, headers: {} };
  if (body !== undefined) {
    opts.body = typeof body === "string" ? body : JSON.stringify(body);
    opts.headers["Content-Type"] = ctype || "application/json";
  }
  const res = await fetch(path, opts);
  const text = await res.text();
  let data;
  try { data = JSON.parse(text); } catch { data = { error: text }; }
  if (!res.ok) {
    const err = new Error(data.error || `HTTP ${res.status}`);
    err.details = data.details || {};
    err.data = data;
    throw err;
  }
  return data;
}

function fmtMs(ts) {
  if (ts === null || ts === undefined) return "—";
  const d = new Date(ts);
  return d.toISOString().replace("T", " ").replace("Z", " UTC");
}
function fmtGap(ms) {
  if (ms === null || ms === undefined) return "—";
  const s = Math.round(ms / 1000);
  if (Math.abs(s) < 90) return `${s}s`;
  const m = Math.round(s / 60);
  if (Math.abs(m) < 90) return `${m}m`;
  const h = Math.round(m / 60);
  if (Math.abs(h) < 48) return `${h}h`;
  return `${Math.round(h / 24)}d`;
}
function pct(x) {
  if (x === null || x === undefined) return null;
  return `${(x * 100).toFixed(1)}%`;
}
function el(tag, cls, text) {
  const node = document.createElement(tag);
  if (cls) node.className = cls;
  if (text !== undefined) node.textContent = text;
  return node;
}

/* ---------- step editor ---------- */

function blankStep(index) {
  return {
    name: index === 0 ? "Entry" : `Step ${index + 1}`,
    event_name: index === 0 ? "app_open" : "",
    min_wait_ms: null,
    max_wait_ms: null,
    same_session: false,
    same_session_scope: "session",
    count_repeats: false,
    description: "",
  };
}

function rulesPayload() {
  return {
    // Unchecked means "no limit". A zero-length window would mean "only the
    // same instant", which is the opposite of what the checkbox implies.
    conversion_window_ms: state.useWindow === false
      ? 90 * DAY_MS
      : Math.round(state.windowDays * DAY_MS),
    steps: state.steps.map((s) => ({
      name: s.name,
      event_name: s.event_name || null,
      min_wait_ms: s.min_wait_ms,
      max_wait_ms: s.max_wait_ms,
      same_session: !!s.same_session,
      same_session_scope: s.same_session_scope || "session",
      count_repeats: !!s.count_repeats,
      description: s.description || "",
    })),
  };
}

function renderSteps() {
  els.steps.replaceChildren();
  state.steps.forEach((step, i) => {
    const row = el("div", "step");
    row.appendChild(el("div", "idx", String(i + 1)));

    row.setAttribute("role", "listitem");
    const nameInput = el("input");
    nameInput.type = "text";
    nameInput.value = step.name;
    nameInput.title = "Step label";
    nameInput.setAttribute("aria-label", `step ${i + 1} label`);
    nameInput.oninput = () => { step.name = nameInput.value; schedulePreview(); };
    row.appendChild(nameInput);

    const evInput = el("input");
    evInput.type = "text";
    evInput.value = step.event_name || "";
    evInput.placeholder = i === 0 ? "entry event" : "event name";
    evInput.title = "Event name that satisfies this step";
    evInput.setAttribute("aria-label", `step ${i + 1} event`);
    evInput.oninput = () => { step.event_name = evInput.value || null; schedulePreview(); };
    row.appendChild(evInput);

    const minInput = el("input");
    minInput.type = "number";
    minInput.min = "0";
    minInput.placeholder = "min wait ms";
    minInput.value = step.min_wait_ms ?? "";
    minInput.title = "Minimum wait since the previous step (ms)";
    minInput.setAttribute("aria-label", `step ${i + 1} min wait ms`);
    minInput.oninput = () => { step.min_wait_ms = numOrNull(minInput.value); schedulePreview(); };
    row.appendChild(minInput);

    const maxInput = el("input");
    maxInput.type = "number";
    maxInput.min = "0";
    maxInput.placeholder = "max wait ms";
    maxInput.value = step.max_wait_ms ?? "";
    maxInput.title = "Maximum wait since the previous step (ms)";
    maxInput.setAttribute("aria-label", `step ${i + 1} max wait ms`);
    maxInput.oninput = () => { step.max_wait_ms = numOrNull(maxInput.value); schedulePreview(); };
    row.appendChild(maxInput);

    const flags = el("div", "flags");
    if (i > 0) {
      flags.appendChild(checkbox("same session", step.same_session, (v) => {
        step.same_session = v; schedulePreview();
      }));
    }
    flags.appendChild(checkbox("repeats", step.count_repeats, (v) => {
      step.count_repeats = v; schedulePreview();
    }));
    row.appendChild(flags);

    const actions = el("div", "step-actions");
    const up = el("button", "ghost", "↑");
    up.title = "Move up";
    up.disabled = i === 0;
    up.onclick = () => { swap(i, i - 1); };
    const down = el("button", "ghost", "↓");
    down.title = "Move down";
    down.disabled = i === state.steps.length - 1;
    down.onclick = () => { swap(i, i + 1); };
    const del = el("button", "ghost", "✕");
    del.title = "Remove step";
    del.disabled = state.steps.length <= 1;
    del.onclick = () => { state.steps.splice(i, 1); renderSteps(); schedulePreview(); };
    actions.append(up, down, del);
    row.appendChild(actions);

    els.steps.appendChild(row);
  });
  els.windowDays.value = state.windowDays;
  els.windowDays.disabled = state.useWindow === false;
  els.useWindow.checked = state.useWindow !== false;
}

function checkbox(label, checked, onChange) {
  const wrapper = el("label");
  const input = el("input");
  input.type = "checkbox";
  input.checked = !!checked;
  input.onchange = () => onChange(input.checked);
  wrapper.append(input, document.createTextNode(label));
  return wrapper;
}
function numOrNull(v) {
  if (v === "" || v === null || v === undefined) return null;
  const n = Number(v);
  return Number.isFinite(n) ? Math.max(0, Math.round(n)) : null;
}
function swap(a, b) {
  [state.steps[a], state.steps[b]] = [state.steps[b], state.steps[a]];
  renderSteps();
  schedulePreview();
}

let previewTimer = null;
function schedulePreview() {
  els.rulesHash.textContent = "editing…";
  clearTimeout(previewTimer);
  previewTimer = setTimeout(() => preview(true), 400);
}

/* ---------- preview & drawing ---------- */

async function preview(quiet) {
  const funnel = els.funnel.value.trim() || "checkout";
  try {
    const data = await api("POST", `/api/funnels/${encodeURIComponent(funnel)}/preview`, rulesPayload());
    state.lastPreview = data;
    els.rulesHash.textContent = `hash ${data.rules_hash.slice(0, 12)} · live preview${data.comparable_to_current ? " · current" : ""}`;
    renderDiff(data.diff || []);
    drawFunnel(data.steps || [], data.entries || 0, data.conversions || 0);
    if (!quiet) flash("Preview computed against the live log.");
    return data;
  } catch (err) {
    state.lastPreview = null;
    els.rulesHash.textContent = "preview failed";
    renderDiff([`${err.message}`], "warn");
    if (!quiet) flash(err.message, true);
    return null;
  }
}

function renderDiff(lines, cls) {
  els.diff.replaceChildren();
  if (!lines.length) return;
  for (const line of lines) {
    els.diff.appendChild(el("div", cls || (line.startsWith("+") ? "added" : "warn"), line));
  }
}

function drawFunnel(steps, entries, conversions) {
  const canvas = els.canvas;
  const ctx = canvas.getContext("2d");
  const dpr = window.devicePixelRatio || 1;
  const cssW = canvas.clientWidth || 880;
  const cssH = 240;
  canvas.width = Math.round(cssW * dpr);
  canvas.height = Math.round(cssH * dpr);
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, cssW, cssH);

  const css = getComputedStyle(document.body);
  const ink = css.getPropertyValue("--ink").trim();
  const muted = css.getPropertyValue("--muted").trim();
  const accent = css.getPropertyValue("--accent").trim();
  const bar2 = css.getPropertyValue("--bar2").trim();
  ctx.font = "12px ui-sans-serif, system-ui, sans-serif";

  if (!steps.length) {
    ctx.fillStyle = muted;
    ctx.fillText("Add steps to draw the funnel.", 12, 30);
    return;
  }
  const n = steps.length;
  const gap = 12;
  const left = 40;
  const width = Math.max(60, (cssW - left * 2 - gap * (n - 1)) / n);
  const maxH = cssH - 62;
  const top = 26;

  steps.forEach((step, i) => {
    const reached = step.reached || 0;
    const base = entries || 0;
    const h = base > 0 ? Math.max(2, (reached / base) * maxH) : 2;
    const x = left + i * (width + gap);
    const y = top + (maxH - h);

    // backdrop
    ctx.fillStyle = css.getPropertyValue("--step").trim();
    ctx.beginPath();
    ctx.roundRect(x, top, width, maxH, 6);
    ctx.fill();

    ctx.fillStyle = i === 0 ? accent : bar2;
    ctx.beginPath();
    ctx.roundRect(x, y, width, h, 6);
    ctx.fill();

    ctx.fillStyle = ink;
    ctx.textAlign = "center";
    ctx.fillText(trimLabel(step.name), x + width / 2, 16);
    ctx.fillStyle = muted;
    ctx.fillText(String(reached), x + width / 2, cssH - 34);
    if (base > 0) {
      ctx.fillText(`${((reached / base) * 100).toFixed(0)}%`, x + width / 2, cssH - 18);
    }
    ctx.textAlign = "left";
  });
  ctx.fillStyle = muted;
  ctx.fillText(`entries ${entries} · conversions ${conversions}`, left, cssH - 2);
}

function trimLabel(s) {
  const t = String(s || "");
  return t.length > 14 ? t.slice(0, 13) + "…" : t;
}

/* ---------- status / report ---------- */

function rateNode(report) {
  const wrap = el("div");
  wrap.appendChild(el("span", "label", "conversion rate"));
  if (!report) {
    wrap.appendChild(el("span", "value rate-null", "—"));
    wrap.appendChild(el("span", "hint", "no report yet"));
    return wrap;
  }
  if (report.status !== "complete") {
    wrap.appendChild(el("span", "value rate-null", "stale"));
    wrap.appendChild(el("span", "hint", report.stale_reason || "rebuild to refresh"));
    return wrap;
  }
  if (!report.conversion_rate_available || report.conversion_rate === null) {
    wrap.appendChild(el("span", "value rate-null", "not yet computable"));
    wrap.appendChild(el("span", "hint", "all entrants still inside their window"));
    return wrap;
  }
  wrap.appendChild(el("span", "value", pct(report.conversion_rate)));
  const hint = report.conversion_rate_provisional
    ? `provisional · ${report.conversions}/${report.entries} entrants · settled ${pct(report.settled_conversion_rate)}`
    : `${report.conversions} / ${report.entries} entrants`;
  wrap.appendChild(el("span", "hint", hint));
  return wrap;
}

function renderStatus(data) {
  const badge = els.stateBadge;
  badge.textContent = data.state.replace(/_/g, " ");
  badge.className = `badge ${data.state}`;
  els.stateMessage.textContent = data.message || "";
  els.eventCount.textContent = `${data.event_count} events`;
  els.health.textContent = "online";

  const report = data.report;
  const headline = els.headline;
  headline.replaceChildren();
  const entriesBox = el("div");
  entriesBox.append(el("span", "label", "entries"), el("span", "value", report ? String(report.entries) : "—"));
  const convBox = el("div");
  convBox.append(el("span", "label", "conversions"), el("span", "value", report ? String(report.conversions) : "—"));
  const unresolvedBox = el("div");
  unresolvedBox.append(
    el("span", "label", "unresolved"),
    el("span", "value", report ? String(report.unresolved_entries) : "—"),
    el("span", "hint", "inside conversion window"),
  );
  headline.append(entriesBox, convBox, unresolvedBox, rateNode(report));

  if (report) {
    const rules = data.rules;
    els.provenance.textContent =
      `report ${report.report_id.slice(0, 8)} · rules v${report.rules_version} (${report.rules_hash.slice(0, 12)})` +
      ` · snapshot ${String(report.rules_sha256 || "").slice(0, 12)}` +
      ` · input ${report.input_digest.slice(0, 12)} · ${report.event_count} events` +
      ` · engine ${report.engine_version}` +
      (rules && rules.rules_hash !== report.rules_hash ? " · report rules differ from current" : "");

  } else {
    els.provenance.textContent = data.input_digest
      ? `no frozen report · input ${data.input_digest.slice(0, 12)} · ${data.event_count} events`
      : "";
  }

  const rb = data.rebuild;
  if (rb) {
    state.jobId = rb.job_id;
    els.rbStatus.textContent = rb.status;
    els.rbPasses.textContent = String(rb.passes);
    els.rbReused.textContent = String(rb.reused_intervals);
    els.rbRecomputed.textContent = String(rb.recomputed_intervals);
    const p = rb.progress || {};
    const total = p.intervals_total || 0;
    const done = p.intervals_done || 0;
    els.progress.style.width = total > 0 ? `${Math.min(100, (done / total) * 100)}%` : "0%";
    const bits = [];
    if (rb.affected_reason) bits.push(rb.affected_reason);
    if (rb.affected_from_ms) bits.push(`affected from ${fmtMs(rb.affected_from_ms)}`);
    if (rb.affected_from_event_id) bits.push(`event ${rb.affected_from_event_id}`);
    if (rb.input_changed_during_rebuild) bits.push("input changed during rebuild → extra pass");
    if (rb.error) bits.push(`error: ${rb.error}`);
    els.affected.textContent = bits.length ? bits.join(" · ") : "no affected boundary reported";
    els.stopBtn.disabled = !["queued", "running"].includes(rb.status);
    els.resumeBtn.disabled = !["paused", "failed"].includes(rb.status);
    if (["queued", "running"].includes(rb.status)) {
      ensurePolling();
    }
  } else {
    els.stopBtn.disabled = true;
    els.resumeBtn.disabled = true;
  }

  if (data.rules) {
    state.frozen = data.rules;
  }
  if (state.stepsTouched && state.lastPreview) {
    // The editor is the thing on screen; don't overwrite it with report data.
    drawFunnel(state.lastPreview.steps || [], state.lastPreview.entries, state.lastPreview.conversions);
  } else if (report) {
    drawFunnel(report.steps || [], report.entries, report.conversions);
  } else if (state.lastPreview) {
    drawFunnel(state.lastPreview.steps || [], state.lastPreview.entries, state.lastPreview.conversions);
  }
}

async function refresh() {
  const funnel = els.funnel.value.trim() || "checkout";
  try {
    const data = await api("GET", `/api/funnels/${encodeURIComponent(funnel)}/status`);
    renderStatus(data);
    return data;
  } catch (err) {
    els.health.textContent = "offline";
    flash(err.message, true);
    return null;
  }
}

let pollTimer = null;
function ensurePolling() {
  if (pollTimer) return;
  pollTimer = setInterval(async () => {
    const data = await refresh();
    if (!data || !data.rebuild || !["queued", "running"].includes(data.rebuild.status)) {
      clearInterval(pollTimer);
      pollTimer = null;
    }
  }, 700);
}

/* ---------- actions ---------- */

function flash(msg, isError) {
  els.ingestOut.textContent = msg;
  els.ingestOut.style.color = isError ? "var(--bad)" : "var(--muted)";
}

async function freezeAndRebuild() {
  const funnel = els.funnel.value.trim() || "checkout";
  const payload = rulesPayload();
  // Always preview immediately before freezing: the debounce may not have
  // fired yet, and freezing a hash the server never saw would be a lie.
  const previewData = await preview(true);
  if (!previewData) {
    flash("Cannot freeze: the rules do not validate (see the diff line).", true);
    return;
  }
  try {
    const frozen = await api("POST", `/api/funnels/${encodeURIComponent(funnel)}/versions`, {
      rules: payload,
      expected_hash: previewData.rules_hash,
      note: "frozen from console",
    });
    state.stepsTouched = false;
    els.rulesHash.textContent = `frozen v${frozen.version} · ${frozen.rules_hash.slice(0, 12)}`;
    await api("POST", `/api/funnels/${encodeURIComponent(funnel)}/reports`, {
      version: frozen.version,
      reason: "console freeze + rebuild",
    });
    ensurePolling();
    await refresh();
    flash(`Froze rules v${frozen.version} and started the rebuild.`);
  } catch (err) {
    flash(err.message + (err.details && err.details.actual ? ` (preview ${err.details.expected}, now ${err.details.actual})` : ""), true);
  }
}

async function ingestText() {
  const text = els.ingestText.value.trim();
  if (!text) return;
  let body;
  let ctype = "application/json";
  try {
    body = JSON.parse(text);
  } catch {
    body = text;
    ctype = "application/x-ndjson";
  }
  try {
    const out = await api("POST", "/api/collect", body, ctype);
    flash(`accepted ${out.accepted} · inserted ${out.inserted} · corrected ${out.corrected} · unchanged ${out.unchanged}` +
      (out.rejected.length ? ` · rejected ${out.rejected.length}` : "") +
      (out.min_ts ? ` · span ${fmtMs(out.min_ts)} → ${fmtMs(out.max_ts)}` : ""));
    await refresh();
  } catch (err) {
    flash(err.message, true);
  }
}

async function seed() {
  try {
    const out = await api("POST", "/api/seed", { users: 240, days: 10, seed: Math.floor(Math.random() * 10000) });
    flash(`generated ${out.inserted} events across ${out.affected_users.length} users` +
      (out.corrected ? ` · ${out.corrected} corrections delivered afterwards` : ""));
    await refresh();
  } catch (err) {
    flash(err.message, true);
  }
}

async function showPath() {
  const funnel = els.funnel.value.trim() || "checkout";
  const userId = els.userId.value.trim();
  if (!userId) return;
  let url = `/api/users/${encodeURIComponent(userId)}/path?funnel_id=${encodeURIComponent(funnel)}`;
  if (state.stepsTouched || !state.frozen) {
    // Show the path under the rules currently being edited, not the last
    // frozen version, so the editor and the path agree while experimenting.
    url += `&rules_json=${encodeURIComponent(JSON.stringify(rulesPayload()))}`;
  }
  try {
    const data = await api("GET", url);
    renderPath(data);
  } catch (err) {
    els.userSummary.textContent = err.message;
    els.userPath.replaceChildren();
  }
}

function renderPath(data) {
  els.userSummary.replaceChildren();
  const line = el("div");
  line.append(
    el("span", "label", `${data.user_id} · `),
    document.createTextNode(
      `${data.completed ? "completed" : data.entered ? `at step ${data.max_step}` : "never entered"} · ` +
      `rules ${data.rules_hash.slice(0, 12)}${data.rules_stale ? " (not the latest frozen version)" : ""}`
    ),
  );
  els.userSummary.appendChild(line);

  els.userPath.replaceChildren();
  for (const ev of data.path || []) {
    const li = el("li");
    if (ev.kind === "identity_merge") li.classList.add("merge");
    if (ev.revision > 0) li.classList.add("corrected");
    const hits = ev.matched_steps || [];
    if (hits.length) li.classList.add("matched");
    li.append(
      el("span", "t", fmtMs(ev.ts_ms).slice(11, 19)),
      el("span", "", `${ev.name}${ev.kind !== "event" ? ` · ${ev.kind}` : ""}${ev.revision ? ` · rev ${ev.revision}` : ""}`),
      el("span", "sess", ev.session_id || "—"),
    );
    if (hits.length) {
      li.title = `satisfied step${hits.length > 1 ? "s" : ""} ${hits.map((i) => i + 1).join(", ")}`;
      li.appendChild(el("span", "hit", `✓ ${hits.map((i) => i + 1).join(",")}`));
    }
    els.userPath.appendChild(li);
  }
  if (!els.userPath.children.length) {
    els.userPath.appendChild(el("li", "empty", "No events for this user."));
  }
}

/* ---------- wiring ---------- */

$("#add-step").onclick = () => {
  state.steps.push(blankStep(state.steps.length));
  state.stepsTouched = true;
  renderSteps();
  schedulePreview();
};
$("#preview").onclick = () => preview(false);
$("#freeze").onclick = freezeAndRebuild;
$("#ingest-btn").onclick = ingestText;
$("#seed-btn").onclick = seed;
$("#path-btn").onclick = showPath;
els.stopBtn.onclick = async () => { if (state.jobId) { await api("POST", `/api/rebuilds/${state.jobId}/stop`, {}); await refresh(); } };
els.resumeBtn.onclick = async () => { if (state.jobId) { await api("POST", `/api/rebuilds/${state.jobId}/resume`, {}); ensurePolling(); await refresh(); } };
els.windowDays.oninput = () => { state.windowDays = Math.max(0, Number(els.windowDays.value) || 0); schedulePreview(); };
els.useWindow.onchange = () => {
  state.useWindow = els.useWindow.checked;
  els.windowDays.disabled = !state.useWindow;
  schedulePreview();
};
els.funnel.onchange = async () => { await loadFunnel(); };
els.ingestFile = $("#ingest-file");
els.ingestFile.onchange = async (event) => {
  const file = event.target.files[0];
  if (!file) return;
  els.ingestText.value = await file.text();
  await ingestText();
};
window.addEventListener("resize", () => {
  if (state.lastPreview) drawFunnel(state.lastPreview.steps, state.lastPreview.entries, state.lastPreview.conversions);
});

async function loadFunnel() {
  const funnel = els.funnel.value.trim() || "checkout";
  state.funnelId = funnel;
  state.stepsTouched = false;
  const data = await refresh();
  if (data && data.rules) {
    state.steps = data.rules.steps.map((s) => ({
      name: s.name,
      event_name: s.event_name,
      min_wait_ms: s.min_wait_ms,
      max_wait_ms: s.max_wait_ms,
      same_session: !!s.same_session,
      same_session_scope: s.same_session_scope || "session",
      count_repeats: !!s.count_repeats,
      description: "",
    }));
    state.windowDays = Math.max(
      1 / 24,
      (data.rules.conversion_window_ms || 7 * DAY_MS) / DAY_MS,
    );
  } else {
    state.steps = [
      { name: "Entry", event_name: "app_open", min_wait_ms: null, max_wait_ms: null, same_session: false, count_repeats: false, description: "" },
      { name: "Product", event_name: "view_product", min_wait_ms: null, max_wait_ms: null, same_session: false, count_repeats: false, description: "" },
      { name: "Cart", event_name: "add_to_cart", min_wait_ms: null, max_wait_ms: null, same_session: false, count_repeats: false, description: "" },
      { name: "Checkout", event_name: "checkout_started", min_wait_ms: null, max_wait_ms: null, same_session: false, count_repeats: false, description: "" },
      { name: "Purchase", event_name: "purchase", min_wait_ms: null, max_wait_ms: 7 * DAY_MS, same_session: false, count_repeats: false, description: "" },
    ];
    state.windowDays = 7;
  }
  state.useWindow = true;
  renderSteps();
  await preview(true);
  const health = await api("GET", "/api/health");
  els.health.textContent = `${health.event_count} events · online`;
}

loadFunnel();
