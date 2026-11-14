// The panel's operation area (architecture §3.17.1; implementation §6, The
// operator console): the view of the network through the park queries, the
// requests on the fleet, the Operator Channel's questions, the administration
// affordance generated from the schema, and the secrets and rates. Like the
// work area, it sets what the network sends as text, never as markup.
"use strict";

// --- state -----------------------------------------------------------------

// The views read by a park query: each one keeps its answer, when it was read
// and the query in flight, if any.
const parkViews = {
  census: { topic: "node_query", data: null, at: null, pending: null, error: "" },
  library: { topic: "node_catalog_query", data: null, at: null, pending: null, error: "" },
  topics: { topic: "topic_catalog_query", data: null, at: null, pending: null, error: "" },
};
const configView = { snapshot: null, schema: null, at: null, error: "" };

let opView = "network";      // the operation view on screen
let opStarted = false;
let libraryBySize = false;   // the library sorted by artifact_size
const traces = new Map();    // trace_id → what opened it: {view} or {act}
const early = new Map();     // outcomes that arrived before their trace was known
const activity = [];         // the acts and their outcomes, newest first
const opShown = new Set();   // operator questions already reported as shown
let freshTimer = null;

// --- areas -----------------------------------------------------------------

function showArea(area) {
  const op = area === "operation";
  $("area-work").classList.toggle("current", !op);
  $("area-operation").classList.toggle("current", op);
  $("work-side").hidden = op;
  $("work").hidden = op;
  $("op-nav").hidden = !op;
  $("operation").hidden = !op;
  if (op) showView(opView);
}

$("area-work").addEventListener("click", () => showArea("work"));
$("area-operation").addEventListener("click", () => showArea("operation"));

function showView(view) {
  opView = view;
  for (const v of ["network", "questions", "activity", "config", "secrets"]) {
    $("view-" + v).hidden = v !== view;
  }
  for (const b of document.querySelectorAll("#op-nav [data-view]")) {
    b.classList.toggle("current", b.dataset.view === view);
  }
  if (view === "network") {
    for (const name of Object.keys(parkViews)) {
      if (!parkViews[name].at && !parkViews[name].pending) refresh(name);
    }
  }
  if (view === "questions") loadQuestions();
  if (view === "config" && !configView.at) loadConfig();
  if (view === "secrets") loadHandles();
}

for (const b of document.querySelectorAll("#op-nav [data-view]")) {
  b.addEventListener("click", () => showView(b.dataset.view));
}

// operationStart runs on sign-in, once the event stream is open.
function operationStart() {
  opStarted = true;
  if (traceUI) {
    $("op-traces").href = traceUI;
    $("op-traces").hidden = false;
  }
  loadQuestions();
  clearInterval(freshTimer);
  freshTimer = setInterval(renderFreshness, 30000);
  if (!$("operation").hidden) showView(opView);
}

// operationReset discards what was on screen when the session closes.
function operationReset() {
  opStarted = false;
  clearInterval(freshTimer);
  for (const v of Object.values(parkViews)) Object.assign(v, { data: null, at: null, pending: null, error: "" });
  Object.assign(configView, { snapshot: null, schema: null, at: null, error: "" });
  traces.clear();
  early.clear();
  activity.length = 0;
  opShown.clear();
  for (const id of ["status", "op-queries", "op-expired", "op-activity", "config-form"]) $(id).replaceChildren();
  for (const b of document.querySelectorAll("[data-body]")) b.replaceChildren();
  renderFreshness();
  setBadge(0);
}

function operationEvents(events) {
  events.addEventListener("outcome", (e) => onOutcome(JSON.parse(e.data)));
  events.addEventListener("operator", () => loadQuestions());
}

// --- freshness -------------------------------------------------------------

// ago says how long ago a view was read, so the person knows how fresh what
// they look at is.
function ago(at) {
  const s = Math.round((Date.now() - at.getTime()) / 1000);
  if (s < 60) return "just now";
  const m = Math.round(s / 60);
  if (m < 60) return m + " min ago";
  return Math.round(m / 60) + " h ago";
}

function freshText(v) {
  if (v.pending) return v.at ? `Updating… (last read ${v.at.toLocaleTimeString()})` : "Reading…";
  if (v.error) return "Not read: " + v.error;
  if (!v.at) return "Not read yet";
  return `Read at ${v.at.toLocaleTimeString()} · ${ago(v.at)}`;
}

function renderFreshness() {
  for (const span of document.querySelectorAll("[data-fresh]")) {
    const name = span.dataset.fresh;
    span.textContent = freshText(name === "config" ? configView : parkViews[name]);
  }
  for (const b of document.querySelectorAll("[data-refresh]")) {
    const v = parkViews[b.dataset.refresh];
    b.disabled = !!(v && v.pending);
  }
}

for (const b of document.querySelectorAll("[data-refresh]")) {
  b.addEventListener("click", () => b.dataset.refresh === "config" ? loadConfig() : refresh(b.dataset.refresh));
}

// --- park queries ----------------------------------------------------------

// refresh opens the park query of a view; its answer arrives over the event
// stream (architecture §3.17.1: each query is a task on the Bus).
async function refresh(name) {
  const v = parkViews[name];
  if (v.pending) return;
  v.pending = "opening";
  v.error = "";
  renderFreshness();
  try {
    const opened = await api("POST", "/api/queries", { topic: v.topic }, true);
    v.pending = opened.trace_id;
    traces.set(opened.trace_id, { view: name });
    if (early.has(opened.trace_id)) {
      const o = early.get(opened.trace_id);
      early.delete(opened.trace_id);
      onOutcome(o);
    }
  } catch (err) {
    v.pending = null;
    v.error = err.message;
  }
  renderFreshness();
}

function onOutcome(o) {
  if (o.channel !== "operator") return;
  const t = traces.get(o.trace_id);
  if (!t) {
    // The outcome of an act opened elsewhere, like the command line, or of a
    // request whose answer has not come back yet.
    early.set(o.trace_id, o);
    setTimeout(() => {
      if (early.delete(o.trace_id)) addActivity({ label: "An act from another client", trace: o.trace_id, at: new Date() }, o);
    }, 3000);
    return;
  }
  traces.delete(o.trace_id);
  const d = o.details || {};
  if (t.view) {
    const v = parkViews[t.view];
    v.pending = null;
    if (d.failure_code || d.failure_reason) {
      v.error = [d.failure_code, d.failure_reason].filter(Boolean).join(": ");
    } else {
      v.data = o.result || d.data || {};
      v.at = new Date();
      v.error = "";
    }
    renderFreshness();
    renderPark();
    return;
  }
  addActivity(t.act, o);
  for (const name of t.act.refresh || []) {
    if (name === "config") loadConfig();
    else refresh(name);
  }
}

// --- acts ------------------------------------------------------------------

// act opens a request and records it, with the views its outcome changes: those
// are read again once it closes. onError, if given, takes a request the
// console refused instead of an alert.
async function act(label, path, body, refreshViews, onError) {
  try {
    const opened = await api("POST", path, body);
    const a = { label, trace: opened.trace_id, at: new Date(), refresh: refreshViews };
    traces.set(opened.trace_id, { act: a });
    addActivity(a, null);
    if (early.has(opened.trace_id)) {
      const o = early.get(opened.trace_id);
      early.delete(opened.trace_id);
      onOutcome(o);
    }
    return true;
  } catch (err) {
    if (onError) onError(err); else alert(label + ": " + err.message);
    return false;
  }
}

function addActivity(a, outcome) {
  const i = activity.findIndex((x) => x.trace === a.trace);
  const entry = { ...a, outcome };
  if (i >= 0) activity[i] = entry; else activity.unshift(entry);
  renderActivity();
}

function outcomeLine(o) {
  if (!o) return el("div", { class: "state" }, "Waiting for the outcome…");
  const d = o.details || {};
  if (d.failure_code || d.failure_reason) {
    return el("div", { class: "failure" }, "Rejected" + (d.failure_code ? ` (${d.failure_code})` : "") + (d.failure_reason ? ": " + d.failure_reason : ""));
  }
  const data = d.data || {};
  if (data.node_id) return el("div", {}, "Node up: " + data.node_id);
  if (data.node_ids) return el("div", {}, data.node_ids.length ? "Archived nodes: " + data.node_ids.join(", ") : "Done: no node was running it.");
  if (data.config_version) return el("div", {}, "Published version " + data.config_version);
  return el("div", {}, "Done");
}

function renderActivity() {
  $("op-activity").replaceChildren(...activity.map((a) => el("li", { class: "card" },
    el("div", { class: "meta" }, a.at.toLocaleString(), traceLink(a.trace)),
    el("div", { class: "text" }, a.label),
    outcomeLine(a.outcome))));
}

// --- the network view ------------------------------------------------------

function short(s, n = 12) {
  s = String(s ?? "");
  const hex = s.replace(/^sha256:/, "");
  return hex.length > n ? hex.slice(0, n) + "…" : hex;
}

function table(head, rows) {
  if (!rows.length) return el("p", { class: "hint" }, "Nothing to show.");
  return el("div", { class: "table-wrap" }, el("table", {},
    el("thead", {}, el("tr", {}, ...head.map((h) => el("th", {}, h)))),
    el("tbody", {}, ...rows)));
}

function renderPark() {
  renderStatus();
  renderCensus();
  renderLibrary();
  renderTopics();
}

function body(name) { return document.querySelector(`[data-body="${name}"]`); }

function renderCensus() {
  const nodes = parkViews.census.data?.nodes;
  if (!nodes) return;
  body("census").replaceChildren(nodes.length ? el("ol", { class: "cards" }, ...nodes.map((n) => el("li", { class: "card" },
    el("div", { class: "card-head" }, el("strong", {}, n.topic), el("span", { class: "pill" }, "node " + short(n.node_id, 10))),
    el("dl", { class: "facts" }, ...[
      ["Node", n.node_id],
      ["Artifact", n.artifact_ref],
      ["Similarity floor", String(n.similarity_floor)],
      ["Memory", n.memory_required + " MB"],
      ["Egress grants", (n.egress_grants || []).map((g) => `${g.slot} → ${g.entry}`).join(", ") || "—"],
      ["Vector space", n.model_id],
    ].flatMap(([k, v]) => [el("dt", {}, k), el("dd", {}, v)])),
    el("div", { class: "actions" }, el("button", { class: "danger", onclick: () => removeNode(n) }, "Remove"))))) : el("p", { class: "hint" }, "Nothing to show."));
}

// The removal and the veto are explained together, because what the person
// wants decides which one: removing lets the network bring the entry up again
// on a vacancy, vetoing prevents it.
const removeVsVeto = "The network can bring the entry up again if its topic goes vacant. To prevent that, veto the entry in the library.";

function removeNode(n) {
  if (!confirm(`Remove node ${n.node_id} on ${n.topic}?\n\n${removeVsVeto}`)) return;
  act(`Remove node ${short(n.node_id, 10)} on ${n.topic}`, "/api/nodes/retire", { kind: "remove", node_id: n.node_id }, ["census", "library", "topics"]);
}

function renderLibrary() {
  const lib = parkViews.library.data;
  if (!lib || !lib.entries) return;
  const entries = [...lib.entries];
  if (libraryBySize) entries.sort((a, b) => b.artifact_size - a.artifact_size);
  const storage = el("p", { class: "hint" },
    `Artifact storage: ${size(lib.storage_occupied)} of ${size(lib.storage_capacity)} used. `,
    el("button", { class: "link", onclick: () => { libraryBySize = !libraryBySize; renderLibrary(); } },
      libraryBySize ? "Sort by topic" : "Sort by size"));
  const cards = entries.map((e) => {
    const marks = [];
    if (e.veto_mark) marks.push("vetoed");
    if (e.deletion_mark) marks.push("image deleted");
    const seeded = e.acquisition?.provenance === "seeded";
    const facts = [
      ["Artifact", e.artifact_ref],
      ["Size", size(e.artifact_size)],
      ["Running with floor", e.active_floors.join(", ") || "—"],
      ["Floor history", e.floor_history.map((h) => `${h.similarity_floor} (${h.trigger_reason}, ${new Date(h.archived_at).toLocaleDateString()})`).join(", ") || "—"],
      ["Present in", e.present_in.join(", ") || "—"],
      ["Obtained", e.acquisition ? `${e.acquisition.provenance}, cost ${e.acquisition.cost}` : "—"],
    ];
    return el("li", { class: "card" },
      el("div", { class: "card-head" }, el("strong", {}, e.topic), el("span", { class: "pill" }, e.state),
        ...marks.map((m) => el("span", { class: "pill warn" }, m))),
      el("div", { class: "text" }, e.capability_text),
      el("dl", { class: "facts" }, ...facts.flatMap(([k, v]) => [el("dt", {}, k), el("dd", {}, v)])),
      el("div", { class: "actions" },
        el("button", { disabled: e.veto_mark || e.deletion_mark, onclick: () => provisionForm(e) }, "Provision"),
        e.veto_mark
          ? el("button", { onclick: () => retireEntry(e, "unveto") }, "Lift veto")
          : el("button", { onclick: () => retireEntry(e, "veto") }, "Veto"),
        el("button", {
          class: "danger", disabled: seeded || e.deletion_mark,
          title: seeded ? "A seeded entry's image is not deleted (architecture §3.13.8)." : null,
          onclick: () => retireEntry(e, "purge"),
        }, "Delete image")));
  });
  const bundles = (lib.bundles || []).map((b) => el("li", { class: "card" },
    el("div", { class: "text" }, b.purpose_text),
    el("div", { class: "meta" }, b.members.map((m) => {
      const e = lib.entries.find((x) => x.artifact_ref === m);
      return e ? e.topic : short(m);
    }).join(" · ")),
    el("div", { class: "actions" }, el("button", { onclick: () => instantiateBundle(b) }, "Instantiate bundle"))));
  body("library").replaceChildren(storage,
    cards.length ? el("ol", { class: "cards" }, ...cards) : el("p", { class: "hint" }, "Nothing to show."),
    bundles.length ? el("h3", {}, "Bundles") : "", bundles.length ? el("ol", { class: "cards" }, ...bundles) : "");
}

const retireText = {
  veto: (e) => `Veto ${e.topic}'s entry?\n\nThe nodes that run it are removed, and the network does not bring it up again until you lift the veto.`,
  unveto: (e) => `Lift the veto on ${e.topic}'s entry?\n\nThe network can bring it up again on a vacancy.`,
  purge: (e) => `Delete the image of ${e.topic}'s entry (${size(e.artifact_size)})?\n\nIt frees its storage. The entry stays in the library, and no network brings it up again until it is registered again.`,
};

function retireEntry(e, kind) {
  if (!confirm(retireText[kind](e))) return;
  const label = { veto: "Veto", unveto: "Lift the veto on", purge: "Delete the image of" }[kind];
  act(`${label} ${e.topic} (${short(e.artifact_ref)})`, "/api/nodes/retire", { kind, artifact_ref: e.artifact_ref }, ["census", "library", "topics"]);
}

// provisionForm asks for the egress slots the person associates, each with
// one of the registry's entries, and emits the node_provision (architecture
// §3.13.8). The Spawner rejects, with its reason, a slot left without one.
async function provisionForm(e) {
  const registry = await egressRegistry();
  const entries = Object.keys(registry).sort();
  const rows = el("div", { class: "stack-form" });
  const addRow = () => rows.append(el("div", { class: "row-form" },
    el("input", { name: "slot", placeholder: "Slot" }),
    el("select", { name: "entry" }, ...entries.map((k) => el("option", { value: k }, k))),
    el("button", { type: "button", class: "link", onclick: (ev) => ev.target.parentElement.remove() }, "Remove")));
  const dialog = el("dialog", { class: "dialog" },
    el("form", { method: "dialog" },
      el("h3", {}, "Provision " + e.topic),
      el("p", { class: "hint" }, e.capability_text),
      el("p", { class: "hint" }, "Associate each egress slot the entry needs with a registry entry. A slot left out closes the request with its reason."),
      rows,
      entries.length ? el("button", { type: "button", class: "link", onclick: addRow }, "Add a slot") : el("p", { class: "hint" }, "The egress registry has no entries."),
      el("div", { class: "actions" },
        el("button", { value: "cancel", type: "submit", class: "link" }, "Cancel"),
        el("button", { value: "ok", type: "submit" }, "Provision"))));
  document.body.append(dialog);
  dialog.addEventListener("close", () => {
    if (dialog.returnValue === "ok") {
      const associations = [...rows.children]
        .map((r) => ({ slot: r.querySelector("[name=slot]").value.trim(), entry: r.querySelector("[name=entry]").value }))
        .filter((a) => a.slot && a.entry);
      act(`Provision ${e.topic} (${short(e.artifact_ref)})`, "/api/nodes",
        { artifact_ref: e.artifact_ref, egress_associations: associations.length ? associations : undefined },
        ["census", "library", "topics"]);
    }
    dialog.remove();
  });
  dialog.showModal();
}

// instantiateBundle expands the bundle: one node_provision per member, each
// validated and closed on its own (architecture §3.17.1).
async function instantiateBundle(b) {
  const lib = parkViews.library.data;
  const members = b.members.map((m) => lib.entries.find((e) => e.artifact_ref === m) || { artifact_ref: m, topic: short(m) });
  if (!confirm(`Instantiate the bundle "${b.purpose_text}"?\n\nOne node per member: ${members.map((m) => m.topic).join(", ")}. Each one is provisioned on its own; if one fails, the others stay up and you retry only that one.`)) return;
  for (const m of members) {
    await act(`Provision ${m.topic} (${short(m.artifact_ref)}), from the bundle "${b.purpose_text}"`, "/api/nodes", { artifact_ref: m.artifact_ref }, ["census", "library", "topics"]);
  }
  showView("activity");
}

function renderTopics() {
  const topics = parkViews.topics.data?.topics;
  if (!topics) return;
  const label = { active: "nodes running", available: "can be brought up", uncovered: "uncovered" };
  body("topics").replaceChildren(table(["Topic", "State", "Description"],
    topics.map((t) => el("tr", {},
      el("td", {}, t.topic),
      el("td", {}, (label[t.state] || t.state) + (t.capped ? " · capped" : "")),
      el("td", { class: "wide" }, t.description)))));
}

// --- the network's state ---------------------------------------------------

// renderStatus shows each condition the park queries return that waits for
// the operator: what happens, since when and what resolves it, with the link
// to the act when the panel has it (architecture §3.13.3, §3.15.7).
function renderStatus() {
  const items = [];
  const since = (t) => new Date(t).toLocaleString();
  const lib = parkViews.library.data;
  const c = lib?.conditions;
  if (c?.capacity_pressure) {
    items.push(el("li", {},
      el("strong", {}, "The fleet is short of memory. "),
      `The Spawner evicts nodes to bring up others: ${c.capacity_pressure.evictions} evictions within the window, since ${since(c.capacity_pressure.since)}. `,
      "What resolves it is giving the fleet more capacity, an act of deployment."));
  }
  if (c?.storage_pressure) {
    items.push(el("li", {},
      el("strong", {}, "Artifact storage is filling up. "),
      `${size(c.storage_pressure.occupied)} of ${size(c.storage_pressure.capacity)} used, since ${since(c.storage_pressure.since)}. `,
      "Delete the images you no longer need, or enlarge the storage. ",
      el("button", { class: "link", onclick: () => { libraryBySize = true; renderLibrary(); body("library").scrollIntoView(); } }, "See the library by size")));
  }
  if (c?.vector_space) {
    items.push(el("li", {},
      el("strong", {}, "The vector space cannot be repaired. "),
      `${c.vector_space.pending.length} entries could not be regenerated under ${c.vector_space.model_id}, since ${since(c.vector_space.since)}. `,
      "Wait for the embedding model to answer again, or go back to the previous model. ",
      el("button", { class: "link", onclick: () => showView("config") }, "Open the configuration")));
  }
  for (const a of c?.audiences || []) {
    items.push(el("li", {},
      el("strong", {}, `Nobody answers the ${a.audience} audience. `),
      `Its questions close without an answer since ${since(a.since)}. `,
      "What resolves it is a channel of that audience that takes them."));
  }
  for (const t of (parkViews.topics.data?.topics || []).filter((t) => t.capped)) {
    items.push(el("li", {},
      el("strong", {}, `The network stopped trying to cover ${t.topic}. `),
      "It reached the limit of successive candidates for that topic, and does not try again until the topic stops being vacant."));
  }
  if (!lib && !parkViews.topics.data) {
    $("status").replaceChildren();
    return;
  }
  const fresh = el("span", { class: "fresh" }, "As of the library and topics reads.");
  $("status").replaceChildren(items.length
    ? el("div", { class: "status-box warn" }, el("h2", {}, "Network state"), el("ul", {}, ...items), fresh)
    : el("div", { class: "status-box ok" }, "Nothing waits for you on the network. ", fresh));
}

// --- the operator's questions ----------------------------------------------

function setBadge(n) {
  for (const id of ["op-badge", "op-questions-badge"]) {
    $(id).textContent = String(n);
    $(id).hidden = n === 0;
  }
}

async function loadQuestions() {
  if (!opStarted) return;
  let list;
  try {
    list = await api("GET", "/api/operator/queries", undefined, true);
  } catch (err) {
    $("op-queries").replaceChildren(el("li", { class: "hint" }, err.message));
    return;
  }
  setBadge(list.queries.length);
  $("op-queries").replaceChildren(...(list.queries.length
    ? list.queries.map(renderQuestion)
    : [el("li", { class: "hint" }, "No questions wait for you.")]));
  const onScreen = !$("operation").hidden && opView === "questions";
  if (list.expired.length && onScreen) {
    $("op-expired").replaceChildren(el("section", { class: "notices" },
      el("header", {}, el("strong", {}, "While you were away")),
      el("ul", {}, ...list.expired.map((n) => el("li", {},
        n.kind === "result_delivery"
          ? `The outcome of an act ${n.failure_code === "unanswered" ? "was received without your answer" : "did not reach you"}.`
          : `${n.kind || "A question"}: ${n.failure_code === "unanswered" ? "nobody answered it in time" : "it closed before reaching you"}.`,
        traceLink(n.trace_id))))));
    // The panel showed them: the console confirms them to the channel.
    api("POST", "/api/operator/expired", { delivery_ids: list.expired.map((n) => n.delivery_id) }, true).catch(() => {});
  }
  // A question or a notice is shown when the Questions view is on screen.
  if (onScreen) {
    const toShow = list.queries.map((q) => q.task_id).filter((t) => !opShown.has(t));
    if (toShow.length) {
      toShow.forEach((t) => opShown.add(t));
      api("POST", "/api/operator/shown", { task_ids: toShow }, true).catch(() => toShow.forEach((t) => opShown.delete(t)));
    }
  }
}

// renderQuestion is the single renderer of architecture §3.17.1: the
// enumeration, the facts that support it and the element chosen.
function renderQuestion(q) {
  const d = q.data || {};
  const li = el("li", { class: "card" },
    el("div", { class: "meta" }, `${q.kind || "Question"} · ${new Date(q.arrived).toLocaleString()}`, traceLink(q.trace_id)));
  if (q.kind === "gate_approval") {
    li.append(gateBody(d));
  } else {
    if (typeof d.task_text === "string") li.append(el("div", { class: "text" }, d.task_text));
    const since = d.since && !Number.isNaN(Date.parse(d.since)) ? new Date(d.since).toLocaleString() : d.since;
    const facts = [["Scope", d.scope], ["Since", since]].filter(([, v]) => v);
    if (facts.length) li.append(el("dl", { class: "facts" }, ...facts.flatMap(([k, v]) => [el("dt", {}, k), el("dd", {}, String(v))])));
    const evidence = d.evidence ?? d.details;
    if (evidence !== undefined) li.append(el("pre", {}, JSON.stringify(evidence, null, 2)));
  }
  if (q.outcome_if_silent) li.append(el("div", { class: "state" }, `If nobody answers: ${q.outcome_if_silent}.`));
  if (q.final_round) li.append(el("div", { class: "state" }, "Last round: it is not asked again."));
  const opts = q.options || [];
  const answer = async (a) => {
    try {
      await api("POST", "/api/operator/answers", { task_id: q.task_id, ...a });
    } catch (err) {
      alert(err.message);
    }
    loadQuestions();
  };
  if (opts.length) {
    li.append(el("div", { class: "actions" }, ...opts.map((o) => el("button", { onclick: () => answer({ decision: o }) }, o))));
  } else {
    const input = el("textarea", { rows: "2", placeholder: "Your answer" });
    li.append(el("div", { class: "actions" }, input, el("button", { onclick: () => answer({ text: input.value }) }, "Answer")));
  }
  return li;
}

// --- the configuration -----------------------------------------------------

// egressRegistry is the egress_registry of the head last read, which it reads
// if the panel has not yet.
async function egressRegistry() {
  if (!configView.snapshot) await loadConfig();
  return configView.snapshot?.values.egress_registry || {};
}

// loadConfig reads the head and the schema the form is generated from.
async function loadConfig() {
  try {
    const [snapshot, schema] = await Promise.all([
      api("GET", "/api/config", undefined, true),
      configView.schema ? Promise.resolve(configView.schema) : api("GET", "/api/config/schema", undefined, true),
    ]);
    Object.assign(configView, { snapshot, schema, at: new Date(), error: "" });
  } catch (err) {
    configView.error = err.message;
  }
  renderFreshness();
  renderConfig();
}

let configFields = {};

// renderConfig generates the administration affordance from the schema
// (architecture §3.17.1): the snapshot with its version, one field per key
// with its current value, and the edit of the keys the person changes. It
// carries: it derives, completes and reorders nothing.
function renderConfig() {
  const form = $("config-form");
  const snap = configView.snapshot;
  if (!snap || !configView.schema) {
    form.replaceChildren(el("p", { class: "error" }, configView.error));
    return;
  }
  configFields = {};
  const keys = Object.entries(configView.schema.keys).sort(([a], [b]) => a.localeCompare(b));
  const fields = keys.map(([name, schema]) => {
    const has = Object.prototype.hasOwnProperty.call(snap.values, name);
    const f = keyField(name, schema, has ? snap.values[name] : undefined, has);
    configFields[name] = { field: f, current: has ? canon(snap.values[name]) : undefined };
    return f.node;
  });
  form.replaceChildren(
    el("p", { class: "hint" }, `Version ${snap.config_version}, published ${new Date(snap.published_at).toLocaleString()}.`),
    ...fields,
    el("p", { id: "config-error", class: "error" }),
    el("div", { class: "actions sticky" }, el("button", {}, "Publish changes")));
}

$("config-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const assignments = {};
  for (const [name, { field, current }] of Object.entries(configFields)) {
    let v;
    try { v = field.get(); } catch (err) { $("config-error").textContent = `${name}: ${err.message}`; return; }
    if (v === undefined) continue;
    if (canon(v) !== current) assignments[name] = v;
  }
  const names = Object.keys(assignments);
  if (!names.length) { $("config-error").textContent = "Nothing changed."; return; }
  if (!confirm(`Publish an edit of ${names.join(", ")} on version ${configView.snapshot.config_version}?`)) return;
  const sent = await act(`Edit the configuration: ${names.join(", ")}`, "/api/config",
    { base_version: configView.snapshot.config_version, assignments }, ["config"],
    (err) => { $("config-error").textContent = err.message; });
  if (sent) {
    $("config-error").textContent = "";
    showView("activity");
  }
});

// keyField is a key of the configuration: its description and its editor. A
// key without a value shows a button that opens the editor.
function keyField(name, schema, value, has) {
  const editor = field(schema, value);
  const box = el("div", { class: "config-key" }, el("label", { class: "key-name" }, name),
    schema.description ? el("p", { class: "hint" }, schema.description) : "");
  if (has) {
    box.append(editor.node);
    return { node: box, get: editor.get };
  }
  let open = false;
  const holder = el("div", { hidden: "" }, editor.node);
  const opener = el("button", { type: "button", class: "link", onclick: () => { open = true; holder.hidden = false; opener.remove(); } }, "Not set · Set a value");
  box.append(opener, holder);
  return { node: box, get: () => (open ? editor.get() : undefined) };
}

// field generates an editor from a schema node: each one returns its value as
// the person left it, or undefined when it is an optional field left empty. A
// required string left empty is the empty string.
function field(schema, value, required) {
  schema = schema || {};
  if (schema.opaque || !schema.type) return jsonField(value);
  switch (schema.type) {
    case "integer":
    case "number": {
      const input = el("input", { type: "number", step: schema.type === "integer" ? "1" : "any", value: value ?? "" });
      return {
        node: input,
        get: () => {
          if (input.value === "") return undefined;
          const n = Number(input.value);
          if (!Number.isFinite(n) || (schema.type === "integer" && !Number.isInteger(n))) throw new Error("not a " + schema.type);
          return n;
        },
      };
    }
    case "boolean": {
      const input = el("input", { type: "checkbox" });
      input.checked = !!value;
      return { node: el("label", { class: "check" }, input, " yes"), get: () => input.checked };
    }
    case "string": {
      if (schema.enum) {
        const select = el("select", {}, el("option", { value: "" }, "—"), ...schema.enum.map((v) => el("option", { value: v }, v)));
        select.value = value ?? "";
        return { node: select, get: () => (select.value === "" ? undefined : select.value) };
      }
      const input = el("input", { value: value ?? "" });
      return { node: input, get: () => (input.value === "" && value === undefined && !required ? undefined : input.value) };
    }
    case "array":
      return optional(arrayField(schema, Array.isArray(value) ? value : []), value, required);
    case "object":
      if (schema.properties) return optional(objectField(schema, value && typeof value === "object" ? value : {}), value, required);
      if (schema.additionalProperties) return optional(mapField(schema, value && typeof value === "object" ? value : {}), value, required);
      return jsonField(value);
    default:
      return jsonField(value);
  }
}

// optional leaves out an optional array or object that was absent and that the
// person left empty, so an untouched value comes back as it was.
function optional(f, value, required) {
  return {
    node: f.node,
    get: () => {
      const v = f.get();
      const empty = Array.isArray(v) ? v.length === 0 : Object.keys(v).length === 0;
      return empty && value === undefined && !required ? undefined : v;
    },
  };
}

// canon is a value's JSON with its object keys sorted, to tell whether the
// person changed it whatever the order the form builds it in.
function canon(v) {
  if (Array.isArray(v)) return "[" + v.map(canon).join(",") + "]";
  if (v && typeof v === "object") return "{" + Object.keys(v).sort().map((k) => JSON.stringify(k) + ":" + canon(v[k])).join(",") + "}";
  return JSON.stringify(v);
}

function jsonField(value) {
  const area = el("textarea", { rows: "4", class: "mono" });
  area.value = value === undefined ? "" : JSON.stringify(value, null, 2);
  return {
    node: area,
    get: () => {
      if (area.value.trim() === "") return undefined;
      try { return JSON.parse(area.value); } catch (_) { throw new Error("not valid JSON"); }
    },
  };
}

function objectField(schema, value) {
  const required = new Set(schema.required || []);
  const parts = Object.entries(schema.properties).map(([k, s]) => {
    const f = field(s, value[k], required.has(k));
    return {
      k, f, node: el("div", { class: "prop" },
        el("label", { class: "prop-name" }, k + (required.has(k) ? "" : " (optional)")),
        s.description ? el("p", { class: "hint" }, s.description) : "", f.node),
    };
  });
  return {
    node: el("fieldset", { class: "object" }, ...parts.map((p) => p.node)),
    get: () => {
      const out = {};
      for (const p of parts) {
        const v = p.f.get();
        if (v !== undefined) out[p.k] = v;
      }
      return out;
    },
  };
}

function arrayField(schema, value) {
  const list = el("div", { class: "array" });
  const items = [];
  const add = (v) => {
    const f = field(schema.items, v);
    const row = el("div", { class: "array-item" }, f.node);
    const item = { f, row };
    row.append(el("button", { type: "button", class: "link danger", onclick: () => { items.splice(items.indexOf(item), 1); row.remove(); } }, "Remove"));
    items.push(item);
    list.append(row);
  };
  value.forEach(add);
  return {
    node: el("div", {}, list, el("button", { type: "button", class: "link", onclick: () => add(undefined) }, "Add")),
    get: () => items.map((i) => i.f.get()).filter((v) => v !== undefined),
  };
}

// mapField edits an object whose keys the person names, like the
// egress_registry: one editor per entry.
function mapField(schema, value) {
  const list = el("div", { class: "map" });
  const items = [];
  const add = (k, v) => {
    const f = field(schema.additionalProperties, v);
    const name = el("input", { value: k, placeholder: "Key", class: "map-key" });
    const details = el("details", { class: "map-item" }, el("summary", {}, name), f.node);
    const item = { name, f, details };
    details.append(el("button", { type: "button", class: "link danger", onclick: () => { items.splice(items.indexOf(item), 1); details.remove(); } }, "Remove this entry"));
    items.push(item);
    list.append(details);
    return details;
  };
  Object.entries(value).forEach(([k, v]) => add(k, v));
  return {
    node: el("div", {}, list, el("button", { type: "button", class: "link", onclick: () => { add("", undefined).open = true; } }, "Add an entry")),
    get: () => {
      const out = {};
      for (const i of items) {
        const k = i.name.value.trim();
        if (!k) throw new Error("an entry without a key");
        if (Object.prototype.hasOwnProperty.call(out, k)) throw new Error(`the key ${k} twice`);
        out[k] = i.f.get();
      }
      return out;
    },
  };
}

// --- secrets and rates -----------------------------------------------------

// loadHandles offers the handles and rate sources the egress registry names.
async function loadHandles() {
  const registry = await egressRegistry();
  const handles = new Set();
  const sources = new Set();
  for (const e of Object.values(registry)) {
    if (e.credential_handle) handles.add(e.credential_handle);
    if (e.verify_handle) handles.add(e.verify_handle);
    if (e.rate_source) sources.add(e.rate_source);
  }
  $("secret-handles").replaceChildren(...[...handles].sort().map((h) => el("option", { value: h })));
  $("rate-sources").replaceChildren(...[...sources].sort().map((h) => el("option", { value: h })));
}

// escapePath escapes each segment of a handle or a rate source, which the
// console takes as the rest of the path, as the command line does.
function escapePath(name) {
  return name.split("/").map(encodeURIComponent).join("/");
}

function utf8Base64(s) {
  const bytes = new TextEncoder().encode(s);
  let bin = "";
  for (const b of bytes) bin += String.fromCharCode(b);
  return btoa(bin);
}

$("secret-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const f = e.target;
  const handle = f.elements.handle.value.trim();
  if (!f.elements.value.value) { $("secret-result").textContent = "Write the value."; return; }
  try {
    await api("PUT", "/api/secrets/" + escapePath(handle), { value: utf8Base64(f.elements.value.value) });
    $("secret-result").textContent = `Written: ${handle}.`;
    f.elements.value.value = "";
  } catch (err) {
    $("secret-result").textContent = err.message;
  }
});

$("secret-form").elements.delete.addEventListener("click", async () => {
  const f = $("secret-form");
  const handle = f.elements.handle.value.trim();
  if (!handle || !confirm(`Delete the secret ${handle}? What uses it stops working until it is written again.`)) return;
  try {
    await api("DELETE", "/api/secrets/" + escapePath(handle));
    $("secret-result").textContent = `Deleted: ${handle}.`;
  } catch (err) {
    $("secret-result").textContent = err.message;
  }
});

$("rates-form").elements.file.addEventListener("change", async (e) => {
  const file = e.target.files[0];
  if (file) $("rates-form").elements.table.value = await file.text();
});

$("rates-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const f = e.target;
  const name = f.elements.name.value.trim();
  const r = await fetch("/api/rates/" + escapePath(name), { method: "PUT", headers: { "Content-Type": "application/json" }, body: f.elements.table.value });
  if (r.status === 401) { signedOut(); return; }
  if (r.ok) {
    $("rates-result").textContent = `Written: the rate table of ${name}.`;
    return;
  }
  let msg = r.statusText;
  try { msg = (await r.json()).error || msg; } catch (_) { /* no body */ }
  $("rates-result").textContent = msg;
});

// Both scripts are loaded: the panel starts.
start();
