// The panel's work area (implementation §6, The operator console): the
// conversations, the requests they open, and the outcomes and queries of the
// Entry Node. Everything the network sends is set as text, never as markup.
"use strict";

const $ = (id) => document.getElementById(id);

let current = null;   // the open conversation's id
const openKey = "zobik.open"; // where the browser remembers the open conversation
let reopen = remembered(); // the conversation to open on sign-in
let events = null;    // the event stream
const shown = new Set(); // task ids already reported as shown
let traceUI = "";     // where the console serves the tracing backend's UI, if it does

// api calls the console. refresh marks a request the panel makes on its own,
// which is not the person's activity.
async function api(method, path, body, refresh) {
  const opts = { method, headers: {} };
  if (refresh) opts.headers["Zobik-Refresh"] = "1";
  if (body instanceof FormData) {
    opts.body = body;
  } else if (body !== undefined) {
    opts.headers["Content-Type"] = "application/json";
    opts.body = JSON.stringify(body);
  }
  const r = await fetch(path, opts);
  if (r.status === 401) {
    signedOut();
    throw new Error("no session");
  }
  if (!r.ok) {
    let msg = r.statusText;
    try { msg = (await r.json()).error || msg; } catch (_) { /* no body */ }
    throw new Error(msg);
  }
  if (r.status === 204) return null;
  return r.json();
}

function el(tag, attrs, ...children) {
  const e = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs || {})) {
    if (k === "class") e.className = v;
    else if (k.startsWith("on")) e.addEventListener(k.slice(2), v);
    else e.setAttribute(k, v);
  }
  for (const c of children) {
    if (c === null || c === undefined) continue;
    e.append(typeof c === "string" ? document.createTextNode(c) : c);
  }
  return e;
}

// --- session ---------------------------------------------------------------

function signedOut() {
  if (events) { events.close(); events = null; }
  // The conversation reopens on sign-in; what was on screen is discarded, so
  // nothing stale stays shown.
  reopen = current || reopen;
  closeConversation();
  $("conversations").replaceChildren();
  $("expired").hidden = true;
  $("expired-list").replaceChildren();
  shown.clear();
  $("panel").hidden = true;
  $("signin").hidden = false;
}

// The open conversation is remembered in the browser, so a reload reopens it.
// The storage may be unavailable; then nothing is remembered.
function remembered() {
  try { return localStorage.getItem(openKey); } catch (_) { return null; }
}
function remember(id) {
  try { id ? localStorage.setItem(openKey, id) : localStorage.removeItem(openKey); } catch (_) { /* not remembered */ }
}

function closeConversation() {
  current = null;
  $("conversation").hidden = true;
  $("messages").replaceChildren();
  $("empty").hidden = false;
}

async function start() {
  try {
    await api("GET", "/api/conversations", undefined, true);
  } catch (e) {
    if (e.message === "no session") return;
    $("signin").hidden = true;
    $("panel").hidden = false;
    $("unavailable").textContent = e.message;
    $("unavailable").hidden = false;
    return;
  }
  $("signin").hidden = true;
  $("panel").hidden = false;
  $("unavailable").hidden = true;
  try { traceUI = (await api("GET", "/api/session", undefined, true)).trace_ui || ""; } catch (_) { traceUI = ""; }
  openEvents();
  await Promise.all([loadList(), loadExpired()]);
  if (reopen) {
    const id = reopen;
    reopen = null;
    await select(id);
  }
}

$("signin").addEventListener("submit", async (e) => {
  e.preventDefault();
  const f = new FormData(e.target);
  try {
    await api("POST", "/api/session", { username: f.get("username"), password: f.get("password") });
    $("signin-error").textContent = "";
    e.target.reset();
    start();
  } catch (err) {
    $("signin-error").textContent = err.message;
  }
});

$("signout").addEventListener("click", async () => {
  try { await api("DELETE", "/api/session"); } catch (_) { /* already closed */ }
  signedOut();
});

function openEvents() {
  if (events) events.close();
  events = new EventSource("/api/events");
  events.addEventListener("conversation", (e) => {
    const { id } = JSON.parse(e.data);
    loadList();
    if (id === current) loadConversation(id);
  });
  events.addEventListener("expired", () => loadExpired());
  events.onerror = async () => {
    // The stream ends with the session; check whether it is still open.
    try { await api("GET", "/api/conversations", undefined, true); } catch (_) { /* signedOut ran */ }
  };
}

// --- conversations ---------------------------------------------------------

async function loadList() {
  const list = await api("GET", "/api/conversations", undefined, true);
  const ul = $("conversations");
  ul.replaceChildren(...list.map((c) => el("li",
    { class: c.id === current ? "current" : "", onclick: () => select(c.id) },
    el("span", { class: c.title ? "" : "untitled" }, c.title || "New conversation"),
    c.awaiting ? el("span", { class: "badge", title: "Waiting for you" }, String(c.awaiting)) : null,
  )));
}

async function select(id) {
  current = id;
  remember(id);
  $("composer-error").textContent = "";
  await Promise.all([loadConversation(id), loadList()]);
}

$("new").addEventListener("click", async () => {
  const c = await api("POST", "/api/conversations");
  await select(c.id);
  $("composer").elements.text.focus();
});

$("delete").addEventListener("click", async () => {
  if (!current || !confirm("Delete this conversation? Its traces are purged from the network.")) return;
  try {
    await api("DELETE", "/api/conversations/" + current);
    closeConversation();
    remember(null);
    loadList();
  } catch (err) {
    alert(err.message);
  }
});

$("composer").addEventListener("submit", async (e) => {
  e.preventDefault();
  const form = e.target;
  const data = new FormData();
  data.append("text", form.elements.text.value);
  for (const f of form.elements.file.files) data.append("file", f, f.name);
  const button = form.querySelector("button");
  button.disabled = true;
  try {
    await api("POST", "/api/conversations/" + current + "/messages", data);
    form.reset();
    $("composer-error").textContent = "";
    await loadConversation(current);
  } catch (err) {
    $("composer-error").textContent = await requestError(err);
  } finally {
    button.disabled = false;
  }
});

// requestError says why a request was not sent. The one too long for the
// vector space names its limit, max_text_size (implementation §1.2.15).
async function requestError(err) {
  if (!err.message.includes("max_text_size")) return err.message;
  try {
    const max = (await api("GET", "/api/config", undefined, true)).values.embedding_model.max_text_size;
    return `The request is too long: the network takes up to ${max} bytes of text.`;
  } catch (_) {
    return "The request is too long for the network.";
  }
}

async function loadConversation(id) {
  let c;
  try {
    c = await api("GET", "/api/conversations/" + id, undefined, true);
  } catch (err) {
    if (id === current) { closeConversation(); remember(null); }
    return;
  }
  if (id !== current) return;
  $("empty").hidden = true;
  $("conversation").hidden = false;
  $("title").textContent = c.title || "New conversation";
  $("messages").replaceChildren(...c.messages.map((m) => renderMessage(c, m)));
  $("messages").lastElementChild?.scrollIntoView({ block: "end" });
  // What the panel shows to the person is shown: the console tells the channel.
  const toShow = c.messages.filter((m) => m.state === "awaiting" && !shown.has(m.task_id)).map((m) => m.task_id);
  if (toShow.length) {
    toShow.forEach((t) => shown.add(t));
    api("POST", "/api/shown", { task_ids: toShow }, true).catch(() => toShow.forEach((t) => shown.delete(t)));
  }
}

// --- messages --------------------------------------------------------------

function renderMessage(c, m) {
  const time = new Date(m.time).toLocaleString();
  const li = el("li", { class: "msg " + m.kind, id: "m-" + m.id });
  if (m.kind === "request") {
    li.append(el("div", { class: "meta" }, "You · " + time));
    if (m.text) li.append(el("div", { class: "text" }, m.text));
    li.append(files(c, m));
  } else if (m.kind === "outcome") {
    li.append(el("div", { class: "meta" }, "Network · " + time, traceLink(m.trace_id)));
    if (m.outcome !== "task.completed") {
      li.append(el("div", { class: "failure" },
        (m.outcome === "task.aborted" ? "Aborted" : "Failed") + (m.failure_reason ? ": " + m.failure_reason : "")));
    }
    if (m.text) li.append(el("div", { class: "text" }, m.text));
    li.append(files(c, m));
    li.append(outcomeState(m));
  } else {
    li.append(el("div", { class: "meta" }, "Question · " + time));
    li.append(queryBody(m));
    li.append(queryState(m));
  }
  return li;
}

// traceLink opens an outcome's trace in the tracing backend's UI, which the
// console serves behind the session on a port of its own (implementation §1.2.17).
function traceLink(traceID) {
  if (!traceUI || !/^[0-9a-f]{32}$/.test(traceID || "")) return null;
  return el("a", { class: "trace", href: traceUI + "/trace/" + traceID, target: "_blank", rel: "noopener noreferrer", title: "Trace " + traceID }, "Trace");
}

function files(c, m) {
  const div = el("div", { class: "files" });
  for (const a of m.attachments || []) {
    div.append(el("a", { href: `/api/conversations/${c.id}/attachments/${a.id}`, download: a.name }, `📎 ${a.name} (${size(a.size)})`));
  }
  return div;
}

function size(n) {
  if (n < 1024) return n + " B";
  if (n < 1 << 20) return (n / 1024).toFixed(1) + " KB";
  return (n / (1 << 20)).toFixed(1) + " MB";
}

function decisionOf(m) {
  return m.answer && typeof m.answer === "object" ? m.answer : {};
}

function expiredText(m) {
  if (m.kind === "outcome") {
    return m.expired === "unanswered" ? "Accepted: you did not answer in time." : "Not delivered yet: it comes back in the next round.";
  }
  return m.expired === "unanswered" ? "Expired without your answer." : "Expired before reaching you.";
}

function outcomeState(m) {
  if (m.state === "answered") {
    const a = decisionOf(m);
    const label = { accept: "Accepted", reject: "Rejected", received: "Received" }[a.decision] || "Received";
    return el("div", { class: "state" }, label + (a.failure_reason ? ": " + a.failure_reason : ""));
  }
  if (m.state === "expired") return el("div", { class: "state" }, expiredText(m));
  const opts = m.options || [];
  if (!opts.includes("accept")) return el("div", { class: "state" }, "");
  const reason = el("textarea", { rows: "2", placeholder: "Why do you reject it? The network tries again with this.", hidden: "" });
  const reject = el("button", {
    onclick: async () => {
      if (reason.hidden) { reason.hidden = false; reason.focus(); return; }
      if (!reason.value.trim()) { reason.focus(); return; }
      await answer({ task_id: m.task_id, decision: "reject", failure_reason: reason.value.trim() });
    },
  }, "Reject");
  return el("div", { class: "actions" },
    el("button", { class: "primary", onclick: () => answer({ task_id: m.task_id, decision: "accept" }) }, "Accept"),
    reject, reason);
}

function queryText(m) {
  return m.data && typeof m.data === "object" ? m.data : null;
}

function queryBody(m) {
  const d = queryText(m);
  if (d && d.kind === "gate_approval") return gateBody(d);
  if (d && typeof d.task_text === "string") return el("div", { class: "text" }, d.task_text);
  // A query without task_text shows its data as it arrived (implementation §6).
  return el("pre", {}, JSON.stringify(m.data, null, 2));
}

// gateBody shows the egress gate by its kind (implementation §6): the call
// as the Integration Sidecar rendered it, and the rule that asks for it
// (architecture §3.16.4).
function gateBody(d) {
  const det = d.details || {};
  const call = det.call || {};
  const rows = [
    ["Destination", call.target], ["Operation", call.operation], ["Entry", det.entry],
    ["Rule", det.rule], ["Requested by", det.topic],
  ];
  if (call.body_digest) rows.push(["Body", size(call.body_size) + " · " + call.body_digest]);
  return el("div", { class: "gate" },
    el("div", { class: "text" }, "A node asks to make this call. It runs only if you approve it."),
    el("dl", {}, ...rows.flatMap(([k, v]) => [el("dt", {}, k), el("dd", {}, String(v ?? ""))])),
    el("pre", {}, JSON.stringify(call.fields ?? {}, null, 2)));
}

function queryState(m) {
  if (m.state === "answered") {
    const a = decisionOf(m);
    return el("div", { class: "state" }, "You answered: " + (a.decision ?? a.answer ?? ""));
  }
  if (m.state === "expired") return el("div", { class: "state" }, expiredText(m));
  const opts = m.options || [];
  if (opts.length) {
    return el("div", { class: "actions" }, ...opts.map((o) =>
      el("button", { onclick: () => answer({ task_id: m.task_id, decision: o }) }, o)));
  }
  const input = el("textarea", { rows: "2", placeholder: "Your answer" });
  return el("div", { class: "actions" }, input,
    el("button", { onclick: () => answer({ task_id: m.task_id, text: input.value }) }, "Answer"));
}

async function answer(a) {
  try {
    await api("POST", "/api/answers", a);
  } catch (err) {
    alert(err.message);
  }
  if (current) loadConversation(current);
}

// --- expired notices -------------------------------------------------------

async function loadExpired() {
  const notices = await api("GET", "/api/expired", undefined, true);
  if (!notices.length) return;
  const list = await api("GET", "/api/conversations", undefined, true);
  const titles = Object.fromEntries(list.map((c) => [c.id, c.title || "New conversation"]));
  const what = (n) => n.kind === "outcome"
    ? (n.failure_code === "unanswered" ? "An outcome was accepted without your answer" : "An outcome did not reach you")
    : (n.failure_code === "unanswered" ? "A question expired without your answer" : "A question expired before reaching you");
  $("expired-list").append(...notices.map((n) => el("li",
    { onclick: () => select(n.conversation_id) },
    `${what(n)} in “${titles[n.conversation_id] || "a conversation"}”.`)));
  $("expired").hidden = false;
  // The panel showed them: the console confirms them to the channel.
  api("POST", "/api/expired", { delivery_ids: notices.map((n) => n.delivery_id) }, true).catch(() => {});
}

$("expired-close").addEventListener("click", () => {
  $("expired").hidden = true;
  $("expired-list").replaceChildren();
});

start();
