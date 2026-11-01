const BADGE = {
  AVAILABLE: "AVAILABLE / Disponible",
  WATERING: "WATERING / Regando",
  LEAK: "LEAK / Fuga",
  OUT_OF_SERVICE: "OUT_OF_SERVICE / Fuera de servicio",
  DISCONNECTED: "DISCONNECTED / Desconectada",
};
const REASON = {
  TIEMPO: "TIEMPO / Tiempo cumplido", MANUAL_WS: "MANUAL_WS / Finalizado en la WS", FUGA: "FUGA / Fuga / avería",
  BLOQUEO: "BLOQUEO / Bloqueada por Central", MONITOR: "MONITOR / Monitor caído", CENTRAL: "CENTRAL / Parada por Central",
  INTERRUMPIDO: "INTERRUMPIDO / Engine interrumpido",
};

const $ = (id) => document.getElementById(id);
const cards = {};

// ------------------------------------------------------------------ órdenes
async function sendCmd(action, ws, duration) {
  const names = { start: "Iniciar riego", block: "Bloquear", activate: "Activar" };
  try {
    const r = await fetch("/api/cmd", {
      method: "POST",
      body: JSON.stringify({ action, ws, duration: Number(duration) || 30 }),
    });
    toast(r.ok ? `Orden enviada: ${names[action]} · ${ws === "ALL" ? "todas las WS" : ws}`
               : "La Central rechazó la orden");
  } catch {
    toast("Imposible conectar con WM_Central");
  }
}

document.addEventListener("click", (e) => {
  const b = e.target.closest("button[data-action]");
  if (!b) return;
  const ws = b.dataset.ws;
  const dur = ws === "ALL" ? $("dur-all").value : cards[ws]?.dur.value;
  sendCmd(b.dataset.action, ws, dur);
});

let toastTimer;
function toast(text) {
  const t = $("toast");
  t.textContent = text;
  t.classList.add("show");
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => t.classList.remove("show"), 2200);
}

// utilidades DOM
function el(tag, cls, text) {
  const e = document.createElement(tag);
  if (cls) e.className = cls;
  if (text !== undefined) e.textContent = text;
  return e;
}

function stat() {
  const box = el("div", "stat");
  const label = el("small");
  const value = el("b");
  box.append(label, value);
  return { box, label, value };
}

function button(cls, text, action, ws) {
  const b = el("button", `btn sm ${cls}`, text);
  b.dataset.action = action;
  b.dataset.ws = ws;
  return b;
}

//tarjetas
function createCard(id) {
  const root = el("article", "card");
  const head = el("div", "card-head");
  const name = el("div", "card-id", id);
  const badge = el("div", "badge");
  head.append(name, badge);

  const location = el("div", "location");
  const supplied = el("div", "supplied");
  const reason = el("div", "reason");
  const stats = el("div", "stats");
  const s = [stat(), stat(), stat()];
  stats.append(...s.map((x) => x.box));
  const progress = el("div", "progress");
  const bar = el("div");
  progress.append(bar);

  const actions = el("div", "actions");
  const dur = el("input");
  dur.type = "number";
  dur.min = "1";
  dur.value = "30";
  dur.title = "Duración del riego (s)";
  actions.append(dur,
    button("start", "▶ Regar", "start", id),
    button("block", "■ Bloquear", "block", id),
    button("activate", "✔ Activar", "activate", id));

  root.append(head, location, supplied, reason, stats, progress, actions);
  return { root, badge, location, supplied, reason, s, progress, bar, dur };
}

function setStat(st, label, value, unit = "") {
  st.label.textContent = label;
  st.value.textContent = value;
  if (unit) st.value.append(el("span", "unit", ` ${unit}`));   // unidad en letra pequeña
  st.value.title = `${value} ${unit}`.trim();
}

function updateCard(c, w) {
  c.root.className = `card ${w.state}`;
  c.badge.textContent = BADGE[w.state] || w.label;
  c.location.textContent = w.location || "—";

  const watering = w.state === "WATERING";
  c.supplied.hidden = !watering;
  c.progress.hidden = !watering;
  c.reason.hidden = !(w.state === "LEAK" && w.reason);
  c.reason.textContent = w.reason || "";

  if (watering) {
    c.supplied.innerHTML = "";
    c.supplied.append("Agua suministrada total: ", el("b", "", `${(w.total + w.volume).toFixed(2)} L`));
    setStat(c.s[0], "Caudal", w.flow.toFixed(1), "L/min");
    setStat(c.s[1], "Volumen", w.volume.toFixed(2), "L");
    setStat(c.s[2], "Operario", w.op || "?");
    const pct = w.duration ? Math.min(100, (100 * w.elapsed) / w.duration) : 0;
    c.bar.style.width = `${pct}%`;
    c.progress.title = `${w.elapsed}/${w.duration} s`;
  } else {
    setStat(c.s[0], "Agua total", w.total.toFixed(2), "L");
    setStat(c.s[1], "Estado monitor", w.health || "—");
    setStat(c.s[2], "Bloqueo", w.blocked ? "sí" : "no");
    c.bar.style.width = "0";
  }
}

function renderCards(list) {
  const grid = $("grid");
  $("empty").hidden = list.length > 0;
  const seen = new Set();
  for (const w of list) {
    seen.add(w.id);
    if (!cards[w.id]) {
      cards[w.id] = createCard(w.id);
      grid.append(cards[w.id].root);
    }
    updateCard(cards[w.id], w);
  }
  for (const id of Object.keys(cards)) {
    if (!seen.has(id)) {
      cards[id].root.remove();
      delete cards[id];
    }
  }
}

//  eventos y riegos
function renderEvents(list) {
  const ul = $("events");
  const prev = ul.scrollTop;
  const atTop = prev < 10;                         
  ul.innerHTML = "";
  for (const ev of list.slice().reverse()) {          // el más reciente arriba
    const li = el("li", ev.col || "");
    li.append(el("span", "ts", ev.ts), document.createTextNode(ev.msg));
    ul.append(li);
  }
  ul.scrollTop = atTop ? 0 : prev;
}

function renderWaterings(list) {
  const tb = $("waterings");
  tb.innerHTML = "";
  if (!list.length) {
    const tr = el("tr");
    const td = el("td", "muted", "Sin riegos todavía");
    td.colSpan = 6;
    tr.append(td);
    tb.append(tr);
    return;
  }
  for (const r of list) {
    const tr = el("tr");
    const hora = r.ts ? new Date(r.ts * 1000).toLocaleTimeString() : "";
    tr.append(
      el("td", "", hora),
      el("td", "", r.ws),
      el("td", "", r.op || "?"),
      el("td", `st-${r.state}`, r.state),
      el("td", "", Number(r.volume || 0).toFixed(2)),
      el("td", "", REASON[r.reason] || r.reason || ""),
    );
    tb.append(tr);
  }
}

function setConn(ok, text) {
  $("conn").className = `conn ${ok ? "ok" : "bad"}`;
  $("conn-text").textContent = text;
}

//bucle de refresco
async function getJSON(url) {
  const r = await fetch(url, { cache: "no-store" });
  if (!r.ok) throw new Error(r.status);
  return r.json();
}

let tick = 0;
async function refresh() {
  try {
    const state = await getJSON("/api/state");
    renderCards(state.ws);
    setConn(state.kafka, state.kafka ? "Kafka OK" : "Kafka caído");
    renderEvents(await getJSON("/api/events"));
    if (tick++ % 2 === 0) renderWaterings(await getJSON("/api/waterings"));
  } catch {
    setConn(false, "Sin conexión con WM_Central");
  }
}

refresh();
setInterval(refresh, 1000);