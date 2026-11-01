const STATE_BADGE = {
  AVAILABLE: "AVAILABLE / Disponible",
  WATERING: "WATERING / Regando",
  LEAK: "LEAK / Fuga",
  OUT_OF_SERVICE: "OUT_OF_SERVICE / Fuera de servicio",
  DISCONNECTED: "DISCONNECTED / Desconectada",
};
const END_REASON = {
  TIEMPO: "TIEMPO / Tiempo cumplido", MANUAL_WS: "MANUAL_WS / Finalizado en la WS", FUGA: "FUGA / Fuga / avería",
  BLOQUEO: "BLOQUEO / Bloqueada por Central", MONITOR: "MONITOR / Monitor caído", CENTRAL: "CENTRAL / Parada por Central",
  INTERRUMPIDO: "INTERRUMPIDO / Engine interrumpido",
};

const byId = (id) => document.getElementById(id);
const stationCards = {};

async function sendCommand(action, wsId, duration) {
  const actionNames = { start: "Iniciar riego", block: "Bloquear", activate: "Activar" };
  try {
    const response = await fetch("/api/cmd", {
      method: "POST",
      body: JSON.stringify({ action, ws: wsId, duration: Number(duration) || 30 }),
    });
    showToast(response.ok ? `Orden enviada: ${actionNames[action]} · ${wsId === "ALL" ? "todas las WS" : wsId}`
                          : "La Central rechazó la orden");
  } catch {
    showToast("Imposible conectar con WM_Central");
  }
}

document.addEventListener("click", (event) => {
  const clickedButton = event.target.closest("button[data-action]");
  if (!clickedButton) return;
  const wsId = clickedButton.dataset.ws;
  const duration = wsId === "ALL" ? byId("dur-all").value : stationCards[wsId]?.durationInput.value;
  sendCommand(clickedButton.dataset.action, wsId, duration);
});

let toastTimeout;
function showToast(text) {
  const toast = byId("toast");
  toast.textContent = text;
  toast.classList.add("show");
  clearTimeout(toastTimeout);
  toastTimeout = setTimeout(() => toast.classList.remove("show"), 2200);
}

function makeElement(tag, className, text) {
  const element = document.createElement(tag);
  if (className) element.className = className;
  if (text !== undefined) element.textContent = text;
  return element;
}

function createStat() {
  const box = makeElement("div", "stat");
  const label = makeElement("small");
  const value = makeElement("b");
  box.append(label, value);
  return { box, label, value };
}

function createButton(className, text, action, wsId) {
  const button = makeElement("button", `btn sm ${className}`, text);
  button.dataset.action = action;
  button.dataset.ws = wsId;
  return button;
}

function createCard(wsId) {
  const root = makeElement("article", "card");
  const head = makeElement("div", "card-head");
  const name = makeElement("div", "card-id", wsId);
  const badge = makeElement("div", "badge");
  head.append(name, badge);

  const location = makeElement("div", "location");
  const supplied = makeElement("div", "supplied");
  const reason = makeElement("div", "reason");
  const stats = makeElement("div", "stats");
  const statBoxes = [createStat(), createStat(), createStat()];
  stats.append(...statBoxes.map((item) => item.box));
  const progress = makeElement("div", "progress");
  const bar = makeElement("div");
  progress.append(bar);

  const actions = makeElement("div", "actions");
  const durationInput = makeElement("input");
  durationInput.type = "number";
  durationInput.min = "1";
  durationInput.value = "30";
  durationInput.title = "Duración del riego (s)";
  actions.append(durationInput,
    createButton("start", "▶ Regar", "start", wsId),
    createButton("block", "■ Bloquear", "block", wsId),
    createButton("activate", "✔ Activar", "activate", wsId));

  root.append(head, location, supplied, reason, stats, progress, actions);
  return { root, badge, location, supplied, reason, statBoxes, progress, bar, durationInput };
}

function setStatValue(stat, label, value, unit = "") {
  stat.label.textContent = label;
  stat.value.textContent = value;
  if (unit) stat.value.append(makeElement("span", "unit", ` ${unit}`));
  stat.value.title = `${value} ${unit}`.trim();
}

function updateCard(card, station) {
  card.root.className = `card ${station.state}`;
  card.badge.textContent = STATE_BADGE[station.state] || station.label;
  card.location.textContent = station.location || "—";

  const watering = station.state === "WATERING";
  card.supplied.hidden = !watering;
  card.progress.hidden = !watering;
  card.reason.hidden = !(station.state === "LEAK" && station.reason);
  card.reason.textContent = station.reason || "";

  if (watering) {
    card.supplied.innerHTML = "";
    card.supplied.append("Agua suministrada total: ", makeElement("b", "", `${(station.total + station.volume).toFixed(2)} L`));
    setStatValue(card.statBoxes[0], "Caudal", station.flow.toFixed(1), "L/min");
    setStatValue(card.statBoxes[1], "Volumen", station.volume.toFixed(2), "L");
    setStatValue(card.statBoxes[2], "Operario", station.op || "?");
    const percent = station.duration ? Math.min(100, (100 * station.elapsed) / station.duration) : 0;
    card.bar.style.width = `${percent}%`;
    card.progress.title = `${station.elapsed}/${station.duration} s`;
  } else {
    setStatValue(card.statBoxes[0], "Agua total", station.total.toFixed(2), "L");
    setStatValue(card.statBoxes[1], "Estado monitor", station.health || "—");
    setStatValue(card.statBoxes[2], "Bloqueo", station.blocked ? "sí" : "no");
    card.bar.style.width = "0";
  }
}

function renderStationCards(stations) {
  const grid = byId("grid");
  byId("empty").hidden = stations.length > 0;
  const seenIds = new Set();
  for (const station of stations) {
    seenIds.add(station.id);
    if (!stationCards[station.id]) {
      stationCards[station.id] = createCard(station.id);
      grid.append(stationCards[station.id].root);
    }
    updateCard(stationCards[station.id], station);
  }
  for (const wsId of Object.keys(stationCards)) {
    if (!seenIds.has(wsId)) {
      stationCards[wsId].root.remove();
      delete stationCards[wsId];
    }
  }
}

function renderEvents(events) {
  const list = byId("events");
  const previousScroll = list.scrollTop;
  const atTop = previousScroll < 10;
  list.innerHTML = "";
  for (const event of events.slice().reverse()) {
    const item = makeElement("li", event.col || "");
    item.append(makeElement("span", "ts", event.ts), document.createTextNode(event.msg));
    list.append(item);
  }
  list.scrollTop = atTop ? 0 : previousScroll;
}

function renderWaterings(waterings) {
  const tbody = byId("waterings");
  tbody.innerHTML = "";
  if (!waterings.length) {
    const row = makeElement("tr");
    const cell = makeElement("td", "muted", "Sin riegos todavía");
    cell.colSpan = 6;
    row.append(cell);
    tbody.append(row);
    return;
  }
  for (const watering of waterings) {
    const row = makeElement("tr");
    const time = watering.ts ? new Date(watering.ts * 1000).toLocaleTimeString() : "";
    row.append(
      makeElement("td", "", time),
      makeElement("td", "", watering.ws),
      makeElement("td", "", watering.op || "?"),
      makeElement("td", `st-${watering.state}`, watering.state),
      makeElement("td", "", Number(watering.volume || 0).toFixed(2)),
      makeElement("td", "", END_REASON[watering.reason] || watering.reason || ""),
    );
    tbody.append(row);
  }
}

function setConnection(ok, text) {
  byId("conn").className = `conn ${ok ? "ok" : "bad"}`;
  byId("conn-text").textContent = text;
}

const REFRESH_INTERVAL_MS = 1000;
const FETCH_TIMEOUT_MS = 4000;
const MAX_CONSECUTIVE_FAILS = 3;

async function fetchJSON(url) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);
  try {
    const response = await fetch(url, { cache: "no-store", signal: controller.signal });
    if (!response.ok) throw new Error(response.status);
    return await response.json();
  } finally {
    clearTimeout(timer);
  }
}

let refreshCount = 0;
let failedAttempts = 0;
async function refreshPanel() {
  try {
    const state = await fetchJSON("/api/state");
    renderStationCards(state.ws);
    setConnection(state.kafka, state.kafka ? "Kafka OK" : "Kafka caído");
    renderEvents(await fetchJSON("/api/events"));
    if (refreshCount++ % 2 === 0) renderWaterings(await fetchJSON("/api/waterings"));
    failedAttempts = 0;
  } catch {
    if (++failedAttempts >= MAX_CONSECUTIVE_FAILS) setConnection(false, "Sin conexión con WM_Central");
  } finally {
    setTimeout(refreshPanel, REFRESH_INTERVAL_MS);
  }
}

refreshPanel();