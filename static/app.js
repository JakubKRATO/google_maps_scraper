const form = document.querySelector("#scrape-form");
const startButton = document.querySelector("#start-button");
const progressPanel = document.querySelector("#progress-panel");
const progressStatus = document.querySelector("#progress-status");
const progressCount = document.querySelector("#progress-count");
const progressTrack = document.querySelector("#progress-track");
const progressBar = document.querySelector("#progress-bar");
const progressMessage = document.querySelector("#progress-message");
const downloadLink = document.querySelector("#download-link");
const townResults = document.querySelector("#town-results");

let pollTimer = null;
let socket = null;
let activeJobId = "";
let townsById = new Map();

const ACTIVE_STATUSES = ["queued", "running", "starting", "opening", "searching", "collecting", "scraping"];

function formatStatus(status) {
  return status.charAt(0).toUpperCase() + status.slice(1);
}

function countRequestedTowns() {
  const rawLocation = String(form.elements.location.value || "");
  const towns = rawLocation.split(",").map((town) => town.trim()).filter(Boolean);
  return Math.max(1, towns.length);
}

function resetTownResults() {
  townsById = new Map();
  townResults.replaceChildren();
  townResults.hidden = true;
}

function renderTownResults() {
  const towns = Array.from(townsById.values()).sort((first, second) => {
    const firstIndex = Number(String(first.id || "").split("-", 1)[0]);
    const secondIndex = Number(String(second.id || "").split("-", 1)[0]);
    return firstIndex - secondIndex;
  });

  townResults.replaceChildren();
  townResults.hidden = towns.length === 0;

  for (const town of towns) {
    const item = document.createElement("div");
    item.className = `town-result town-result--${town.status || "queued"}`;

    const details = document.createElement("div");
    details.className = "town-result__details";

    const name = document.createElement("strong");
    name.textContent = town.name || town.search_text || "Town";

    const meta = document.createElement("span");
    const scraped = Number(town.scraped || 0);
    const total = Number(town.total || 0);
    meta.textContent = `${formatStatus(String(town.status || "queued"))} - ${scraped} / ${total}`;

    details.append(name, meta);
    item.append(details);

    if (town.status === "completed" && town.download_url) {
      const link = document.createElement("a");
      link.href = town.download_url;
      link.textContent = "Download CSV";
      link.className = "town-result__link";
      item.append(link);
    } else if (town.status === "failed") {
      const error = document.createElement("span");
      error.className = "town-result__error";
      error.textContent = town.error || "Failed";
      item.append(error);
    } else {
      const pending = document.createElement("span");
      pending.className = "town-result__pending";
      pending.textContent = town.message || "Waiting";
      item.append(pending);
    }

    townResults.append(item);
  }
}

function upsertTown(town) {
  if (!town || !town.id) {
    return;
  }
  townsById.set(town.id, { ...(townsById.get(town.id) || {}), ...town });
  renderTownResults();
}

function createLiteSocket() {
  if (!window.WebSocket) {
    return null;
  }

  const listeners = new Map();
  const queuedMessages = [];
  const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
  const webSocket = new WebSocket(`${protocol}//${window.location.host}/socket.io/?EIO=4&transport=websocket`);

  const client = {
    connected: false,
    on(eventName, handler) {
      const handlers = listeners.get(eventName) || [];
      handlers.push(handler);
      listeners.set(eventName, handlers);
    },
    emit(eventName, payload) {
      const message = `42${JSON.stringify([eventName, payload])}`;
      if (client.connected) {
        webSocket.send(message);
      } else {
        queuedMessages.push(message);
      }
    },
  };

  function notify(eventName, payload) {
    for (const handler of listeners.get(eventName) || []) {
      handler(payload);
    }
  }

  function flushQueuedMessages() {
    while (queuedMessages.length > 0 && client.connected) {
      webSocket.send(queuedMessages.shift());
    }
  }

  webSocket.addEventListener("message", (event) => {
    const packet = String(event.data || "");
    if (packet.startsWith("0")) {
      webSocket.send("40");
      return;
    }
    if (packet === "2") {
      webSocket.send("3");
      return;
    }
    if (packet.startsWith("40")) {
      client.connected = true;
      notify("connect");
      flushQueuedMessages();
      return;
    }
    if (packet.startsWith("42")) {
      try {
        const [eventName, payload] = JSON.parse(packet.slice(2));
        notify(eventName, payload);
      } catch (error) {
        console.warn("Could not read socket event.", error);
      }
    }
  });

  webSocket.addEventListener("close", () => {
    client.connected = false;
  });

  return client;
}

function createSocketClient() {
  if (window.io) {
    return window.io();
  }
  return createLiteSocket();
}

function setProgress(progress) {
  const percent = Math.max(0, Math.min(100, Number(progress.percent || 0)));
  const scraped = Number(progress.scraped || 0);
  const total = Number(progress.total || 0);
  const collected = Number(progress.collected || 0);
  const status = String(progress.status || "queued");
  const completedTowns = Number(progress.completed_towns || 0);
  const failedTowns = Number(progress.failed_towns || 0);
  const totalTowns = Number(progress.total_towns || 1);
  const townProgress = totalTowns > 1 ? ` - ${completedTowns + failedTowns}/${totalTowns} towns` : "";

  if (Array.isArray(progress.towns)) {
    progress.towns.forEach(upsertTown);
  }

  progressStatus.textContent = `${formatStatus(status)}${townProgress}`;
  progressCount.textContent = `${scraped} / ${total}`;
  progressTrack.setAttribute("aria-valuenow", String(percent));
  progressBar.style.width = `${percent}%`;

  const collectedText = collected > scraped ? ` Collected ${collected} link(s).` : "";
  progressMessage.textContent = `${progress.message || ""}${collectedText}`;

  if (status === "completed" && progress.download_url) {
    downloadLink.href = progress.download_url;
    downloadLink.textContent = totalTowns > 1 ? "Download all CSVs (.zip)" : "Download CSV";
    downloadLink.hidden = false;
    startButton.disabled = false;
  }

  if (status === "failed") {
    downloadLink.hidden = true;
    startButton.disabled = false;
  }
}

async function pollProgress(progressUrl) {
  const response = await fetch(progressUrl, { cache: "no-store" });
  const progress = await response.json();

  if (!response.ok) {
    throw new Error(progress.error || "Could not read scrape progress.");
  }

  setProgress(progress);

  if (ACTIVE_STATUSES.includes(progress.status)) {
    pollTimer = window.setTimeout(() => pollProgress(progressUrl).catch(showError), 1000);
  }
}

function showError(error) {
  progressStatus.textContent = "Failed";
  progressMessage.textContent = error.message || "Scrape failed.";
  progressBar.style.width = "100%";
  progressTrack.setAttribute("aria-valuenow", "100");
  downloadLink.hidden = true;
  startButton.disabled = false;
}

function setupSocket() {
  if (socket) {
    return socket;
  }

  socket = createSocketClient();
  if (!socket) {
    return null;
  }

  socket.on("connect", () => {
    if (activeJobId) {
      socket.emit("join_job", { job_id: activeJobId });
    }
  });
  socket.on("scrape_progress", setProgress);
  socket.on("town_completed", (event) => upsertTown(event.town));
  socket.on("town_failed", (event) => upsertTown(event.town));
  socket.on("scrape_error", (event) => showError(new Error(event.error || "Socket connection failed.")));

  return socket;
}

function joinSocketJob(jobId) {
  activeJobId = jobId;
  const currentSocket = setupSocket();
  if (currentSocket && currentSocket.connected) {
    currentSocket.emit("join_job", { job_id: jobId });
  }
}

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  window.clearTimeout(pollTimer);
  activeJobId = "";
  resetTownResults();

  const requestedTowns = countRequestedTowns();
  const maxResults = Number(form.elements.max_results.value || 0);
  progressPanel.hidden = false;
  downloadLink.hidden = true;
  startButton.disabled = true;
  setProgress({
    status: "queued",
    message: "Queued.",
    scraped: 0,
    collected: 0,
    total: maxResults * requestedTowns,
    total_towns: requestedTowns,
    percent: 0,
  });

  try {
    const response = await fetch(form.dataset.startUrl, {
      method: "POST",
      body: new FormData(form),
    });
    const payload = await response.json();

    if (!response.ok) {
      throw new Error(payload.error || "Could not start scrape.");
    }

    joinSocketJob(payload.job_id);
    await pollProgress(payload.progress_url);
  } catch (error) {
    showError(error);
  }
});
