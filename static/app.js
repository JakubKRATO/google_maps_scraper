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

function parseCommaInput(value) {
  return String(value || "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

function countRequestedQueries() {
  const queries = parseCommaInput(form.elements.query.value);
  return Math.max(1, queries.length);
}

function countRequestedTowns() {
  const towns = parseCommaInput(form.elements.location.value);
  return Math.max(1, towns.length);
}

function toNumber(value, fallback = 0) {
  const number = Number(value);
  return Number.isFinite(number) ? number : fallback;
}

function resetTownResults() {
  townsById = new Map();
  townResults.replaceChildren();
  townResults.hidden = true;
}

function summarizeQuery(towns) {
  let scraped = 0;
  let collected = 0;
  let total = 0;
  let completedTowns = 0;
  let failedTowns = 0;
  let hasActive = false;

  for (const town of towns) {
    const status = String(town.status || "queued");
    scraped += toNumber(town.scraped);
    collected += toNumber(town.collected || town.scraped);
    total += toNumber(town.total);
    if (status === "completed") {
      completedTowns += 1;
    } else if (status === "failed") {
      failedTowns += 1;
    }
    if (ACTIVE_STATUSES.includes(status)) {
      hasActive = true;
    }
  }

  const totalTowns = towns.length;
  const finishedTowns = completedTowns + failedTowns;
  let status = "queued";
  if (totalTowns > 0 && finishedTowns === totalTowns) {
    status = completedTowns === 0 ? "failed" : "completed";
  } else if (hasActive || finishedTowns > 0) {
    status = "running";
  }

  let percent = 0;
  if (status === "completed" || status === "failed") {
    percent = 100;
  } else if (total > 0) {
    percent = Math.min(99, Math.round((scraped / total) * 100));
  } else if (totalTowns > 0) {
    percent = Math.min(99, Math.round((finishedTowns / totalTowns) * 100));
  }

  return {
    status,
    percent,
    scraped,
    collected,
    total,
    completedTowns,
    failedTowns,
    totalTowns,
    finishedTowns,
  };
}

function buildQueryGroups() {
  const towns = Array.from(townsById.values()).sort((first, second) => {
    const firstQuery = toNumber(first.query_index, 1);
    const secondQuery = toNumber(second.query_index, 1);
    if (firstQuery !== secondQuery) {
      return firstQuery - secondQuery;
    }

    const firstTown = toNumber(first.town_index, 1);
    const secondTown = toNumber(second.town_index, 1);
    if (firstTown !== secondTown) {
      return firstTown - secondTown;
    }

    return String(first.id || "").localeCompare(String(second.id || ""));
  });

  const groupsById = new Map();
  for (const town of towns) {
    const queryId = String(town.query_id || `query-${town.query_index || 1}`);
    const queryIndex = toNumber(town.query_index, 1);
    const queryText = String(town.query || "").trim();
    const queryLabel = queryText || town.search_text || "Search query";

    if (!groupsById.has(queryId)) {
      groupsById.set(queryId, {
        id: queryId,
        queryIndex,
        queryLabel,
        towns: [],
      });
    }
    groupsById.get(queryId).towns.push(town);
  }

  return Array.from(groupsById.values()).sort((first, second) => first.queryIndex - second.queryIndex);
}

function createTownRow(town) {
  const item = document.createElement("div");
  item.className = `town-result town-result--${town.status || "queued"}`;

  const details = document.createElement("div");
  details.className = "town-result__details";

  const name = document.createElement("strong");
  name.textContent = town.name || town.search_text || "Town";

  const meta = document.createElement("span");
  const scraped = toNumber(town.scraped);
  const total = toNumber(town.total);
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

  return item;
}

function renderTownResults() {
  const groups = buildQueryGroups();
  townResults.replaceChildren();
  townResults.hidden = groups.length === 0;

  for (const group of groups) {
    const summary = summarizeQuery(group.towns);
    const section = document.createElement("section");
    section.className = `query-job query-job--${summary.status}`;

    const header = document.createElement("div");
    header.className = "query-job__header";

    const title = document.createElement("h3");
    title.className = "query-job__title";
    title.textContent = `Job ${group.queryIndex}: ${group.queryLabel}`;

    const meta = document.createElement("span");
    meta.className = "query-job__meta";
    meta.textContent = `${formatStatus(summary.status)} - towns ${summary.finishedTowns}/${summary.totalTowns} - results ${summary.scraped}/${summary.total}`;

    header.append(title, meta);
    section.append(header);

    const queryTrack = document.createElement("div");
    queryTrack.className = "query-job__track";
    const queryBar = document.createElement("div");
    queryBar.className = "query-job__bar";
    queryBar.style.width = `${summary.percent}%`;
    queryTrack.append(queryBar);
    section.append(queryTrack);

    const townsGrid = document.createElement("div");
    townsGrid.className = "query-job__towns";
    for (const town of group.towns) {
      townsGrid.append(createTownRow(town));
    }

    section.append(townsGrid);
    townResults.append(section);
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
  const percent = Math.max(0, Math.min(100, toNumber(progress.percent)));
  const scraped = toNumber(progress.scraped);
  const total = toNumber(progress.total);
  const collected = toNumber(progress.collected);
  const status = String(progress.status || "queued");
  const completedTowns = toNumber(progress.completed_towns);
  const failedTowns = toNumber(progress.failed_towns);
  const totalTowns = Math.max(1, toNumber(progress.total_towns, 1));
  const completedQueries = toNumber(progress.completed_queries);
  const totalQueries = Math.max(1, toNumber(progress.total_queries, 1));
  const currentQueryIndex = toNumber(progress.current_query_index);

  if (Array.isArray(progress.towns)) {
    progress.towns.forEach(upsertTown);
  }

  const townsDone = completedTowns + failedTowns;
  const townProgressText = totalTowns > 1 ? ` - towns ${townsDone}/${totalTowns}` : "";
  const queryProgressText =
    totalQueries > 1 && ACTIVE_STATUSES.includes(status) && currentQueryIndex > 0
      ? ` - job ${currentQueryIndex}/${totalQueries}`
      : totalQueries > 1
        ? ` - jobs ${completedQueries}/${totalQueries}`
        : "";

  progressStatus.textContent = `${formatStatus(status)}${queryProgressText}${townProgressText}`;
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

  const requestedQueries = countRequestedQueries();
  const requestedTowns = countRequestedTowns();
  const maxResults = toNumber(form.elements.max_results.value);
  progressPanel.hidden = false;
  downloadLink.hidden = true;
  startButton.disabled = true;
  setProgress({
    status: "queued",
    message: `Queued ${requestedQueries} query job(s) across ${requestedQueries * requestedTowns} town scrape(s).`,
    scraped: 0,
    collected: 0,
    total: maxResults * requestedTowns * requestedQueries,
    total_towns: requestedTowns * requestedQueries,
    total_queries: requestedQueries,
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
