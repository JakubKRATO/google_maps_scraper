const form = document.querySelector("#scrape-form");
const startButton = document.querySelector("#start-button");
const progressPanel = document.querySelector("#progress-panel");
const progressStatus = document.querySelector("#progress-status");
const progressCount = document.querySelector("#progress-count");
const progressTrack = document.querySelector("#progress-track");
const progressBar = document.querySelector("#progress-bar");
const progressMessage = document.querySelector("#progress-message");
const downloadLink = document.querySelector("#download-link");

let pollTimer = null;

function setProgress(progress) {
  const percent = Math.max(0, Math.min(100, Number(progress.percent || 0)));
  const scraped = Number(progress.scraped || 0);
  const total = Number(progress.total || 0);
  const collected = Number(progress.collected || 0);
  const status = String(progress.status || "queued");

  progressStatus.textContent = status.charAt(0).toUpperCase() + status.slice(1);
  progressCount.textContent = `${scraped} / ${total}`;
  progressTrack.setAttribute("aria-valuenow", String(percent));
  progressBar.style.width = `${percent}%`;

  const collectedText = collected > scraped ? ` Collected ${collected} link(s).` : "";
  progressMessage.textContent = `${progress.message || ""}${collectedText}`;

  if (status === "completed" && progress.download_url) {
    downloadLink.href = progress.download_url;
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

  if (["queued", "running", "starting", "opening", "searching", "collecting", "scraping"].includes(progress.status)) {
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

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  window.clearTimeout(pollTimer);

  progressPanel.hidden = false;
  downloadLink.hidden = true;
  startButton.disabled = true;
  setProgress({
    status: "queued",
    message: "Queued.",
    scraped: 0,
    collected: 0,
    total: form.elements.max_results.value,
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

    await pollProgress(payload.progress_url);
  } catch (error) {
    showError(error);
  }
});
