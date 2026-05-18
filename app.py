from __future__ import annotations

import csv
import io
import os
import re
import time
from dataclasses import asdict
from threading import Lock, Thread
from typing import Any
from uuid import uuid4

from flask import Flask, Response, jsonify, render_template, request, url_for

from google_maps_scraper import GoogleMapsScraper, ScrapeConfig, ScrapeError


app = Flask(__name__)

CSV_COLUMNS = ["id", "company_name", "telephone_num", "email", "street", "website"]
JOBS: dict[str, dict[str, Any]] = {}
JOBS_LOCK = Lock()
JOB_TTL_SECONDS = 60 * 60


def clean_filename(value: str) -> str:
    name = re.sub(r"[^a-zA-Z0-9_-]+", "_", value.strip()).strip("_")
    return name.lower() or "google_maps_results"


def request_int(name: str, default: int, minimum: int, maximum: int) -> int:
    raw_value = request.values.get(name, str(default))
    try:
        value = int(raw_value)
    except (TypeError, ValueError):
        return default
    return max(minimum, min(maximum, value))


def build_csv(rows: list[dict[str, str | int]]) -> str:
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=CSV_COLUMNS, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    return buffer.getvalue()


def request_checkbox(name: str, default: bool = True) -> bool:
    if request.method == "POST":
        return request.values.get(name) == "on"
    default_value = "on" if default else ""
    return request.values.get(name, default_value) == "on"


def scrape_request_config() -> tuple[str, ScrapeConfig]:
    query = request.values.get("query", "").strip()
    location = request.values.get("location", "").strip()
    max_results = request_int("max_results", default=10, minimum=1, maximum=1000)
    batch_size = request_int("batch_size", default=50, minimum=1, maximum=100)
    include_emails = request_checkbox("include_emails", default=True)
    headless = not request_checkbox("show_browser", default=False)

    if not query:
        raise ValueError("Enter a search query, for example 'dentists' or 'coffee shops'.")

    search_text = f"{query} {location}".strip()
    return search_text, ScrapeConfig(
        max_results=max_results,
        batch_size=batch_size,
        include_emails=include_emails,
        headless=headless,
    )


def update_job(job_id: str, **updates: Any) -> None:
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        if not job:
            return
        job.update(updates)
        job["updated_at"] = time.time()


def public_job(job_id: str, job: dict[str, Any]) -> dict[str, Any]:
    total = int(job.get("total") or 0)
    scraped = int(job.get("scraped") or 0)
    status = str(job.get("status") or "queued")
    percent = 100 if status == "completed" else 0
    if total > 0 and status != "completed":
        percent = min(99, round((scraped / total) * 100))

    return {
        "id": job_id,
        "status": status,
        "message": job.get("message", ""),
        "scraped": scraped,
        "collected": int(job.get("collected") or scraped),
        "total": total,
        "percent": percent,
        "batch_start": job.get("batch_start"),
        "batch_end": job.get("batch_end"),
        "error": job.get("error", ""),
        "download_url": url_for("scrape_download", job_id=job_id) if status == "completed" else "",
    }


def cleanup_old_jobs() -> None:
    cutoff = time.time() - JOB_TTL_SECONDS
    with JOBS_LOCK:
        expired_job_ids = [
            job_id
            for job_id, job in JOBS.items()
            if job.get("status") in {"completed", "failed"} and float(job.get("updated_at", 0)) < cutoff
        ]
        for job_id in expired_job_ids:
            del JOBS[job_id]


def run_scrape_job(job_id: str, search_text: str, config: ScrapeConfig) -> None:
    rows: list[dict[str, str | int]] = []

    def on_progress(event: dict[str, Any]) -> None:
        update_job(job_id, **{key: value for key, value in event.items() if value is not None})

    config.progress_callback = on_progress

    try:
        update_job(job_id, status="running", message="Starting scrape.")
        scraper = GoogleMapsScraper(config)
        for row in scraper.iter_scrape(search_text):
            rows.append(asdict(row))
            update_job(
                job_id,
                status="scraping",
                message=f"Scraped {len(rows)} of {config.max_results} requested result(s).",
                scraped=len(rows),
                collected=max(len(rows), int(JOBS.get(job_id, {}).get("collected") or 0)),
                rows=rows.copy(),
            )

        update_job(
            job_id,
            status="completed",
            message=f"Completed with {len(rows)} result(s).",
            scraped=len(rows),
            collected=max(len(rows), int(JOBS.get(job_id, {}).get("collected") or 0)),
            rows=rows.copy(),
        )
    except ScrapeError as exc:
        update_job(job_id, status="failed", message=str(exc), error=str(exc), rows=rows.copy())
    except Exception as exc:
        message = f"Scraping failed: {exc}"
        update_job(job_id, status="failed", message=message, error=message, rows=rows.copy())


@app.get("/")
def index() -> str:
    return render_template("index.html")


@app.route("/scrape", methods=["GET", "POST"])
def scrape() -> Response | str:
    try:
        search_text, config = scrape_request_config()
    except ValueError as exc:
        return render_template(
            "index.html",
            error=str(exc),
            form=request.values,
        )

    try:
        scraper = GoogleMapsScraper(config)
        results = [asdict(row) for row in scraper.scrape(search_text)]
    except ScrapeError as exc:
        return render_template("index.html", error=str(exc), form=request.values)

    csv_text = build_csv(results)
    filename = f"{clean_filename(search_text)}.csv"
    return Response(
        csv_text,
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@app.post("/scrape/start")
def scrape_start() -> Response:
    cleanup_old_jobs()
    try:
        search_text, config = scrape_request_config()
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

    job_id = uuid4().hex
    now = time.time()
    with JOBS_LOCK:
        JOBS[job_id] = {
            "id": job_id,
            "status": "queued",
            "message": "Queued.",
            "search_text": search_text,
            "filename": f"{clean_filename(search_text)}.csv",
            "rows": [],
            "scraped": 0,
            "collected": 0,
            "total": config.max_results,
            "batch_size": config.batch_size,
            "created_at": now,
            "updated_at": now,
        }

    thread = Thread(target=run_scrape_job, args=(job_id, search_text, config), daemon=True)
    thread.start()

    return jsonify(
        {
            "job_id": job_id,
            "progress_url": url_for("scrape_progress", job_id=job_id),
            "download_url": url_for("scrape_download", job_id=job_id),
        }
    )


@app.get("/scrape/progress/<job_id>")
def scrape_progress(job_id: str) -> Response:
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        if not job:
            return jsonify({"error": "Scrape job was not found."}), 404
        payload = public_job(job_id, job.copy())
    return jsonify(payload)


@app.get("/scrape/download/<job_id>")
def scrape_download(job_id: str) -> Response:
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        if not job:
            return jsonify({"error": "Scrape job was not found."}), 404
        if job.get("status") != "completed":
            return jsonify({"error": "Scrape job is not finished yet."}), 409
        rows = list(job.get("rows", []))
        filename = str(job.get("filename") or "google_maps_results.csv")

    return Response(
        build_csv(rows),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=6767, # KEEP THE PORT BE AS IS
        debug=True,
        threaded=True,
        use_reloader=False,
    )
