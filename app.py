from __future__ import annotations

import csv
import io
import os
import re
import time
from dataclasses import asdict, replace
from threading import Lock
from typing import Any
from uuid import uuid4
from zipfile import ZIP_DEFLATED, ZipFile

from flask import Flask, Response, jsonify, render_template, request, url_for
from flask_socketio import SocketIO, emit, join_room

from google_maps_scraper import GoogleMapsScraper, ScrapeConfig, ScrapeError


app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "google-maps-scraper")
socketio = SocketIO(app, async_mode="threading", cors_allowed_origins="*")

CSV_COLUMNS = ["id", "company_name", "telephone_num", "email", "street", "website"]
JOBS: dict[str, dict[str, Any]] = {}
JOBS_LOCK = Lock()
JOB_TTL_SECONDS = 60 * 60
ACTIVE_STATUSES = {"queued", "running", "starting", "opening", "searching", "collecting", "scraping"}


def clean_filename(value: str) -> str:
    name = re.sub(r"[^a-zA-Z0-9_-]+", "_", value.strip()).strip("_")
    return name.lower() or "google_maps_results"


def download_path(job_id: str, town_id: str | None = None) -> str:
    if town_id:
        return f"/scrape/download/{job_id}/{town_id}"
    return f"/scrape/download/{job_id}"


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


def build_zip(files: list[tuple[str, str]]) -> bytes:
    buffer = io.BytesIO()
    used_names: set[str] = set()
    with ZipFile(buffer, "w", ZIP_DEFLATED) as archive:
        for filename, csv_text in files:
            safe_name = filename or "google_maps_results.csv"
            stem, dot, suffix = safe_name.rpartition(".")
            if not dot:
                stem, suffix = safe_name, "csv"
            candidate = f"{stem}.{suffix}"
            counter = 2
            while candidate in used_names:
                candidate = f"{stem}_{counter}.{suffix}"
                counter += 1
            used_names.add(candidate)
            archive.writestr(candidate, csv_text)
    return buffer.getvalue()


def request_checkbox(name: str, default: bool = True) -> bool:
    if request.method == "POST":
        return request.values.get(name) == "on"
    default_value = "on" if default else ""
    return request.values.get(name, default_value) == "on"


def split_towns(location: str) -> list[str]:
    towns = [town.strip() for town in location.split(",") if town.strip()]
    return towns or [location.strip()]


def build_search_targets(query: str, location: str) -> list[dict[str, str]]:
    towns = split_towns(location)
    targets: list[dict[str, str]] = []
    for index, town in enumerate(towns, start=1):
        search_text = f"{query} {town}".strip()
        town_name = town or "No location"
        town_slug = clean_filename(town_name)
        targets.append(
            {
                "id": f"{index}-{town_slug}",
                "name": town_name,
                "search_text": search_text,
                "filename": f"{clean_filename(search_text)}.csv",
            }
        )
    return targets


def scrape_request_config() -> tuple[list[dict[str, str]], ScrapeConfig]:
    query = request.values.get("query", "").strip()
    location = request.values.get("location", "").strip()
    max_results = request_int("max_results", default=10, minimum=1, maximum=1000)
    batch_size = request_int("batch_size", default=50, minimum=1, maximum=100)
    include_emails = request_checkbox("include_emails", default=True)
    headless = not request_checkbox("show_browser", default=False)

    if not query:
        raise ValueError("Enter a search query, for example 'dentists' or 'coffee shops'.")

    return build_search_targets(query, location), ScrapeConfig(
        max_results=max_results,
        batch_size=batch_size,
        include_emails=include_emails,
        headless=headless,
    )


def public_town(job_id: str, town: dict[str, Any]) -> dict[str, Any]:
    total = int(town.get("total") or 0)
    scraped = int(town.get("scraped") or 0)
    status = str(town.get("status") or "queued")
    percent = 100 if status in {"completed", "failed"} else 0
    if total > 0 and status not in {"completed", "failed"}:
        percent = min(99, round((scraped / total) * 100))

    return {
        "id": town.get("id", ""),
        "name": town.get("name", ""),
        "search_text": town.get("search_text", ""),
        "status": status,
        "message": town.get("message", ""),
        "scraped": scraped,
        "collected": int(town.get("collected") or scraped),
        "total": total,
        "percent": percent,
        "batch_start": town.get("batch_start"),
        "batch_end": town.get("batch_end"),
        "error": town.get("error", ""),
        "download_url": download_path(job_id, str(town.get("id"))) if status == "completed" else "",
        "filename": town.get("filename", ""),
    }


def public_job(job_id: str, job: dict[str, Any]) -> dict[str, Any]:
    total = int(job.get("total") or 0)
    scraped = int(job.get("scraped") or 0)
    status = str(job.get("status") or "queued")
    percent = 100 if status in {"completed", "failed"} else 0
    if total > 0 and status not in {"completed", "failed"}:
        percent = min(99, round((scraped / total) * 100))
    towns = [public_town(job_id, town) for town in job.get("towns", [])]

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
        "current_town": job.get("current_town", ""),
        "current_town_index": int(job.get("current_town_index") or 0),
        "total_towns": int(job.get("total_towns") or len(towns)),
        "completed_towns": int(job.get("completed_towns") or 0),
        "failed_towns": int(job.get("failed_towns") or 0),
        "towns": towns,
        "download_url": download_path(job_id) if status == "completed" else "",
    }


def recompute_job_counts(job: dict[str, Any]) -> None:
    towns = list(job.get("towns", []))
    job["scraped"] = sum(int(town.get("scraped") or 0) for town in towns)
    job["collected"] = sum(int(town.get("collected") or town.get("scraped") or 0) for town in towns)
    job["completed_towns"] = sum(1 for town in towns if town.get("status") == "completed")
    job["failed_towns"] = sum(1 for town in towns if town.get("status") == "failed")


def update_job_state(
    job_id: str,
    job_updates: dict[str, Any] | None = None,
    town_id: str | None = None,
    town_updates: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        if not job:
            return None
        if job_updates:
            job.update(job_updates)
        if town_id and town_updates:
            for town in job.get("towns", []):
                if town.get("id") == town_id:
                    town.update(town_updates)
                    break
        recompute_job_counts(job)
        job["updated_at"] = time.time()
        return public_job(job_id, job)


def emit_job_progress(job_id: str, payload: dict[str, Any] | None = None) -> None:
    payload = payload or snapshot_public_job(job_id)
    if payload:
        socketio.emit("scrape_progress", payload, to=job_id)


def emit_town_event(event_name: str, job_id: str, town_id: str) -> None:
    payload = snapshot_public_job(job_id)
    if not payload:
        return
    town = next((item for item in payload.get("towns", []) if item.get("id") == town_id), None)
    if town:
        socketio.emit(event_name, {"job_id": job_id, "town": town}, to=job_id)


def snapshot_public_job(job_id: str) -> dict[str, Any] | None:
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        if not job:
            return None
        return public_job(job_id, job)


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


def run_scrape_job(job_id: str, targets: list[dict[str, str]], base_config: ScrapeConfig) -> None:
    try:
        for index, target in enumerate(targets, start=1):
            town_id = target["id"]
            town_name = target["name"]
            rows: list[dict[str, str | int]] = []
            collected_count = 0

            def on_progress(event: dict[str, Any]) -> None:
                nonlocal collected_count
                clean_event = {key: value for key, value in event.items() if value is not None}
                if "collected" in clean_event:
                    collected_count = max(collected_count, int(clean_event.get("collected") or 0))
                    clean_event["collected"] = collected_count
                town_message = str(clean_event.get("message") or f"Scraping {town_name}.")
                town_status = str(clean_event.get("status") or "running")
                payload = update_job_state(
                    job_id,
                    {
                        "status": town_status if town_status in ACTIVE_STATUSES else "running",
                        "message": f"{town_name}: {town_message}",
                        "current_town": town_name,
                        "current_town_index": index,
                        "batch_start": clean_event.get("batch_start"),
                        "batch_end": clean_event.get("batch_end"),
                    },
                    town_id,
                    clean_event | {"status": town_status, "message": town_message},
                )
                emit_job_progress(job_id, payload)

            payload = update_job_state(
                job_id,
                {
                    "status": "running",
                    "message": f"Starting {town_name} ({index} of {len(targets)}).",
                    "current_town": town_name,
                    "current_town_index": index,
                    "error": "",
                },
                town_id,
                {
                    "status": "running",
                    "message": "Starting scrape.",
                    "error": "",
                    "scraped": 0,
                    "collected": 0,
                    "rows": [],
                },
            )
            emit_job_progress(job_id, payload)

            config = replace(base_config, progress_callback=on_progress)
            try:
                scraper = GoogleMapsScraper(config)
                for row in scraper.iter_scrape(target["search_text"]):
                    rows.append(asdict(row))
                    payload = update_job_state(
                        job_id,
                        {
                            "status": "scraping",
                            "message": (
                                f"{town_name}: Scraped {len(rows)} of "
                                f"{config.max_results} requested result(s)."
                            ),
                            "current_town": town_name,
                            "current_town_index": index,
                        },
                        town_id,
                        {
                            "status": "scraping",
                            "message": f"Scraped {len(rows)} of {config.max_results} requested result(s).",
                            "scraped": len(rows),
                            "collected": max(collected_count, len(rows)),
                            "rows": rows.copy(),
                        },
                    )
                    emit_job_progress(job_id, payload)

                payload = update_job_state(
                    job_id,
                    {
                        "status": "running",
                        "message": f"{town_name}: Completed with {len(rows)} result(s).",
                        "current_town": town_name,
                        "current_town_index": index,
                    },
                    town_id,
                    {
                        "status": "completed",
                        "message": f"Completed with {len(rows)} result(s).",
                        "scraped": len(rows),
                        "collected": max(collected_count, len(rows)),
                        "rows": rows.copy(),
                    },
                )
                emit_job_progress(job_id, payload)
                emit_town_event("town_completed", job_id, town_id)
            except ScrapeError as exc:
                payload = update_job_state(
                    job_id,
                    {
                        "status": "running",
                        "message": f"{town_name}: {exc}",
                        "current_town": town_name,
                        "current_town_index": index,
                    },
                    town_id,
                    {
                        "status": "failed",
                        "message": str(exc),
                        "error": str(exc),
                        "rows": rows.copy(),
                    },
                )
                emit_job_progress(job_id, payload)
                emit_town_event("town_failed", job_id, town_id)
            except Exception as exc:
                message = f"Scraping failed: {exc}"
                payload = update_job_state(
                    job_id,
                    {
                        "status": "running",
                        "message": f"{town_name}: {message}",
                        "current_town": town_name,
                        "current_town_index": index,
                    },
                    town_id,
                    {
                        "status": "failed",
                        "message": message,
                        "error": message,
                        "rows": rows.copy(),
                    },
                )
                emit_job_progress(job_id, payload)
                emit_town_event("town_failed", job_id, town_id)

        snapshot = snapshot_public_job(job_id) or {}
        completed_towns = int(snapshot.get("completed_towns") or 0)
        failed_towns = int(snapshot.get("failed_towns") or 0)
        total_towns = int(snapshot.get("total_towns") or len(targets))
        if completed_towns == 0 and failed_towns > 0:
            final_status = "failed"
            final_message = f"All {failed_towns} town scrape(s) failed."
        elif failed_towns > 0:
            final_status = "completed"
            final_message = f"Completed {completed_towns} of {total_towns} town scrape(s); {failed_towns} failed."
        else:
            final_status = "completed"
            final_message = f"Completed {completed_towns} town scrape(s)."

        payload = update_job_state(
            job_id,
            {
                "status": final_status,
                "message": final_message,
                "current_town": "",
                "current_town_index": total_towns,
                "error": final_message if final_status == "failed" else "",
            },
        )
        emit_job_progress(job_id, payload)
    except Exception as exc:
        message = f"Scraping failed: {exc}"
        payload = update_job_state(job_id, {"status": "failed", "message": message, "error": message})
        emit_job_progress(job_id, payload)


@app.get("/")
def index() -> str:
    return render_template("index.html")


@socketio.on("join_job")
def join_job(data: dict[str, Any]) -> None:
    job_id = str((data or {}).get("job_id") or "")
    if not job_id:
        return
    join_room(job_id)
    payload = snapshot_public_job(job_id)
    if not payload:
        emit("scrape_error", {"error": "Scrape job was not found."})
        return
    emit("scrape_progress", payload)
    for town in payload.get("towns", []):
        if town.get("status") == "completed":
            emit("town_completed", {"job_id": job_id, "town": town})
        elif town.get("status") == "failed":
            emit("town_failed", {"job_id": job_id, "town": town})


@app.route("/scrape", methods=["GET", "POST"])
def scrape() -> Response | str:
    try:
        targets, config = scrape_request_config()
    except ValueError as exc:
        return render_template(
            "index.html",
            error=str(exc),
            form=request.values,
        )

    try:
        files: list[tuple[str, str]] = []
        for target in targets:
            scraper = GoogleMapsScraper(config)
            results = [asdict(row) for row in scraper.scrape(target["search_text"])]
            files.append((target["filename"], build_csv(results)))
    except ScrapeError as exc:
        return render_template("index.html", error=str(exc), form=request.values)

    if len(files) == 1:
        filename, csv_text = files[0]
        return Response(
            csv_text,
            mimetype="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )

    return Response(
        build_zip(files),
        mimetype="application/zip",
        headers={"Content-Disposition": "attachment; filename=google_maps_results.zip"},
    )


@app.post("/scrape/start")
def scrape_start() -> Response:
    cleanup_old_jobs()
    try:
        targets, config = scrape_request_config()
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

    job_id = uuid4().hex
    now = time.time()
    towns = [
        {
            **target,
            "status": "queued",
            "message": "Queued.",
            "rows": [],
            "scraped": 0,
            "collected": 0,
            "total": config.max_results,
            "created_at": now,
            "updated_at": now,
        }
        for target in targets
    ]
    with JOBS_LOCK:
        JOBS[job_id] = {
            "id": job_id,
            "status": "queued",
            "message": f"Queued {len(targets)} town scrape(s).",
            "towns": towns,
            "scraped": 0,
            "collected": 0,
            "total": config.max_results * len(targets),
            "total_towns": len(targets),
            "completed_towns": 0,
            "failed_towns": 0,
            "current_town": "",
            "current_town_index": 0,
            "batch_size": config.batch_size,
            "created_at": now,
            "updated_at": now,
        }

    socketio.start_background_task(run_scrape_job, job_id, targets, config)

    return jsonify(
        {
            "job_id": job_id,
            "progress_url": url_for("scrape_progress", job_id=job_id),
            "download_url": url_for("scrape_download", job_id=job_id),
        }
    )


@app.get("/scrape/progress/<job_id>")
def scrape_progress(job_id: str) -> Response:
    payload = snapshot_public_job(job_id)
    if not payload:
        return jsonify({"error": "Scrape job was not found."}), 404
    return jsonify(payload)


@app.get("/scrape/download/<job_id>")
def scrape_download(job_id: str) -> Response:
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        if not job:
            return jsonify({"error": "Scrape job was not found."}), 404
        if job.get("status") != "completed":
            return jsonify({"error": "Scrape job is not finished yet."}), 409
        completed_towns = [town for town in job.get("towns", []) if town.get("status") == "completed"]

    if not completed_towns:
        return jsonify({"error": "No completed CSV files are available."}), 404

    if len(completed_towns) == 1:
        town = completed_towns[0]
        rows = list(town.get("rows", []))
        filename = str(town.get("filename") or "google_maps_results.csv")
        return Response(
            build_csv(rows),
            mimetype="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )

    return Response(
        build_zip(
            [
                (str(town.get("filename") or "google_maps_results.csv"), build_csv(list(town.get("rows", []))))
                for town in completed_towns
            ]
        ),
        mimetype="application/zip",
        headers={"Content-Disposition": "attachment; filename=google_maps_results.zip"},
    )


@app.get("/scrape/download/<job_id>/<town_id>")
def scrape_town_download(job_id: str, town_id: str) -> Response:
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        if not job:
            return jsonify({"error": "Scrape job was not found."}), 404
        town = next((item for item in job.get("towns", []) if item.get("id") == town_id), None)
        if not town:
            return jsonify({"error": "Town scrape was not found."}), 404
        if town.get("status") != "completed":
            return jsonify({"error": "Town scrape is not finished yet."}), 409
        rows = list(town.get("rows", []))
        filename = str(town.get("filename") or "google_maps_results.csv")

    return Response(
        build_csv(rows),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


if __name__ == "__main__":
    socketio.run(
        app,
        host="0.0.0.0",
        port=6767, # KEEP THE PORT BE AS IS
        debug=True,
        use_reloader=False,
        allow_unsafe_werkzeug=True,
    )
