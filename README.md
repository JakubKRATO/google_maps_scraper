# Google Maps CSV Scraper

A small Flask app that searches Google Maps and returns town-by-town CSV downloads with:

```csv
id,company_name,telephone_num,email,street,website
```

## Setup

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python -m playwright install chromium
python app.py
```

On Windows you can also start it with:

```powershell
.\run_server.ps1
```

Set `SCRAPER_PORT=5050` first if you want to run it on another port.

Open `http://127.0.0.1:6767`, enter a query such as `coffee shops` and towns such as `Bratislava, Trnava, Kosice`, then submit the form. Use `batch_size` to control how many listings are collected and scraped at a time.

The website starts scraping as a background job and streams progress with Socket.IO, with polling kept as a fallback. Each town gets its own CSV link as soon as that town finishes. When the full job finishes, the page also shows one download link for all completed CSVs. If there is more than one town, that combined download is a ZIP file.

You can also call the CSV endpoint directly:

```text
http://127.0.0.1:6767/scrape?query=coffee%20shops&location=Bratislava,Trnava&max_results=1000&batch_size=50
```

Progress-based endpoints:

```text
POST /scrape/start
GET /scrape/progress/<job_id>
GET /scrape/download/<job_id>
GET /scrape/download/<job_id>/<town_id>
```

Google Maps can change its page structure or challenge automated traffic, so this is best for small internal jobs. For production or high-volume use, use the official Google Places API.
