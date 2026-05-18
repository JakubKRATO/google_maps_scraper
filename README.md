# Google Maps CSV Scraper

A small Flask app that searches Google Maps and returns a CSV download with:

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

Open `http://127.0.0.1:5000`, enter a query such as `coffee shops` and a location such as `Bratislava`, then submit the form. Use `batch_size` to control how many listings are collected and scraped at a time.

The website starts scraping as a background job and polls progress from the backend. When the job finishes, the page shows a CSV download link.

You can also call the CSV endpoint directly:

```text
http://127.0.0.1:5000/scrape?query=coffee%20shops&location=Bratislava&max_results=1000&batch_size=50
```

Progress-based endpoints:

```text
POST /scrape/start
GET /scrape/progress/<job_id>
GET /scrape/download/<job_id>
```

Google Maps can change its page structure or challenge automated traffic, so this is best for small internal jobs. For production or high-volume use, use the official Google Places API.
