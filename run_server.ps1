$ErrorActionPreference = "Continue"

$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$python = Join-Path $projectRoot ".venv\Scripts\python.exe"
$app = Join-Path $projectRoot "app.py"
$log = Join-Path $projectRoot "flask.log"

Set-Location -LiteralPath $projectRoot
$env:SCRAPER_HOST = "127.0.0.1"
$env:SCRAPER_PORT = "5000"
& $python $app *> $log
