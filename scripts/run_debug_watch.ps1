# MailTrace — Run & Watch (Windows PowerShell)
# Simple dev runner; keeps window open on exit

#requires -Version 5
[CmdletBinding()]
param()

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

try {
    $scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
    Set-Location (Split-Path -Parent $scriptDir)

    if (-not (Test-Path ".\logs")) { New-Item -ItemType Directory -Path ".\logs" | Out-Null }

    if (-not (Get-Command python -ErrorAction SilentlyContinue)) { throw "Python not found in PATH" }

    if (-not (Test-Path ".\.venv\Scripts\Activate.ps1")) { python -m venv .venv }
    . .\.venv\Scripts\Activate.ps1

    python -m pip install -U pip
    if (Test-Path ".\requirements.txt") { pip install -r requirements.txt }

    if (-not (Test-Path ".\instance")) { New-Item -ItemType Directory -Path ".\instance" | Out-Null }
    if (-not (Test-Path ".\.env")) { Set-Content ".\.env" "DATABASE_URL=sqlite:///instance/local.db`nMAPBOX_TOKEN=" -Encoding UTF8 }

    $env:FLASK_APP = "app.wsgi:app"
    $env:PYTHONUNBUFFERED = "1"
    $env:PYTHONPATH = (Get-Location).Path

    Write-Host "[db] alembic upgrade head" -ForegroundColor Cyan
    alembic upgrade head

    $ts = Get-Date -Format "yyyyMMdd-HHmmss"
    $logPath = "logs\server-$ts.log"
    Write-Host "[run] http://127.0.0.1:8000  (logging to $logPath)" -ForegroundColor Green
    & flask run --host 127.0.0.1 --port 8000 2>&1 | Tee-Object -FilePath $logPath
} catch {
    Write-Host ("[fatal] " + $_.Exception.Message) -ForegroundColor Red
} finally {
    Read-Host "Press ENTER to close this window"
}
