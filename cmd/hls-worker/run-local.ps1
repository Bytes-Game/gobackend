# run-local.ps1 — convenience launcher for running the HLS transcode
# worker on your local Windows PC instead of paying for a Render
# Background Worker. Reads the sibling .env file (KEY=VALUE one per
# line, no quotes), loads each into the current process environment,
# then `go run`s the worker. Ctrl-C exits cleanly.
#
# Usage:
#   cd F:\devf\devb\cmd\hls-worker
#   .\run-local.ps1
#
# Requirements on the host:
#   * Go installed and on PATH (you already have this)
#   * FFmpeg installed and on PATH (winget install Gyan.FFmpeg)
#   * .env file in this directory with the required keys
#
# Why a .ps1 wrapper instead of just `go run`: the worker reads its
# config from process env vars (same as the Render deployment) and
# Windows PowerShell doesn't have an `env $(cat .env) cmd` idiom built
# in. Five lines of script saves the user from having to remember
# seven $env:VARNAME assignments every session.

$envPath = Join-Path $PSScriptRoot ".env"
if (-not (Test-Path $envPath)) {
    Write-Host "ERROR: $envPath does not exist." -ForegroundColor Red
    Write-Host "Copy .env.example to .env and fill in your secrets." -ForegroundColor Yellow
    exit 1
}

Write-Host "Loading env from $envPath..." -ForegroundColor Cyan
Get-Content $envPath | ForEach-Object {
    $line = $_.Trim()
    # Skip blanks and comment lines
    if ($line -eq "" -or $line.StartsWith("#")) { return }
    $eq = $line.IndexOf("=")
    if ($eq -lt 1) { return }
    $key = $line.Substring(0, $eq).Trim()
    $val = $line.Substring($eq + 1).Trim()
    # Strip surrounding quotes if the user added them
    if (($val.StartsWith('"') -and $val.EndsWith('"')) -or
        ($val.StartsWith("'") -and $val.EndsWith("'"))) {
        $val = $val.Substring(1, $val.Length - 2)
    }
    Set-Item -Path "env:$key" -Value $val
}

# Verify ffmpeg is reachable before launching — fail fast with a
# friendlier error than the Go binary's exec.LookPath would give.
$null = Get-Command ffmpeg -ErrorAction SilentlyContinue
if (-not $?) {
    Write-Host "ERROR: ffmpeg not found on PATH." -ForegroundColor Red
    Write-Host "Install with: winget install Gyan.FFmpeg" -ForegroundColor Yellow
    Write-Host "Then close + reopen PowerShell and try again." -ForegroundColor Yellow
    exit 1
}

Write-Host "Starting HLS worker (Ctrl-C to stop)..." -ForegroundColor Green
go run .
