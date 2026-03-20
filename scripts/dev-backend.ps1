param(
  [int]$BackendPort = 8008,
  [string]$DataDir = ".local-dev/data",
  [string]$FrontendOrigins = "http://127.0.0.1:4173,http://localhost:4173,http://127.0.0.1:5173,http://localhost:5173,http://127.0.0.1:5500,http://localhost:5500"
)

$ErrorActionPreference = "Stop"

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$resolvedDataDir = Join-Path $repoRoot $DataDir

New-Item -ItemType Directory -Force -Path $resolvedDataDir | Out-Null

$env:CLOUDV2_DATA_DIR = $resolvedDataDir
$env:SQLITE_DB_PATH = Join-Path $resolvedDataDir "telemetry.sqlite3"
$env:BACKEND_PUBLIC_PORT = [string]$BackendPort
$env:DASHBOARD_HOST = "127.0.0.1"
$env:AUTH_BASE_URL = "http://127.0.0.1:$BackendPort"
$env:AUTH_COOKIE_SECURE = "0"
$env:AUTH_COOKIE_SAMESITE = "Lax"
$env:CORS_ALLOWED_ORIGINS = $FrontendOrigins

if (-not $env:CLOUDV2_DEV_HOT_RELOAD) {
  $env:CLOUDV2_DEV_HOT_RELOAD = "1"
}

Write-Host ""
Write-Host "Backend local de desenvolvimento"
Write-Host "  Porta:           http://127.0.0.1:$BackendPort"
Write-Host "  SQLite local:    $env:SQLITE_DB_PATH"
Write-Host "  Data dir local:  $env:CLOUDV2_DATA_DIR"
Write-Host "  CORS liberado:   $env:CORS_ALLOWED_ORIGINS"
Write-Host ""

Set-Location $repoRoot
python backend/run_monitor.py
