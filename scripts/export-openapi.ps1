# Exports the OpenAPI documents of a running cluster into .\docs
#   .\scripts\export-openapi.ps1 [-CoordinatorUrl http://localhost:5000] [-ShardUrl http://localhost:5001]
param(
    [string]$CoordinatorUrl = "http://localhost:5000",
    [string]$ShardUrl = "http://localhost:5001"
)

$ErrorActionPreference = "Stop"
$outDir = Join-Path (Split-Path $PSScriptRoot -Parent) "docs"
New-Item -ItemType Directory -Force $outDir | Out-Null

Invoke-WebRequest "$CoordinatorUrl/openapi/v1.json" -OutFile (Join-Path $outDir "coordinator.openapi.json") -UseBasicParsing
Write-Host "Saved $outDir\coordinator.openapi.json"

try {
    Invoke-WebRequest "$ShardUrl/openapi/v1.json" -OutFile (Join-Path $outDir "shard.openapi.json") -UseBasicParsing
    Write-Host "Saved $outDir\shard.openapi.json"
} catch {
    Write-Warning "Shard at $ShardUrl is not reachable; skipped."
}
