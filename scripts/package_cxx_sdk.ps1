param(
    [string]$BuildDir = "",
    [string]$Config = "Release"
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
if ([string]::IsNullOrWhiteSpace($BuildDir)) {
    $BuildDir = Join-Path $repoRoot "builds\release"
}

if (-not (Test-Path $BuildDir)) {
    throw "Build directory not found: $BuildDir"
}

Write-Host "Packaging AkkaraDB CXX SDK from build dir: $BuildDir"
& cmake --build $BuildDir --target akkaradb_package_cxx_sdk --config $Config
if ($LASTEXITCODE -ne 0) {
    throw "CXX SDK packaging failed with exit code $LASTEXITCODE"
}
