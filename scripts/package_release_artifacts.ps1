param(
    [string]$Config = "Release",
    [string]$LinuxPreset = "release-linux",
    [string]$WslDistro = "",
    [switch]$SkipWindows,
    [switch]$SkipLinux
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$windowsBuildDir = Join-Path $repoRoot "builds\release"

function Invoke-Checked {
    param(
        [string]$Description,
        [scriptblock]$Command
    )

    Write-Host $Description
    & $Command
    if ($LASTEXITCODE -ne 0) {
        throw "$Description failed with exit code $LASTEXITCODE"
    }
}

if (-not $SkipWindows) {
    & (Join-Path $PSScriptRoot "package_cxx_sdk.ps1") -BuildDir $windowsBuildDir -Config $Config
    if ($LASTEXITCODE -ne 0) {
        throw "Windows CXX SDK packaging failed with exit code $LASTEXITCODE"
    }
}

if (-not $SkipLinux) {
    $wslArgs = @()
    if (-not [string]::IsNullOrWhiteSpace($WslDistro)) {
        $wslArgs += @("--distribution", $WslDistro)
    }

    $wslArgs += @(
        "--cd", $repoRoot,
        "bash", "-lc",
        "set -euo pipefail; command -v cmake >/dev/null || { echo 'cmake is required in WSL' >&2; exit 127; }; cmake --preset $LinuxPreset; cmake --build --preset $LinuxPreset --target akkaradb_package_cxx_sdk --config $Config"
    )

    Invoke-Checked "Packaging AkkaraDB Linux CXX SDK via WSL preset '$LinuxPreset'" {
        & wsl.exe @wslArgs
    }
}