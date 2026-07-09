[CmdletBinding()]
param(
    [string]$SourceDir = "",
    [string]$WorkDir = "",
    [string]$ConfigurePreset = "windows-clang-cl-release",
    [string]$BuildPreset = "",
    [string]$BuildTarget = "",
    [string]$PluginPreset = "windows-msys2-ucrt64-release",
    [string]$PluginPath = "",
    [string]$RewriteCompiler = "C:\msys64\ucrt64\bin\clang++.exe",
    [string]$Msys2Bash = "C:\msys64\usr\bin\bash.exe",
    [string[]]$SourceExtensions = @(".cpp", ".cc", ".cxx"),
    [string[]]$ExtraRewriteArg = @(),
    [switch]$Clean,
    [switch]$SkipConfigure,
    [switch]$SkipPluginBuild,
    [switch]$SkipRewrite,
    [switch]$ContinueOnRewriteError,
    [switch]$NoBuild
)

$ErrorActionPreference = "Stop"

function Resolve-FullPath {
    param([Parameter(Mandatory = $true)][string]$Path)

    if ([string]::IsNullOrWhiteSpace($Path)) {
        throw "Cannot resolve an empty path."
    }

    if (Test-Path -LiteralPath $Path) {
        return (Resolve-Path -LiteralPath $Path).Path
    }

    if ([System.IO.Path]::IsPathRooted($Path)) {
        return [System.IO.Path]::GetFullPath($Path)
    }

    return [System.IO.Path]::GetFullPath((Join-Path (Get-Location).Path $Path))
}

function Test-PathInside {
    param(
        [Parameter(Mandatory = $true)][string]$Parent,
        [Parameter(Mandatory = $true)][string]$Child
    )

    $parentFull = [System.IO.Path]::GetFullPath($Parent).TrimEnd('\', '/')
    $childFull = [System.IO.Path]::GetFullPath($Child).TrimEnd('\', '/')

    if ([string]::Equals($parentFull, $childFull, [System.StringComparison]::OrdinalIgnoreCase)) {
        return $true
    }

    $parentWithSeparator = $parentFull + [System.IO.Path]::DirectorySeparatorChar
    return $childFull.StartsWith($parentWithSeparator, [System.StringComparison]::OrdinalIgnoreCase)
}

function Assert-ChildPath {
    param(
        [Parameter(Mandatory = $true)][string]$Parent,
        [Parameter(Mandatory = $true)][string]$Child,
        [Parameter(Mandatory = $true)][string]$Label
    )

    if (-not (Test-PathInside -Parent $Parent -Child $Child)) {
        throw "$Label must stay inside '$Parent'. Resolved path: '$Child'"
    }
}

function Get-RelativePathCompat {
    param(
        [Parameter(Mandatory = $true)][string]$BasePath,
        [Parameter(Mandatory = $true)][string]$Path
    )

    $baseFull = [System.IO.Path]::GetFullPath($BasePath).TrimEnd('\', '/')
    $pathFull = [System.IO.Path]::GetFullPath($Path)

    if ([string]::Equals($baseFull, $pathFull.TrimEnd('\', '/'), [System.StringComparison]::OrdinalIgnoreCase)) {
        return "."
    }

    $baseWithSeparator = $baseFull + [System.IO.Path]::DirectorySeparatorChar
    if (-not $pathFull.StartsWith($baseWithSeparator, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "Path '$pathFull' is not inside '$baseFull'."
    }

    return $pathFull.Substring($baseWithSeparator.Length)
}

function ConvertTo-MsysPath {
    param([Parameter(Mandatory = $true)][string]$Path)

    $full = [System.IO.Path]::GetFullPath($Path)
    if ($full.Length -ge 3 -and $full[1] -eq ':') {
        $drive = $full[0].ToString().ToLowerInvariant()
        $rest = $full.Substring(2).Replace('\', '/')
        return "/$drive$rest"
    }

    return $full.Replace('\', '/')
}

function Quote-Bash {
    param([Parameter(Mandatory = $true)][string]$Text)
    return "'" + $Text.Replace("'", "'\''") + "'"
}

function Invoke-LoggedProcess {
    param(
        [Parameter(Mandatory = $true)][string]$FilePath,
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [Parameter(Mandatory = $true)][string]$WorkingDirectory,
        [Parameter(Mandatory = $true)][string]$LogBase,
        [switch]$AllowFailure
    )

    $stdoutPath = "$LogBase.stdout.log"
    $stderrPath = "$LogBase.stderr.log"

    Push-Location $WorkingDirectory
    try {
        $oldErrorActionPreference = $ErrorActionPreference
        $ErrorActionPreference = "Continue"
        & $FilePath @Arguments > $stdoutPath 2> $stderrPath
        $exitCode = $LASTEXITCODE
        $ErrorActionPreference = $oldErrorActionPreference
    }
    finally {
        if ($null -ne $oldErrorActionPreference) {
            $ErrorActionPreference = $oldErrorActionPreference
        }
        Pop-Location
    }

    if ($exitCode -ne 0 -and -not $AllowFailure) {
        Write-Host "Command failed: $FilePath $($Arguments -join ' ')"
        if (Test-Path -LiteralPath $stderrPath) {
            Get-Content -LiteralPath $stderrPath -Tail 80 | ForEach-Object { Write-Host $_ }
        }
        throw "Process exited with code $exitCode. Logs: $stdoutPath, $stderrPath"
    }

    return $exitCode
}

function Test-IsExcludedRelativePath {
    param([Parameter(Mandatory = $true)][string]$RelativePath)

    $parts = $RelativePath -split '[\\/]+' | Where-Object { $_ -ne "" }
    foreach ($part in $parts) {
        if ($part -in @(".git", ".idea", "build", "builds", "dist")) {
            return $true
        }
        if ($part.StartsWith("build-", [System.StringComparison]::OrdinalIgnoreCase)) {
            return $true
        }
        if ($part.StartsWith("cmake-build-", [System.StringComparison]::OrdinalIgnoreCase)) {
            return $true
        }
    }

    return $false
}

function Test-IsRewriteExcludedRelativePath {
    param([Parameter(Mandatory = $true)][string]$RelativePath)

    $normalized = $RelativePath.Replace('\', '/')
    if ($normalized -eq "akkara/akkserver/src/grpc/AkkaraGRPCServer.cpp") {
        return $true
    }
    if ($normalized.StartsWith("akkara/jni/", [System.StringComparison]::OrdinalIgnoreCase)) {
        return $true
    }
    if ($normalized.StartsWith("benchmarks/throughput/", [System.StringComparison]::OrdinalIgnoreCase)) {
        return $true
    }

    return $false
}

function Copy-SourceTree {
    param(
        [Parameter(Mandatory = $true)][string]$From,
        [Parameter(Mandatory = $true)][string]$To,
        [Parameter(Mandatory = $true)][string]$SafeParent
    )

    Assert-ChildPath -Parent $SafeParent -Child $To -Label "source copy directory"
    if (Test-Path -LiteralPath $To) {
        Remove-Item -LiteralPath $To -Recurse -Force
    }
    New-Item -ItemType Directory -Path $To -Force | Out-Null

    $files = Get-ChildItem -LiteralPath $From -Recurse -File -Force
    $copied = 0
    foreach ($file in $files) {
        if ($file.Name -eq "NUL") {
            continue
        }
        $relative = Get-RelativePathCompat -BasePath $From -Path $file.FullName
        if (Test-IsExcludedRelativePath -RelativePath $relative) {
            continue
        }

        $destination = Join-Path $To $relative
        $destinationDirectory = Split-Path -Parent $destination
        if (-not (Test-Path -LiteralPath $destinationDirectory)) {
            New-Item -ItemType Directory -Path $destinationDirectory -Force | Out-Null
        }

        Copy-Item -LiteralPath $file.FullName -Destination $destination -Force
        $copied += 1
    }

    Write-Host "Copied $copied file(s) to $To"
}

function Get-RewriteIncludeArgs {
    param(
        [Parameter(Mandatory = $true)][string]$CopiedSourceDir
    )

    $includeDirs = New-Object System.Collections.Generic.List[string]
    foreach ($path in @(
        (Join-Path $CopiedSourceDir "akkara\akkaradb\include"),
        (Join-Path $CopiedSourceDir "akkara\akkengine\include"),
        (Join-Path $CopiedSourceDir "akkara\akkserver\include"),
        (Join-Path $CopiedSourceDir "benchmarks")
    )) {
        if (Test-Path -LiteralPath $path) {
            $includeDirs.Add((Resolve-FullPath $path))
        }
    }

    $buildsDir = Join-Path $CopiedSourceDir "builds"
    if (Test-Path -LiteralPath $buildsDir) {
        Get-ChildItem -LiteralPath $buildsDir -Recurse -Directory -Filter "include" -Force |
            ForEach-Object { $includeDirs.Add($_.FullName) }

        foreach ($path in @(
            (Join-Path $buildsDir "*\_deps\monocypher-src\src"),
            (Join-Path $buildsDir "*\_deps\zstd-src\lib")
        )) {
            Get-ChildItem -Path $path -Directory -Force -ErrorAction SilentlyContinue |
                ForEach-Object { $includeDirs.Add($_.FullName) }
        }
    }

    $seen = New-Object System.Collections.Generic.HashSet[string]([System.StringComparer]::OrdinalIgnoreCase)
    $args = New-Object System.Collections.Generic.List[string]
    foreach ($dir in $includeDirs) {
        if ($seen.Add($dir)) {
            $args.Add("-I$dir")
        }
    }

    return [string[]]$args
}

function Get-RewriteSourceFiles {
    param(
        [Parameter(Mandatory = $true)][string]$CopiedSourceDir,
        [Parameter(Mandatory = $true)][string[]]$Extensions
    )

    $extensionSet = New-Object System.Collections.Generic.HashSet[string]([System.StringComparer]::OrdinalIgnoreCase)
    foreach ($extension in $Extensions) {
        if (-not $extension.StartsWith(".")) {
            [void]$extensionSet.Add(".$extension")
        }
        else {
            [void]$extensionSet.Add($extension)
        }
    }

    Get-ChildItem -LiteralPath $CopiedSourceDir -Recurse -File -Force |
        Where-Object {
            $relative = Get-RelativePathCompat -BasePath $CopiedSourceDir -Path $_.FullName
            -not (Test-IsExcludedRelativePath -RelativePath $relative) `
                -and -not (Test-IsRewriteExcludedRelativePath -RelativePath $relative) `
                -and $extensionSet.Contains($_.Extension)
        }
}

if ([string]::IsNullOrWhiteSpace($SourceDir)) {
    $SourceDir = Resolve-FullPath (Join-Path $PSScriptRoot "..")
}
else {
    $SourceDir = Resolve-FullPath $SourceDir
}

if ([string]::IsNullOrWhiteSpace($BuildPreset)) {
    $BuildPreset = $ConfigurePreset
}

if ([string]::IsNullOrWhiteSpace($WorkDir)) {
    $WorkDir = Join-Path $SourceDir (Join-Path "builds" (Join-Path "akkara-query-rewrite" $ConfigurePreset))
}
$WorkDir = Resolve-FullPath $WorkDir

if ([string]::IsNullOrWhiteSpace($PluginPath)) {
    $PluginPath = Join-Path $SourceDir "builds\akkquery-clang-plugin-ucrt64\libakkara-query.dll"
}
$PluginPath = Resolve-FullPath $PluginPath

$sourceCopyDir = Join-Path $WorkDir "source"
$rewriteDir = Join-Path $WorkDir "rewritten"
$logDir = Join-Path $WorkDir "logs"

Assert-ChildPath -Parent $WorkDir -Child $sourceCopyDir -Label "source copy directory"
Assert-ChildPath -Parent $WorkDir -Child $rewriteDir -Label "rewrite output directory"
Assert-ChildPath -Parent $WorkDir -Child $logDir -Label "log directory"

New-Item -ItemType Directory -Path $WorkDir -Force | Out-Null
if ($Clean) {
    foreach ($path in @($sourceCopyDir, $rewriteDir, $logDir)) {
        Assert-ChildPath -Parent $WorkDir -Child $path -Label "clean target"
        if (Test-Path -LiteralPath $path) {
            Remove-Item -LiteralPath $path -Recurse -Force
        }
    }
}
New-Item -ItemType Directory -Path $rewriteDir -Force | Out-Null
New-Item -ItemType Directory -Path $logDir -Force | Out-Null

Write-Host "Akkara query rewrite build"
Write-Host "  source:          $SourceDir"
Write-Host "  work:            $WorkDir"
Write-Host "  configurePreset: $ConfigurePreset"
Write-Host "  buildPreset:     $BuildPreset"

Copy-SourceTree -From $SourceDir -To $sourceCopyDir -SafeParent $WorkDir

if (-not $SkipConfigure) {
    Write-Host "Configuring copied source..."
    Invoke-LoggedProcess `
        -FilePath "cmake" `
        -Arguments @("--preset", $ConfigurePreset) `
        -WorkingDirectory $sourceCopyDir `
        -LogBase (Join-Path $logDir "configure") | Out-Null
}

if (-not $SkipPluginBuild) {
    Write-Host "Building query rewrite plugin..."
    $pluginSourceDir = Join-Path $SourceDir "akkara\akkquery-clang-plugin"
    if (Test-Path -LiteralPath $Msys2Bash) {
        $pluginSourceMsys = ConvertTo-MsysPath $pluginSourceDir
        $command = "export PATH=/ucrt64/bin:/usr/bin:`$PATH; cd $(Quote-Bash $pluginSourceMsys) && cmake --preset $(Quote-Bash $PluginPreset) && cmake --build --preset $(Quote-Bash $PluginPreset)"
        Invoke-LoggedProcess `
            -FilePath $Msys2Bash `
            -Arguments @("-lc", $command) `
            -WorkingDirectory $SourceDir `
            -LogBase (Join-Path $logDir "plugin-build") | Out-Null
    }
    else {
        Invoke-LoggedProcess `
            -FilePath "cmake" `
            -Arguments @("--preset", $PluginPreset) `
            -WorkingDirectory $pluginSourceDir `
            -LogBase (Join-Path $logDir "plugin-configure") | Out-Null
        Invoke-LoggedProcess `
            -FilePath "cmake" `
            -Arguments @("--build", "--preset", $PluginPreset) `
            -WorkingDirectory $pluginSourceDir `
            -LogBase (Join-Path $logDir "plugin-build") | Out-Null
    }
}

if (-not (Test-Path -LiteralPath $PluginPath)) {
    throw "Query rewrite plugin was not found: $PluginPath"
}

if (-not $SkipRewrite) {
    if (-not (Test-Path -LiteralPath $RewriteCompiler)) {
        throw "Rewrite compiler was not found: $RewriteCompiler"
    }

    $rewriteCompilerDir = Split-Path -Parent (Resolve-FullPath $RewriteCompiler)
    $oldPath = $env:PATH
    $env:PATH = "$rewriteCompilerDir;$oldPath"
    try {
        $includeArgs = Get-RewriteIncludeArgs -CopiedSourceDir $sourceCopyDir
        $sourceFiles = @(Get-RewriteSourceFiles -CopiedSourceDir $sourceCopyDir -Extensions $SourceExtensions)
        $rewriteDefines = @(
            "-DAKKARADB_BUILD_SHARED",
            "-DAKKARADB_API_SERVER_BUILD_SHARED",
            "-DAKKARADB_CLUSTER_RUNTIME_BUILD_SHARED"
        )
        Write-Host "Rewriting $($sourceFiles.Count) translation unit(s)..."

        $manifestPath = Join-Path $WorkDir "rewrite-manifest.jsonl"
        if (Test-Path -LiteralPath $manifestPath) {
            Remove-Item -LiteralPath $manifestPath -Force
        }

        $rewriteIndex = 0
        $failed = 0
        foreach ($sourceFile in $sourceFiles) {
            $relative = Get-RelativePathCompat -BasePath $sourceCopyDir -Path $sourceFile.FullName
            $outputPath = Join-Path $rewriteDir $relative
            $outputDirectory = Split-Path -Parent $outputPath
            if (-not (Test-Path -LiteralPath $outputDirectory)) {
                New-Item -ItemType Directory -Path $outputDirectory -Force | Out-Null
            }

            $logStem = Join-Path $logDir ("rewrite-{0:0000}" -f $rewriteIndex)
            $args = New-Object System.Collections.Generic.List[string]
            $args.Add("-std=c++20")
            $args.Add("-fsyntax-only")
            $args.Add("-DAKKARADB_QUERY_REWRITE_PASS")
            foreach ($define in $rewriteDefines) { $args.Add($define) }
            $args.Add("-Wno-ignored-attributes")
            foreach ($includeArg in $includeArgs) { $args.Add($includeArg) }
            foreach ($extraArg in $ExtraRewriteArg) { $args.Add($extraArg) }
            foreach ($arg in @(
                "-Xclang", "-load", "-Xclang", $PluginPath,
                "-Xclang", "-add-plugin", "-Xclang", "akkara-query",
                "-Xclang", "-plugin-arg-akkara-query", "-Xclang", "rewrite",
                "-Xclang", "-plugin-arg-akkara-query", "-Xclang", "rewrite-in-place",
                $sourceFile.FullName
            )) {
                $args.Add($arg)
            }

            $exitCode = Invoke-LoggedProcess `
                -FilePath $RewriteCompiler `
                -Arguments ([string[]]$args) `
                -WorkingDirectory $sourceCopyDir `
                -LogBase $logStem `
                -AllowFailure:$ContinueOnRewriteError

            if ($exitCode -ne 0) {
                $failed += 1
            }
            else {
                [pscustomobject]@{
                    source = $relative
                    rewritten = $relative
                } | ConvertTo-Json -Compress | Add-Content -LiteralPath $manifestPath
            }

            $rewriteIndex += 1
        }

        if ($failed -gt 0) {
            throw "Rewrite failed for $failed translation unit(s). See logs in $logDir"
        }
    }
    finally {
        $env:PATH = $oldPath
    }
}

if (-not $NoBuild) {
    Write-Host "Building rewritten source tree..."
    $buildArgs = New-Object System.Collections.Generic.List[string]
    $buildArgs.Add("--build")
    $buildArgs.Add("--preset")
    $buildArgs.Add($BuildPreset)
    if (-not [string]::IsNullOrWhiteSpace($BuildTarget)) {
        $buildArgs.Add("--target")
        $buildArgs.Add($BuildTarget)
    }

    Invoke-LoggedProcess `
        -FilePath "cmake" `
        -Arguments ([string[]]$buildArgs) `
        -WorkingDirectory $sourceCopyDir `
        -LogBase (Join-Path $logDir "build") | Out-Null
}

Write-Host "Akkara query rewrite build completed."
Write-Host "  copied source: $sourceCopyDir"
Write-Host "  rewritten:     $rewriteDir"
Write-Host "  logs:          $logDir"
