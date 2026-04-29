param(
  [int]$Port = 3002,
  [string]$Hostname = "127.0.0.1",
  [string]$RepoRoot = "",
  [ValidateSet("auto", "prod", "dev")]
  [string]$Mode = "auto"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Start-NextDev {
  param(
    [Parameter(Mandatory = $true)]
    [string]$WebDir,
    [Parameter(Mandatory = $true)]
    [string]$NextCmd,
    [Parameter(Mandatory = $true)]
    [string]$Hostname,
    [Parameter(Mandatory = $true)]
    [int]$Port
  )

  Push-Location $WebDir
  try {
    Write-Warning "[emis] falling back to next dev on http://$Hostname`:$Port"
    & $NextCmd dev --hostname $Hostname --port $Port
  } finally {
    Pop-Location
  }
}

$repoRoot = if ([string]::IsNullOrWhiteSpace($RepoRoot)) {
  Split-Path -Parent $PSScriptRoot
} else {
  $RepoRoot
}
$webDir = Join-Path $repoRoot "apps\\web"
$nextDir = Join-Path $webDir ".next"
$nextCmd = Join-Path $webDir "node_modules\\.bin\\next.cmd"
$prepareCacheScript = Join-Path $PSScriptRoot "prepare-next-cache.ps1"
$pnpmCmd = Join-Path $env:APPDATA "npm\\pnpm.cmd"
$logsDir = Join-Path $repoRoot ".logs"
$buildLog = Join-Path $logsDir "web-build-last.log"

Write-Host "[emis] repo root: $repoRoot"
Write-Host "[emis] web dir:   $webDir"
Write-Host "[emis] mode:      $Mode"

if (-not (Test-Path -LiteralPath $logsDir)) {
  New-Item -ItemType Directory -Path $logsDir -Force | Out-Null
}

if (Test-Path -LiteralPath $prepareCacheScript) {
  if (Test-Path -LiteralPath $nextDir) {
    Write-Host "[emis] clearing Next build output contents (preserving cache)"
    Get-ChildItem -LiteralPath $nextDir -Force -ErrorAction SilentlyContinue |
      Where-Object { $_.Name -ne "cache" } |
      Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
  }

  Write-Host "[emis] preparing Next cache junction"
  & $prepareCacheScript -RepoRoot $repoRoot
} elseif (Test-Path -LiteralPath $nextDir) {
  Write-Host "[emis] clearing Next build output contents"
  Get-ChildItem -LiteralPath $nextDir -Force -ErrorAction SilentlyContinue |
    Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
}

if ($Mode -eq "dev") {
  Start-NextDev -WebDir $webDir -NextCmd $nextCmd -Hostname $Hostname -Port $Port
  exit 0
}

$buildSucceeded = $false
Push-Location $repoRoot
try {
  Write-Host "[emis] rebuilding web app from a clean state"
  if (Test-Path -LiteralPath $buildLog) {
    Remove-Item -LiteralPath $buildLog -Force -ErrorAction SilentlyContinue
  }
  if (Test-Path -LiteralPath $pnpmCmd) {
    & $pnpmCmd --filter @emis/web build *>&1 | Tee-Object -FilePath $buildLog
  } else {
    npm run build *>&1 | Tee-Object -FilePath $buildLog
  }
  if ($LASTEXITCODE -ne 0) {
    $buildFailed = $true
  } else {
    $buildSucceeded = $true
  }
} finally {
  Pop-Location
}

if (-not $buildSucceeded) {
  $buildLogText = if (Test-Path -LiteralPath $buildLog) {
    Get-Content -Raw $buildLog
  } else {
    ""
  }

  $shouldFallbackToDev = $Mode -eq "auto" -and $buildLogText -match "spawn EPERM"
  if ($shouldFallbackToDev) {
    Write-Warning "[emis] detected Windows spawn EPERM during web build"
    Start-NextDev -WebDir $webDir -NextCmd $nextCmd -Hostname $Hostname -Port $Port
    exit 0
  }

  throw "web build failed. See $buildLog"
}

Push-Location $webDir
try {
  Write-Host "[emis] starting Next on http://$Hostname`:$Port"
  & $nextCmd start --hostname $Hostname --port $Port
} finally {
  Pop-Location
}
