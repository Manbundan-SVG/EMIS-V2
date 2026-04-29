param(
  [int]$Port = 3002,
  [string]$Hostname = "127.0.0.1",
  [string]$MirrorRoot = "",
  [switch]$SkipInstall,
  [ValidateSet("auto", "prod", "dev")]
  [string]$Mode = "auto"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Get-NodeDir {
  $candidates = @()

  if ($env:NVM_SYMLINK) {
    $candidates += $env:NVM_SYMLINK
  }

  $candidates += "C:\nvm4w\nodejs"

  foreach ($candidate in $candidates | Select-Object -Unique) {
    if (-not [string]::IsNullOrWhiteSpace($candidate)) {
      $nodeExe = Join-Path $candidate "node.exe"
      if (Test-Path -LiteralPath $nodeExe) {
        return $candidate
      }
    }
  }

  $nodeCommand = Get-Command node -ErrorAction SilentlyContinue
  if ($nodeCommand) {
    return Split-Path -Parent $nodeCommand.Source
  }

  throw "Unable to locate a Node.js runtime. Install Node 22 via nvm-windows first."
}

$repoRoot = Split-Path -Parent $PSScriptRoot
if ([string]::IsNullOrWhiteSpace($MirrorRoot)) {
  $base = if ($env:LOCALAPPDATA) { $env:LOCALAPPDATA } else { $env:TEMP }
  $MirrorRoot = Join-Path $base "EMIS-run\emis-phase1"
}

$nodeDir = Get-NodeDir
$corepackCmd = Join-Path $nodeDir "corepack.cmd"

if (-not (Test-Path -LiteralPath $corepackCmd)) {
  throw "Could not find corepack.cmd under $nodeDir"
}

$mirrorParent = Split-Path -Parent $MirrorRoot
New-Item -ItemType Directory -Path $mirrorParent -Force | Out-Null

Write-Host "[emis] source repo:   $repoRoot"
Write-Host "[emis] mirror repo:   $MirrorRoot"
Write-Host "[emis] node runtime:  $nodeDir"
Write-Host "[emis] mode:         $Mode"

$robocopyArgs = @(
  $repoRoot,
  $MirrorRoot,
  "/MIR",
  "/R:1",
  "/W:1",
  "/NFL",
  "/NDL",
  "/NJH",
  "/NJS",
  "/NP",
  "/XD",
  "node_modules",
  ".next",
  ".logs",
  "_archive",
  ".git",
  ".npm-cache",
  "apps\web\.next",
  "apps\web\node_modules",
  "apps\worker\__pycache__",
  "packages\types\dist",
  "packages\signal-registry\dist"
)

Write-Host "[emis] syncing source tree into run-safe mirror"
& robocopy @robocopyArgs | Out-Null
$robocopyExit = $LASTEXITCODE
if ($robocopyExit -ge 8) {
  throw "robocopy failed with exit code $robocopyExit"
}

$env:Path = "$nodeDir;$env:APPDATA\npm;$env:Path"

Push-Location $MirrorRoot
try {
  if (-not $SkipInstall) {
    Write-Host "[emis] refreshing mirror dependencies with corepack pnpm"
    & $corepackCmd pnpm install --prefer-offline
  }

  $cleanStart = Join-Path $repoRoot "scripts\web-clean-start.ps1"
  if (-not (Test-Path -LiteralPath $cleanStart)) {
    throw "Source repo is missing scripts\web-clean-start.ps1"
  }

  Write-Host "[emis] launching EMIS web from the run-safe mirror"
  & $cleanStart -Port $Port -Hostname $Hostname -RepoRoot $MirrorRoot -Mode $Mode
} finally {
  Pop-Location
}
