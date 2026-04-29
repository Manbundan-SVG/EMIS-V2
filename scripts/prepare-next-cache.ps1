param(
  [string]$CacheRoot = "",
  [string]$RepoRoot = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Get-PathCacheSlug {
  param(
    [Parameter(Mandatory = $true)]
    [string]$Path
  )

  $normalized = [System.IO.Path]::GetFullPath($Path).ToLowerInvariant()
  $bytes = [System.Text.Encoding]::UTF8.GetBytes($normalized)
  $sha = [System.Security.Cryptography.SHA256]::Create()
  try {
    $hash = $sha.ComputeHash($bytes)
  } finally {
    $sha.Dispose()
  }

  $hex = [System.BitConverter]::ToString($hash).Replace("-", "").Substring(0, 12).ToLowerInvariant()
  return "next-build-cache-$hex"
}

function Remove-LinkOnly {
  param(
    [Parameter(Mandatory = $true)]
    [string]$Path
  )

  Remove-Item -LiteralPath $Path -Force
}

function Try-RotateDirectory {
  param(
    [Parameter(Mandatory = $true)]
    [string]$Path
  )

  $stamp = Get-Date -Format "yyyyMMddHHmmss"
  $rotatedPath = "$Path.stale.$stamp"
  try {
    Move-Item -LiteralPath $Path -Destination $rotatedPath -Force
    Write-Host "[emis] rotated locked cache directory to $rotatedPath"
    return $true
  } catch {
    Write-Warning "[emis] unable to rotate cache directory at $Path; reusing existing directory"
    return $false
  }
}

$repoRoot = if ([string]::IsNullOrWhiteSpace($RepoRoot)) {
  Split-Path -Parent $PSScriptRoot
} else {
  $RepoRoot
}
$webDir = Join-Path $repoRoot "apps\web"
$nextPath = Join-Path $webDir ".next"
$nextCachePath = Join-Path $nextPath "cache"

if ([string]::IsNullOrWhiteSpace($CacheRoot)) {
  $baseCacheRoot = if ($env:LOCALAPPDATA) { $env:LOCALAPPDATA } else { $env:TEMP }
  $cacheSlug = Get-PathCacheSlug -Path $repoRoot
  $CacheRoot = Join-Path $baseCacheRoot "EMIS\cache\$cacheSlug"
}

Write-Host "[emis] next cache target: $CacheRoot"

if (-not (Test-Path -LiteralPath $CacheRoot)) {
  New-Item -ItemType Directory -Path $CacheRoot -Force | Out-Null
}

$nextItem = Get-Item -LiteralPath $nextPath -ErrorAction SilentlyContinue
if ($nextItem) {
  $isReparse = ($nextItem.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0
  if ($isReparse) {
    Write-Host "[emis] removing legacy .next junction so build output stays repo-local"
    Remove-Item -LiteralPath $nextPath -Recurse -Force
    $nextItem = $null
  }
}

if (-not (Test-Path -LiteralPath $nextPath)) {
  New-Item -ItemType Directory -Path $nextPath -Force | Out-Null
}

$cacheItem = Get-Item -LiteralPath $nextCachePath -ErrorAction SilentlyContinue
if ($cacheItem) {
  $isCacheReparse = ($cacheItem.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0
  if ($isCacheReparse) {
    try {
      $resolved = [System.IO.Path]::GetFullPath($cacheItem.Target)
    } catch {
      $resolved = $null
    }
    if ($resolved -and $resolved -eq [System.IO.Path]::GetFullPath($CacheRoot)) {
      Write-Host "[emis] existing .next\\cache junction already points to cache target"
      exit 0
    }

    Write-Host "[emis] removing existing .next\\cache junction"
    try {
      Remove-LinkOnly -Path $nextCachePath
    } catch {
      Write-Warning "[emis] unable to replace cache junction; leaving current cache path in place"
      exit 0
    }
  } else {
    Write-Host "[emis] found regular .next\\cache directory"
    if (-not (Try-RotateDirectory -Path $nextCachePath)) {
      exit 0
    }
  }
}

Write-Host "[emis] creating .next\\cache junction -> $CacheRoot"
New-Item -ItemType Junction -Path $nextCachePath -Target $CacheRoot | Out-Null
