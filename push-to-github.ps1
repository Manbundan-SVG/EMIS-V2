# push-to-github.ps1 — one-shot push of emis-phase1 to https://github.com/Manbundan-SVG/EMIS-V2.git
#
# Usage (PowerShell, from anywhere):
#   cd "C:\Users\manbu\OneDrive\Desktop\EMIS V2\emis-phase1"
#   ./push-to-github.ps1
#
# When prompted by git for credentials, use your GitHub username and a
# personal access token (https://github.com/settings/tokens) with `repo` scope.

$ErrorActionPreference = "Stop"

# Run a git command line and abort if it fails. Using a param-less function
# so PowerShell doesn't try to bind switches like -A as parameters of this
# wrapper — every token goes verbatim to git via the automatic $args.
function Invoke-Git {
    & git @args
    if ($LASTEXITCODE -ne 0) {
        throw "git $($args -join ' ') failed with exit code $LASTEXITCODE"
    }
}

$RepoUrl = "https://github.com/Manbundan-SVG/EMIS-V2.git"
$Branch  = "main"

# Pin CWD to where this script lives, regardless of how it was invoked.
# Also keeps .NET's CurrentDirectory in sync with PowerShell's $PWD.
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location -LiteralPath $ScriptDir
[System.Environment]::CurrentDirectory = $ScriptDir
Write-Host ("Working directory: " + $ScriptDir) -ForegroundColor Cyan

# Detect stale/corrupt .git left over from earlier failed runs and reset.
$gitConfig = Join-Path $ScriptDir ".git/config"
if (Test-Path -LiteralPath ".git") {
    $needsReinit = $false
    if (-not (Test-Path -LiteralPath $gitConfig)) {
        $needsReinit = $true
    } else {
        $cfgText = Get-Content -LiteralPath $gitConfig -Raw -ErrorAction SilentlyContinue
        if ([string]::IsNullOrWhiteSpace($cfgText)) { $needsReinit = $true }
    }
    if ($needsReinit) {
        Write-Host "WARNING: .git is empty or unreadable. Re-initializing." -ForegroundColor Yellow
        Remove-Item -Recurse -Force -LiteralPath ".git"
    }
}

if (-not (Test-Path ".git")) {
    Write-Host "Initializing git repo..." -ForegroundColor Cyan
    Invoke-Git init -b $Branch
}

# Windows long-path support + tame CRLF noise. Both are repo-local.
Invoke-Git config core.longpaths true
Invoke-Git config core.autocrlf  false
Invoke-Git config core.safecrlf  false

# Local user identity (avoids the global-config OneDrive permission trap).
Invoke-Git config user.email "chickendan5423@gmail.com"
Invoke-Git config user.name  "Manbundan-SVG"

# .npm-cache holds an npx scratch space whose paths blow past Windows
# MAX_PATH; it's not source code and is in .gitignore anyway. Remove it
# before staging so traversal can't trip over it.
if (Test-Path -LiteralPath ".npm-cache") {
    Write-Host "Removing .npm-cache (ephemeral, breaks Windows MAX_PATH)..." -ForegroundColor Yellow
    cmd /c "rmdir /s /q .npm-cache" | Out-Null
}

# Drop any partially-staged garbage from a previous failed run so we get a
# clean stage every time. Soft reset — does not touch the working tree.
& git reset 2>&1 | Out-Null

# Remote
$existing = git remote 2>$null
if ($existing -contains "origin") {
    Invoke-Git remote set-url origin $RepoUrl
} else {
    Invoke-Git remote add origin $RepoUrl
}
Write-Host ("Remote: " + (git remote get-url origin)) -ForegroundColor Cyan

# Stage everything — .gitignore in the same dir excludes node_modules, etc.
# If `git add` fails for ANY reason we abort instead of silently committing
# a partial tree.
Write-Host "Staging changes (this may take a moment)..." -ForegroundColor Cyan
Invoke-Git add -A

# Commit only if there are staged changes.
$status = git status --porcelain
if (-not [string]::IsNullOrWhiteSpace($status)) {
    $msg = "Phase 4.7A -> 4.8B: signal decay, decay-aware attribution/composite/replay validation, cross-layer conflict diagnostics, conflict-aware attribution"
    Invoke-Git commit -m $msg
    Write-Host "Committed pending changes." -ForegroundColor Green
} else {
    Write-Host "No pending changes to commit." -ForegroundColor Yellow
}

Write-Host ("Pushing to origin/" + $Branch + " (auth prompt expected on first run)...") -ForegroundColor Cyan
& git push -u origin $Branch
if ($LASTEXITCODE -ne 0) {
    Write-Host "Normal push rejected (remote has unrelated history). Retrying with --force-with-lease..." -ForegroundColor Yellow
    & git push -u --force-with-lease origin $Branch
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Force-with-lease push also failed. As a last resort try:" -ForegroundColor Yellow
        Write-Host "  git push -u --force origin $Branch" -ForegroundColor Yellow
        throw "git push failed with exit code $LASTEXITCODE"
    }
}

Write-Host "Done." -ForegroundColor Green
