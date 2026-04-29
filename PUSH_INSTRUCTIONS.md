# Pushing to GitHub

The Cowork sandbox blocks outbound HTTPS to github.com (its proxy returns
HTTP 403 for everything except api.anthropic.com), so the push has to happen
from your local machine.

Everything you need is already in this folder:

- `.gitignore` — excludes `node_modules`, `.next`, `.env*`, OneDrive crash dumps, etc.
- `push-to-github.ps1` — one-shot init + commit + push script.

## Run

Open PowerShell and run:

```powershell
cd "C:\Users\manbu\OneDrive\Desktop\EMIS V2\emis-phase1"
./push-to-github.ps1
```

The first time, git will prompt for GitHub credentials. Use your GitHub
username and a personal access token (https://github.com/settings/tokens)
with `repo` scope as the password.

## What gets pushed

The script will:

1. `git init -b main` (only if there's no `.git` already)
2. `git remote add origin https://github.com/Manbundan-SVG/EMIS.git`
3. `git add -A` (respects `.gitignore`)
4. Commit with the Phase 4.7A → 4.8B message
5. `git push -u origin main`

If the remote already has commits and you want to force-overwrite, run:

```powershell
git push -u --force-with-lease origin main
```

## Troubleshooting

If you see `bad config line 1 in file .git/config` or other empty/null-byte
errors from git inside this folder, OneDrive has corrupted the local `.git`
metadata. Delete the `.git` directory and re-run the script:

```powershell
Remove-Item -Recurse -Force .git
./push-to-github.ps1
```

The script also auto-detects this case and re-initializes for you.
