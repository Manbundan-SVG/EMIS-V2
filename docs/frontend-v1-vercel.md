# Frontend V1.0 — Vercel deployment

The Next.js app under `apps/web` is the EMIS Intelligence Console. V1.0 deploys it as a read-only window over the existing Supabase data — no write paths, no auth changes, no execution surfaces.

## What V1.0 ships

- **`/ops`** — new Intelligence Console (summary-first, tabbed).
- **`/ops/legacy`** — the previous panel-dump dashboard, preserved for power users.
- **`/api/metrics/ops-intelligence`** — server route that aggregates the executive-summary card data into one fetch.
- **Tabs:** Overview (Executive Summary), Attribution (ladder visualization), and stubs linking to `/ops/legacy` for Data Health, Signals, Composite, Replay, and Conflict & Decay (V1.1 will flesh these out).

## Vercel project setup

In the Vercel dashboard for the new project:

| Setting | Value |
|---|---|
| Framework Preset | Next.js |
| Root Directory | `apps/web` |
| Build Command | `pnpm build` |
| Install Command | `pnpm install --frozen-lockfile` |
| Output Directory | (leave default) |
| Node version | 22 |

The repo's `vercel.json` at the root sets `framework`, `installCommand`, and `buildCommand`, but Vercel still needs the **Root Directory** set to `apps/web` in its UI for monorepo detection.

## Required environment variables

Set these in **Project → Settings → Environment Variables** (Production + Preview + Development as appropriate):

| Variable | Scope | Purpose |
|---|---|---|
| `NEXT_PUBLIC_SUPABASE_URL` | Public | Supabase project URL — read by browser bundle |
| `NEXT_PUBLIC_SUPABASE_ANON_KEY` | Public | Anon key for Supabase client SDK |
| `SUPABASE_SERVICE_ROLE_KEY` | Server-only | Service role key — only used inside `app/api/*` routes for privileged reads (executive summary aggregator needs this) |
| `NEXT_PUBLIC_APP_ENV` | Public | One of `dev` / `preview` / `prod`; available to client code for environment-aware UI |
| `NEXT_PUBLIC_DEFAULT_WORKSPACE_SLUG` | Public | Default workspace shown when `?workspace=` is not in the URL (default in code: `demo`) |

`SUPABASE_SERVICE_ROLE_KEY` should **not** be marked public. The lib/queries/* code that uses it (`createServiceSupabaseClient`) is only ever called server-side.

## Smoke test after deploy

1. Visit `https://<your-project>.vercel.app/ops` — should render the Intelligence Console with workspace `demo` (or your default).
2. If no runs have been generated yet for that workspace, you'll see "Awaiting first run" — that's expected.
3. Click **Attribution** tab — should render the attribution ladder for the latest run.
4. Click **Full Dashboard →** in the header — should land at `/ops/legacy` and load every cross-asset panel as before.
5. Network tab should show one `GET /api/metrics/ops-intelligence?workspace=...` returning `200 { ok: true, intelligence: {...} }`.

## V1.1 backlog (next session)

- **Run selector** — pick any past run, not just the latest.
- **Run inspector** — `/ops/runs/[runId]` deep-dive page with full attribution + composite ladders, replay diagnostics, and the explanation chain.
- **Replay health tab** — surface the 4.8D aggregate match rates as their own tab.
- **Conflict & Decay tab** — combined view of 4.7A freshness diagnostics + 4.8A layer-conflict events.
- **Data Health tab** — worker heartbeats, queue depth, sync state, SLA — pulled from the existing panels.
- **Signals tab** — cross-asset signal summary plus explainability bridge.
- **Composite tab** — composite ladder (separate from attribution ladder) with composite-pre/post deltas per layer.

## Constraints honored

- Read-only — no write paths, no execution surfaces.
- No new dependencies — uses existing `@supabase/supabase-js`, no chart library, no new UI framework.
- Repo-native styling — extends `globals.css` with a small set of color-badge / table classes that the existing cross-asset panels already reference.
- No auth redesign — workspace is selected via URL parameter as the existing app does.
- `/ops/legacy` preserves the previous experience for power users; the V1 console is the new default at `/ops`.
