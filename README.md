# EMIS Phase 1 scaffold

Phase 1.1 turns the original scaffold into a live vertical slice for Supabase + Vercel.

## Included
- Next.js dashboard shell on `apps/web`
- Python worker on `apps/worker`
- shared types in `packages/types`
- signal registry in `packages/signal-registry`
- Supabase schema in `supabase/migrations/0001_phase1_schema.sql`
- live data wiring pack in `docs/architecture/phase1-live-data-pack.md`

## Environment

### Web
- `NEXT_PUBLIC_SUPABASE_URL`
- `NEXT_PUBLIC_SUPABASE_ANON_KEY`
- `SUPABASE_SERVICE_ROLE_KEY`
- `EMIS_DEFAULT_WORKSPACE_SLUG=default`

### Worker
- `DATABASE_URL`
- `EMIS_DEFAULT_WORKSPACE_SLUG=default`
- `EMIS_SOURCE_NAME=demo`
- `EMIS_DEFAULT_TIMEFRAME=1h`
- `WORKER_POLL_INTERVAL_SECONDS=30`

## Runtime hardening

- Use `Node 22` locally. The repo engine range is `>=20.9.0 <25`, and Node `25.x` has produced unstable Next runtime behavior on this machine.
- If the web app starts throwing missing chunk errors from `apps/web/.next/server/webpack-runtime.js`, treat that as stale build output first, not a feature regression.
- The repo now supports a safer Windows build path by keeping `apps/web/.next` repo-local while turning `apps/web/.next/cache` into a junction backed by a repo-specific cache under `%LOCALAPPDATA%\\EMIS\\cache\\...`, which keeps transient cache artifacts out of OneDrive sync pressure without forcing the source repo and the run-safe mirror to fight over the same cache directory.
- Known-good local startup paths:
  - prepare the safe cache path: `npm run web:prepare:cache`
  - foreground only: `npm run web:start:3002`
  - clean rebuild + start: `npm run web:reset:start`
  - OneDrive-safe mirror run: `npm run web:safe:reset:start`
- `web:safe:reset:start` mirrors the repo into `%LOCALAPPDATA%\EMIS-run\emis-phase1`, refreshes dependencies there, and runs the clean web startup from the mirror. If Windows still throws a `spawn EPERM` during `next build`, the launcher now falls back to `next dev` automatically instead of failing cold.
- If you want the fallback path explicitly, run `npm run web:safe:dev:start`.
- Until the Windows detached launcher path is hardened, prefer a foreground terminal run for `apps/web`.

## Demo flow
1. Apply the migration.
2. Set environment variables.
3. Run the seed script:
   - `pnpm seed:demo`
4. Start the worker for ongoing recomputes:
   - `python apps/worker/src/main.py`
5. Start the web app:
   - `pnpm dev`

## API routes
- `GET /api/watchlists`
- `GET /api/signals`
- `GET /api/composites`
- `POST /api/recompute` (still placeholder for queue insertion)
