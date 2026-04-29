# Phase 1.1 live data wiring pack

## Included
- Supabase admin client modules for server-side reads
- typed query modules for watchlists, signals, and composite snapshots
- worker-side DB repositories and upsert writers
- first regime-conditioned composite score calculator
- demo seed script that populates market data and runs one full recompute

## Vertical slice
1. Seed demo market + macro data
2. Worker computes features
3. Worker derives signal values
4. Worker writes composite scores
5. Next.js dashboard reads latest composites from Supabase

## Main seams kept explicit
- no RLS policies yet
- no durable queue table yet
- no Realtime broadcast yet
- no auth-scoped workspace selection yet
