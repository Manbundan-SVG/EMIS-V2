# EMIS Phase 1 architecture

## Goal
Ship a narrow but production-shaped EMIS slice on Supabase + Vercel.

## Included
- canonical assets and watchlists
- normalized market data tables
- feature and signal persistence
- first signal registry
- Next.js dashboard shell
- worker recompute loop skeleton

## Not included yet
- queue integration
- realtime channels
- auth policies and RLS
- backfill orchestration
- options and sentiment pipelines
- cross-asset graph
- regime engine beyond simple regime labels

## Read path
1. UI requests dashboard payloads from Vercel routes.
2. Routes read current watchlists, signal registry, and latest composite scores.
3. UI renders watchlist state and signal explanations.

## Write path
1. External ingestion writes normalized market data to Supabase.
2. Worker recomputes Phase 1 features.
3. Worker writes feature values, signal values, and composite scores.
4. Realtime broadcasting can be added in Phase 2.

## Design rules
- Raw provider data should not be queried directly by the UI.
- Features and signals must be versionable.
- Asset identifiers should remain stable across all tables.
- Explanations are stored as JSON for dashboard and alert rendering.
