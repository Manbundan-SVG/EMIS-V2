// Phase Frontend V1.0 — Ops Intelligence Console
//
// Server-side aggregator: pulls the minimum data required for the V1
// executive summary and attribution-ladder views. Reads from existing
// summary views — does not duplicate per-layer query logic.

import { createServiceSupabaseClient } from "@/lib/supabase";
import type { OpsIntelligence } from "@/lib/types/ops-intelligence";

function _num(v: number | string | null | undefined): number | null {
  if (v === null || v === undefined) return null;
  const n = typeof v === "number" ? v : Number(v);
  return Number.isFinite(n) ? n : null;
}

function _str(v: string | null | undefined): string | null {
  return v ?? null;
}

function _bool(v: boolean | null | undefined): boolean | null {
  return v ?? null;
}

async function _resolveWorkspaceId(supabase: ReturnType<typeof createServiceSupabaseClient>, slug: string) {
  type R = { data: { id: string } | null; error: { message: string } | null };
  const r = await supabase.from("workspaces").select("id").eq("slug", slug).single() as unknown as R;
  if (r.error || !r.data) throw new Error(`Workspace not found: ${slug}`);
  return r.data.id;
}

export async function getOpsIntelligence(
  workspaceSlug: string,
  watchlistSlug?: string,
): Promise<OpsIntelligence | null> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await _resolveWorkspaceId(supabase, workspaceSlug);

  // Resolve watchlist if provided.
  let watchlistId: string | null = null;
  if (watchlistSlug) {
    type WlResult = { data: { id: string } | null; error: { message: string } | null };
    const wl = await supabase
      .from("watchlists")
      .select("id")
      .eq("workspace_id", workspaceId)
      .eq("slug", watchlistSlug)
      .single() as unknown as WlResult;
    if (wl.error || !wl.data) throw new Error(`Watchlist not found: ${watchlistSlug}`);
    watchlistId = wl.data.id;
  }

  // === Pull the run-level conflict integration summary (4.8C view).
  // This single view carries the entire attribution ladder, the
  // post-conflict composite, the consensus state, agreement/conflict
  // scores, dominant source, integration mode, source-layer metadata,
  // and a per-layer dominant-family chain. Most of the executive
  // summary comes from this row.
  let conflictRunQuery = supabase
    .from("run_cross_asset_conflict_integration_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(1);
  if (watchlistId) conflictRunQuery = conflictRunQuery.eq("watchlist_id", watchlistId);

  const conflictRun = await conflictRunQuery;
  if (conflictRun.error) throw new Error(`run_cross_asset_conflict_integration_summary: ${conflictRun.error.message}`);
  const ciRow = (conflictRun.data ?? [])[0] as Record<string, unknown> | undefined;

  // No data yet — return a near-empty shell so the UI can render an
  // "awaiting first run" state without crashing.
  if (!ciRow) {
    return {
      workspaceId,
      workspaceSlug,
      watchlistId,
      latestRunId: null,
      latestRunCreatedAt: null,
      cards: {
        composite: { preConflict: null, netContribution: null, postConflict: null },
        layerConsensus: { state: null, agreementScore: null },
        conflict: { score: null, dominantSource: null },
        freshness: { state: null, aggregateDecayScore: null, staleMemoryFlag: null, contradictionFlag: null },
        persistence: { state: null },
        replay: {
          validationCount: null, driftDetectedCount: null,
          contextMatchRate: null, conflictAttributionMatchRate: null, conflictCompositeMatchRate: null,
          latestValidatedAt: null,
        },
        dominantFamily: {
          raw: null, weighted: null, regime: null, timing: null, transition: null,
          archetype: null, cluster: null, persistence: null, decay: null, conflict: null,
        },
        latestEvent: { type: null, family: null, ts: null },
      },
      attributionLadder: {
        raw: null, weighted: null, regime: null, timing: null, transition: null,
        archetype: null, cluster: null, persistence: null, decay: null, conflict: null,
        compositePre: null, compositePost: null,
      },
      metadata: {
        scoringVersion: null, integrationMode: null,
        sourceContributionLayer: null, sourceCompositeLayer: null,
      },
    };
  }

  const latestRunId = String(ciRow.run_id);
  const latestRunWatchlist = String(ciRow.watchlist_id);

  // === Freshness + persistence card — pull from 4.7A run summary.
  const decayRunQuery = supabase
    .from("cross_asset_signal_decay_summary")
    .select("freshness_state, aggregate_decay_score, stale_memory_flag, contradiction_flag, persistence_state")
    .eq("run_id", latestRunId)
    .limit(1);
  const decayRun = await decayRunQuery;
  const decayRow = !decayRun.error ? ((decayRun.data ?? [])[0] as Record<string, unknown> | undefined) : undefined;

  // === Replay aggregate (4.8D).
  const replayAggQuery = supabase
    .from("cross_asset_conflict_replay_stability_aggregate")
    .select("*")
    .eq("workspace_id", workspaceId)
    .limit(1);
  const replayAgg = await replayAggQuery;
  const aggRow = !replayAgg.error
    ? ((replayAgg.data ?? [])[0] as Record<string, unknown> | undefined)
    : undefined;

  // === Latest layer-conflict event (4.8A) — best-effort.
  const eventQuery = supabase
    .from("cross_asset_layer_conflict_event_summary")
    .select("event_type, dependency_family, created_at")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(1);
  const eventRes = await eventQuery;
  const eventRow = !eventRes.error
    ? ((eventRes.data ?? [])[0] as Record<string, unknown> | undefined)
    : undefined;

  return {
    workspaceId,
    workspaceSlug,
    watchlistId: watchlistId ?? latestRunWatchlist,
    latestRunId,
    latestRunCreatedAt: _str(ciRow.created_at as string | null),
    cards: {
      composite: {
        preConflict:    _num(ciRow.composite_pre_conflict as number | string | null),
        netContribution:_num(ciRow.conflict_net_contribution as number | string | null),
        postConflict:   _num(ciRow.composite_post_conflict as number | string | null),
      },
      layerConsensus: {
        state:          _str(ciRow.layer_consensus_state as string | null),
        agreementScore: _num(ciRow.agreement_score as number | string | null),
      },
      conflict: {
        score:          _num(ciRow.conflict_score as number | string | null),
        dominantSource: _str(ciRow.dominant_conflict_source as string | null),
      },
      freshness: {
        state:                 _str((decayRow?.freshness_state as string | null) ?? null),
        aggregateDecayScore:   _num((decayRow?.aggregate_decay_score as number | string | null) ?? null),
        staleMemoryFlag:       _bool((decayRow?.stale_memory_flag as boolean | null) ?? null),
        contradictionFlag:     _bool((decayRow?.contradiction_flag as boolean | null) ?? null),
      },
      persistence: {
        state: _str((decayRow?.persistence_state as string | null) ?? null),
      },
      replay: {
        validationCount:                _num((aggRow?.validation_count as number | string | null) ?? null),
        driftDetectedCount:             _num((aggRow?.drift_detected_count as number | string | null) ?? null),
        contextMatchRate:               _num((aggRow?.context_match_rate as number | string | null) ?? null),
        conflictAttributionMatchRate:   _num((aggRow?.conflict_attribution_match_rate as number | string | null) ?? null),
        conflictCompositeMatchRate:     _num((aggRow?.conflict_composite_match_rate as number | string | null) ?? null),
        latestValidatedAt:              _str((aggRow?.latest_validated_at as string | null) ?? null),
      },
      dominantFamily: {
        raw:         _str(ciRow.dominant_dependency_family as string | null),
        weighted:    _str(ciRow.weighted_dominant_dependency_family as string | null),
        regime:      _str(ciRow.regime_dominant_dependency_family as string | null),
        timing:      _str(ciRow.timing_dominant_dependency_family as string | null),
        transition:  _str(ciRow.transition_dominant_dependency_family as string | null),
        archetype:   _str(ciRow.archetype_dominant_dependency_family as string | null),
        cluster:     _str(ciRow.cluster_dominant_dependency_family as string | null),
        persistence: _str(ciRow.persistence_dominant_dependency_family as string | null),
        decay:       _str(ciRow.decay_dominant_dependency_family as string | null),
        conflict:    _str(ciRow.conflict_dominant_dependency_family as string | null),
      },
      latestEvent: {
        type:   _str((eventRow?.event_type as string | null) ?? null),
        family: _str((eventRow?.dependency_family as string | null) ?? null),
        ts:     _str((eventRow?.created_at as string | null) ?? null),
      },
    },
    attributionLadder: {
      raw:         _num(ciRow.cross_asset_net_contribution as number | string | null),
      weighted:    _num(ciRow.weighted_cross_asset_net_contribution as number | string | null),
      regime:      _num(ciRow.regime_adjusted_cross_asset_contribution as number | string | null),
      timing:      _num(ciRow.timing_adjusted_cross_asset_contribution as number | string | null),
      transition:  _num(ciRow.transition_adjusted_cross_asset_contribution as number | string | null),
      archetype:   _num(ciRow.archetype_adjusted_cross_asset_contribution as number | string | null),
      cluster:     _num(ciRow.cluster_adjusted_cross_asset_contribution as number | string | null),
      persistence: _num(ciRow.persistence_adjusted_cross_asset_contribution as number | string | null),
      decay:       _num(ciRow.decay_adjusted_cross_asset_contribution as number | string | null),
      conflict:    _num(ciRow.conflict_adjusted_cross_asset_contribution as number | string | null),
      compositePre:  _num(ciRow.composite_pre_conflict as number | string | null),
      compositePost: _num(ciRow.composite_post_conflict as number | string | null),
    },
    metadata: {
      scoringVersion:          _str(ciRow.scoring_version as string | null),
      integrationMode:         _str(ciRow.integration_mode as string | null),
      sourceContributionLayer: _str(ciRow.source_contribution_layer as string | null),
      sourceCompositeLayer:    _str(ciRow.source_composite_layer as string | null),
    },
  };
}
