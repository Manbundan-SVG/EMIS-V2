"use client";

import type {
  CrossAssetDecayReplayValidationSummaryRow,
  CrossAssetFamilyDecayReplayStabilitySummaryRow,
  CrossAssetDecayReplayStabilityAggregateRow,
} from "@/lib/queries/metrics";

type Props = {
  decayReplayValidationSummary: CrossAssetDecayReplayValidationSummaryRow[];
  familyDecayReplayStabilitySummary: CrossAssetFamilyDecayReplayStabilitySummaryRow[];
  decayReplayStabilityAggregate: CrossAssetDecayReplayStabilityAggregateRow | null;
  loading: boolean;
};

const VALIDATION_STATE_BADGE: Record<string, string> = {
  validated:            "badge-green",
  drift_detected:       "badge-red",
  insufficient_source:  "badge-muted",
  insufficient_replay:  "badge-muted",
  context_mismatch:     "badge-yellow",
  timing_mismatch:      "badge-yellow",
  transition_mismatch:  "badge-yellow",
  archetype_mismatch:   "badge-yellow",
  cluster_mismatch:     "badge-yellow",
  persistence_mismatch: "badge-yellow",
  decay_mismatch:       "badge-red",
};

const FAMILY_BADGE: Record<string, string> = {
  macro:        "badge-muted",
  fx:           "badge-yellow",
  rates:        "badge-muted",
  equity_index: "badge-green",
  commodity:    "badge-muted",
  crypto_cross: "badge-green",
  risk:         "badge-yellow",
};

function fmtTs(ts: string | null | undefined): string {
  if (!ts) return "—";
  return new Date(ts).toLocaleString(undefined, { dateStyle: "short", timeStyle: "short" });
}

function fmtScore(value: number | string | null | undefined, digits = 4): string {
  if (value === null || value === undefined) return "—";
  const n = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(n)) return "—";
  return n.toFixed(digits);
}

function fmtRatio(value: number | string | null | undefined): string {
  if (value === null || value === undefined) return "—";
  const n = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(n)) return "—";
  return `${(n * 100).toFixed(1)}%`;
}

function ratioBadge(value: number | string | null | undefined): string {
  if (value === null || value === undefined) return "badge-muted";
  const n = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(n)) return "badge-muted";
  if (n >= 0.95) return "badge-green";
  if (n >= 0.80) return "badge-yellow";
  return "badge-red";
}

function matchBadge(match: boolean): string {
  return match ? "badge-green" : "badge-red";
}

function shortId(id: string | null | undefined): string {
  if (!id) return "—";
  return `${id.slice(0, 8)}…`;
}

function reasonList(codes: string[] | null | undefined, maxShown = 4): string {
  if (!codes || codes.length === 0) return "—";
  if (codes.length <= maxShown) return codes.join(", ");
  return `${codes.slice(0, maxShown).join(", ")} +${codes.length - maxShown}`;
}

export function CrossAssetDecayReplayValidationPanel({
  decayReplayValidationSummary,
  familyDecayReplayStabilitySummary,
  decayReplayStabilityAggregate,
  loading,
}: Props) {
  return (
    <section className="section">
      <div className="panel-header">
        <div>
          <h2>Decay Replay Validation</h2>
          <p className="panel-subtitle">
            Phase 4.7D. Compares source vs replay runs across context, regime, timing, transition state,
            sequence class, archetype, cluster, persistence, memory, freshness, aggregate decay score,
            stale-memory flag, contradiction flag, decay-aware attribution, decay-aware composite, and decay
            dominant family. Tolerances: 1e-9 for contributions, 1e-6 for memory and decay scores. Diagnostic only.
          </p>
        </div>
      </div>

      {loading && <p className="muted">Loading decay replay validation…</p>}

      {!loading && (
        <>
          {/* ── Decay Replay Validation Summary ────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Decay Replay Validation Summary</h3>
                <p className="panel-subtitle">Latest validation row per (source, replay) pair.</p>
              </div>
            </div>
            {decayReplayValidationSummary.length === 0 ? (
              <p className="muted">No decay replay validations yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Source</th>
                    <th>Replay</th>
                    <th>Validation</th>
                    <th>Context</th>
                    <th>Regime</th>
                    <th>Timing</th>
                    <th>Transition</th>
                    <th>Archetype</th>
                    <th>Cluster</th>
                    <th>Persistence</th>
                    <th>Memory</th>
                    <th>Freshness</th>
                    <th>Aggregate</th>
                    <th>Stale</th>
                    <th>Contradiction</th>
                    <th>Decay attr</th>
                    <th>Decay comp</th>
                    <th>Decay dom</th>
                    <th>Drift</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {decayReplayValidationSummary.map((row, idx) => (
                    <tr key={`${row.source_run_id}:${row.replay_run_id}:${idx}`}>
                      <td className="mono-cell text-sm">{shortId(row.source_run_id)}</td>
                      <td className="mono-cell text-sm">{shortId(row.replay_run_id)}</td>
                      <td>
                        <span className={VALIDATION_STATE_BADGE[row.validation_state] ?? "badge-muted"}>
                          {row.validation_state}
                        </span>
                      </td>
                      <td><span className={matchBadge(row.context_hash_match)}>{row.context_hash_match ? "✓" : "✗"}</span></td>
                      <td><span className={matchBadge(row.regime_match)}>{row.regime_match ? "✓" : "✗"}</span></td>
                      <td><span className={matchBadge(row.timing_class_match)}>{row.timing_class_match ? "✓" : "✗"}</span></td>
                      <td><span className={matchBadge(row.transition_state_match)}>{row.transition_state_match ? "✓" : "✗"}</span></td>
                      <td><span className={matchBadge(row.archetype_match)}>{row.archetype_match ? "✓" : "✗"}</span></td>
                      <td><span className={matchBadge(row.cluster_state_match)}>{row.cluster_state_match ? "✓" : "✗"}</span></td>
                      <td><span className={matchBadge(row.persistence_state_match)}>{row.persistence_state_match ? "✓" : "✗"}</span></td>
                      <td><span className={matchBadge(row.memory_score_match)}>{row.memory_score_match ? "✓" : "✗"}</span></td>
                      <td><span className={matchBadge(row.freshness_state_match)}>{row.freshness_state_match ? "✓" : "✗"}</span></td>
                      <td><span className={matchBadge(row.aggregate_decay_score_match)}>{row.aggregate_decay_score_match ? "✓" : "✗"}</span></td>
                      <td><span className={matchBadge(row.stale_memory_flag_match)}>{row.stale_memory_flag_match ? "✓" : "✗"}</span></td>
                      <td><span className={matchBadge(row.contradiction_flag_match)}>{row.contradiction_flag_match ? "✓" : "✗"}</span></td>
                      <td><span className={matchBadge(row.decay_attribution_match)}>{row.decay_attribution_match ? "✓" : "✗"}</span></td>
                      <td><span className={matchBadge(row.decay_composite_match)}>{row.decay_composite_match ? "✓" : "✗"}</span></td>
                      <td><span className={matchBadge(row.decay_dominant_family_match)}>{row.decay_dominant_family_match ? "✓" : "✗"}</span></td>
                      <td className="mono-cell text-sm">{reasonList(row.drift_reason_codes)}</td>
                      <td className="text-sm muted">{fmtTs(row.created_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* ── Family Decay Replay Stability ──────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Family Decay Replay Stability</h3>
                <p className="panel-subtitle">Per-family deltas across replay pairs.</p>
              </div>
            </div>
            {familyDecayReplayStabilitySummary.length === 0 ? (
              <p className="muted">No family decay stability rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Source</th>
                    <th>Replay</th>
                    <th>Family</th>
                    <th>Adj Δ</th>
                    <th>Int Δ</th>
                    <th>Freshness</th>
                    <th>Aggregate decay</th>
                    <th>Family decay</th>
                    <th>Stale</th>
                    <th>Contradiction</th>
                    <th>Attr rank</th>
                    <th>Comp rank</th>
                    <th>Drift</th>
                  </tr>
                </thead>
                <tbody>
                  {familyDecayReplayStabilitySummary.map((row, idx) => (
                    <tr key={`${row.source_run_id}:${row.replay_run_id}:${row.dependency_family}:${idx}`}>
                      <td className="mono-cell text-sm">{shortId(row.source_run_id)}</td>
                      <td className="mono-cell text-sm">{shortId(row.replay_run_id)}</td>
                      <td>
                        <span className={FAMILY_BADGE[row.dependency_family] ?? "badge-muted"}>
                          {row.dependency_family}
                        </span>
                      </td>
                      <td className="text-sm">{fmtScore(row.decay_adjusted_delta)}</td>
                      <td className="text-sm">{fmtScore(row.decay_integration_delta)}</td>
                      <td><span className={matchBadge(row.freshness_state_match)}>{row.freshness_state_match ? "✓" : "✗"}</span></td>
                      <td><span className={matchBadge(row.aggregate_decay_score_match)}>{row.aggregate_decay_score_match ? "✓" : "✗"}</span></td>
                      <td><span className={matchBadge(row.family_decay_score_match)}>{row.family_decay_score_match ? "✓" : "✗"}</span></td>
                      <td><span className={matchBadge(row.stale_memory_flag_match)}>{row.stale_memory_flag_match ? "✓" : "✗"}</span></td>
                      <td><span className={matchBadge(row.contradiction_flag_match)}>{row.contradiction_flag_match ? "✓" : "✗"}</span></td>
                      <td><span className={matchBadge(row.decay_family_rank_match)}>{row.decay_family_rank_match ? "✓" : "✗"}</span></td>
                      <td><span className={matchBadge(row.decay_composite_family_rank_match)}>{row.decay_composite_family_rank_match ? "✓" : "✗"}</span></td>
                      <td className="mono-cell text-sm">{reasonList(row.drift_reason_codes)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* ── Decay Replay Stability Aggregate ───────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Decay Replay Stability Aggregate</h3>
                <p className="panel-subtitle">Workspace-level match-rate rollup across all decay replay validations.</p>
              </div>
            </div>
            {!decayReplayStabilityAggregate ? (
              <p className="muted">No aggregate yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Validations</th>
                    <th>Context</th>
                    <th>Regime</th>
                    <th>Timing</th>
                    <th>Transition</th>
                    <th>Archetype</th>
                    <th>Cluster</th>
                    <th>Persistence</th>
                    <th>Memory</th>
                    <th>Freshness</th>
                    <th>Aggregate</th>
                    <th>Stale</th>
                    <th>Contradiction</th>
                    <th>Decay attr</th>
                    <th>Decay comp</th>
                    <th>Decay dom</th>
                    <th>Drift count</th>
                    <th>Latest</th>
                  </tr>
                </thead>
                <tbody>
                  <tr>
                    <td className="text-sm">{decayReplayStabilityAggregate.validation_count}</td>
                    <td><span className={ratioBadge(decayReplayStabilityAggregate.context_match_rate)}>{fmtRatio(decayReplayStabilityAggregate.context_match_rate)}</span></td>
                    <td><span className={ratioBadge(decayReplayStabilityAggregate.regime_match_rate)}>{fmtRatio(decayReplayStabilityAggregate.regime_match_rate)}</span></td>
                    <td><span className={ratioBadge(decayReplayStabilityAggregate.timing_class_match_rate)}>{fmtRatio(decayReplayStabilityAggregate.timing_class_match_rate)}</span></td>
                    <td><span className={ratioBadge(decayReplayStabilityAggregate.transition_state_match_rate)}>{fmtRatio(decayReplayStabilityAggregate.transition_state_match_rate)}</span></td>
                    <td><span className={ratioBadge(decayReplayStabilityAggregate.archetype_match_rate)}>{fmtRatio(decayReplayStabilityAggregate.archetype_match_rate)}</span></td>
                    <td><span className={ratioBadge(decayReplayStabilityAggregate.cluster_state_match_rate)}>{fmtRatio(decayReplayStabilityAggregate.cluster_state_match_rate)}</span></td>
                    <td><span className={ratioBadge(decayReplayStabilityAggregate.persistence_state_match_rate)}>{fmtRatio(decayReplayStabilityAggregate.persistence_state_match_rate)}</span></td>
                    <td><span className={ratioBadge(decayReplayStabilityAggregate.memory_score_match_rate)}>{fmtRatio(decayReplayStabilityAggregate.memory_score_match_rate)}</span></td>
                    <td><span className={ratioBadge(decayReplayStabilityAggregate.freshness_state_match_rate)}>{fmtRatio(decayReplayStabilityAggregate.freshness_state_match_rate)}</span></td>
                    <td><span className={ratioBadge(decayReplayStabilityAggregate.aggregate_decay_score_match_rate)}>{fmtRatio(decayReplayStabilityAggregate.aggregate_decay_score_match_rate)}</span></td>
                    <td><span className={ratioBadge(decayReplayStabilityAggregate.stale_memory_flag_match_rate)}>{fmtRatio(decayReplayStabilityAggregate.stale_memory_flag_match_rate)}</span></td>
                    <td><span className={ratioBadge(decayReplayStabilityAggregate.contradiction_flag_match_rate)}>{fmtRatio(decayReplayStabilityAggregate.contradiction_flag_match_rate)}</span></td>
                    <td><span className={ratioBadge(decayReplayStabilityAggregate.decay_attribution_match_rate)}>{fmtRatio(decayReplayStabilityAggregate.decay_attribution_match_rate)}</span></td>
                    <td><span className={ratioBadge(decayReplayStabilityAggregate.decay_composite_match_rate)}>{fmtRatio(decayReplayStabilityAggregate.decay_composite_match_rate)}</span></td>
                    <td><span className={ratioBadge(decayReplayStabilityAggregate.decay_dominant_family_match_rate)}>{fmtRatio(decayReplayStabilityAggregate.decay_dominant_family_match_rate)}</span></td>
                    <td className="text-sm">{decayReplayStabilityAggregate.drift_detected_count}</td>
                    <td className="text-sm muted">{fmtTs(decayReplayStabilityAggregate.latest_validated_at)}</td>
                  </tr>
                </tbody>
              </table>
            )}
          </div>
        </>
      )}
    </section>
  );
}
