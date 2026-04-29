"use client";

import type {
  CrossAssetClusterReplayValidationSummaryRow,
  CrossAssetFamilyClusterReplayStabilitySummaryRow,
  CrossAssetClusterReplayStabilityAggregateRow,
} from "@/lib/queries/metrics";

type Props = {
  clusterReplayValidationSummary: CrossAssetClusterReplayValidationSummaryRow[];
  familyClusterReplayStabilitySummary: CrossAssetFamilyClusterReplayStabilitySummaryRow[];
  clusterReplayStabilityAggregate: CrossAssetClusterReplayStabilityAggregateRow | null;
  loading: boolean;
};

const FAMILY_BADGE: Record<string, string> = {
  macro: "badge-muted",
  fx: "badge-yellow",
  rates: "badge-muted",
  equity_index: "badge-green",
  commodity: "badge-muted",
  crypto_cross: "badge-green",
  risk: "badge-yellow",
};

const ARCHETYPE_BADGE: Record<string, string> = {
  rotation_handoff:         "badge-yellow",
  reinforcing_continuation: "badge-green",
  recovering_reentry:       "badge-green",
  deteriorating_breakdown:  "badge-red",
  mixed_transition_noise:   "badge-muted",
  insufficient_history:     "badge-muted",
};

const CLUSTER_STATE_BADGE: Record<string, string> = {
  stable:               "badge-green",
  rotating:             "badge-yellow",
  recovering:           "badge-green",
  deteriorating:        "badge-red",
  mixed:                "badge-muted",
  insufficient_history: "badge-muted",
};

const STATE_BADGE: Record<string, string> = {
  validated: "badge-green",
  drift_detected: "badge-red",
  insufficient_source: "badge-muted",
  insufficient_replay: "badge-muted",
  context_mismatch: "badge-yellow",
  timing_mismatch: "badge-yellow",
  transition_mismatch: "badge-yellow",
  archetype_mismatch: "badge-yellow",
  cluster_mismatch: "badge-yellow",
};

function fmtTs(ts: string | null | undefined): string {
  if (!ts) return "—";
  return new Date(ts).toLocaleString(undefined, { dateStyle: "short", timeStyle: "short" });
}

function fmtRate(value: number | string | null | undefined): string {
  if (value === null || value === undefined) return "—";
  const n = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(n)) return "—";
  return `${(n * 100).toFixed(1)}%`;
}

function fmtDelta(value: number | string | null | undefined, digits = 4): string {
  if (value === null || value === undefined) return "—";
  const n = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(n)) return "—";
  return n.toFixed(digits);
}

function fmtScore(value: number | string | null | undefined, digits = 3): string {
  if (value === null || value === undefined) return "—";
  const n = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(n)) return "—";
  return n.toFixed(digits);
}

function matchBadge(v: boolean | null | undefined): string {
  if (v === true) return "badge-green";
  if (v === false) return "badge-red";
  return "badge-muted";
}

function matchLabel(v: boolean | null | undefined): string {
  if (v === true) return "match";
  if (v === false) return "differ";
  return "—";
}

function rateBadge(value: number | string | null | undefined): string {
  if (value === null || value === undefined) return "badge-muted";
  const n = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(n)) return "badge-muted";
  if (n >= 0.95) return "badge-green";
  if (n >= 0.75) return "badge-yellow";
  return "badge-red";
}

function shortId(id: string | null | undefined): string {
  if (!id) return "—";
  return `${id.slice(0, 8)}…`;
}

function reasonList(codes: string[] | null | undefined, maxShown = 3): string {
  if (!codes || codes.length === 0) return "—";
  if (codes.length <= maxShown) return codes.join(", ");
  return `${codes.slice(0, maxShown).join(", ")} +${codes.length - maxShown}`;
}

export function CrossAssetClusterReplayValidationPanel({
  clusterReplayValidationSummary,
  familyClusterReplayStabilitySummary,
  clusterReplayStabilityAggregate,
  loading,
}: Props) {
  return (
    <section className="section">
      <div className="panel-header">
        <div>
          <h2>Cluster Replay Validation</h2>
          <p className="panel-subtitle">
            Phase 4.5D. Deterministic replay comparison across 4.5A pattern-cluster diagnostics +
            4.5B cluster-aware attribution + 4.5C cluster-aware composite. Numeric tolerance 1e-9
            (drift score 1e-6); drift reason codes explicit. validation_state separates
            context_mismatch, timing_mismatch, transition_mismatch, archetype_mismatch, and
            cluster_mismatch so operators can see the primary driver.
          </p>
        </div>
      </div>

      {loading && <p className="muted">Loading cluster replay validation…</p>}

      {!loading && (
        <>
          {/* ── Cluster Replay Stability Aggregate ─────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Cluster Replay Stability Aggregate</h3>
                <p className="panel-subtitle">Workspace rollup of cluster-layer match rates.</p>
              </div>
            </div>
            {!clusterReplayStabilityAggregate ? (
              <p className="muted">No cluster replay validations recorded yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Validations</th>
                    <th>Context</th>
                    <th>Regime</th>
                    <th>Timing</th>
                    <th>Transition</th>
                    <th>Sequence</th>
                    <th>Archetype</th>
                    <th>Cluster state</th>
                    <th>Drift score</th>
                    <th>Cluster attr</th>
                    <th>Cluster comp</th>
                    <th>Cluster dom family</th>
                    <th>Drift count</th>
                    <th>Latest</th>
                  </tr>
                </thead>
                <tbody>
                  <tr>
                    <td className="text-sm">{clusterReplayStabilityAggregate.validation_count}</td>
                    <td><span className={rateBadge(clusterReplayStabilityAggregate.context_match_rate)}>{fmtRate(clusterReplayStabilityAggregate.context_match_rate)}</span></td>
                    <td><span className={rateBadge(clusterReplayStabilityAggregate.regime_match_rate)}>{fmtRate(clusterReplayStabilityAggregate.regime_match_rate)}</span></td>
                    <td><span className={rateBadge(clusterReplayStabilityAggregate.timing_class_match_rate)}>{fmtRate(clusterReplayStabilityAggregate.timing_class_match_rate)}</span></td>
                    <td><span className={rateBadge(clusterReplayStabilityAggregate.transition_state_match_rate)}>{fmtRate(clusterReplayStabilityAggregate.transition_state_match_rate)}</span></td>
                    <td><span className={rateBadge(clusterReplayStabilityAggregate.sequence_class_match_rate)}>{fmtRate(clusterReplayStabilityAggregate.sequence_class_match_rate)}</span></td>
                    <td><span className={rateBadge(clusterReplayStabilityAggregate.archetype_match_rate)}>{fmtRate(clusterReplayStabilityAggregate.archetype_match_rate)}</span></td>
                    <td><span className={rateBadge(clusterReplayStabilityAggregate.cluster_state_match_rate)}>{fmtRate(clusterReplayStabilityAggregate.cluster_state_match_rate)}</span></td>
                    <td><span className={rateBadge(clusterReplayStabilityAggregate.drift_score_match_rate)}>{fmtRate(clusterReplayStabilityAggregate.drift_score_match_rate)}</span></td>
                    <td><span className={rateBadge(clusterReplayStabilityAggregate.cluster_attribution_match_rate)}>{fmtRate(clusterReplayStabilityAggregate.cluster_attribution_match_rate)}</span></td>
                    <td><span className={rateBadge(clusterReplayStabilityAggregate.cluster_composite_match_rate)}>{fmtRate(clusterReplayStabilityAggregate.cluster_composite_match_rate)}</span></td>
                    <td><span className={rateBadge(clusterReplayStabilityAggregate.cluster_dominant_family_match_rate)}>{fmtRate(clusterReplayStabilityAggregate.cluster_dominant_family_match_rate)}</span></td>
                    <td className="text-sm">{clusterReplayStabilityAggregate.drift_detected_count}</td>
                    <td className="text-sm muted">{fmtTs(clusterReplayStabilityAggregate.latest_validated_at)}</td>
                  </tr>
                </tbody>
              </table>
            )}
          </div>

          {/* ── Cluster Replay Validation Summary ──────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Cluster Replay Validation Summary</h3>
                <p className="panel-subtitle">Per-pair match flags and drift reason codes.</p>
              </div>
            </div>
            {clusterReplayValidationSummary.length === 0 ? (
              <p className="muted">No cluster replay validation rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>State</th>
                    <th>Source run</th>
                    <th>Replay run</th>
                    <th>Src cluster</th>
                    <th>Replay cluster</th>
                    <th>Src drift</th>
                    <th>Replay drift</th>
                    <th>Ctx</th>
                    <th>Regime</th>
                    <th>Timing</th>
                    <th>Transition</th>
                    <th>Sequence</th>
                    <th>Archetype</th>
                    <th>Cluster</th>
                    <th>Drift</th>
                    <th>Cl attr</th>
                    <th>Cl comp</th>
                    <th>Cl dom</th>
                    <th>Drift codes</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {clusterReplayValidationSummary.map((row) => (
                    <tr key={`${row.source_run_id}:${row.replay_run_id}`}>
                      <td>
                        <span className={STATE_BADGE[row.validation_state] ?? "badge-muted"}>
                          {row.validation_state}
                        </span>
                      </td>
                      <td className="mono-cell text-sm">{shortId(row.source_run_id)}</td>
                      <td className="mono-cell text-sm">{shortId(row.replay_run_id)}</td>
                      <td>
                        {row.source_cluster_state ? (
                          <span className={CLUSTER_STATE_BADGE[row.source_cluster_state] ?? "badge-muted"}>
                            {row.source_cluster_state}
                          </span>
                        ) : <span className="badge-muted">—</span>}
                      </td>
                      <td>
                        {row.replay_cluster_state ? (
                          <span className={CLUSTER_STATE_BADGE[row.replay_cluster_state] ?? "badge-muted"}>
                            {row.replay_cluster_state}
                          </span>
                        ) : <span className="badge-muted">—</span>}
                      </td>
                      <td className="text-sm">{fmtScore(row.source_drift_score)}</td>
                      <td className="text-sm">{fmtScore(row.replay_drift_score)}</td>
                      <td><span className={matchBadge(row.context_hash_match)}>{matchLabel(row.context_hash_match)}</span></td>
                      <td><span className={matchBadge(row.regime_match)}>{matchLabel(row.regime_match)}</span></td>
                      <td><span className={matchBadge(row.timing_class_match)}>{matchLabel(row.timing_class_match)}</span></td>
                      <td><span className={matchBadge(row.transition_state_match)}>{matchLabel(row.transition_state_match)}</span></td>
                      <td><span className={matchBadge(row.sequence_class_match)}>{matchLabel(row.sequence_class_match)}</span></td>
                      <td><span className={matchBadge(row.archetype_match)}>{matchLabel(row.archetype_match)}</span></td>
                      <td><span className={matchBadge(row.cluster_state_match)}>{matchLabel(row.cluster_state_match)}</span></td>
                      <td><span className={matchBadge(row.drift_score_match)}>{matchLabel(row.drift_score_match)}</span></td>
                      <td><span className={matchBadge(row.cluster_attribution_match)}>{matchLabel(row.cluster_attribution_match)}</span></td>
                      <td><span className={matchBadge(row.cluster_composite_match)}>{matchLabel(row.cluster_composite_match)}</span></td>
                      <td><span className={matchBadge(row.cluster_dominant_family_match)}>{matchLabel(row.cluster_dominant_family_match)}</span></td>
                      <td className="mono-cell text-sm">{reasonList(row.drift_reason_codes)}</td>
                      <td className="text-sm muted">{fmtTs(row.created_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* ── Family Cluster Replay Stability ────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Family Cluster Replay Stability</h3>
                <p className="panel-subtitle">
                  Per-family deltas for cluster-adjusted attribution and cluster integration contribution.
                </p>
              </div>
            </div>
            {familyClusterReplayStabilitySummary.length === 0 ? (
              <p className="muted">No family cluster replay stability rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Source run</th>
                    <th>Replay run</th>
                    <th>Family</th>
                    <th>Src cluster</th>
                    <th>Replay cluster</th>
                    <th>Src archetype</th>
                    <th>Replay archetype</th>
                    <th>Cluster adj Δ</th>
                    <th>Cluster int Δ</th>
                    <th>Cluster</th>
                    <th>Archetype</th>
                    <th>Rank (attr)</th>
                    <th>Rank (comp)</th>
                    <th>Drift codes</th>
                  </tr>
                </thead>
                <tbody>
                  {familyClusterReplayStabilitySummary.slice(0, 80).map((row, idx) => (
                    <tr key={`${row.source_run_id}:${row.replay_run_id}:${row.dependency_family}:${idx}`}>
                      <td className="mono-cell text-sm">{shortId(row.source_run_id)}</td>
                      <td className="mono-cell text-sm">{shortId(row.replay_run_id)}</td>
                      <td>
                        <span className={FAMILY_BADGE[row.dependency_family] ?? "badge-muted"}>
                          {row.dependency_family}
                        </span>
                      </td>
                      <td>
                        {row.source_cluster_state ? (
                          <span className={CLUSTER_STATE_BADGE[row.source_cluster_state] ?? "badge-muted"}>
                            {row.source_cluster_state}
                          </span>
                        ) : <span className="badge-muted">—</span>}
                      </td>
                      <td>
                        {row.replay_cluster_state ? (
                          <span className={CLUSTER_STATE_BADGE[row.replay_cluster_state] ?? "badge-muted"}>
                            {row.replay_cluster_state}
                          </span>
                        ) : <span className="badge-muted">—</span>}
                      </td>
                      <td>
                        {row.source_dominant_archetype_key ? (
                          <span className={ARCHETYPE_BADGE[row.source_dominant_archetype_key] ?? "badge-muted"}>
                            {row.source_dominant_archetype_key}
                          </span>
                        ) : <span className="badge-muted">—</span>}
                      </td>
                      <td>
                        {row.replay_dominant_archetype_key ? (
                          <span className={ARCHETYPE_BADGE[row.replay_dominant_archetype_key] ?? "badge-muted"}>
                            {row.replay_dominant_archetype_key}
                          </span>
                        ) : <span className="badge-muted">—</span>}
                      </td>
                      <td className="text-sm">{fmtDelta(row.cluster_adjusted_delta)}</td>
                      <td className="text-sm">{fmtDelta(row.cluster_integration_delta)}</td>
                      <td><span className={matchBadge(row.cluster_state_match)}>{matchLabel(row.cluster_state_match)}</span></td>
                      <td><span className={matchBadge(row.archetype_match)}>{matchLabel(row.archetype_match)}</span></td>
                      <td><span className={matchBadge(row.cluster_family_rank_match)}>{matchLabel(row.cluster_family_rank_match)}</span></td>
                      <td><span className={matchBadge(row.cluster_composite_family_rank_match)}>{matchLabel(row.cluster_composite_family_rank_match)}</span></td>
                      <td className="mono-cell text-sm">{reasonList(row.drift_reason_codes)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </>
      )}
    </section>
  );
}
