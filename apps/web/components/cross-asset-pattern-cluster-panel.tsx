"use client";

import type {
  CrossAssetArchetypeClusterSummaryRow,
  CrossAssetArchetypeRegimeRotationSummaryRow,
  CrossAssetPatternDriftEventSummaryRow,
  RunCrossAssetPatternClusterSummaryRow,
} from "@/lib/queries/metrics";

type Props = {
  clusterSummary: CrossAssetArchetypeClusterSummaryRow[];
  regimeRotationSummary: CrossAssetArchetypeRegimeRotationSummaryRow[];
  driftEventSummary: CrossAssetPatternDriftEventSummaryRow[];
  runPatternClusterSummary: RunCrossAssetPatternClusterSummaryRow[];
  loading: boolean;
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

const DRIFT_EVENT_BADGE: Record<string, string> = {
  archetype_rotation:        "badge-yellow",
  reinforcement_break:       "badge-red",
  recovery_break:            "badge-red",
  degradation_acceleration:  "badge-red",
  mixed_noise_increase:      "badge-muted",
  stabilization:             "badge-green",
  insufficient_history:      "badge-muted",
};

function fmtTs(ts: string | null | undefined): string {
  if (!ts) return "—";
  return new Date(ts).toLocaleString(undefined, { dateStyle: "short", timeStyle: "short" });
}

function fmtScore(value: number | string | null | undefined, digits = 3): string {
  if (value === null || value === undefined) return "—";
  const n = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(n)) return "—";
  return n.toFixed(digits);
}

function fmtShare(value: number | string | null | undefined): string {
  if (value === null || value === undefined) return "—";
  const n = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(n)) return "—";
  return `${(n * 100).toFixed(0)}%`;
}

function driftBadge(value: number | string | null | undefined): string {
  if (value === null || value === undefined) return "badge-muted";
  const n = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(n)) return "badge-muted";
  if (n >= 0.50) return "badge-red";
  if (n >= 0.20) return "badge-yellow";
  return "badge-green";
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

export function CrossAssetPatternClusterPanel({
  clusterSummary,
  regimeRotationSummary,
  driftEventSummary,
  runPatternClusterSummary,
  loading,
}: Props) {
  return (
    <section className="section">
      <div className="panel-header">
        <div>
          <h2>Pattern Clusters & Drift</h2>
          <p className="panel-subtitle">
            Phase 4.5A. Recent-window archetype distribution, regime-conditioned rotation, and
            discrete drift events. Cluster state is classified deterministically from bucket
            shares + entropy; drift score combines TVD over archetype shares (0.6) + entropy
            delta (0.2) + dominant-change boost (0.2). All windows fixed and metadata-stamped.
          </p>
        </div>
      </div>

      {loading && <p className="muted">Loading pattern clusters…</p>}

      {!loading && (
        <>
          {/* ── Pattern Cluster Summary ────────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Pattern Cluster Summary</h3>
                <p className="panel-subtitle">Per-run archetype mix and cluster state.</p>
              </div>
            </div>
            {clusterSummary.length === 0 ? (
              <p className="muted">No cluster snapshots yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Run</th>
                    <th>Regime</th>
                    <th>Window</th>
                    <th>Dominant archetype</th>
                    <th>Cluster state</th>
                    <th>Reinforce</th>
                    <th>Recovery</th>
                    <th>Rotation</th>
                    <th>Degrade</th>
                    <th>Mixed</th>
                    <th>Entropy</th>
                    <th>Drift score</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {clusterSummary.map((row) => (
                    <tr key={row.run_id}>
                      <td className="mono-cell text-sm">{shortId(row.run_id)}</td>
                      <td className="text-sm">{row.regime_key ?? "—"}</td>
                      <td className="text-sm">{row.window_label}</td>
                      <td>
                        <span className={ARCHETYPE_BADGE[row.dominant_archetype_key] ?? "badge-muted"}>
                          {row.dominant_archetype_key}
                        </span>
                      </td>
                      <td>
                        <span className={CLUSTER_STATE_BADGE[row.cluster_state] ?? "badge-muted"}>
                          {row.cluster_state}
                        </span>
                      </td>
                      <td className="text-sm">{fmtShare(row.reinforcement_share)}</td>
                      <td className="text-sm">{fmtShare(row.recovery_share)}</td>
                      <td className="text-sm">{fmtShare(row.rotation_share)}</td>
                      <td className="text-sm">{fmtShare(row.degradation_share)}</td>
                      <td className="text-sm">{fmtShare(row.mixed_share)}</td>
                      <td className="text-sm">{fmtScore(row.pattern_entropy, 2)}</td>
                      <td>
                        <span className={driftBadge(row.drift_score)}>
                          {fmtScore(row.drift_score)}
                        </span>
                      </td>
                      <td className="text-sm muted">{fmtTs(row.created_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* ── Regime Rotation Summary ────────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Regime Rotation Summary</h3>
                <p className="panel-subtitle">Per-regime archetype rotation across recent runs.</p>
              </div>
            </div>
            {regimeRotationSummary.length === 0 ? (
              <p className="muted">No regime rotation rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Regime</th>
                    <th>Window</th>
                    <th>Prior dominant</th>
                    <th>Current dominant</th>
                    <th>Rotations</th>
                    <th>Reinforce runs</th>
                    <th>Recovery runs</th>
                    <th>Degrade runs</th>
                    <th>Mixed runs</th>
                    <th>Rotation state</th>
                    <th>Drift score</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {regimeRotationSummary.map((row, idx) => (
                    <tr key={`${row.regime_key}:${row.window_label}:${idx}`}>
                      <td className="text-sm">{row.regime_key}</td>
                      <td className="text-sm">{row.window_label}</td>
                      <td>
                        {row.prior_dominant_archetype_key ? (
                          <span className={ARCHETYPE_BADGE[row.prior_dominant_archetype_key] ?? "badge-muted"}>
                            {row.prior_dominant_archetype_key}
                          </span>
                        ) : "—"}
                      </td>
                      <td>
                        {row.current_dominant_archetype_key ? (
                          <span className={ARCHETYPE_BADGE[row.current_dominant_archetype_key] ?? "badge-muted"}>
                            {row.current_dominant_archetype_key}
                          </span>
                        ) : "—"}
                      </td>
                      <td className="text-sm">{row.rotation_count}</td>
                      <td className="text-sm">{row.reinforcement_run_count}</td>
                      <td className="text-sm">{row.recovery_run_count}</td>
                      <td className="text-sm">{row.degradation_run_count}</td>
                      <td className="text-sm">{row.mixed_run_count}</td>
                      <td>
                        <span className={CLUSTER_STATE_BADGE[row.rotation_state] ?? "badge-muted"}>
                          {row.rotation_state}
                        </span>
                      </td>
                      <td>
                        <span className={driftBadge(row.regime_drift_score)}>
                          {fmtScore(row.regime_drift_score)}
                        </span>
                      </td>
                      <td className="text-sm muted">{fmtTs(row.created_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* ── Pattern Drift Events ───────────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Pattern Drift Events</h3>
                <p className="panel-subtitle">Discrete cluster-state transitions with reason codes.</p>
              </div>
            </div>
            {driftEventSummary.length === 0 ? (
              <p className="muted">No drift events recorded yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Source run</th>
                    <th>Target run</th>
                    <th>Regime</th>
                    <th>Prior state</th>
                    <th>Current state</th>
                    <th>Prior archetype</th>
                    <th>Current archetype</th>
                    <th>Event type</th>
                    <th>Drift score</th>
                    <th>Reasons</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {driftEventSummary.map((row, idx) => (
                    <tr key={`${row.target_run_id ?? "no_target"}:${row.drift_event_type}:${idx}`}>
                      <td className="mono-cell text-sm">{shortId(row.source_run_id)}</td>
                      <td className="mono-cell text-sm">{shortId(row.target_run_id)}</td>
                      <td className="text-sm">{row.regime_key ?? "—"}</td>
                      <td>
                        {row.prior_cluster_state ? (
                          <span className={CLUSTER_STATE_BADGE[row.prior_cluster_state] ?? "badge-muted"}>
                            {row.prior_cluster_state}
                          </span>
                        ) : "—"}
                      </td>
                      <td>
                        <span className={CLUSTER_STATE_BADGE[row.current_cluster_state] ?? "badge-muted"}>
                          {row.current_cluster_state}
                        </span>
                      </td>
                      <td>
                        {row.prior_dominant_archetype_key ? (
                          <span className={ARCHETYPE_BADGE[row.prior_dominant_archetype_key] ?? "badge-muted"}>
                            {row.prior_dominant_archetype_key}
                          </span>
                        ) : "—"}
                      </td>
                      <td>
                        <span className={ARCHETYPE_BADGE[row.current_dominant_archetype_key] ?? "badge-muted"}>
                          {row.current_dominant_archetype_key}
                        </span>
                      </td>
                      <td>
                        <span className={DRIFT_EVENT_BADGE[row.drift_event_type] ?? "badge-muted"}>
                          {row.drift_event_type}
                        </span>
                      </td>
                      <td>
                        <span className={driftBadge(row.drift_score)}>
                          {fmtScore(row.drift_score)}
                        </span>
                      </td>
                      <td className="mono-cell text-sm">{reasonList(row.reason_codes)}</td>
                      <td className="text-sm muted">{fmtTs(row.created_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* ── Run Pattern-Cluster Summary ────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Run Pattern-Cluster Summary</h3>
                <p className="panel-subtitle">Compact run-linked pattern-cluster row.</p>
              </div>
            </div>
            {runPatternClusterSummary.length === 0 ? (
              <p className="muted">No run pattern-cluster rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Run</th>
                    <th>Regime</th>
                    <th>Dominant archetype</th>
                    <th>Cluster state</th>
                    <th>Rotation state</th>
                    <th>Drift score</th>
                    <th>Entropy</th>
                    <th>Latest drift event</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {runPatternClusterSummary.map((row) => (
                    <tr key={row.run_id}>
                      <td className="mono-cell text-sm">{shortId(row.run_id)}</td>
                      <td className="text-sm">{row.regime_key ?? "—"}</td>
                      <td>
                        {row.dominant_archetype_key ? (
                          <span className={ARCHETYPE_BADGE[row.dominant_archetype_key] ?? "badge-muted"}>
                            {row.dominant_archetype_key}
                          </span>
                        ) : "—"}
                      </td>
                      <td>
                        {row.cluster_state ? (
                          <span className={CLUSTER_STATE_BADGE[row.cluster_state] ?? "badge-muted"}>
                            {row.cluster_state}
                          </span>
                        ) : "—"}
                      </td>
                      <td>
                        {row.current_rotation_state ? (
                          <span className={CLUSTER_STATE_BADGE[row.current_rotation_state] ?? "badge-muted"}>
                            {row.current_rotation_state}
                          </span>
                        ) : "—"}
                      </td>
                      <td>
                        <span className={driftBadge(row.drift_score)}>
                          {fmtScore(row.drift_score)}
                        </span>
                      </td>
                      <td className="text-sm">{fmtScore(row.pattern_entropy, 2)}</td>
                      <td>
                        {row.latest_drift_event_type ? (
                          <span className={DRIFT_EVENT_BADGE[row.latest_drift_event_type] ?? "badge-muted"}>
                            {row.latest_drift_event_type}
                          </span>
                        ) : "—"}
                      </td>
                      <td className="text-sm muted">{fmtTs(row.created_at)}</td>
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
