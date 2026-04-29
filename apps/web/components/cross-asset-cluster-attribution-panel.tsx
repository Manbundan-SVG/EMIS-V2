"use client";

import type {
  CrossAssetFamilyClusterAttributionSummaryRow,
  CrossAssetSymbolClusterAttributionSummaryRow,
  RunCrossAssetClusterAttributionSummaryRow,
} from "@/lib/queries/metrics";

type Props = {
  familyClusterAttributionSummary: CrossAssetFamilyClusterAttributionSummaryRow[];
  symbolClusterAttributionSummary: CrossAssetSymbolClusterAttributionSummaryRow[];
  runClusterAttributionSummary: RunCrossAssetClusterAttributionSummaryRow[];
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

function fmtWeight(value: number | string | null | undefined): string {
  return fmtScore(value, 3);
}

function netBadge(value: number | string | null | undefined): string {
  if (value === null || value === undefined) return "badge-muted";
  const n = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(n)) return "badge-muted";
  if (n > 0.05) return "badge-green";
  if (n < -0.05) return "badge-red";
  return "badge-muted";
}

function shiftBadge(
  a: string | null | undefined, b: string | null | undefined,
): string {
  if (!a && !b) return "badge-muted";
  if (!a || !b) return "badge-yellow";
  return a === b ? "badge-green" : "badge-yellow";
}

function shortId(id: string | null | undefined): string {
  if (!id) return "—";
  return `${id.slice(0, 8)}…`;
}

function symbolList(syms: string[] | null | undefined, maxShown = 4): string {
  if (!syms || syms.length === 0) return "—";
  if (syms.length <= maxShown) return syms.join(", ");
  return `${syms.slice(0, maxShown).join(", ")} +${syms.length - maxShown}`;
}

function reasonList(codes: string[] | null | undefined, maxShown = 3): string {
  if (!codes || codes.length === 0) return "—";
  if (codes.length <= maxShown) return codes.join(", ");
  return `${codes.slice(0, maxShown).join(", ")} +${codes.length - maxShown}`;
}

export function CrossAssetClusterAttributionPanel({
  familyClusterAttributionSummary,
  symbolClusterAttributionSummary,
  runClusterAttributionSummary,
  loading,
}: Props) {
  return (
    <section className="section">
      <div className="panel-header">
        <div>
          <h2>Cluster-Aware Cross-Asset Attribution</h2>
          <p className="panel-subtitle">
            Phase 4.5B. Conditions archetype-aware family/symbol contribution on the 4.5A cluster
            state, drift score, and pattern entropy. Weight clipped to [0.75, 1.20]. Recovering /
            rotating clusters earn small positive bonuses (rotation suppressed when drift ≥ 0.50);
            deteriorating / mixed states get sign-aware penalties; high drift / high entropy add
            additional penalty. All upstream attribution layers persist side-by-side.
          </p>
        </div>
      </div>

      {loading && <p className="muted">Loading cluster-aware attribution…</p>}

      {!loading && (
        <>
          {/* ── Cluster Attribution Summary ────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Cluster Attribution Summary</h3>
                <p className="panel-subtitle">Raw vs weighted vs regime vs timing vs transition vs archetype vs cluster-adjusted contribution per run.</p>
              </div>
            </div>
            {runClusterAttributionSummary.length === 0 ? (
              <p className="muted">No cluster integration rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Run</th>
                    <th>Raw</th>
                    <th>Weighted</th>
                    <th>Regime</th>
                    <th>Timing</th>
                    <th>Transition</th>
                    <th>Archetype</th>
                    <th>Cluster</th>
                    <th>Cluster state</th>
                    <th>Dom. archetype</th>
                    <th>Raw dom.</th>
                    <th>Cluster dom.</th>
                    <th>Shift</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {runClusterAttributionSummary.map((row) => (
                    <tr key={row.run_id}>
                      <td className="mono-cell text-sm">{shortId(row.run_id)}</td>
                      <td className="text-sm">{fmtScore(row.cross_asset_net_contribution)}</td>
                      <td className="text-sm">{fmtScore(row.weighted_cross_asset_net_contribution)}</td>
                      <td className="text-sm">{fmtScore(row.regime_adjusted_cross_asset_contribution)}</td>
                      <td className="text-sm">{fmtScore(row.timing_adjusted_cross_asset_contribution)}</td>
                      <td className="text-sm">{fmtScore(row.transition_adjusted_cross_asset_contribution)}</td>
                      <td className="text-sm">{fmtScore(row.archetype_adjusted_cross_asset_contribution)}</td>
                      <td>
                        <span className={netBadge(row.cluster_adjusted_cross_asset_contribution)}>
                          {fmtScore(row.cluster_adjusted_cross_asset_contribution)}
                        </span>
                      </td>
                      <td>
                        {row.cluster_state ? (
                          <span className={CLUSTER_STATE_BADGE[row.cluster_state] ?? "badge-muted"}>
                            {row.cluster_state}
                          </span>
                        ) : "—"}
                      </td>
                      <td>
                        {row.dominant_archetype_key ? (
                          <span className={ARCHETYPE_BADGE[row.dominant_archetype_key] ?? "badge-muted"}>
                            {row.dominant_archetype_key}
                          </span>
                        ) : "—"}
                      </td>
                      <td>
                        {row.dominant_dependency_family ? (
                          <span className={FAMILY_BADGE[row.dominant_dependency_family] ?? "badge-muted"}>
                            {row.dominant_dependency_family}
                          </span>
                        ) : "—"}
                      </td>
                      <td>
                        {row.cluster_dominant_dependency_family ? (
                          <span className={FAMILY_BADGE[row.cluster_dominant_dependency_family] ?? "badge-muted"}>
                            {row.cluster_dominant_dependency_family}
                          </span>
                        ) : "—"}
                      </td>
                      <td>
                        <span className={shiftBadge(row.dominant_dependency_family, row.cluster_dominant_dependency_family)}>
                          {row.dominant_dependency_family === row.cluster_dominant_dependency_family ? "stable" : "shifted"}
                        </span>
                      </td>
                      <td className="text-sm muted">{fmtTs(row.created_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* ── Cluster-Aware Family Attribution ───────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Cluster-Aware Family Attribution</h3>
                <p className="panel-subtitle">Per-family cluster state, weight, bonus/penalty, and adjusted contribution.</p>
              </div>
            </div>
            {familyClusterAttributionSummary.length === 0 ? (
              <p className="muted">No cluster-aware family attribution rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Rank</th>
                    <th>Run</th>
                    <th>Family</th>
                    <th>Cluster state</th>
                    <th>Dom. archetype</th>
                    <th>Drift</th>
                    <th>Entropy</th>
                    <th>Archetype adj</th>
                    <th>Weight</th>
                    <th>Bonus</th>
                    <th>Penalty</th>
                    <th>Cluster adj</th>
                    <th>Top symbols</th>
                    <th>Reasons</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {familyClusterAttributionSummary.slice(0, 120).map((row, idx) => (
                    <tr key={`${row.run_id}:${row.dependency_family}:${idx}`}>
                      <td className="text-sm">{row.cluster_family_rank ?? "—"}</td>
                      <td className="mono-cell text-sm">{shortId(row.run_id)}</td>
                      <td>
                        <span className={FAMILY_BADGE[row.dependency_family] ?? "badge-muted"}>
                          {row.dependency_family}
                        </span>
                      </td>
                      <td>
                        <span className={CLUSTER_STATE_BADGE[row.cluster_state] ?? "badge-muted"}>
                          {row.cluster_state}
                        </span>
                      </td>
                      <td>
                        <span className={ARCHETYPE_BADGE[row.dominant_archetype_key] ?? "badge-muted"}>
                          {row.dominant_archetype_key}
                        </span>
                      </td>
                      <td className="text-sm">{fmtScore(row.drift_score, 2)}</td>
                      <td className="text-sm">{fmtScore(row.pattern_entropy, 2)}</td>
                      <td className="text-sm">{fmtScore(row.archetype_adjusted_family_contribution)}</td>
                      <td className="text-sm">{fmtWeight(row.cluster_weight)}</td>
                      <td className="text-sm">{fmtScore(row.cluster_bonus)}</td>
                      <td className="text-sm">{fmtScore(row.cluster_penalty)}</td>
                      <td>
                        <span className={netBadge(row.cluster_adjusted_family_contribution)}>
                          {fmtScore(row.cluster_adjusted_family_contribution)}
                        </span>
                      </td>
                      <td className="mono-cell text-sm">{symbolList(row.top_symbols)}</td>
                      <td className="mono-cell text-sm">{reasonList(row.reason_codes)}</td>
                      <td className="text-sm muted">{fmtTs(row.created_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* ── Cluster-Aware Symbol Attribution ───────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Cluster-Aware Symbol Attribution</h3>
                <p className="panel-subtitle">Top symbols by cluster-adjusted score. Symbols inherit the run's cluster context.</p>
              </div>
            </div>
            {symbolClusterAttributionSummary.length === 0 ? (
              <p className="muted">No cluster-aware symbol rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Rank</th>
                    <th>Run</th>
                    <th>Symbol</th>
                    <th>Family</th>
                    <th>Cluster state</th>
                    <th>Archetype adj</th>
                    <th>Weight</th>
                    <th>Cluster adj</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {symbolClusterAttributionSummary.slice(0, 150).map((row, idx) => (
                    <tr key={`${row.run_id}:${row.symbol}:${idx}`}>
                      <td className="text-sm">{row.symbol_rank ?? "—"}</td>
                      <td className="mono-cell text-sm">{shortId(row.run_id)}</td>
                      <td className="mono-cell">{row.symbol}</td>
                      <td>
                        <span className={FAMILY_BADGE[row.dependency_family] ?? "badge-muted"}>
                          {row.dependency_family}
                        </span>
                      </td>
                      <td>
                        <span className={CLUSTER_STATE_BADGE[row.cluster_state] ?? "badge-muted"}>
                          {row.cluster_state}
                        </span>
                      </td>
                      <td className="text-sm">{fmtScore(row.archetype_adjusted_symbol_score)}</td>
                      <td className="text-sm">{fmtWeight(row.cluster_weight)}</td>
                      <td>
                        <span className={netBadge(row.cluster_adjusted_symbol_score)}>
                          {fmtScore(row.cluster_adjusted_symbol_score)}
                        </span>
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
