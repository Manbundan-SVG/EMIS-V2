"use client";

import type {
  CrossAssetDecayCompositeSummaryRow,
  CrossAssetFamilyDecayCompositeSummaryRow,
  RunCrossAssetDecayIntegrationSummaryRow,
} from "@/lib/queries/metrics";

type Props = {
  decayCompositeSummary: CrossAssetDecayCompositeSummaryRow[];
  familyDecayCompositeSummary: CrossAssetFamilyDecayCompositeSummaryRow[];
  finalDecayIntegrationSummary: RunCrossAssetDecayIntegrationSummaryRow[];
  loading: boolean;
};

const FRESHNESS_BADGE: Record<string, string> = {
  fresh:                "badge-green",
  decaying:             "badge-yellow",
  stale:                "badge-red",
  contradicted:         "badge-red",
  mixed:                "badge-yellow",
  insufficient_history: "badge-muted",
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

const MODE_BADGE: Record<string, string> = {
  decay_additive_guardrailed:      "badge-muted",
  fresh_confirmation_only:         "badge-green",
  stale_suppression_only:          "badge-red",
  contradiction_suppression_only:  "badge-red",
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

function decayBadge(value: number | string | null | undefined): string {
  if (value === null || value === undefined) return "badge-muted";
  const n = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(n)) return "badge-muted";
  if (n >= 0.75) return "badge-green";
  if (n >= 0.50) return "badge-yellow";
  if (n <= 0.30) return "badge-red";
  return "badge-yellow";
}

function deltaBadge(value: number | string | null | undefined): string {
  if (value === null || value === undefined) return "badge-muted";
  const n = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(n)) return "badge-muted";
  if (n >= 0.02) return "badge-green";
  if (n <= -0.02) return "badge-red";
  return "badge-muted";
}

function flagBadge(flag: boolean): string {
  return flag ? "badge-red" : "badge-muted";
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

export function CrossAssetDecayCompositePanel({
  decayCompositeSummary,
  familyDecayCompositeSummary,
  finalDecayIntegrationSummary,
  loading,
}: Props) {
  return (
    <section className="section">
      <div className="panel-header">
        <div>
          <h2>Decay-Aware Composite Refinement</h2>
          <p className="panel-subtitle">
            Phase 4.7C. Adds a bounded decay-aware delta to the most mature upstream composite (4.6C → 4.5C →
            4.4C → 4.3C → 4.2C → 4.1A fallback). Net contribution clamped to [−0.15, +0.15] and per-profile
            [−0.20, +0.20]. Stale and contradiction flags suppress magnitude toward zero (never invert sign).
            Diagnostic only — no predictive forecasting.
          </p>
        </div>
      </div>

      {loading && <p className="muted">Loading decay-aware composite…</p>}

      {!loading && (
        <>
          {/* ── Decay-Aware Composite ──────────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Decay-Aware Composite</h3>
                <p className="panel-subtitle">
                  Pre-decay composite, decay net contribution, post-decay composite, freshness context, and integration mode.
                </p>
              </div>
            </div>
            {decayCompositeSummary.length === 0 ? (
              <p className="muted">No decay-aware composite rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Run</th>
                    <th>Pre-decay</th>
                    <th>Decay Δ</th>
                    <th>Post-decay</th>
                    <th>Freshness</th>
                    <th>Aggregate decay</th>
                    <th>Stale</th>
                    <th>Contradicted</th>
                    <th>Integration mode</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {decayCompositeSummary.map((row) => (
                    <tr key={row.run_id}>
                      <td className="mono-cell text-sm">{shortId(row.run_id)}</td>
                      <td className="text-sm">{fmtScore(row.composite_pre_decay)}</td>
                      <td>
                        <span className={deltaBadge(row.decay_net_contribution)}>
                          {fmtScore(row.decay_net_contribution)}
                        </span>
                      </td>
                      <td className="text-sm">
                        <strong>{fmtScore(row.composite_post_decay)}</strong>
                      </td>
                      <td>
                        <span className={FRESHNESS_BADGE[row.freshness_state] ?? "badge-muted"}>
                          {row.freshness_state}
                        </span>
                      </td>
                      <td>
                        <span className={decayBadge(row.aggregate_decay_score)}>
                          {fmtScore(row.aggregate_decay_score, 3)}
                        </span>
                      </td>
                      <td>
                        <span className={flagBadge(row.stale_memory_flag)}>
                          {row.stale_memory_flag ? "stale" : "ok"}
                        </span>
                      </td>
                      <td>
                        <span className={flagBadge(row.contradiction_flag)}>
                          {row.contradiction_flag ? "contradicted" : "ok"}
                        </span>
                      </td>
                      <td>
                        <span className={MODE_BADGE[row.integration_mode] ?? "badge-muted"}>
                          {row.integration_mode}
                        </span>
                      </td>
                      <td className="text-sm muted">{fmtTs(row.created_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* ── Family Decay Composite Contribution ────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Family Decay Composite Contribution</h3>
                <p className="panel-subtitle">
                  Per-family decay-adjusted contribution alongside the integration weight applied and the resulting integration delta.
                </p>
              </div>
            </div>
            {familyDecayCompositeSummary.length === 0 ? (
              <p className="muted">No family decay composite rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Run</th>
                    <th>Family</th>
                    <th>Decay-adjusted</th>
                    <th>Weight</th>
                    <th>Integration Δ</th>
                    <th>Rank</th>
                    <th>Freshness</th>
                    <th>Stale</th>
                    <th>Contradicted</th>
                    <th>Reasons</th>
                  </tr>
                </thead>
                <tbody>
                  {familyDecayCompositeSummary.map((row, idx) => (
                    <tr key={`${row.run_id}:${row.dependency_family}:${idx}`}>
                      <td className="mono-cell text-sm">{shortId(row.run_id)}</td>
                      <td>
                        <span className={FAMILY_BADGE[row.dependency_family] ?? "badge-muted"}>
                          {row.dependency_family}
                        </span>
                      </td>
                      <td className="text-sm">{fmtScore(row.decay_adjusted_family_contribution)}</td>
                      <td className="text-sm">{fmtScore(row.integration_weight_applied, 3)}</td>
                      <td>
                        <span className={deltaBadge(row.decay_integration_contribution)}>
                          {fmtScore(row.decay_integration_contribution)}
                        </span>
                      </td>
                      <td className="text-sm">{row.family_rank ?? "—"}</td>
                      <td>
                        <span className={FRESHNESS_BADGE[row.freshness_state] ?? "badge-muted"}>
                          {row.freshness_state}
                        </span>
                      </td>
                      <td>
                        <span className={flagBadge(row.stale_memory_flag)}>
                          {row.stale_memory_flag ? "stale" : "ok"}
                        </span>
                      </td>
                      <td>
                        <span className={flagBadge(row.contradiction_flag)}>
                          {row.contradiction_flag ? "contradicted" : "ok"}
                        </span>
                      </td>
                      <td className="mono-cell text-sm">{reasonList(row.reason_codes)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* ── Final Decay Integration Summary ────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Final Decay Integration Summary</h3>
                <p className="panel-subtitle">
                  Run-level comparison: raw → weighted → regime → timing → transition → archetype → cluster → persistence → decay-adjusted → final decay-integrated composite.
                </p>
              </div>
            </div>
            {finalDecayIntegrationSummary.length === 0 ? (
              <p className="muted">No final decay-integration rows yet.</p>
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
                    <th>Persistence</th>
                    <th>Decay-adj.</th>
                    <th>Decay Δ</th>
                    <th>Pre-decay</th>
                    <th>Post-decay</th>
                    <th>Decay dominant</th>
                    <th>Freshness</th>
                    <th>Stale</th>
                    <th>Contradicted</th>
                  </tr>
                </thead>
                <tbody>
                  {finalDecayIntegrationSummary.map((row) => (
                    <tr key={row.run_id}>
                      <td className="mono-cell text-sm">{shortId(row.run_id)}</td>
                      <td className="text-sm">{fmtScore(row.cross_asset_net_contribution)}</td>
                      <td className="text-sm">{fmtScore(row.weighted_cross_asset_net_contribution)}</td>
                      <td className="text-sm">{fmtScore(row.regime_adjusted_cross_asset_contribution)}</td>
                      <td className="text-sm">{fmtScore(row.timing_adjusted_cross_asset_contribution)}</td>
                      <td className="text-sm">{fmtScore(row.transition_adjusted_cross_asset_contribution)}</td>
                      <td className="text-sm">{fmtScore(row.archetype_adjusted_cross_asset_contribution)}</td>
                      <td className="text-sm">{fmtScore(row.cluster_adjusted_cross_asset_contribution)}</td>
                      <td className="text-sm">{fmtScore(row.persistence_adjusted_cross_asset_contribution)}</td>
                      <td className="text-sm">{fmtScore(row.decay_adjusted_cross_asset_contribution)}</td>
                      <td>
                        <span className={deltaBadge(row.decay_net_contribution)}>
                          {fmtScore(row.decay_net_contribution)}
                        </span>
                      </td>
                      <td className="text-sm">{fmtScore(row.composite_pre_decay)}</td>
                      <td className="text-sm">
                        <strong>{fmtScore(row.composite_post_decay)}</strong>
                      </td>
                      <td>
                        {row.decay_dominant_dependency_family ? (
                          <span className={FAMILY_BADGE[row.decay_dominant_dependency_family] ?? "badge-muted"}>
                            {row.decay_dominant_dependency_family}
                          </span>
                        ) : "—"}
                      </td>
                      <td>
                        {row.freshness_state ? (
                          <span className={FRESHNESS_BADGE[row.freshness_state] ?? "badge-muted"}>
                            {row.freshness_state}
                          </span>
                        ) : "—"}
                      </td>
                      <td>
                        <span className={flagBadge(row.stale_memory_flag)}>
                          {row.stale_memory_flag ? "stale" : "ok"}
                        </span>
                      </td>
                      <td>
                        <span className={flagBadge(row.contradiction_flag)}>
                          {row.contradiction_flag ? "contradicted" : "ok"}
                        </span>
                      </td>
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
