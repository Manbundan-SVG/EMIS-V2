"use client";

import type {
  CrossAssetFamilyDecayAttributionSummaryRow,
  CrossAssetSymbolDecayAttributionSummaryRow,
  RunCrossAssetDecayAttributionSummaryRow,
} from "@/lib/queries/metrics";

type Props = {
  familyDecayAttributionSummary: CrossAssetFamilyDecayAttributionSummaryRow[];
  symbolDecayAttributionSummary: CrossAssetSymbolDecayAttributionSummaryRow[];
  runDecayAttributionSummary: RunCrossAssetDecayAttributionSummaryRow[];
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

function fmtMultiplier(value: number | string | null | undefined): string {
  if (value === null || value === undefined) return "—";
  const n = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(n)) return "—";
  return n.toFixed(3);
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

export function CrossAssetDecayAttributionPanel({
  familyDecayAttributionSummary,
  symbolDecayAttributionSummary,
  runDecayAttributionSummary,
  loading,
}: Props) {
  return (
    <section className="section">
      <div className="panel-header">
        <div>
          <h2>Decay-Aware Attribution</h2>
          <p className="panel-subtitle">
            Phase 4.7B. Conditions family + symbol contribution on signal freshness, aggregate decay score,
            stale-memory flag, and contradiction flag from 4.7A. Fresh memory gets a small bonus, stale and
            contradicted states are suppressed (contradiction also soft-floors contribution toward zero), and
            multipliers stay bounded in [0.60, 1.20]. Diagnostic only — no predictive forecasting.
          </p>
        </div>
      </div>

      {loading && <p className="muted">Loading decay-aware attribution…</p>}

      {!loading && (
        <>
          {/* ── Family Decay-Aware Attribution ─────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Decay-Aware Family Attribution</h3>
                <p className="panel-subtitle">
                  Per-family decay-adjusted contribution alongside persistence-adjusted, with freshness state and stale / contradiction flags.
                </p>
              </div>
            </div>
            {familyDecayAttributionSummary.length === 0 ? (
              <p className="muted">No decay-aware family attribution rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Run</th>
                    <th>Family</th>
                    <th>Freshness</th>
                    <th>Aggregate decay</th>
                    <th>Family decay</th>
                    <th>Persistence-adj.</th>
                    <th>Weight</th>
                    <th>Bonus</th>
                    <th>Penalty</th>
                    <th>Decay-adjusted</th>
                    <th>Rank</th>
                    <th>Stale</th>
                    <th>Contradicted</th>
                    <th>Reasons</th>
                  </tr>
                </thead>
                <tbody>
                  {familyDecayAttributionSummary.map((row, idx) => (
                    <tr key={`${row.run_id}:${row.dependency_family}:${idx}`}>
                      <td className="mono-cell text-sm">{shortId(row.run_id)}</td>
                      <td>
                        <span className={FAMILY_BADGE[row.dependency_family] ?? "badge-muted"}>
                          {row.dependency_family}
                        </span>
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
                        <span className={decayBadge(row.family_decay_score)}>
                          {fmtScore(row.family_decay_score, 3)}
                        </span>
                      </td>
                      <td className="text-sm">{fmtScore(row.persistence_adjusted_family_contribution)}</td>
                      <td className="text-sm">{fmtMultiplier(row.decay_weight)}</td>
                      <td className="text-sm">{fmtScore(row.decay_bonus)}</td>
                      <td className="text-sm">{fmtScore(row.decay_penalty)}</td>
                      <td className="text-sm">
                        <strong>{fmtScore(row.decay_adjusted_family_contribution)}</strong>
                      </td>
                      <td className="text-sm">{row.decay_family_rank ?? "—"}</td>
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

          {/* ── Symbol Decay-Aware Attribution ─────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Decay-Aware Symbol Attribution</h3>
                <p className="panel-subtitle">
                  Per-symbol decay-adjusted score with freshness and decay multipliers.
                </p>
              </div>
            </div>
            {symbolDecayAttributionSummary.length === 0 ? (
              <p className="muted">No decay-aware symbol attribution rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Run</th>
                    <th>Symbol</th>
                    <th>Family</th>
                    <th>Freshness</th>
                    <th>Aggregate decay</th>
                    <th>Persistence-adj.</th>
                    <th>Weight</th>
                    <th>Decay-adjusted</th>
                    <th>Rank</th>
                    <th>Stale</th>
                    <th>Contradicted</th>
                    <th>Reasons</th>
                  </tr>
                </thead>
                <tbody>
                  {symbolDecayAttributionSummary.map((row, idx) => (
                    <tr key={`${row.run_id}:${row.symbol}:${idx}`}>
                      <td className="mono-cell text-sm">{shortId(row.run_id)}</td>
                      <td className="text-sm"><strong>{row.symbol}</strong></td>
                      <td>
                        <span className={FAMILY_BADGE[row.dependency_family] ?? "badge-muted"}>
                          {row.dependency_family}
                        </span>
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
                      <td className="text-sm">{fmtScore(row.persistence_adjusted_symbol_score)}</td>
                      <td className="text-sm">{fmtMultiplier(row.decay_weight)}</td>
                      <td className="text-sm">
                        <strong>{fmtScore(row.decay_adjusted_symbol_score)}</strong>
                      </td>
                      <td className="text-sm">{row.symbol_rank ?? "—"}</td>
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

          {/* ── Decay Attribution Summary ──────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Decay Attribution Summary</h3>
                <p className="panel-subtitle">
                  Run-level comparison: raw → weighted → regime → timing → transition → archetype → cluster → persistence → decay,
                  with dominant family shifts and freshness context.
                </p>
              </div>
            </div>
            {runDecayAttributionSummary.length === 0 ? (
              <p className="muted">No run decay-attribution rows yet.</p>
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
                    <th>Decay</th>
                    <th>Decay dominant</th>
                    <th>Freshness</th>
                    <th>Stale</th>
                    <th>Contradicted</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {runDecayAttributionSummary.map((row) => (
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
                      <td className="text-sm">
                        <strong>{fmtScore(row.decay_adjusted_cross_asset_contribution)}</strong>
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
