"use client";

import type {
  CrossAssetFamilyTimingAttributionSummaryRow,
  CrossAssetSymbolTimingAttributionSummaryRow,
  RunCrossAssetTimingAttributionSummaryRow,
} from "@/lib/queries/metrics";

type Props = {
  familyTimingAttributionSummary: CrossAssetFamilyTimingAttributionSummaryRow[];
  symbolTimingAttributionSummary: CrossAssetSymbolTimingAttributionSummaryRow[];
  runTimingAttributionSummary: RunCrossAssetTimingAttributionSummaryRow[];
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

const BUCKET_BADGE: Record<string, string> = {
  lead: "badge-green",
  coincident: "badge-muted",
  lag: "badge-yellow",
  insufficient_data: "badge-red",
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

function fmtLagHours(value: number | null): string {
  if (value === null || value === undefined) return "—";
  if (value === 0) return "0h";
  const sign = value > 0 ? "+" : "";
  return `${sign}${value}h`;
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

export function CrossAssetTimingAttributionPanel({
  familyTimingAttributionSummary,
  symbolTimingAttributionSummary,
  runTimingAttributionSummary,
  loading,
}: Props) {
  return (
    <section className="section">
      <div className="panel-header">
        <div>
          <h2>Timing-Aware Cross-Asset Attribution</h2>
          <p className="panel-subtitle">
            Phase 4.2B. Conditions regime-adjusted contributions on 4.2A timing class. Multipliers
            clipped to [0.75, 1.15]. Lead bonus and lag penalty are small, sign-aware, and
            explicit. Raw / weighted / regime / timing layers persist side-by-side.
          </p>
        </div>
      </div>

      {loading && <p className="muted">Loading timing-aware attribution…</p>}

      {!loading && (
        <>
          {/* ── Family Timing Attribution ──────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Timing-Aware Family Attribution</h3>
                <p className="panel-subtitle">
                  Per-family timing class, adjusted contribution, and rank.
                </p>
              </div>
            </div>
            {familyTimingAttributionSummary.length === 0 ? (
              <p className="muted">No timing-aware family attribution rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Rank</th>
                    <th>Run</th>
                    <th>Family</th>
                    <th>Timing class</th>
                    <th>Raw</th>
                    <th>Weighted</th>
                    <th>Regime</th>
                    <th>Class wt</th>
                    <th>Bonus</th>
                    <th>Penalty</th>
                    <th>Timing adj</th>
                    <th>Top leading</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {familyTimingAttributionSummary.map((row, idx) => (
                    <tr key={`${row.run_id}:${row.dependency_family}:${idx}`}>
                      <td className="text-sm">{row.timing_family_rank ?? "—"}</td>
                      <td className="mono-cell text-sm">{shortId(row.run_id)}</td>
                      <td>
                        <span className={FAMILY_BADGE[row.dependency_family] ?? "badge-muted"}>
                          {row.dependency_family}
                        </span>
                      </td>
                      <td>
                        <span className={BUCKET_BADGE[row.dominant_timing_class] ?? "badge-muted"}>
                          {row.dominant_timing_class}
                        </span>
                      </td>
                      <td className="text-sm">{fmtScore(row.raw_family_net_contribution)}</td>
                      <td className="text-sm">{fmtScore(row.weighted_family_net_contribution)}</td>
                      <td className="text-sm">{fmtScore(row.regime_adjusted_family_contribution)}</td>
                      <td className="text-sm">{fmtWeight(row.timing_class_weight)}</td>
                      <td className="text-sm">{fmtScore(row.timing_bonus)}</td>
                      <td className="text-sm">{fmtScore(row.timing_penalty)}</td>
                      <td>
                        <span className={netBadge(row.timing_adjusted_family_contribution)}>
                          {fmtScore(row.timing_adjusted_family_contribution)}
                        </span>
                      </td>
                      <td className="mono-cell text-sm">{symbolList(row.top_leading_symbols)}</td>
                      <td className="text-sm muted">{fmtTs(row.created_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* ── Symbol Timing Attribution ──────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Timing-Aware Symbol Attribution</h3>
                <p className="panel-subtitle">
                  Per-symbol lag bucket, timing weight, and adjusted score, ranked across families.
                </p>
              </div>
            </div>
            {symbolTimingAttributionSummary.length === 0 ? (
              <p className="muted">No timing-aware symbol attribution rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Rank</th>
                    <th>Run</th>
                    <th>Symbol</th>
                    <th>Family</th>
                    <th>Lag bucket</th>
                    <th>Best lag</th>
                    <th>Raw</th>
                    <th>Weighted</th>
                    <th>Regime</th>
                    <th>Class wt</th>
                    <th>Timing adj</th>
                  </tr>
                </thead>
                <tbody>
                  {symbolTimingAttributionSummary.slice(0, 80).map((row, idx) => (
                    <tr key={`${row.run_id}:${row.symbol}:${idx}`}>
                      <td className="text-sm">{row.symbol_rank ?? "—"}</td>
                      <td className="mono-cell text-sm">{shortId(row.run_id)}</td>
                      <td className="mono-cell text-sm">{row.symbol}</td>
                      <td>
                        <span className={FAMILY_BADGE[row.dependency_family] ?? "badge-muted"}>
                          {row.dependency_family}
                        </span>
                      </td>
                      <td>
                        <span className={BUCKET_BADGE[row.lag_bucket] ?? "badge-muted"}>
                          {row.lag_bucket}
                        </span>
                      </td>
                      <td className="text-sm">{fmtLagHours(row.best_lag_hours)}</td>
                      <td className="text-sm">{fmtScore(row.raw_symbol_score)}</td>
                      <td className="text-sm">{fmtScore(row.weighted_symbol_score)}</td>
                      <td className="text-sm">{fmtScore(row.regime_adjusted_symbol_score)}</td>
                      <td className="text-sm">{fmtWeight(row.timing_class_weight)}</td>
                      <td>
                        <span className={netBadge(row.timing_adjusted_symbol_score)}>
                          {fmtScore(row.timing_adjusted_symbol_score)}
                        </span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* ── Run Timing Integration Summary ─────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Timing Integration Summary</h3>
                <p className="panel-subtitle">
                  Raw vs weighted vs regime vs timing aggregates per run; dominant family shift
                  across all four layers.
                </p>
              </div>
            </div>
            {runTimingAttributionSummary.length === 0 ? (
              <p className="muted">No timing integration rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Run</th>
                    <th>Watchlist</th>
                    <th>Raw net</th>
                    <th>Weighted net</th>
                    <th>Regime net</th>
                    <th>Timing net</th>
                    <th>Raw dominant</th>
                    <th>Wt dominant</th>
                    <th>Regime dominant</th>
                    <th>Timing dominant</th>
                    <th>Shift (regime→timing)</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {runTimingAttributionSummary.map((row) => (
                    <tr key={row.run_id}>
                      <td className="mono-cell text-sm">{shortId(row.run_id)}</td>
                      <td className="mono-cell text-sm">{shortId(row.watchlist_id)}</td>
                      <td>
                        <span className={netBadge(row.cross_asset_net_contribution)}>
                          {fmtScore(row.cross_asset_net_contribution)}
                        </span>
                      </td>
                      <td>
                        <span className={netBadge(row.weighted_cross_asset_net_contribution)}>
                          {fmtScore(row.weighted_cross_asset_net_contribution)}
                        </span>
                      </td>
                      <td>
                        <span className={netBadge(row.regime_adjusted_cross_asset_contribution)}>
                          {fmtScore(row.regime_adjusted_cross_asset_contribution)}
                        </span>
                      </td>
                      <td>
                        <span className={netBadge(row.timing_adjusted_cross_asset_contribution)}>
                          {fmtScore(row.timing_adjusted_cross_asset_contribution)}
                        </span>
                      </td>
                      <td>
                        {row.dominant_dependency_family
                          ? <span className={FAMILY_BADGE[row.dominant_dependency_family] ?? "badge-muted"}>
                              {row.dominant_dependency_family}
                            </span>
                          : <span className="badge-muted">—</span>}
                      </td>
                      <td>
                        {row.weighted_dominant_dependency_family
                          ? <span className={FAMILY_BADGE[row.weighted_dominant_dependency_family] ?? "badge-muted"}>
                              {row.weighted_dominant_dependency_family}
                            </span>
                          : <span className="badge-muted">—</span>}
                      </td>
                      <td>
                        {row.regime_dominant_dependency_family
                          ? <span className={FAMILY_BADGE[row.regime_dominant_dependency_family] ?? "badge-muted"}>
                              {row.regime_dominant_dependency_family}
                            </span>
                          : <span className="badge-muted">—</span>}
                      </td>
                      <td>
                        {row.timing_dominant_dependency_family
                          ? <span className={FAMILY_BADGE[row.timing_dominant_dependency_family] ?? "badge-muted"}>
                              {row.timing_dominant_dependency_family}
                            </span>
                          : <span className="badge-muted">—</span>}
                      </td>
                      <td>
                        <span className={shiftBadge(row.regime_dominant_dependency_family, row.timing_dominant_dependency_family)}>
                          {row.regime_dominant_dependency_family === row.timing_dominant_dependency_family
                            ? "stable"
                            : "shifted"}
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
