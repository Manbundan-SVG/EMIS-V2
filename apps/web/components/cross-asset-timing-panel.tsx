"use client";

import type {
  CrossAssetLeadLagPairSummaryRow,
  CrossAssetFamilyTimingSummaryRow,
  RunCrossAssetTimingSummaryRow,
} from "@/lib/queries/metrics";

type Props = {
  pairSummary: CrossAssetLeadLagPairSummaryRow[];
  familyTimingSummary: CrossAssetFamilyTimingSummaryRow[];
  runTimingSummary: RunCrossAssetTimingSummaryRow[];
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

function fmtScore(value: number | string | null | undefined, digits = 3): string {
  if (value === null || value === undefined) return "—";
  const n = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(n)) return "—";
  return n.toFixed(digits);
}

function fmtLagHours(value: number | null): string {
  if (value === null || value === undefined) return "—";
  if (value === 0) return "0h";
  const sign = value > 0 ? "+" : "";
  return `${sign}${value}h`;
}

function strengthBadge(value: number | string | null | undefined): string {
  if (value === null || value === undefined) return "badge-muted";
  const n = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(n)) return "badge-muted";
  if (n >= 0.5) return "badge-green";
  if (n >= 0.25) return "badge-yellow";
  return "badge-muted";
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

export function CrossAssetTimingPanel({
  pairSummary,
  familyTimingSummary,
  runTimingSummary,
  loading,
}: Props) {
  return (
    <section className="section">
      <div className="panel-header">
        <div>
          <h2>Cross-Asset Lead/Lag Timing</h2>
          <p className="panel-subtitle">
            Phase 4.2A. Deterministic lagged-correlation measurement between base watchlist
            symbols and dependency context. Negative lag = dependency leads base. Descriptive
            timing only — no forecasting or causality claims.
          </p>
        </div>
      </div>

      {loading && <p className="muted">Loading cross-asset timing…</p>}

      {!loading && (
        <>
          {/* ── Run Timing Summary ─────────────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Run Timing Summary</h3>
                <p className="panel-subtitle">
                  One compact row per run with dominant leading family and strongest leading symbol.
                </p>
              </div>
            </div>
            {runTimingSummary.length === 0 ? (
              <p className="muted">No run-linked timing summaries yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Run</th>
                    <th>Watchlist</th>
                    <th>Lead</th>
                    <th>Coincident</th>
                    <th>Lag</th>
                    <th>Dominant leading family</th>
                    <th>Strongest leading symbol</th>
                    <th>Avg strength</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {runTimingSummary.map((row) => (
                    <tr key={row.run_id}>
                      <td className="mono-cell text-sm">{shortId(row.run_id)}</td>
                      <td className="mono-cell text-sm">{shortId(row.watchlist_id)}</td>
                      <td className="text-sm">{row.lead_pair_count}</td>
                      <td className="text-sm">{row.coincident_pair_count}</td>
                      <td className="text-sm">{row.lag_pair_count}</td>
                      <td>
                        {row.dominant_leading_family
                          ? <span className={FAMILY_BADGE[row.dominant_leading_family] ?? "badge-muted"}>
                              {row.dominant_leading_family}
                            </span>
                          : <span className="badge-muted">—</span>}
                      </td>
                      <td className="mono-cell text-sm">{row.strongest_leading_symbol ?? "—"}</td>
                      <td>
                        <span className={strengthBadge(row.avg_timing_strength)}>
                          {fmtScore(row.avg_timing_strength)}
                        </span>
                      </td>
                      <td className="text-sm muted">{fmtTs(row.created_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* ── Family Timing Summary ──────────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Family Timing Summary</h3>
                <p className="panel-subtitle">
                  Per-family pair counts, avg best lag, and dominant timing class (tie-break by
                  count then family name).
                </p>
              </div>
            </div>
            {familyTimingSummary.length === 0 ? (
              <p className="muted">No family timing rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Run</th>
                    <th>Family</th>
                    <th>Lead</th>
                    <th>Coincident</th>
                    <th>Lag</th>
                    <th>Avg best lag</th>
                    <th>Avg strength</th>
                    <th>Dominant class</th>
                    <th>Top leading</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {familyTimingSummary.map((row, idx) => (
                    <tr key={`${row.run_id ?? "none"}:${row.dependency_family}:${idx}`}>
                      <td className="mono-cell text-sm">{shortId(row.run_id)}</td>
                      <td>
                        <span className={FAMILY_BADGE[row.dependency_family] ?? "badge-muted"}>
                          {row.dependency_family}
                        </span>
                      </td>
                      <td className="text-sm">{row.lead_pair_count}</td>
                      <td className="text-sm">{row.coincident_pair_count}</td>
                      <td className="text-sm">{row.lag_pair_count}</td>
                      <td className="text-sm">{fmtScore(row.avg_best_lag_hours, 1)}</td>
                      <td>
                        <span className={strengthBadge(row.avg_timing_strength)}>
                          {fmtScore(row.avg_timing_strength)}
                        </span>
                      </td>
                      <td>
                        <span className={BUCKET_BADGE[row.dominant_timing_class] ?? "badge-muted"}>
                          {row.dominant_timing_class}
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

          {/* ── Lead/Lag Pair Summary ──────────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Lead/Lag Pair Summary</h3>
                <p className="panel-subtitle">
                  Pairwise timing: best_lag_hours negative = dependency leads base. Strength is the
                  absolute correlation at the best lag on the chosen resolution grid.
                </p>
              </div>
            </div>
            {pairSummary.length === 0 ? (
              <p className="muted">No pair timing rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Base</th>
                    <th>Dependency</th>
                    <th>Family</th>
                    <th>Type</th>
                    <th>Bucket</th>
                    <th>Best lag</th>
                    <th>Strength</th>
                    <th>Corr @ best</th>
                    <th>Window</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {pairSummary.slice(0, 80).map((row, idx) => (
                    <tr key={`${row.run_id ?? "none"}:${row.base_symbol}:${row.dependency_symbol}:${idx}`}>
                      <td className="mono-cell text-sm">{row.base_symbol}</td>
                      <td className="mono-cell text-sm">{row.dependency_symbol}</td>
                      <td>
                        <span className={FAMILY_BADGE[row.dependency_family] ?? "badge-muted"}>
                          {row.dependency_family}
                        </span>
                      </td>
                      <td className="text-sm">{row.dependency_type ?? "—"}</td>
                      <td>
                        <span className={BUCKET_BADGE[row.lag_bucket] ?? "badge-muted"}>
                          {row.lag_bucket}
                        </span>
                      </td>
                      <td className="text-sm">{fmtLagHours(row.best_lag_hours)}</td>
                      <td>
                        <span className={strengthBadge(row.timing_strength)}>
                          {fmtScore(row.timing_strength)}
                        </span>
                      </td>
                      <td className="text-sm">{fmtScore(row.correlation_at_best_lag)}</td>
                      <td className="text-sm">{row.window_label}</td>
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
