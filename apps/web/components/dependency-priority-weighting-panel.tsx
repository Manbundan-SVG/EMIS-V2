"use client";

import type {
  CrossAssetFamilyWeightedAttributionSummaryRow,
  CrossAssetSymbolWeightedAttributionSummaryRow,
  RunCrossAssetWeightedIntegrationSummaryRow,
} from "@/lib/queries/metrics";

type Props = {
  familyWeightedSummary: CrossAssetFamilyWeightedAttributionSummaryRow[];
  symbolWeightedSummary: CrossAssetSymbolWeightedAttributionSummaryRow[];
  runWeightedSummary: RunCrossAssetWeightedIntegrationSummaryRow[];
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
  raw: string | null | undefined,
  weighted: string | null | undefined,
): string {
  if (!raw && !weighted) return "badge-muted";
  if (!raw || !weighted) return "badge-yellow";
  return raw === weighted ? "badge-green" : "badge-yellow";
}

function shortId(id: string | null | undefined): string {
  if (!id) return "—";
  return `${id.slice(0, 8)}…`;
}

function symbolList(syms: string[] | null | undefined, maxShown = 5): string {
  if (!syms || syms.length === 0) return "—";
  if (syms.length <= maxShown) return syms.join(", ");
  return `${syms.slice(0, maxShown).join(", ")} +${syms.length - maxShown}`;
}

export function DependencyPriorityWeightingPanel({
  familyWeightedSummary,
  symbolWeightedSummary,
  runWeightedSummary,
  loading,
}: Props) {
  return (
    <section className="section">
      <div className="panel-header">
        <div>
          <h2>Dependency-Priority Weighting</h2>
          <p className="panel-subtitle">
            Phase 4.1B. Weighted refinement of 4.1A raw attribution: priority × family × type ×
            coverage, total multiplier clipped to [0.5, 1.25]. Raw and weighted scores persist
            side-by-side.
          </p>
        </div>
      </div>

      {loading && <p className="muted">Loading weighted attribution…</p>}

      {!loading && (
        <>
          {/* ── Weighted Family Attribution ────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Weighted Family Attribution</h3>
                <p className="panel-subtitle">Per-family weight channels and weighted net contribution.</p>
              </div>
            </div>
            {familyWeightedSummary.length === 0 ? (
              <p className="muted">No weighted family attribution rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Rank</th>
                    <th>Run</th>
                    <th>Family</th>
                    <th>Raw net</th>
                    <th>Priority</th>
                    <th>Family</th>
                    <th>Type</th>
                    <th>Coverage</th>
                    <th>Weighted net</th>
                    <th>Top symbols</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {familyWeightedSummary.map((row, idx) => (
                    <tr key={`${row.run_id}:${row.dependency_family}:${idx}`}>
                      <td className="text-sm">{row.weighted_family_rank ?? "—"}</td>
                      <td className="mono-cell text-sm">{shortId(row.run_id)}</td>
                      <td>
                        <span className={FAMILY_BADGE[row.dependency_family] ?? "badge-muted"}>
                          {row.dependency_family}
                        </span>
                      </td>
                      <td className="text-sm">{fmtScore(row.raw_family_net_contribution)}</td>
                      <td className="text-sm">{fmtWeight(row.priority_weight)}</td>
                      <td className="text-sm">{fmtWeight(row.family_weight)}</td>
                      <td className="text-sm">{fmtWeight(row.type_weight)}</td>
                      <td className="text-sm">{fmtWeight(row.coverage_weight)}</td>
                      <td>
                        <span className={netBadge(row.weighted_family_net_contribution)}>
                          {fmtScore(row.weighted_family_net_contribution)}
                        </span>
                      </td>
                      <td className="mono-cell text-sm">{symbolList(row.top_symbols)}</td>
                      <td className="text-sm muted">{fmtTs(row.created_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* ── Weighted Symbol Attribution ────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Weighted Symbol Attribution</h3>
                <p className="panel-subtitle">
                  Per-symbol weighted scores. Graph priority and dependency type drive weighting;
                  direct/secondary flag is preserved for future multi-hop expansion.
                </p>
              </div>
            </div>
            {symbolWeightedSummary.length === 0 ? (
              <p className="muted">No weighted symbol attribution rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Rank</th>
                    <th>Run</th>
                    <th>Symbol</th>
                    <th>Family</th>
                    <th>Type</th>
                    <th>Priority</th>
                    <th>Direct?</th>
                    <th>Raw</th>
                    <th>P wt</th>
                    <th>F wt</th>
                    <th>T wt</th>
                    <th>C wt</th>
                    <th>Weighted</th>
                  </tr>
                </thead>
                <tbody>
                  {symbolWeightedSummary.slice(0, 80).map((row, idx) => (
                    <tr key={`${row.run_id}:${row.symbol}:${idx}`}>
                      <td className="text-sm">{row.symbol_rank ?? "—"}</td>
                      <td className="mono-cell text-sm">{shortId(row.run_id)}</td>
                      <td className="mono-cell text-sm">{row.symbol}</td>
                      <td>
                        <span className={FAMILY_BADGE[row.dependency_family] ?? "badge-muted"}>
                          {row.dependency_family}
                        </span>
                      </td>
                      <td className="text-sm">{row.dependency_type ?? "—"}</td>
                      <td className="text-sm">{row.graph_priority ?? "—"}</td>
                      <td>
                        {row.is_direct_dependency
                          ? <span className="badge-green">direct</span>
                          : <span className="badge-muted">secondary</span>}
                      </td>
                      <td className="text-sm">{fmtScore(row.raw_symbol_score)}</td>
                      <td className="text-sm">{fmtWeight(row.priority_weight)}</td>
                      <td className="text-sm">{fmtWeight(row.family_weight)}</td>
                      <td className="text-sm">{fmtWeight(row.type_weight)}</td>
                      <td className="text-sm">{fmtWeight(row.coverage_weight)}</td>
                      <td>
                        <span className={netBadge(row.weighted_symbol_score)}>
                          {fmtScore(row.weighted_symbol_score)}
                        </span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* ── Weighted Integration Summary ───────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Weighted Integration Summary</h3>
                <p className="panel-subtitle">
                  Raw vs weighted net contribution and dominant family shift per run.
                </p>
              </div>
            </div>
            {runWeightedSummary.length === 0 ? (
              <p className="muted">No weighted integration rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Run</th>
                    <th>Watchlist</th>
                    <th>Base</th>
                    <th>Raw net</th>
                    <th>Weighted net</th>
                    <th>Raw dominant</th>
                    <th>Weighted dominant</th>
                    <th>Shift</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {runWeightedSummary.map((row) => (
                    <tr key={row.run_id}>
                      <td className="mono-cell text-sm">{shortId(row.run_id)}</td>
                      <td className="mono-cell text-sm">{shortId(row.watchlist_id)}</td>
                      <td className="text-sm">{fmtScore(row.base_signal_score)}</td>
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
                        <span className={shiftBadge(row.dominant_dependency_family, row.weighted_dominant_dependency_family)}>
                          {row.dominant_dependency_family === row.weighted_dominant_dependency_family
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
