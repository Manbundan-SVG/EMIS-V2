"use client";

import type {
  CrossAssetFamilyRegimeAttributionSummaryRow,
  CrossAssetSymbolRegimeAttributionSummaryRow,
  RunCrossAssetRegimeIntegrationSummaryRow,
} from "@/lib/queries/metrics";

type Props = {
  familyRegimeSummary: CrossAssetFamilyRegimeAttributionSummaryRow[];
  symbolRegimeSummary: CrossAssetSymbolRegimeAttributionSummaryRow[];
  runRegimeSummary: RunCrossAssetRegimeIntegrationSummaryRow[];
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

const STATE_BADGE: Record<string, string> = {
  computed: "badge-green",
  partial: "badge-yellow",
  missing_regime: "badge-red",
  regime_mismatch: "badge-yellow",
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

function shortId(id: string | null | undefined): string {
  if (!id) return "—";
  return `${id.slice(0, 8)}…`;
}

function symbolList(syms: string[] | null | undefined, maxShown = 5): string {
  if (!syms || syms.length === 0) return "—";
  if (syms.length <= maxShown) return syms.join(", ");
  return `${syms.slice(0, maxShown).join(", ")} +${syms.length - maxShown}`;
}

function shiftBadge(
  weighted: string | null | undefined,
  regime: string | null | undefined,
): string {
  if (!weighted && !regime) return "badge-muted";
  if (!weighted || !regime) return "badge-yellow";
  return weighted === regime ? "badge-green" : "badge-yellow";
}

export function RegimeAwareCrossAssetPanel({
  familyRegimeSummary,
  symbolRegimeSummary,
  runRegimeSummary,
  loading,
}: Props) {
  return (
    <section className="section">
      <div className="panel-header">
        <div>
          <h2>Regime-Aware Cross-Asset Interpretation</h2>
          <p className="panel-subtitle">
            Phase 4.1C. Conditions 4.1B weighted attribution on the active regime from
            regime_transition_events. Regime multipliers clipped to [0.75, 1.25]; direction scales
            to [0.50, 1.50]. Raw / weighted / regime-adjusted persist side-by-side.
          </p>
        </div>
      </div>

      {loading && <p className="muted">Loading regime-aware attribution…</p>}

      {!loading && (
        <>
          {/* ── Regime-Aware Family Attribution ────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Regime-Aware Family Attribution</h3>
                <p className="panel-subtitle">
                  Per-family regime weights, direction/penalty scales, and regime-adjusted contribution.
                </p>
              </div>
            </div>
            {familyRegimeSummary.length === 0 ? (
              <p className="muted">No regime-aware family attribution rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Rank</th>
                    <th>Run</th>
                    <th>Regime</th>
                    <th>Family</th>
                    <th>Raw</th>
                    <th>Weighted</th>
                    <th>Fam wt</th>
                    <th>Type wt</th>
                    <th>Conf</th>
                    <th>Contra</th>
                    <th>Miss</th>
                    <th>Stale</th>
                    <th>Regime adj</th>
                    <th>State</th>
                    <th>Top symbols</th>
                  </tr>
                </thead>
                <tbody>
                  {familyRegimeSummary.map((row, idx) => (
                    <tr key={`${row.run_id}:${row.dependency_family}:${idx}`}>
                      <td className="text-sm">{row.regime_family_rank ?? "—"}</td>
                      <td className="mono-cell text-sm">{shortId(row.run_id)}</td>
                      <td className="text-sm">{row.regime_key}</td>
                      <td>
                        <span className={FAMILY_BADGE[row.dependency_family] ?? "badge-muted"}>
                          {row.dependency_family}
                        </span>
                      </td>
                      <td className="text-sm">{fmtScore(row.raw_family_net_contribution)}</td>
                      <td className="text-sm">{fmtScore(row.weighted_family_net_contribution)}</td>
                      <td className="text-sm">{fmtWeight(row.regime_family_weight)}</td>
                      <td className="text-sm">{fmtWeight(row.regime_type_weight)}</td>
                      <td className="text-sm">{fmtWeight(row.regime_confirmation_scale)}</td>
                      <td className="text-sm">{fmtWeight(row.regime_contradiction_scale)}</td>
                      <td className="text-sm">{fmtWeight(row.regime_missing_penalty_scale)}</td>
                      <td className="text-sm">{fmtWeight(row.regime_stale_penalty_scale)}</td>
                      <td>
                        <span className={netBadge(row.regime_adjusted_family_contribution)}>
                          {fmtScore(row.regime_adjusted_family_contribution)}
                        </span>
                      </td>
                      <td>
                        <span className={STATE_BADGE[row.interpretation_state] ?? "badge-muted"}>
                          {row.interpretation_state}
                        </span>
                      </td>
                      <td className="mono-cell text-sm">{symbolList(row.top_symbols)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* ── Regime-Aware Symbol Attribution ────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Regime-Aware Symbol Attribution</h3>
                <p className="panel-subtitle">
                  Per-symbol regime-adjusted scores, ranked across families for the run.
                </p>
              </div>
            </div>
            {symbolRegimeSummary.length === 0 ? (
              <p className="muted">No regime-aware symbol attribution rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Rank</th>
                    <th>Run</th>
                    <th>Regime</th>
                    <th>Symbol</th>
                    <th>Family</th>
                    <th>Type</th>
                    <th>Priority</th>
                    <th>Raw</th>
                    <th>Weighted</th>
                    <th>Fam wt</th>
                    <th>Type wt</th>
                    <th>Regime adj</th>
                  </tr>
                </thead>
                <tbody>
                  {symbolRegimeSummary.slice(0, 80).map((row, idx) => (
                    <tr key={`${row.run_id}:${row.symbol}:${idx}`}>
                      <td className="text-sm">{row.symbol_rank ?? "—"}</td>
                      <td className="mono-cell text-sm">{shortId(row.run_id)}</td>
                      <td className="text-sm">{row.regime_key}</td>
                      <td className="mono-cell text-sm">{row.symbol}</td>
                      <td>
                        <span className={FAMILY_BADGE[row.dependency_family] ?? "badge-muted"}>
                          {row.dependency_family}
                        </span>
                      </td>
                      <td className="text-sm">{row.dependency_type ?? "—"}</td>
                      <td className="text-sm">{row.graph_priority ?? "—"}</td>
                      <td className="text-sm">{fmtScore(row.raw_symbol_score)}</td>
                      <td className="text-sm">{fmtScore(row.weighted_symbol_score)}</td>
                      <td className="text-sm">{fmtWeight(row.regime_family_weight)}</td>
                      <td className="text-sm">{fmtWeight(row.regime_type_weight)}</td>
                      <td>
                        <span className={netBadge(row.regime_adjusted_symbol_score)}>
                          {fmtScore(row.regime_adjusted_symbol_score)}
                        </span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* ── Regime Integration Summary ─────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Regime Integration Summary</h3>
                <p className="panel-subtitle">
                  Raw vs weighted vs regime-adjusted aggregates and dominant family shift per run.
                </p>
              </div>
            </div>
            {runRegimeSummary.length === 0 ? (
              <p className="muted">No regime integration rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Run</th>
                    <th>Watchlist</th>
                    <th>Regime</th>
                    <th>Raw net</th>
                    <th>Weighted net</th>
                    <th>Regime net</th>
                    <th>Confidence</th>
                    <th>Raw dominant</th>
                    <th>Weighted dominant</th>
                    <th>Regime dominant</th>
                    <th>Shift</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {runRegimeSummary.map((row) => (
                    <tr key={row.run_id}>
                      <td className="mono-cell text-sm">{shortId(row.run_id)}</td>
                      <td className="mono-cell text-sm">{shortId(row.watchlist_id)}</td>
                      <td className="text-sm">{row.regime_key}</td>
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
                      <td className="text-sm">{fmtScore(row.cross_asset_confidence_score, 3)}</td>
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
                        <span className={shiftBadge(row.weighted_dominant_dependency_family, row.regime_dominant_dependency_family)}>
                          {row.weighted_dominant_dependency_family === row.regime_dominant_dependency_family
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
