"use client";

import type {
  CrossAssetExplanationSummaryRow,
  CrossAssetFamilyExplanationSummaryRow,
  RunCrossAssetExplanationBridgeRow,
} from "@/lib/queries/metrics";

type Props = {
  explanationSummary: CrossAssetExplanationSummaryRow[];
  familySummary: CrossAssetFamilyExplanationSummaryRow[];
  runBridgeSummary: RunCrossAssetExplanationBridgeRow[];
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
  missing_context: "badge-red",
  stale_context: "badge-yellow",
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

function confidenceBadge(value: number | string | null | undefined): string {
  if (value === null || value === undefined) return "badge-muted";
  const n = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(n)) return "badge-muted";
  if (n >= 0.6) return "badge-green";
  if (n >= 0.3) return "badge-yellow";
  return "badge-red";
}

function shortId(id: string | null | undefined): string {
  if (!id) return "—";
  return `${id.slice(0, 8)}…`;
}

function symbolList(syms: string[] | null | undefined, maxShown = 6): string {
  if (!syms || syms.length === 0) return "—";
  if (syms.length <= maxShown) return syms.join(", ");
  return `${syms.slice(0, maxShown).join(", ")} +${syms.length - maxShown}`;
}

export function CrossAssetExplainabilityPanel({
  explanationSummary,
  familySummary,
  runBridgeSummary,
  loading,
}: Props) {
  const topExplanation = explanationSummary[0];

  return (
    <section className="section">
      <div className="panel-header">
        <div>
          <h2>Cross-Asset Explainability</h2>
          <p className="panel-subtitle">
            Phase 4.0D. Compact explanation over 4.0C cross-asset signals: dominant family,
            confidence, ranked confirming/contradicting symbols, and explicit missing/stale context.
          </p>
        </div>
      </div>

      {loading && <p className="muted">Loading cross-asset explainability…</p>}

      {!loading && (
        <>
          {/* ── Top-level explanation (latest per watchlist) ───────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Cross-Asset Explanation</h3>
                <p className="panel-subtitle">
                  Latest explanation emitted per (watchlist, run). Confidence clipped to [0, 1];
                  never substitutes zero for missing data.
                </p>
              </div>
            </div>
            {explanationSummary.length === 0 ? (
              <p className="muted">No cross-asset explanations persisted yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Watchlist</th>
                    <th>Run</th>
                    <th>Dominant family</th>
                    <th>Confidence</th>
                    <th>Confirmed</th>
                    <th>Contradicted</th>
                    <th>Missing</th>
                    <th>Stale</th>
                    <th>State</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {explanationSummary.map((row, idx) => (
                    <tr key={`${row.watchlist_id}:${row.run_id ?? "none"}:${idx}`}>
                      <td className="mono-cell text-sm">{shortId(row.watchlist_id)}</td>
                      <td className="mono-cell text-sm">{shortId(row.run_id)}</td>
                      <td>
                        {row.dominant_dependency_family
                          ? <span className={FAMILY_BADGE[row.dominant_dependency_family] ?? "badge-muted"}>
                              {row.dominant_dependency_family}
                            </span>
                          : <span className="badge-muted">—</span>}
                      </td>
                      <td>
                        <span className={confidenceBadge(row.cross_asset_confidence_score)}>
                          {fmtScore(row.cross_asset_confidence_score)}
                        </span>
                      </td>
                      <td className="text-sm">{fmtScore(row.confirmation_score)}</td>
                      <td className="text-sm">{fmtScore(row.contradiction_score)}</td>
                      <td className="text-sm">{fmtScore(row.missing_context_score)}</td>
                      <td className="text-sm">{fmtScore(row.stale_context_score)}</td>
                      <td>
                        <span className={STATE_BADGE[row.explanation_state] ?? "badge-muted"}>
                          {row.explanation_state}
                        </span>
                      </td>
                      <td className="text-sm muted">{fmtTs(row.created_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* ── Top confirming / contradicting / missing / stale ───────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Top Confirming / Contradicting / Missing / Stale</h3>
                <p className="panel-subtitle">
                  Ranked symbol lists from the most recent explanation. Deterministic ordering:
                  count desc, then symbol alphabetical.
                </p>
              </div>
            </div>
            {!topExplanation ? (
              <p className="muted">No explanation available yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Category</th>
                    <th>Symbols</th>
                  </tr>
                </thead>
                <tbody>
                  <tr>
                    <td><span className="badge-green">confirming</span></td>
                    <td className="mono-cell text-sm">{symbolList(topExplanation.top_confirming_symbols)}</td>
                  </tr>
                  <tr>
                    <td><span className="badge-red">contradicting</span></td>
                    <td className="mono-cell text-sm">{symbolList(topExplanation.top_contradicting_symbols)}</td>
                  </tr>
                  <tr>
                    <td><span className="badge-red">missing</span></td>
                    <td className="mono-cell text-sm">{symbolList(topExplanation.missing_dependency_symbols)}</td>
                  </tr>
                  <tr>
                    <td><span className="badge-yellow">stale</span></td>
                    <td className="mono-cell text-sm">{symbolList(topExplanation.stale_dependency_symbols)}</td>
                  </tr>
                </tbody>
              </table>
            )}
          </div>

          {/* ── Family Contributions ───────────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Family Contributions</h3>
                <p className="panel-subtitle">Per-dependency-family support, contradiction, and confidence.</p>
              </div>
            </div>
            {familySummary.length === 0 ? (
              <p className="muted">No family contribution rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Watchlist</th>
                    <th>Run</th>
                    <th>Family</th>
                    <th>Signals</th>
                    <th>Confirmed</th>
                    <th>Contradicted</th>
                    <th>Missing</th>
                    <th>Stale</th>
                    <th>Support</th>
                    <th>Contradiction</th>
                    <th>Confidence</th>
                    <th>Top symbols</th>
                  </tr>
                </thead>
                <tbody>
                  {familySummary.map((row, idx) => (
                    <tr key={`${row.watchlist_id}:${row.run_id ?? "none"}:${row.dependency_family}:${idx}`}>
                      <td className="mono-cell text-sm">{shortId(row.watchlist_id)}</td>
                      <td className="mono-cell text-sm">{shortId(row.run_id)}</td>
                      <td>
                        <span className={FAMILY_BADGE[row.dependency_family] ?? "badge-muted"}>
                          {row.dependency_family}
                        </span>
                      </td>
                      <td className="text-sm">{row.family_signal_count}</td>
                      <td className="text-sm">{row.confirmed_count}</td>
                      <td className="text-sm">{row.contradicted_count}</td>
                      <td className="text-sm">{row.missing_count}</td>
                      <td className="text-sm">{row.stale_count}</td>
                      <td className="text-sm">{fmtScore(row.family_support_score)}</td>
                      <td className="text-sm">{fmtScore(row.family_contradiction_score)}</td>
                      <td>
                        <span className={confidenceBadge(row.family_confidence_score)}>
                          {fmtScore(row.family_confidence_score)}
                        </span>
                      </td>
                      <td className="mono-cell text-sm">{symbolList(row.top_symbols, 4)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* ── Run Bridge ─────────────────────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Run Explanation Bridge</h3>
                <p className="panel-subtitle">
                  One compact explanation row per run. Ready for future run-inspection integration.
                </p>
              </div>
            </div>
            {runBridgeSummary.length === 0 ? (
              <p className="muted">No run-linked explanations yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Run</th>
                    <th>Watchlist</th>
                    <th>Dominant family</th>
                    <th>Confidence</th>
                    <th>Confirmed</th>
                    <th>Contradicted</th>
                    <th>Missing</th>
                    <th>Stale</th>
                    <th>State</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {runBridgeSummary.map((row) => (
                    <tr key={row.run_id}>
                      <td className="mono-cell text-sm">{shortId(row.run_id)}</td>
                      <td className="mono-cell text-sm">{shortId(row.watchlist_id)}</td>
                      <td>
                        {row.dominant_dependency_family
                          ? <span className={FAMILY_BADGE[row.dominant_dependency_family] ?? "badge-muted"}>
                              {row.dominant_dependency_family}
                            </span>
                          : <span className="badge-muted">—</span>}
                      </td>
                      <td>
                        <span className={confidenceBadge(row.cross_asset_confidence_score)}>
                          {fmtScore(row.cross_asset_confidence_score)}
                        </span>
                      </td>
                      <td className="text-sm">{fmtScore(row.confirmation_score)}</td>
                      <td className="text-sm">{fmtScore(row.contradiction_score)}</td>
                      <td className="text-sm">{fmtScore(row.missing_context_score)}</td>
                      <td className="text-sm">{fmtScore(row.stale_context_score)}</td>
                      <td>
                        <span className={STATE_BADGE[row.explanation_state] ?? "badge-muted"}>
                          {row.explanation_state}
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
