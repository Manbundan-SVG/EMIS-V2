"use client";

import type {
  CrossAssetTimingCompositeSummaryRow,
  CrossAssetFamilyTimingCompositeSummaryRow,
  RunCrossAssetFinalIntegrationSummaryRow,
} from "@/lib/queries/metrics";

type Props = {
  timingCompositeSummary: CrossAssetTimingCompositeSummaryRow[];
  familyTimingCompositeSummary: CrossAssetFamilyTimingCompositeSummaryRow[];
  finalIntegrationSummary: RunCrossAssetFinalIntegrationSummaryRow[];
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

const MODE_BADGE: Record<string, string> = {
  timing_additive_guardrailed: "badge-green",
  lead_confirmation_only: "badge-muted",
  lag_suppression_only: "badge-yellow",
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

function fmtDelta(pre: number | string | null | undefined, post: number | string | null | undefined): string {
  if (pre === null || pre === undefined || post === null || post === undefined) return "—";
  const preN = typeof pre === "number" ? pre : Number(pre);
  const postN = typeof post === "number" ? post : Number(post);
  if (!Number.isFinite(preN) || !Number.isFinite(postN)) return "—";
  const d = postN - preN;
  const sign = d > 0 ? "+" : "";
  return `${sign}${d.toFixed(4)}`;
}

function netBadge(value: number | string | null | undefined): string {
  if (value === null || value === undefined) return "badge-muted";
  const n = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(n)) return "badge-muted";
  if (n > 0.03) return "badge-green";
  if (n < -0.03) return "badge-red";
  return "badge-muted";
}

function deltaBadge(pre: number | string | null | undefined, post: number | string | null | undefined): string {
  if (pre === null || pre === undefined || post === null || post === undefined) return "badge-muted";
  const preN = typeof pre === "number" ? pre : Number(pre);
  const postN = typeof post === "number" ? post : Number(post);
  if (!Number.isFinite(preN) || !Number.isFinite(postN)) return "badge-muted";
  const d = postN - preN;
  if (d > 0.005) return "badge-green";
  if (d < -0.005) return "badge-red";
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

export function CrossAssetTimingCompositePanel({
  timingCompositeSummary,
  familyTimingCompositeSummary,
  finalIntegrationSummary,
  loading,
}: Props) {
  return (
    <section className="section">
      <div className="panel-header">
        <div>
          <h2>Timing-Aware Composite Refinement</h2>
          <p className="panel-subtitle">
            Phase 4.2C. Refines the final integrated score with a bounded timing-aware delta
            conditioned on dominant timing class. Net contribution clipped to conservative band.
            Upstream raw → weighted → regime → timing layers preserved side-by-side.
          </p>
        </div>
      </div>

      {loading && <p className="muted">Loading timing-aware composite…</p>}

      {!loading && (
        <>
          {/* ── Timing-Aware Composite ─────────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Timing-Aware Composite</h3>
                <p className="panel-subtitle">
                  Pre-timing composite, timing net contribution, and post-timing composite per run.
                </p>
              </div>
            </div>
            {timingCompositeSummary.length === 0 ? (
              <p className="muted">No timing composite rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Run</th>
                    <th>Mode</th>
                    <th>Dominant class</th>
                    <th>Base</th>
                    <th>Raw net</th>
                    <th>Weighted net</th>
                    <th>Regime net</th>
                    <th>Timing adj</th>
                    <th>Pre-timing</th>
                    <th>Timing net</th>
                    <th>Post-timing</th>
                    <th>Δ</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {timingCompositeSummary.map((row) => (
                    <tr key={row.run_id}>
                      <td className="mono-cell text-sm">{shortId(row.run_id)}</td>
                      <td>
                        <span className={MODE_BADGE[row.integration_mode] ?? "badge-muted"}>
                          {row.integration_mode}
                        </span>
                      </td>
                      <td>
                        <span className={BUCKET_BADGE[row.dominant_timing_class] ?? "badge-muted"}>
                          {row.dominant_timing_class}
                        </span>
                      </td>
                      <td className="text-sm">{fmtScore(row.base_signal_score)}</td>
                      <td className="text-sm">{fmtScore(row.cross_asset_net_contribution)}</td>
                      <td className="text-sm">{fmtScore(row.weighted_cross_asset_net_contribution)}</td>
                      <td className="text-sm">{fmtScore(row.regime_adjusted_cross_asset_contribution)}</td>
                      <td className="text-sm">{fmtScore(row.timing_adjusted_cross_asset_contribution)}</td>
                      <td className="text-sm">{fmtScore(row.composite_pre_timing)}</td>
                      <td>
                        <span className={netBadge(row.timing_net_contribution)}>
                          {fmtScore(row.timing_net_contribution)}
                        </span>
                      </td>
                      <td className="text-sm">{fmtScore(row.composite_post_timing)}</td>
                      <td>
                        <span className={deltaBadge(row.composite_pre_timing, row.composite_post_timing)}>
                          {fmtDelta(row.composite_pre_timing, row.composite_post_timing)}
                        </span>
                      </td>
                      <td className="text-sm muted">{fmtTs(row.created_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* ── Family Timing Composite Contribution ───────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Family Timing Composite Contribution</h3>
                <p className="panel-subtitle">Per-family timing integration contribution, ranked.</p>
              </div>
            </div>
            {familyTimingCompositeSummary.length === 0 ? (
              <p className="muted">No family timing composite rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Rank</th>
                    <th>Run</th>
                    <th>Family</th>
                    <th>Timing class</th>
                    <th>Family timing adj</th>
                    <th>Integration wt</th>
                    <th>Integration contrib</th>
                    <th>Top symbols</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {familyTimingCompositeSummary.map((row, idx) => (
                    <tr key={`${row.run_id}:${row.dependency_family}:${idx}`}>
                      <td className="text-sm">{row.family_rank ?? "—"}</td>
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
                      <td className="text-sm">{fmtScore(row.timing_adjusted_family_contribution)}</td>
                      <td className="text-sm">{fmtScore(row.integration_weight_applied, 3)}</td>
                      <td>
                        <span className={netBadge(row.timing_integration_contribution)}>
                          {fmtScore(row.timing_integration_contribution)}
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

          {/* ── Final Integration Summary ──────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Final Integration Summary</h3>
                <p className="panel-subtitle">
                  All five layers (raw → weighted → regime → timing-adjusted → timing-integrated)
                  with dominant family choices side-by-side.
                </p>
              </div>
            </div>
            {finalIntegrationSummary.length === 0 ? (
              <p className="muted">No final integration rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Run</th>
                    <th>Watchlist</th>
                    <th>Raw net</th>
                    <th>Wt net</th>
                    <th>Regime net</th>
                    <th>Timing adj</th>
                    <th>Timing net</th>
                    <th>Pre-timing</th>
                    <th>Post-timing</th>
                    <th>Raw dom</th>
                    <th>Wt dom</th>
                    <th>Regime dom</th>
                    <th>Timing dom</th>
                    <th>Timing class</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {finalIntegrationSummary.map((row) => (
                    <tr key={row.run_id}>
                      <td className="mono-cell text-sm">{shortId(row.run_id)}</td>
                      <td className="mono-cell text-sm">{shortId(row.watchlist_id)}</td>
                      <td className="text-sm">{fmtScore(row.cross_asset_net_contribution)}</td>
                      <td className="text-sm">{fmtScore(row.weighted_cross_asset_net_contribution)}</td>
                      <td className="text-sm">{fmtScore(row.regime_adjusted_cross_asset_contribution)}</td>
                      <td className="text-sm">{fmtScore(row.timing_adjusted_cross_asset_contribution)}</td>
                      <td>
                        <span className={netBadge(row.timing_net_contribution)}>
                          {fmtScore(row.timing_net_contribution)}
                        </span>
                      </td>
                      <td className="text-sm">{fmtScore(row.composite_pre_timing)}</td>
                      <td className="text-sm">{fmtScore(row.composite_post_timing)}</td>
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
                        {row.dominant_timing_class
                          ? <span className={BUCKET_BADGE[row.dominant_timing_class] ?? "badge-muted"}>
                              {row.dominant_timing_class}
                            </span>
                          : <span className="badge-muted">—</span>}
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
