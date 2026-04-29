"use client";

import type {
  CrossAssetAttributionSummaryRow,
  CrossAssetFamilyAttributionSummaryRow,
  RunCompositeIntegrationSummaryRow,
} from "@/lib/queries/metrics";

type Props = {
  attributionSummary: CrossAssetAttributionSummaryRow[];
  familyAttributionSummary: CrossAssetFamilyAttributionSummaryRow[];
  runIntegrationSummary: RunCompositeIntegrationSummaryRow[];
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

const MODE_BADGE: Record<string, string> = {
  additive_guardrailed: "badge-green",
  confirmation_only: "badge-muted",
  suppression_only: "badge-yellow",
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

function netBadge(value: number | string | null | undefined): string {
  if (value === null || value === undefined) return "badge-muted";
  const n = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(n)) return "badge-muted";
  if (n > 0.05) return "badge-green";
  if (n < -0.05) return "badge-red";
  return "badge-muted";
}

function deltaBadge(
  pre: number | string | null | undefined,
  post: number | string | null | undefined,
): string {
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

function symbolList(syms: string[] | null | undefined, maxShown = 5): string {
  if (!syms || syms.length === 0) return "—";
  if (syms.length <= maxShown) return syms.join(", ");
  return `${syms.slice(0, maxShown).join(", ")} +${syms.length - maxShown}`;
}

export function CrossAssetAttributionPanel({
  attributionSummary,
  familyAttributionSummary,
  runIntegrationSummary,
  loading,
}: Props) {
  return (
    <section className="section">
      <div className="panel-header">
        <div>
          <h2>Cross-Asset Attribution</h2>
          <p className="panel-subtitle">
            Phase 4.1A. Controlled composite integration: base + bounded cross-asset contribution
            (clipped to ±0.25, applied at integration_weight 0.10). Deterministic and auditable.
          </p>
        </div>
      </div>

      {loading && <p className="muted">Loading cross-asset attribution…</p>}

      {!loading && (
        <>
          {/* ── Attribution Summary ────────────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Cross-Asset Attribution</h3>
                <p className="panel-subtitle">Latest attribution per run with pre/post composite.</p>
              </div>
            </div>
            {attributionSummary.length === 0 ? (
              <p className="muted">No cross-asset attribution rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Run</th>
                    <th>Mode</th>
                    <th>Base</th>
                    <th>CA signal</th>
                    <th>Confirm</th>
                    <th>Contra</th>
                    <th>Miss pen</th>
                    <th>Stale pen</th>
                    <th>Net</th>
                    <th>Pre</th>
                    <th>Post</th>
                    <th>Δ</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {attributionSummary.map((row) => (
                    <tr key={row.run_id}>
                      <td className="mono-cell text-sm">{shortId(row.run_id)}</td>
                      <td>
                        <span className={MODE_BADGE[row.integration_mode] ?? "badge-muted"}>
                          {row.integration_mode}
                        </span>
                      </td>
                      <td className="text-sm">{fmtScore(row.base_signal_score)}</td>
                      <td className="text-sm">{fmtScore(row.cross_asset_signal_score)}</td>
                      <td className="text-sm">{fmtScore(row.cross_asset_confirmation_score)}</td>
                      <td className="text-sm">{fmtScore(row.cross_asset_contradiction_penalty)}</td>
                      <td className="text-sm">{fmtScore(row.cross_asset_missing_penalty)}</td>
                      <td className="text-sm">{fmtScore(row.cross_asset_stale_penalty)}</td>
                      <td>
                        <span className={netBadge(row.cross_asset_net_contribution)}>
                          {fmtScore(row.cross_asset_net_contribution)}
                        </span>
                      </td>
                      <td className="text-sm">{fmtScore(row.composite_pre_cross_asset)}</td>
                      <td className="text-sm">{fmtScore(row.composite_post_cross_asset)}</td>
                      <td>
                        <span className={deltaBadge(row.composite_pre_cross_asset, row.composite_post_cross_asset)}>
                          {row.composite_pre_cross_asset !== null && row.composite_post_cross_asset !== null
                            ? fmtScore(Number(row.composite_post_cross_asset) - Number(row.composite_pre_cross_asset))
                            : "—"}
                        </span>
                      </td>
                      <td className="text-sm muted">{fmtTs(row.created_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* ── Family Attribution ─────────────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Family Attribution</h3>
                <p className="panel-subtitle">
                  Per-dependency-family net contribution, ranked. Tie-break: net desc, |net| desc,
                  structural priority desc, family name asc.
                </p>
              </div>
            </div>
            {familyAttributionSummary.length === 0 ? (
              <p className="muted">No family attribution rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Rank</th>
                    <th>Run</th>
                    <th>Family</th>
                    <th>Signal</th>
                    <th>Confirm</th>
                    <th>Contra pen</th>
                    <th>Miss pen</th>
                    <th>Stale pen</th>
                    <th>Net</th>
                    <th>Top symbols</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {familyAttributionSummary.map((row, idx) => (
                    <tr key={`${row.run_id}:${row.dependency_family}:${idx}`}>
                      <td className="text-sm">{row.family_rank ?? "—"}</td>
                      <td className="mono-cell text-sm">{shortId(row.run_id)}</td>
                      <td>
                        <span className={FAMILY_BADGE[row.dependency_family] ?? "badge-muted"}>
                          {row.dependency_family}
                        </span>
                      </td>
                      <td className="text-sm">{fmtScore(row.family_signal_score)}</td>
                      <td className="text-sm">{fmtScore(row.family_confirmation_score)}</td>
                      <td className="text-sm">{fmtScore(row.family_contradiction_penalty)}</td>
                      <td className="text-sm">{fmtScore(row.family_missing_penalty)}</td>
                      <td className="text-sm">{fmtScore(row.family_stale_penalty)}</td>
                      <td>
                        <span className={netBadge(row.family_net_contribution)}>
                          {fmtScore(row.family_net_contribution)}
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

          {/* ── Run Integration Summary ────────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Run Integration Summary</h3>
                <p className="panel-subtitle">
                  Composite bridge: base + cross-asset context + dominant family + confidence.
                </p>
              </div>
            </div>
            {runIntegrationSummary.length === 0 ? (
              <p className="muted">No run integration rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Run</th>
                    <th>Watchlist</th>
                    <th>Dominant family</th>
                    <th>Confidence</th>
                    <th>Base</th>
                    <th>CA signal</th>
                    <th>Net</th>
                    <th>Pre</th>
                    <th>Post</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {runIntegrationSummary.map((row) => (
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
                      <td className="text-sm">{fmtScore(row.cross_asset_confidence_score, 3)}</td>
                      <td className="text-sm">{fmtScore(row.base_signal_score)}</td>
                      <td className="text-sm">{fmtScore(row.cross_asset_signal_score)}</td>
                      <td>
                        <span className={netBadge(row.cross_asset_net_contribution)}>
                          {fmtScore(row.cross_asset_net_contribution)}
                        </span>
                      </td>
                      <td className="text-sm">{fmtScore(row.composite_pre_cross_asset)}</td>
                      <td className="text-sm">{fmtScore(row.composite_post_cross_asset)}</td>
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
