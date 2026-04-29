"use client";

import type {
  CrossAssetArchetypeCompositeSummaryRow,
  CrossAssetFamilyArchetypeCompositeSummaryRow,
  RunCrossAssetArchetypeIntegrationSummaryRow,
} from "@/lib/queries/metrics";

type Props = {
  archetypeCompositeSummary: CrossAssetArchetypeCompositeSummaryRow[];
  familyArchetypeCompositeSummary: CrossAssetFamilyArchetypeCompositeSummaryRow[];
  finalArchetypeIntegrationSummary: RunCrossAssetArchetypeIntegrationSummaryRow[];
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

const TRANSITION_BADGE: Record<string, string> = {
  reinforcing: "badge-green",
  deteriorating: "badge-red",
  recovering: "badge-green",
  rotating_in: "badge-yellow",
  rotating_out: "badge-yellow",
  stable: "badge-muted",
  insufficient_history: "badge-muted",
};

const SEQUENCE_BADGE: Record<string, string> = {
  reinforcing_path: "badge-green",
  deteriorating_path: "badge-red",
  recovery_path: "badge-green",
  rotation_path: "badge-yellow",
  mixed_path: "badge-muted",
  insufficient_history: "badge-muted",
};

const ARCHETYPE_BADGE: Record<string, string> = {
  rotation_handoff:         "badge-yellow",
  reinforcing_continuation: "badge-green",
  recovering_reentry:       "badge-green",
  deteriorating_breakdown:  "badge-red",
  mixed_transition_noise:   "badge-muted",
  insufficient_history:     "badge-muted",
};

const MODE_BADGE: Record<string, string> = {
  archetype_additive_guardrailed: "badge-green",
  reinforcing_confirmation_only:  "badge-muted",
  breakdown_suppression_only:     "badge-yellow",
  rotation_sensitive:             "badge-green",
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

export function CrossAssetArchetypeCompositePanel({
  archetypeCompositeSummary,
  familyArchetypeCompositeSummary,
  finalArchetypeIntegrationSummary,
  loading,
}: Props) {
  return (
    <section className="section">
      <div className="panel-header">
        <div>
          <h2>Archetype-Aware Composite Refinement</h2>
          <p className="panel-subtitle">
            Phase 4.4C. Refines the most mature upstream composite (4.3C composite_post_transition →
            4.2C composite_post_timing → regime equivalent → raw fallback) with a bounded archetype-
            aware delta conditioned on the dominant archetype. Net contribution clipped to
            [-0.15, +0.15]; archetype-aware integration never dominates cross-asset integration.
          </p>
        </div>
      </div>

      {loading && <p className="muted">Loading archetype-aware composite…</p>}

      {!loading && (
        <>
          {/* ── Archetype-Aware Composite ──────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Archetype-Aware Composite</h3>
                <p className="panel-subtitle">Pre- vs post-archetype composite and archetype net contribution per run.</p>
              </div>
            </div>
            {archetypeCompositeSummary.length === 0 ? (
              <p className="muted">No archetype-aware composite rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Run</th>
                    <th>Base</th>
                    <th>Archetype adj</th>
                    <th>Pre-archetype</th>
                    <th>Archetype Δ</th>
                    <th>Post-archetype</th>
                    <th>Shift</th>
                    <th>Dominant archetype</th>
                    <th>Mode</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {archetypeCompositeSummary.map((row, idx) => (
                    <tr key={`${row.run_id}:${idx}`}>
                      <td className="mono-cell text-sm">{shortId(row.run_id)}</td>
                      <td className="text-sm">{fmtScore(row.base_signal_score)}</td>
                      <td className="text-sm">{fmtScore(row.archetype_adjusted_cross_asset_contribution)}</td>
                      <td className="text-sm">{fmtScore(row.composite_pre_archetype)}</td>
                      <td>
                        <span className={netBadge(row.archetype_net_contribution)}>
                          {fmtScore(row.archetype_net_contribution)}
                        </span>
                      </td>
                      <td className="text-sm">{fmtScore(row.composite_post_archetype)}</td>
                      <td>
                        <span className={deltaBadge(row.composite_pre_archetype, row.composite_post_archetype)}>
                          {fmtDelta(row.composite_pre_archetype, row.composite_post_archetype)}
                        </span>
                      </td>
                      <td>
                        <span className={ARCHETYPE_BADGE[row.dominant_archetype_key] ?? "badge-muted"}>
                          {row.dominant_archetype_key}
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

          {/* ── Family Archetype Composite Contribution ───────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Family Archetype Composite Contribution</h3>
                <p className="panel-subtitle">Per-family archetype integration contribution and integration-weight applied.</p>
              </div>
            </div>
            {familyArchetypeCompositeSummary.length === 0 ? (
              <p className="muted">No family archetype composite rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Rank</th>
                    <th>Run</th>
                    <th>Family</th>
                    <th>Archetype</th>
                    <th>Transition</th>
                    <th>Sequence</th>
                    <th>Archetype adj</th>
                    <th>Wt applied</th>
                    <th>Integration Δ</th>
                    <th>Top symbols</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {familyArchetypeCompositeSummary.slice(0, 120).map((row, idx) => (
                    <tr key={`${row.run_id}:${row.dependency_family}:${idx}`}>
                      <td className="text-sm">{row.family_rank ?? "—"}</td>
                      <td className="mono-cell text-sm">{shortId(row.run_id)}</td>
                      <td>
                        <span className={FAMILY_BADGE[row.dependency_family] ?? "badge-muted"}>
                          {row.dependency_family}
                        </span>
                      </td>
                      <td>
                        <span className={ARCHETYPE_BADGE[row.archetype_key] ?? "badge-muted"}>
                          {row.archetype_key}
                        </span>
                      </td>
                      <td>
                        <span className={TRANSITION_BADGE[row.transition_state] ?? "badge-muted"}>
                          {row.transition_state}
                        </span>
                      </td>
                      <td>
                        <span className={SEQUENCE_BADGE[row.dominant_sequence_class] ?? "badge-muted"}>
                          {row.dominant_sequence_class}
                        </span>
                      </td>
                      <td className="text-sm">{fmtScore(row.archetype_adjusted_family_contribution)}</td>
                      <td className="text-sm">{fmtScore(row.integration_weight_applied, 3)}</td>
                      <td>
                        <span className={netBadge(row.archetype_integration_contribution)}>
                          {fmtScore(row.archetype_integration_contribution)}
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

          {/* ── Final Archetype Integration Summary ────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Final Archetype Integration Summary</h3>
                <p className="panel-subtitle">Raw vs weighted vs regime vs timing vs transition vs archetype vs final archetype-integrated contribution per run.</p>
              </div>
            </div>
            {finalArchetypeIntegrationSummary.length === 0 ? (
              <p className="muted">No archetype integration summary rows yet.</p>
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
                    <th>Pre-archetype</th>
                    <th>Archetype Δ</th>
                    <th>Post-archetype</th>
                    <th>Raw dom.</th>
                    <th>Archetype dom.</th>
                    <th>Dom. archetype</th>
                    <th>Shift</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {finalArchetypeIntegrationSummary.map((row, idx) => (
                    <tr key={`${row.run_id}:${idx}`}>
                      <td className="mono-cell text-sm">{shortId(row.run_id)}</td>
                      <td className="text-sm">{fmtScore(row.cross_asset_net_contribution)}</td>
                      <td className="text-sm">{fmtScore(row.weighted_cross_asset_net_contribution)}</td>
                      <td className="text-sm">{fmtScore(row.regime_adjusted_cross_asset_contribution)}</td>
                      <td className="text-sm">{fmtScore(row.timing_adjusted_cross_asset_contribution)}</td>
                      <td className="text-sm">{fmtScore(row.transition_adjusted_cross_asset_contribution)}</td>
                      <td className="text-sm">{fmtScore(row.archetype_adjusted_cross_asset_contribution)}</td>
                      <td className="text-sm">{fmtScore(row.composite_pre_archetype)}</td>
                      <td>
                        <span className={netBadge(row.archetype_net_contribution)}>
                          {fmtScore(row.archetype_net_contribution)}
                        </span>
                      </td>
                      <td className="text-sm">{fmtScore(row.composite_post_archetype)}</td>
                      <td>
                        {row.dominant_dependency_family ? (
                          <span className={FAMILY_BADGE[row.dominant_dependency_family] ?? "badge-muted"}>
                            {row.dominant_dependency_family}
                          </span>
                        ) : "—"}
                      </td>
                      <td>
                        {row.archetype_dominant_dependency_family ? (
                          <span className={FAMILY_BADGE[row.archetype_dominant_dependency_family] ?? "badge-muted"}>
                            {row.archetype_dominant_dependency_family}
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
                        <span className={shiftBadge(row.dominant_dependency_family, row.archetype_dominant_dependency_family)}>
                          {row.dominant_dependency_family === row.archetype_dominant_dependency_family ? "stable" : "shifted"}
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
