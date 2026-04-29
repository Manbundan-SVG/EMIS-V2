"use client";

import type {
  CrossAssetPersistenceCompositeSummaryRow,
  CrossAssetFamilyPersistenceCompositeSummaryRow,
  RunCrossAssetPersistenceIntegrationSummaryRow,
} from "@/lib/queries/metrics";

type Props = {
  persistenceCompositeSummary: CrossAssetPersistenceCompositeSummaryRow[];
  familyPersistenceCompositeSummary: CrossAssetFamilyPersistenceCompositeSummaryRow[];
  finalPersistenceIntegrationSummary: RunCrossAssetPersistenceIntegrationSummaryRow[];
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

const PERSISTENCE_STATE_BADGE: Record<string, string> = {
  persistent:           "badge-green",
  recovering:           "badge-green",
  rotating:             "badge-yellow",
  fragile:              "badge-yellow",
  breaking_down:        "badge-red",
  mixed:                "badge-muted",
  insufficient_history: "badge-muted",
};

const MEMORY_BREAK_BADGE: Record<string, string> = {
  persistence_loss:        "badge-red",
  regime_memory_break:     "badge-red",
  cluster_memory_break:    "badge-red",
  archetype_memory_break:  "badge-red",
};

const MODE_BADGE: Record<string, string> = {
  persistence_additive_guardrailed: "badge-green",
  persistent_confirmation_only:     "badge-muted",
  memory_break_suppression_only:    "badge-yellow",
  recovery_sensitive:               "badge-green",
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

function reasonList(codes: string[] | null | undefined, maxShown = 3): string {
  if (!codes || codes.length === 0) return "—";
  if (codes.length <= maxShown) return codes.join(", ");
  return `${codes.slice(0, maxShown).join(", ")} +${codes.length - maxShown}`;
}

export function CrossAssetPersistenceCompositePanel({
  persistenceCompositeSummary,
  familyPersistenceCompositeSummary,
  finalPersistenceIntegrationSummary,
  loading,
}: Props) {
  return (
    <section className="section">
      <div className="panel-header">
        <div>
          <h2>Persistence-Aware Composite Refinement</h2>
          <p className="panel-subtitle">
            Phase 4.6C. Refines the most mature upstream composite (4.5C composite_post_cluster →
            4.4C composite_post_archetype → 4.3C composite_post_transition → 4.2C composite_post_timing →
            regime equivalent → raw fallback) with a bounded persistence-aware delta conditioned on the
            run's persistence state and memory-break events. Net contribution clipped to [-0.15, +0.15];
            persistence-aware integration never dominates upstream cross-asset integration.
          </p>
        </div>
      </div>

      {loading && <p className="muted">Loading persistence-aware composite…</p>}

      {!loading && (
        <>
          {/* ── Persistence-Aware Composite ────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Persistence-Aware Composite</h3>
                <p className="panel-subtitle">Pre- vs post-persistence composite and persistence net contribution per run.</p>
              </div>
            </div>
            {persistenceCompositeSummary.length === 0 ? (
              <p className="muted">No persistence-aware composite rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Run</th>
                    <th>Base</th>
                    <th>Persist adj</th>
                    <th>Pre-persist</th>
                    <th>Persist Δ</th>
                    <th>Post-persist</th>
                    <th>Shift</th>
                    <th>Persist state</th>
                    <th>Memory</th>
                    <th>Age</th>
                    <th>Last event</th>
                    <th>Mode</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {persistenceCompositeSummary.map((row, idx) => (
                    <tr key={`${row.run_id}:${idx}`}>
                      <td className="mono-cell text-sm">{shortId(row.run_id)}</td>
                      <td className="text-sm">{fmtScore(row.base_signal_score)}</td>
                      <td className="text-sm">{fmtScore(row.persistence_adjusted_cross_asset_contribution)}</td>
                      <td className="text-sm">{fmtScore(row.composite_pre_persistence)}</td>
                      <td>
                        <span className={netBadge(row.persistence_net_contribution)}>
                          {fmtScore(row.persistence_net_contribution)}
                        </span>
                      </td>
                      <td className="text-sm">{fmtScore(row.composite_post_persistence)}</td>
                      <td>
                        <span className={deltaBadge(row.composite_pre_persistence, row.composite_post_persistence)}>
                          {fmtDelta(row.composite_pre_persistence, row.composite_post_persistence)}
                        </span>
                      </td>
                      <td>
                        <span className={PERSISTENCE_STATE_BADGE[row.persistence_state] ?? "badge-muted"}>
                          {row.persistence_state}
                        </span>
                      </td>
                      <td className="text-sm">{fmtScore(row.memory_score, 3)}</td>
                      <td className="text-sm">{row.state_age_runs ?? "—"}</td>
                      <td>
                        {row.latest_persistence_event_type ? (
                          <span className={MEMORY_BREAK_BADGE[row.latest_persistence_event_type] ?? "badge-muted"}>
                            {row.latest_persistence_event_type}
                          </span>
                        ) : "—"}
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

          {/* ── Family Persistence Composite Contribution ─────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Family Persistence Composite Contribution</h3>
                <p className="panel-subtitle">Per-family persistence integration contribution and integration-weight applied.</p>
              </div>
            </div>
            {familyPersistenceCompositeSummary.length === 0 ? (
              <p className="muted">No family persistence composite rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Rank</th>
                    <th>Run</th>
                    <th>Family</th>
                    <th>Persist state</th>
                    <th>Memory</th>
                    <th>Age</th>
                    <th>Last event</th>
                    <th>Persist adj</th>
                    <th>Wt applied</th>
                    <th>Integration Δ</th>
                    <th>Top symbols</th>
                    <th>Reasons</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {familyPersistenceCompositeSummary.slice(0, 120).map((row, idx) => (
                    <tr key={`${row.run_id}:${row.dependency_family}:${idx}`}>
                      <td className="text-sm">{row.family_rank ?? "—"}</td>
                      <td className="mono-cell text-sm">{shortId(row.run_id)}</td>
                      <td>
                        <span className={FAMILY_BADGE[row.dependency_family] ?? "badge-muted"}>
                          {row.dependency_family}
                        </span>
                      </td>
                      <td>
                        <span className={PERSISTENCE_STATE_BADGE[row.persistence_state] ?? "badge-muted"}>
                          {row.persistence_state}
                        </span>
                      </td>
                      <td className="text-sm">{fmtScore(row.memory_score, 3)}</td>
                      <td className="text-sm">{row.state_age_runs ?? "—"}</td>
                      <td>
                        {row.latest_persistence_event_type ? (
                          <span className={MEMORY_BREAK_BADGE[row.latest_persistence_event_type] ?? "badge-muted"}>
                            {row.latest_persistence_event_type}
                          </span>
                        ) : "—"}
                      </td>
                      <td className="text-sm">{fmtScore(row.persistence_adjusted_family_contribution)}</td>
                      <td className="text-sm">{fmtScore(row.integration_weight_applied, 3)}</td>
                      <td>
                        <span className={netBadge(row.persistence_integration_contribution)}>
                          {fmtScore(row.persistence_integration_contribution)}
                        </span>
                      </td>
                      <td className="mono-cell text-sm">{symbolList(row.top_symbols)}</td>
                      <td className="mono-cell text-sm">{reasonList(row.reason_codes)}</td>
                      <td className="text-sm muted">{fmtTs(row.created_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* ── Final Persistence Integration Summary ─────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Final Persistence Integration Summary</h3>
                <p className="panel-subtitle">Raw vs weighted vs regime vs timing vs transition vs archetype vs cluster vs persistence vs final persistence-integrated contribution per run.</p>
              </div>
            </div>
            {finalPersistenceIntegrationSummary.length === 0 ? (
              <p className="muted">No persistence integration summary rows yet.</p>
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
                    <th>Persist</th>
                    <th>Pre-persist</th>
                    <th>Persist Δ</th>
                    <th>Post-persist</th>
                    <th>Raw dom.</th>
                    <th>Persist dom.</th>
                    <th>Persist state</th>
                    <th>Shift</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {finalPersistenceIntegrationSummary.map((row, idx) => (
                    <tr key={`${row.run_id}:${idx}`}>
                      <td className="mono-cell text-sm">{shortId(row.run_id)}</td>
                      <td className="text-sm">{fmtScore(row.cross_asset_net_contribution)}</td>
                      <td className="text-sm">{fmtScore(row.weighted_cross_asset_net_contribution)}</td>
                      <td className="text-sm">{fmtScore(row.regime_adjusted_cross_asset_contribution)}</td>
                      <td className="text-sm">{fmtScore(row.timing_adjusted_cross_asset_contribution)}</td>
                      <td className="text-sm">{fmtScore(row.transition_adjusted_cross_asset_contribution)}</td>
                      <td className="text-sm">{fmtScore(row.archetype_adjusted_cross_asset_contribution)}</td>
                      <td className="text-sm">{fmtScore(row.cluster_adjusted_cross_asset_contribution)}</td>
                      <td className="text-sm">{fmtScore(row.persistence_adjusted_cross_asset_contribution)}</td>
                      <td className="text-sm">{fmtScore(row.composite_pre_persistence)}</td>
                      <td>
                        <span className={netBadge(row.persistence_net_contribution)}>
                          {fmtScore(row.persistence_net_contribution)}
                        </span>
                      </td>
                      <td className="text-sm">{fmtScore(row.composite_post_persistence)}</td>
                      <td>
                        {row.dominant_dependency_family ? (
                          <span className={FAMILY_BADGE[row.dominant_dependency_family] ?? "badge-muted"}>
                            {row.dominant_dependency_family}
                          </span>
                        ) : "—"}
                      </td>
                      <td>
                        {row.persistence_dominant_dependency_family ? (
                          <span className={FAMILY_BADGE[row.persistence_dominant_dependency_family] ?? "badge-muted"}>
                            {row.persistence_dominant_dependency_family}
                          </span>
                        ) : "—"}
                      </td>
                      <td>
                        {row.persistence_state ? (
                          <span className={PERSISTENCE_STATE_BADGE[row.persistence_state] ?? "badge-muted"}>
                            {row.persistence_state}
                          </span>
                        ) : "—"}
                      </td>
                      <td>
                        <span className={shiftBadge(row.dominant_dependency_family, row.persistence_dominant_dependency_family)}>
                          {row.dominant_dependency_family === row.persistence_dominant_dependency_family ? "stable" : "shifted"}
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
