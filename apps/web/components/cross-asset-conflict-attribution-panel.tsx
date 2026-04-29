"use client";

import type {
  CrossAssetFamilyConflictAttributionSummaryRow,
  CrossAssetSymbolConflictAttributionSummaryRow,
  RunCrossAssetConflictAttributionSummaryRow,
} from "@/lib/queries/metrics";

type Props = {
  familyConflictAttributionSummary: CrossAssetFamilyConflictAttributionSummaryRow[];
  symbolConflictAttributionSummary: CrossAssetSymbolConflictAttributionSummaryRow[];
  runConflictAttributionSummary: RunCrossAssetConflictAttributionSummaryRow[];
  loading: boolean;
};

const CONSENSUS_BADGE: Record<string, string> = {
  aligned_supportive:   "badge-green",
  aligned_suppressive:  "badge-yellow",
  partial_agreement:    "badge-yellow",
  conflicted:           "badge-red",
  unreliable:           "badge-red",
  insufficient_context: "badge-muted",
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

function shortId(id: string | null | undefined): string {
  if (!id) return "—";
  return `${id.slice(0, 8)}…`;
}

function reasonList(codes: string[] | null | undefined, maxShown = 3): string {
  if (!codes || codes.length === 0) return "—";
  if (codes.length <= maxShown) return codes.join(", ");
  return `${codes.slice(0, maxShown).join(", ")} +${codes.length - maxShown}`;
}

function topSymbolList(symbols: string[] | null | undefined, maxShown = 4): string {
  if (!symbols || symbols.length === 0) return "—";
  if (symbols.length <= maxShown) return symbols.join(", ");
  return `${symbols.slice(0, maxShown).join(", ")} +${symbols.length - maxShown}`;
}

export function CrossAssetConflictAttributionPanel({
  familyConflictAttributionSummary,
  symbolConflictAttributionSummary,
  runConflictAttributionSummary,
  loading,
}: Props) {
  return (
    <section className="section">
      <div className="panel-header">
        <div>
          <h2>Conflict-Aware Attribution</h2>
          <p className="panel-subtitle">
            Phase 4.8B. Conditions family + symbol contribution on the layer-consensus state, agreement
            score, conflict score, and dominant conflict source from 4.8A. Aligned-supportive stacks earn a
            small agreement bonus; conflicted and unreliable stacks are suppressed and soft-floored toward
            zero. Multipliers stay bounded in [0.60, 1.20]. Diagnostic only — no predictive forecasting.
          </p>
        </div>
      </div>

      {loading && <p className="muted">Loading conflict-aware attribution…</p>}

      {!loading && (
        <>
          {/* ── Family Conflict-Aware Attribution ─────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Conflict-Aware Family Attribution</h3>
                <p className="panel-subtitle">
                  Per-family conflict-adjusted contribution alongside decay-adjusted, with consensus state,
                  agreement / conflict scores, and dominant conflict source.
                </p>
              </div>
            </div>
            {familyConflictAttributionSummary.length === 0 ? (
              <p className="muted">No conflict-aware family attribution rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Run</th>
                    <th>Family</th>
                    <th>Consensus</th>
                    <th>Agree</th>
                    <th>Conflict</th>
                    <th>Dominant Conflict</th>
                    <th>Decay-adj</th>
                    <th>Conflict-adj</th>
                    <th>Weight</th>
                    <th>Bonus</th>
                    <th>Penalty</th>
                    <th>Rank</th>
                    <th>Top symbols</th>
                    <th>Reasons</th>
                    <th>When</th>
                  </tr>
                </thead>
                <tbody>
                  {familyConflictAttributionSummary.slice(0, 60).map((r) => (
                    <tr key={`fcar-${r.run_id}-${r.dependency_family}-${r.created_at}`}>
                      <td className="mono">{shortId(r.run_id)}</td>
                      <td>
                        <span className={`badge ${FAMILY_BADGE[r.dependency_family] ?? "badge-muted"}`}>
                          {r.dependency_family}
                        </span>
                      </td>
                      <td>
                        <span className={`badge ${CONSENSUS_BADGE[r.family_consensus_state] ?? "badge-muted"}`}>
                          {r.family_consensus_state}
                        </span>
                      </td>
                      <td>{fmtScore(r.agreement_score, 3)}</td>
                      <td>{fmtScore(r.conflict_score, 3)}</td>
                      <td>{r.dominant_conflict_source ?? "—"}</td>
                      <td>{fmtScore(r.decay_adjusted_family_contribution)}</td>
                      <td>{fmtScore(r.conflict_adjusted_family_contribution)}</td>
                      <td>{fmtMultiplier(r.conflict_weight)}</td>
                      <td>{fmtScore(r.conflict_bonus, 4)}</td>
                      <td>{fmtScore(r.conflict_penalty, 4)}</td>
                      <td>{r.conflict_family_rank ?? "—"}</td>
                      <td>{topSymbolList(r.top_symbols)}</td>
                      <td className="muted">{reasonList(r.reason_codes)}</td>
                      <td>{fmtTs(r.created_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* ── Symbol Conflict-Aware Attribution ─────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Conflict-Aware Symbol Attribution</h3>
                <p className="panel-subtitle">
                  Per-symbol conflict-adjusted score with the family consensus context applied.
                </p>
              </div>
            </div>
            {symbolConflictAttributionSummary.length === 0 ? (
              <p className="muted">No conflict-aware symbol attribution rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Run</th>
                    <th>Symbol</th>
                    <th>Family</th>
                    <th>Consensus</th>
                    <th>Agree</th>
                    <th>Conflict</th>
                    <th>Decay-adj</th>
                    <th>Conflict-adj</th>
                    <th>Weight</th>
                    <th>Rank</th>
                    <th>Reasons</th>
                    <th>When</th>
                  </tr>
                </thead>
                <tbody>
                  {symbolConflictAttributionSummary.slice(0, 80).map((r) => (
                    <tr key={`scar-${r.run_id}-${r.symbol}-${r.created_at}`}>
                      <td className="mono">{shortId(r.run_id)}</td>
                      <td className="mono">{r.symbol}</td>
                      <td>
                        <span className={`badge ${FAMILY_BADGE[r.dependency_family] ?? "badge-muted"}`}>
                          {r.dependency_family}
                        </span>
                      </td>
                      <td>
                        <span className={`badge ${CONSENSUS_BADGE[r.family_consensus_state] ?? "badge-muted"}`}>
                          {r.family_consensus_state}
                        </span>
                      </td>
                      <td>{fmtScore(r.agreement_score, 3)}</td>
                      <td>{fmtScore(r.conflict_score, 3)}</td>
                      <td>{fmtScore(r.decay_adjusted_symbol_score)}</td>
                      <td>{fmtScore(r.conflict_adjusted_symbol_score)}</td>
                      <td>{fmtMultiplier(r.conflict_weight)}</td>
                      <td>{r.symbol_rank ?? "—"}</td>
                      <td className="muted">{reasonList(r.reason_codes)}</td>
                      <td>{fmtTs(r.created_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* ── Conflict Attribution Summary ──────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Conflict Attribution Summary</h3>
                <p className="panel-subtitle">
                  Run-level bridge across raw → weighted → regime → timing → transition → archetype → cluster
                  → persistence → decay → conflict-adjusted contribution, with consensus / conflict context.
                </p>
              </div>
            </div>
            {runConflictAttributionSummary.length === 0 ? (
              <p className="muted">No run-level conflict attribution rows yet.</p>
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
                    <th>Conflict</th>
                    <th>Decay-dom Family</th>
                    <th>Conflict-dom Family</th>
                    <th>Consensus</th>
                    <th>Agree / Conflict</th>
                    <th>Conflict Source</th>
                    <th>When</th>
                  </tr>
                </thead>
                <tbody>
                  {runConflictAttributionSummary.slice(0, 50).map((r) => (
                    <tr key={`rcar-${r.run_id}-${r.created_at}`}>
                      <td className="mono">{shortId(r.run_id)}</td>
                      <td>{fmtScore(r.cross_asset_net_contribution)}</td>
                      <td>{fmtScore(r.weighted_cross_asset_net_contribution)}</td>
                      <td>{fmtScore(r.regime_adjusted_cross_asset_contribution)}</td>
                      <td>{fmtScore(r.timing_adjusted_cross_asset_contribution)}</td>
                      <td>{fmtScore(r.transition_adjusted_cross_asset_contribution)}</td>
                      <td>{fmtScore(r.archetype_adjusted_cross_asset_contribution)}</td>
                      <td>{fmtScore(r.cluster_adjusted_cross_asset_contribution)}</td>
                      <td>{fmtScore(r.persistence_adjusted_cross_asset_contribution)}</td>
                      <td>{fmtScore(r.decay_adjusted_cross_asset_contribution)}</td>
                      <td>{fmtScore(r.conflict_adjusted_cross_asset_contribution)}</td>
                      <td>
                        {r.decay_dominant_dependency_family ? (
                          <span className={`badge ${FAMILY_BADGE[r.decay_dominant_dependency_family] ?? "badge-muted"}`}>
                            {r.decay_dominant_dependency_family}
                          </span>
                        ) : "—"}
                      </td>
                      <td>
                        {r.conflict_dominant_dependency_family ? (
                          <span className={`badge ${FAMILY_BADGE[r.conflict_dominant_dependency_family] ?? "badge-muted"}`}>
                            {r.conflict_dominant_dependency_family}
                          </span>
                        ) : "—"}
                      </td>
                      <td>
                        {r.layer_consensus_state ? (
                          <span className={`badge ${CONSENSUS_BADGE[r.layer_consensus_state] ?? "badge-muted"}`}>
                            {r.layer_consensus_state}
                          </span>
                        ) : "—"}
                      </td>
                      <td>{fmtScore(r.agreement_score, 3)} / {fmtScore(r.conflict_score, 3)}</td>
                      <td>{r.dominant_conflict_source ?? "—"}</td>
                      <td>{fmtTs(r.created_at)}</td>
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
