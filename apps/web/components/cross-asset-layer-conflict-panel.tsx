"use client";

import type {
  CrossAssetLayerAgreementSummaryRow,
  CrossAssetFamilyLayerAgreementSummaryRow,
  CrossAssetLayerConflictEventSummaryRow,
  RunCrossAssetLayerConflictSummaryRow,
} from "@/lib/queries/metrics";

type Props = {
  layerAgreementSummary: CrossAssetLayerAgreementSummaryRow[];
  familyLayerAgreementSummary: CrossAssetFamilyLayerAgreementSummaryRow[];
  layerConflictEventSummary: CrossAssetLayerConflictEventSummaryRow[];
  runLayerConflictSummary: RunCrossAssetLayerConflictSummaryRow[];
  loading: boolean;
};

const CONSENSUS_BADGE: Record<string, string> = {
  aligned_supportive:   "badge-green",
  aligned_suppressive:  "badge-red",
  partial_agreement:    "badge-yellow",
  conflicted:           "badge-red",
  unreliable:           "badge-yellow",
  insufficient_context: "badge-muted",
};

const DIRECTION_BADGE: Record<string, string> = {
  supportive:  "badge-green",
  suppressive: "badge-red",
  neutral:     "badge-muted",
  missing:     "badge-muted",
};

const EVENT_BADGE: Record<string, string> = {
  agreement_strengthened:    "badge-green",
  agreement_weakened:        "badge-yellow",
  conflict_emerged:          "badge-red",
  conflict_resolved:         "badge-green",
  unreliable_stack_detected: "badge-red",
  insufficient_context:      "badge-muted",
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

function fmtScore(value: number | string | null | undefined, digits = 3): string {
  if (value === null || value === undefined) return "—";
  const n = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(n)) return "—";
  return n.toFixed(digits);
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

function directionCell(direction: string | null | undefined): JSX.Element {
  if (!direction) return <span className="badge-muted">—</span>;
  return <span className={DIRECTION_BADGE[direction] ?? "badge-muted"}>{direction}</span>;
}

export function CrossAssetLayerConflictPanel({
  layerAgreementSummary,
  familyLayerAgreementSummary,
  layerConflictEventSummary,
  runLayerConflictSummary,
  loading,
}: Props) {
  return (
    <section className="section">
      <div className="panel-header">
        <div>
          <h2>Cross-Layer Conflict & Agreement</h2>
          <p className="panel-subtitle">
            Phase 4.8A. Maps each interpretation layer (timing, transition, archetype, cluster, persistence,
            decay) to a direction (supportive / suppressive / neutral / missing), then computes a weighted
            agreement score, conflict score, and consensus state. Diagnostic only — no attribution / composite mutation.
          </p>
        </div>
      </div>

      {loading && <p className="muted">Loading layer conflict diagnostics…</p>}

      {!loading && (
        <>
          {/* ── Layer Agreement Summary ────────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Layer Agreement Summary</h3>
                <p className="panel-subtitle">Per-run consensus state, agreement score, conflict score, and per-layer directions.</p>
              </div>
            </div>
            {layerAgreementSummary.length === 0 ? (
              <p className="muted">No layer agreement rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Run</th>
                    <th>Consensus</th>
                    <th>Agreement</th>
                    <th>Conflict</th>
                    <th>Dom. conflict source</th>
                    <th>Timing</th>
                    <th>Transition</th>
                    <th>Archetype</th>
                    <th>Cluster</th>
                    <th>Persistence</th>
                    <th>Decay</th>
                    <th>Reasons</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {layerAgreementSummary.map((row) => (
                    <tr key={row.run_id}>
                      <td className="mono-cell text-sm">{shortId(row.run_id)}</td>
                      <td>
                        <span className={CONSENSUS_BADGE[row.layer_consensus_state] ?? "badge-muted"}>
                          {row.layer_consensus_state}
                        </span>
                      </td>
                      <td className="text-sm">{fmtScore(row.agreement_score)}</td>
                      <td className="text-sm">{fmtScore(row.conflict_score)}</td>
                      <td className="text-sm">{row.dominant_conflict_source ?? "—"}</td>
                      <td>{directionCell(row.timing_direction)}</td>
                      <td>{directionCell(row.transition_direction)}</td>
                      <td>{directionCell(row.archetype_direction)}</td>
                      <td>{directionCell(row.cluster_direction)}</td>
                      <td>{directionCell(row.persistence_direction)}</td>
                      <td>{directionCell(row.decay_direction)}</td>
                      <td className="mono-cell text-sm">{reasonList(row.conflict_reason_codes)}</td>
                      <td className="text-sm muted">{fmtTs(row.created_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* ── Family Layer Agreement ─────────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Family Layer Agreement</h3>
                <p className="panel-subtitle">Per-family consensus state across transition / archetype / cluster / persistence / decay.</p>
              </div>
            </div>
            {familyLayerAgreementSummary.length === 0 ? (
              <p className="muted">No family layer agreement rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Run</th>
                    <th>Family</th>
                    <th>Consensus</th>
                    <th>Agreement</th>
                    <th>Conflict</th>
                    <th>Dom. conflict source</th>
                    <th>Transition</th>
                    <th>Archetype</th>
                    <th>Cluster</th>
                    <th>Persistence</th>
                    <th>Decay</th>
                    <th>Family contribution</th>
                    <th>Rank</th>
                    <th>Reasons</th>
                  </tr>
                </thead>
                <tbody>
                  {familyLayerAgreementSummary.map((row, idx) => (
                    <tr key={`${row.run_id}:${row.dependency_family}:${idx}`}>
                      <td className="mono-cell text-sm">{shortId(row.run_id)}</td>
                      <td>
                        <span className={FAMILY_BADGE[row.dependency_family] ?? "badge-muted"}>
                          {row.dependency_family}
                        </span>
                      </td>
                      <td>
                        <span className={CONSENSUS_BADGE[row.family_consensus_state] ?? "badge-muted"}>
                          {row.family_consensus_state}
                        </span>
                      </td>
                      <td className="text-sm">{fmtScore(row.agreement_score)}</td>
                      <td className="text-sm">{fmtScore(row.conflict_score)}</td>
                      <td className="text-sm">{row.dominant_conflict_source ?? "—"}</td>
                      <td>{directionCell(row.transition_direction)}</td>
                      <td>{directionCell(row.archetype_direction)}</td>
                      <td>{directionCell(row.cluster_direction)}</td>
                      <td>{directionCell(row.persistence_direction)}</td>
                      <td>{directionCell(row.decay_direction)}</td>
                      <td className="text-sm">{fmtScore(row.family_contribution, 4)}</td>
                      <td className="text-sm">{row.family_rank ?? "—"}</td>
                      <td className="mono-cell text-sm">{reasonList(row.conflict_reason_codes)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* ── Layer Conflict Events ──────────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Layer Conflict Events</h3>
                <p className="panel-subtitle">Discrete prior → current consensus transitions with reason codes and score deltas.</p>
              </div>
            </div>
            {layerConflictEventSummary.length === 0 ? (
              <p className="muted">No layer conflict events yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Source run</th>
                    <th>Target run</th>
                    <th>Event</th>
                    <th>Prior</th>
                    <th>Current</th>
                    <th>Prior dom.</th>
                    <th>Current dom.</th>
                    <th>Prior agree</th>
                    <th>Current agree</th>
                    <th>Prior conflict</th>
                    <th>Current conflict</th>
                    <th>Reasons</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {layerConflictEventSummary.map((row, idx) => (
                    <tr key={`${row.target_run_id}:${row.event_type}:${idx}`}>
                      <td className="mono-cell text-sm">{shortId(row.source_run_id)}</td>
                      <td className="mono-cell text-sm">{shortId(row.target_run_id)}</td>
                      <td>
                        <span className={EVENT_BADGE[row.event_type] ?? "badge-muted"}>
                          {row.event_type}
                        </span>
                      </td>
                      <td>
                        {row.prior_consensus_state ? (
                          <span className={CONSENSUS_BADGE[row.prior_consensus_state] ?? "badge-muted"}>
                            {row.prior_consensus_state}
                          </span>
                        ) : "—"}
                      </td>
                      <td>
                        <span className={CONSENSUS_BADGE[row.current_consensus_state] ?? "badge-muted"}>
                          {row.current_consensus_state}
                        </span>
                      </td>
                      <td className="text-sm">{row.prior_dominant_conflict_source ?? "—"}</td>
                      <td className="text-sm">{row.current_dominant_conflict_source ?? "—"}</td>
                      <td className="text-sm">{fmtScore(row.prior_agreement_score)}</td>
                      <td className="text-sm">{fmtScore(row.current_agreement_score)}</td>
                      <td className="text-sm">{fmtScore(row.prior_conflict_score)}</td>
                      <td className="text-sm">{fmtScore(row.current_conflict_score)}</td>
                      <td className="mono-cell text-sm">{reasonList(row.reason_codes)}</td>
                      <td className="text-sm muted">{fmtTs(row.created_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* ── Run Layer Conflict Summary ─────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Run Layer Conflict Summary</h3>
                <p className="panel-subtitle">Compact run-linked conflict row joined with the latest conflict event.</p>
              </div>
            </div>
            {runLayerConflictSummary.length === 0 ? (
              <p className="muted">No run layer conflict rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Run</th>
                    <th>Consensus</th>
                    <th>Agreement</th>
                    <th>Conflict</th>
                    <th>Dom. conflict source</th>
                    <th>Freshness</th>
                    <th>Persistence</th>
                    <th>Cluster</th>
                    <th>Latest event</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {runLayerConflictSummary.map((row) => (
                    <tr key={row.run_id}>
                      <td className="mono-cell text-sm">{shortId(row.run_id)}</td>
                      <td>
                        {row.layer_consensus_state ? (
                          <span className={CONSENSUS_BADGE[row.layer_consensus_state] ?? "badge-muted"}>
                            {row.layer_consensus_state}
                          </span>
                        ) : "—"}
                      </td>
                      <td className="text-sm">{fmtScore(row.agreement_score)}</td>
                      <td className="text-sm">{fmtScore(row.conflict_score)}</td>
                      <td className="text-sm">{row.dominant_conflict_source ?? "—"}</td>
                      <td className="text-sm">{row.freshness_state ?? "—"}</td>
                      <td className="text-sm">{row.persistence_state ?? "—"}</td>
                      <td className="text-sm">{row.cluster_state ?? "—"}</td>
                      <td>
                        {row.latest_conflict_event_type ? (
                          <span className={EVENT_BADGE[row.latest_conflict_event_type] ?? "badge-muted"}>
                            {row.latest_conflict_event_type}
                          </span>
                        ) : "—"}
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
