"use client";

import type {
  CrossAssetTransitionReplayValidationSummaryRow,
  CrossAssetFamilyTransitionReplayStabilitySummaryRow,
  CrossAssetTransitionReplayStabilityAggregateRow,
} from "@/lib/queries/metrics";

type Props = {
  transitionReplayValidationSummary: CrossAssetTransitionReplayValidationSummaryRow[];
  familyTransitionReplayStabilitySummary: CrossAssetFamilyTransitionReplayStabilitySummaryRow[];
  transitionReplayStabilityAggregate: CrossAssetTransitionReplayStabilityAggregateRow | null;
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

const STATE_BADGE: Record<string, string> = {
  validated: "badge-green",
  drift_detected: "badge-red",
  insufficient_source: "badge-muted",
  insufficient_replay: "badge-muted",
  context_mismatch: "badge-yellow",
  timing_mismatch: "badge-yellow",
  transition_mismatch: "badge-yellow",
};

function fmtTs(ts: string | null | undefined): string {
  if (!ts) return "—";
  return new Date(ts).toLocaleString(undefined, { dateStyle: "short", timeStyle: "short" });
}

function fmtRate(value: number | string | null | undefined): string {
  if (value === null || value === undefined) return "—";
  const n = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(n)) return "—";
  return `${(n * 100).toFixed(1)}%`;
}

function fmtDelta(value: number | string | null | undefined, digits = 4): string {
  if (value === null || value === undefined) return "—";
  const n = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(n)) return "—";
  return n.toFixed(digits);
}

function matchBadge(v: boolean | null | undefined): string {
  if (v === true) return "badge-green";
  if (v === false) return "badge-red";
  return "badge-muted";
}

function matchLabel(v: boolean | null | undefined): string {
  if (v === true) return "match";
  if (v === false) return "differ";
  return "—";
}

function rateBadge(value: number | string | null | undefined): string {
  if (value === null || value === undefined) return "badge-muted";
  const n = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(n)) return "badge-muted";
  if (n >= 0.95) return "badge-green";
  if (n >= 0.75) return "badge-yellow";
  return "badge-red";
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

export function CrossAssetTransitionReplayValidationPanel({
  transitionReplayValidationSummary,
  familyTransitionReplayStabilitySummary,
  transitionReplayStabilityAggregate,
  loading,
}: Props) {
  return (
    <section className="section">
      <div className="panel-header">
        <div>
          <h2>Sequencing Replay Validation</h2>
          <p className="panel-subtitle">
            Phase 4.3D. Deterministic replay comparison across 4.3A diagnostics + 4.3B transition-aware
            attribution + 4.3C sequencing-aware composite. Numeric tolerance 1e-9; drift reason codes
            explicit. validation_state separates context_mismatch, timing_mismatch, and
            transition_mismatch so operators can see the primary driver.
          </p>
        </div>
      </div>

      {loading && <p className="muted">Loading sequencing replay validation…</p>}

      {!loading && (
        <>
          {/* ── Sequencing Replay Stability Aggregate ──────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Sequencing Replay Stability Aggregate</h3>
                <p className="panel-subtitle">Workspace rollup of sequencing-layer match rates.</p>
              </div>
            </div>
            {!transitionReplayStabilityAggregate ? (
              <p className="muted">No sequencing replay validations recorded yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Validations</th>
                    <th>Context</th>
                    <th>Regime</th>
                    <th>Timing class</th>
                    <th>Transition state</th>
                    <th>Sequence class</th>
                    <th>Transition attr</th>
                    <th>Transition comp</th>
                    <th>Transition dom family</th>
                    <th>Drift count</th>
                    <th>Latest</th>
                  </tr>
                </thead>
                <tbody>
                  <tr>
                    <td className="text-sm">{transitionReplayStabilityAggregate.validation_count}</td>
                    <td><span className={rateBadge(transitionReplayStabilityAggregate.context_match_rate)}>{fmtRate(transitionReplayStabilityAggregate.context_match_rate)}</span></td>
                    <td><span className={rateBadge(transitionReplayStabilityAggregate.regime_match_rate)}>{fmtRate(transitionReplayStabilityAggregate.regime_match_rate)}</span></td>
                    <td><span className={rateBadge(transitionReplayStabilityAggregate.timing_class_match_rate)}>{fmtRate(transitionReplayStabilityAggregate.timing_class_match_rate)}</span></td>
                    <td><span className={rateBadge(transitionReplayStabilityAggregate.transition_state_match_rate)}>{fmtRate(transitionReplayStabilityAggregate.transition_state_match_rate)}</span></td>
                    <td><span className={rateBadge(transitionReplayStabilityAggregate.sequence_class_match_rate)}>{fmtRate(transitionReplayStabilityAggregate.sequence_class_match_rate)}</span></td>
                    <td><span className={rateBadge(transitionReplayStabilityAggregate.transition_attribution_match_rate)}>{fmtRate(transitionReplayStabilityAggregate.transition_attribution_match_rate)}</span></td>
                    <td><span className={rateBadge(transitionReplayStabilityAggregate.transition_composite_match_rate)}>{fmtRate(transitionReplayStabilityAggregate.transition_composite_match_rate)}</span></td>
                    <td><span className={rateBadge(transitionReplayStabilityAggregate.transition_dominant_family_match_rate)}>{fmtRate(transitionReplayStabilityAggregate.transition_dominant_family_match_rate)}</span></td>
                    <td className="text-sm">{transitionReplayStabilityAggregate.drift_detected_count}</td>
                    <td className="text-sm muted">{fmtTs(transitionReplayStabilityAggregate.latest_validated_at)}</td>
                  </tr>
                </tbody>
              </table>
            )}
          </div>

          {/* ── Sequencing Replay Validation Summary ───────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Sequencing Replay Validation Summary</h3>
                <p className="panel-subtitle">Per-pair match flags and drift reason codes.</p>
              </div>
            </div>
            {transitionReplayValidationSummary.length === 0 ? (
              <p className="muted">No sequencing replay validation rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>State</th>
                    <th>Source run</th>
                    <th>Replay run</th>
                    <th>Src transition</th>
                    <th>Replay transition</th>
                    <th>Src sequence</th>
                    <th>Replay sequence</th>
                    <th>Ctx</th>
                    <th>Regime</th>
                    <th>Timing</th>
                    <th>Transition</th>
                    <th>Sequence</th>
                    <th>Trans attr</th>
                    <th>Trans comp</th>
                    <th>Trans dom</th>
                    <th>Drift codes</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {transitionReplayValidationSummary.map((row) => (
                    <tr key={`${row.source_run_id}:${row.replay_run_id}`}>
                      <td>
                        <span className={STATE_BADGE[row.validation_state] ?? "badge-muted"}>
                          {row.validation_state}
                        </span>
                      </td>
                      <td className="mono-cell text-sm">{shortId(row.source_run_id)}</td>
                      <td className="mono-cell text-sm">{shortId(row.replay_run_id)}</td>
                      <td>
                        {row.source_dominant_transition_state
                          ? <span className={TRANSITION_BADGE[row.source_dominant_transition_state] ?? "badge-muted"}>
                              {row.source_dominant_transition_state}
                            </span>
                          : <span className="badge-muted">—</span>}
                      </td>
                      <td>
                        {row.replay_dominant_transition_state
                          ? <span className={TRANSITION_BADGE[row.replay_dominant_transition_state] ?? "badge-muted"}>
                              {row.replay_dominant_transition_state}
                            </span>
                          : <span className="badge-muted">—</span>}
                      </td>
                      <td>
                        {row.source_dominant_sequence_class
                          ? <span className={SEQUENCE_BADGE[row.source_dominant_sequence_class] ?? "badge-muted"}>
                              {row.source_dominant_sequence_class}
                            </span>
                          : <span className="badge-muted">—</span>}
                      </td>
                      <td>
                        {row.replay_dominant_sequence_class
                          ? <span className={SEQUENCE_BADGE[row.replay_dominant_sequence_class] ?? "badge-muted"}>
                              {row.replay_dominant_sequence_class}
                            </span>
                          : <span className="badge-muted">—</span>}
                      </td>
                      <td><span className={matchBadge(row.context_hash_match)}>{matchLabel(row.context_hash_match)}</span></td>
                      <td><span className={matchBadge(row.regime_match)}>{matchLabel(row.regime_match)}</span></td>
                      <td><span className={matchBadge(row.timing_class_match)}>{matchLabel(row.timing_class_match)}</span></td>
                      <td><span className={matchBadge(row.transition_state_match)}>{matchLabel(row.transition_state_match)}</span></td>
                      <td><span className={matchBadge(row.sequence_class_match)}>{matchLabel(row.sequence_class_match)}</span></td>
                      <td><span className={matchBadge(row.transition_attribution_match)}>{matchLabel(row.transition_attribution_match)}</span></td>
                      <td><span className={matchBadge(row.transition_composite_match)}>{matchLabel(row.transition_composite_match)}</span></td>
                      <td><span className={matchBadge(row.transition_dominant_family_match)}>{matchLabel(row.transition_dominant_family_match)}</span></td>
                      <td className="mono-cell text-sm">{reasonList(row.drift_reason_codes)}</td>
                      <td className="text-sm muted">{fmtTs(row.created_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* ── Family Transition Replay Stability ─────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Family Transition Replay Stability</h3>
                <p className="panel-subtitle">
                  Per-family deltas for transition-adjusted attribution and transition integration contribution.
                </p>
              </div>
            </div>
            {familyTransitionReplayStabilitySummary.length === 0 ? (
              <p className="muted">No family transition replay stability rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Source run</th>
                    <th>Replay run</th>
                    <th>Family</th>
                    <th>Src transition</th>
                    <th>Replay transition</th>
                    <th>Transition adj Δ</th>
                    <th>Transition int Δ</th>
                    <th>State</th>
                    <th>Sequence</th>
                    <th>Rank (attr)</th>
                    <th>Rank (comp)</th>
                    <th>Drift codes</th>
                  </tr>
                </thead>
                <tbody>
                  {familyTransitionReplayStabilitySummary.slice(0, 80).map((row, idx) => (
                    <tr key={`${row.source_run_id}:${row.replay_run_id}:${row.dependency_family}:${idx}`}>
                      <td className="mono-cell text-sm">{shortId(row.source_run_id)}</td>
                      <td className="mono-cell text-sm">{shortId(row.replay_run_id)}</td>
                      <td>
                        <span className={FAMILY_BADGE[row.dependency_family] ?? "badge-muted"}>
                          {row.dependency_family}
                        </span>
                      </td>
                      <td>
                        {row.source_transition_state
                          ? <span className={TRANSITION_BADGE[row.source_transition_state] ?? "badge-muted"}>
                              {row.source_transition_state}
                            </span>
                          : <span className="badge-muted">—</span>}
                      </td>
                      <td>
                        {row.replay_transition_state
                          ? <span className={TRANSITION_BADGE[row.replay_transition_state] ?? "badge-muted"}>
                              {row.replay_transition_state}
                            </span>
                          : <span className="badge-muted">—</span>}
                      </td>
                      <td className="text-sm">{fmtDelta(row.transition_adjusted_delta)}</td>
                      <td className="text-sm">{fmtDelta(row.transition_integration_delta)}</td>
                      <td><span className={matchBadge(row.transition_state_match)}>{matchLabel(row.transition_state_match)}</span></td>
                      <td><span className={matchBadge(row.sequence_class_match)}>{matchLabel(row.sequence_class_match)}</span></td>
                      <td><span className={matchBadge(row.transition_family_rank_match)}>{matchLabel(row.transition_family_rank_match)}</span></td>
                      <td><span className={matchBadge(row.transition_composite_family_rank_match)}>{matchLabel(row.transition_composite_family_rank_match)}</span></td>
                      <td className="mono-cell text-sm">{reasonList(row.drift_reason_codes)}</td>
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
