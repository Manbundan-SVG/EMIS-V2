"use client";

import type {
  CrossAssetArchetypeReplayValidationSummaryRow,
  CrossAssetFamilyArchetypeReplayStabilitySummaryRow,
  CrossAssetArchetypeReplayStabilityAggregateRow,
} from "@/lib/queries/metrics";

type Props = {
  archetypeReplayValidationSummary: CrossAssetArchetypeReplayValidationSummaryRow[];
  familyArchetypeReplayStabilitySummary: CrossAssetFamilyArchetypeReplayStabilitySummaryRow[];
  archetypeReplayStabilityAggregate: CrossAssetArchetypeReplayStabilityAggregateRow | null;
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

const STATE_BADGE: Record<string, string> = {
  validated: "badge-green",
  drift_detected: "badge-red",
  insufficient_source: "badge-muted",
  insufficient_replay: "badge-muted",
  context_mismatch: "badge-yellow",
  timing_mismatch: "badge-yellow",
  transition_mismatch: "badge-yellow",
  archetype_mismatch: "badge-yellow",
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

export function CrossAssetArchetypeReplayValidationPanel({
  archetypeReplayValidationSummary,
  familyArchetypeReplayStabilitySummary,
  archetypeReplayStabilityAggregate,
  loading,
}: Props) {
  return (
    <section className="section">
      <div className="panel-header">
        <div>
          <h2>Archetype Replay Validation</h2>
          <p className="panel-subtitle">
            Phase 4.4D. Deterministic replay comparison across 4.4A archetype classifications +
            4.4B archetype-aware attribution + 4.4C archetype-aware composite. Numeric tolerance
            1e-9; drift reason codes explicit. validation_state separates context_mismatch,
            timing_mismatch, transition_mismatch, and archetype_mismatch so operators can see the
            primary driver.
          </p>
        </div>
      </div>

      {loading && <p className="muted">Loading archetype replay validation…</p>}

      {!loading && (
        <>
          {/* ── Archetype Replay Stability Aggregate ───────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Archetype Replay Stability Aggregate</h3>
                <p className="panel-subtitle">Workspace rollup of archetype-layer match rates.</p>
              </div>
            </div>
            {!archetypeReplayStabilityAggregate ? (
              <p className="muted">No archetype replay validations recorded yet.</p>
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
                    <th>Archetype</th>
                    <th>Archetype attr</th>
                    <th>Archetype comp</th>
                    <th>Archetype dom family</th>
                    <th>Drift count</th>
                    <th>Latest</th>
                  </tr>
                </thead>
                <tbody>
                  <tr>
                    <td className="text-sm">{archetypeReplayStabilityAggregate.validation_count}</td>
                    <td><span className={rateBadge(archetypeReplayStabilityAggregate.context_match_rate)}>{fmtRate(archetypeReplayStabilityAggregate.context_match_rate)}</span></td>
                    <td><span className={rateBadge(archetypeReplayStabilityAggregate.regime_match_rate)}>{fmtRate(archetypeReplayStabilityAggregate.regime_match_rate)}</span></td>
                    <td><span className={rateBadge(archetypeReplayStabilityAggregate.timing_class_match_rate)}>{fmtRate(archetypeReplayStabilityAggregate.timing_class_match_rate)}</span></td>
                    <td><span className={rateBadge(archetypeReplayStabilityAggregate.transition_state_match_rate)}>{fmtRate(archetypeReplayStabilityAggregate.transition_state_match_rate)}</span></td>
                    <td><span className={rateBadge(archetypeReplayStabilityAggregate.sequence_class_match_rate)}>{fmtRate(archetypeReplayStabilityAggregate.sequence_class_match_rate)}</span></td>
                    <td><span className={rateBadge(archetypeReplayStabilityAggregate.archetype_match_rate)}>{fmtRate(archetypeReplayStabilityAggregate.archetype_match_rate)}</span></td>
                    <td><span className={rateBadge(archetypeReplayStabilityAggregate.archetype_attribution_match_rate)}>{fmtRate(archetypeReplayStabilityAggregate.archetype_attribution_match_rate)}</span></td>
                    <td><span className={rateBadge(archetypeReplayStabilityAggregate.archetype_composite_match_rate)}>{fmtRate(archetypeReplayStabilityAggregate.archetype_composite_match_rate)}</span></td>
                    <td><span className={rateBadge(archetypeReplayStabilityAggregate.archetype_dominant_family_match_rate)}>{fmtRate(archetypeReplayStabilityAggregate.archetype_dominant_family_match_rate)}</span></td>
                    <td className="text-sm">{archetypeReplayStabilityAggregate.drift_detected_count}</td>
                    <td className="text-sm muted">{fmtTs(archetypeReplayStabilityAggregate.latest_validated_at)}</td>
                  </tr>
                </tbody>
              </table>
            )}
          </div>

          {/* ── Archetype Replay Validation Summary ────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Archetype Replay Validation Summary</h3>
                <p className="panel-subtitle">Per-pair match flags and drift reason codes.</p>
              </div>
            </div>
            {archetypeReplayValidationSummary.length === 0 ? (
              <p className="muted">No archetype replay validation rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>State</th>
                    <th>Source run</th>
                    <th>Replay run</th>
                    <th>Src archetype</th>
                    <th>Replay archetype</th>
                    <th>Ctx</th>
                    <th>Regime</th>
                    <th>Timing</th>
                    <th>Transition</th>
                    <th>Sequence</th>
                    <th>Archetype</th>
                    <th>Arch attr</th>
                    <th>Arch comp</th>
                    <th>Arch dom</th>
                    <th>Drift codes</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {archetypeReplayValidationSummary.map((row) => (
                    <tr key={`${row.source_run_id}:${row.replay_run_id}`}>
                      <td>
                        <span className={STATE_BADGE[row.validation_state] ?? "badge-muted"}>
                          {row.validation_state}
                        </span>
                      </td>
                      <td className="mono-cell text-sm">{shortId(row.source_run_id)}</td>
                      <td className="mono-cell text-sm">{shortId(row.replay_run_id)}</td>
                      <td>
                        {row.source_dominant_archetype_key ? (
                          <span className={ARCHETYPE_BADGE[row.source_dominant_archetype_key] ?? "badge-muted"}>
                            {row.source_dominant_archetype_key}
                          </span>
                        ) : <span className="badge-muted">—</span>}
                      </td>
                      <td>
                        {row.replay_dominant_archetype_key ? (
                          <span className={ARCHETYPE_BADGE[row.replay_dominant_archetype_key] ?? "badge-muted"}>
                            {row.replay_dominant_archetype_key}
                          </span>
                        ) : <span className="badge-muted">—</span>}
                      </td>
                      <td><span className={matchBadge(row.context_hash_match)}>{matchLabel(row.context_hash_match)}</span></td>
                      <td><span className={matchBadge(row.regime_match)}>{matchLabel(row.regime_match)}</span></td>
                      <td><span className={matchBadge(row.timing_class_match)}>{matchLabel(row.timing_class_match)}</span></td>
                      <td><span className={matchBadge(row.transition_state_match)}>{matchLabel(row.transition_state_match)}</span></td>
                      <td><span className={matchBadge(row.sequence_class_match)}>{matchLabel(row.sequence_class_match)}</span></td>
                      <td><span className={matchBadge(row.archetype_match)}>{matchLabel(row.archetype_match)}</span></td>
                      <td><span className={matchBadge(row.archetype_attribution_match)}>{matchLabel(row.archetype_attribution_match)}</span></td>
                      <td><span className={matchBadge(row.archetype_composite_match)}>{matchLabel(row.archetype_composite_match)}</span></td>
                      <td><span className={matchBadge(row.archetype_dominant_family_match)}>{matchLabel(row.archetype_dominant_family_match)}</span></td>
                      <td className="mono-cell text-sm">{reasonList(row.drift_reason_codes)}</td>
                      <td className="text-sm muted">{fmtTs(row.created_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* ── Family Archetype Replay Stability ──────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Family Archetype Replay Stability</h3>
                <p className="panel-subtitle">
                  Per-family deltas for archetype-adjusted attribution and archetype integration contribution.
                </p>
              </div>
            </div>
            {familyArchetypeReplayStabilitySummary.length === 0 ? (
              <p className="muted">No family archetype replay stability rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Source run</th>
                    <th>Replay run</th>
                    <th>Family</th>
                    <th>Src archetype</th>
                    <th>Replay archetype</th>
                    <th>Src transition</th>
                    <th>Replay transition</th>
                    <th>Archetype adj Δ</th>
                    <th>Archetype int Δ</th>
                    <th>Archetype</th>
                    <th>Transition</th>
                    <th>Sequence</th>
                    <th>Rank (attr)</th>
                    <th>Rank (comp)</th>
                    <th>Drift codes</th>
                  </tr>
                </thead>
                <tbody>
                  {familyArchetypeReplayStabilitySummary.slice(0, 80).map((row, idx) => (
                    <tr key={`${row.source_run_id}:${row.replay_run_id}:${row.dependency_family}:${idx}`}>
                      <td className="mono-cell text-sm">{shortId(row.source_run_id)}</td>
                      <td className="mono-cell text-sm">{shortId(row.replay_run_id)}</td>
                      <td>
                        <span className={FAMILY_BADGE[row.dependency_family] ?? "badge-muted"}>
                          {row.dependency_family}
                        </span>
                      </td>
                      <td>
                        {row.source_archetype_key ? (
                          <span className={ARCHETYPE_BADGE[row.source_archetype_key] ?? "badge-muted"}>
                            {row.source_archetype_key}
                          </span>
                        ) : <span className="badge-muted">—</span>}
                      </td>
                      <td>
                        {row.replay_archetype_key ? (
                          <span className={ARCHETYPE_BADGE[row.replay_archetype_key] ?? "badge-muted"}>
                            {row.replay_archetype_key}
                          </span>
                        ) : <span className="badge-muted">—</span>}
                      </td>
                      <td>
                        {row.source_transition_state ? (
                          <span className={TRANSITION_BADGE[row.source_transition_state] ?? "badge-muted"}>
                            {row.source_transition_state}
                          </span>
                        ) : <span className="badge-muted">—</span>}
                      </td>
                      <td>
                        {row.replay_transition_state ? (
                          <span className={TRANSITION_BADGE[row.replay_transition_state] ?? "badge-muted"}>
                            {row.replay_transition_state}
                          </span>
                        ) : <span className="badge-muted">—</span>}
                      </td>
                      <td className="text-sm">{fmtDelta(row.archetype_adjusted_delta)}</td>
                      <td className="text-sm">{fmtDelta(row.archetype_integration_delta)}</td>
                      <td><span className={matchBadge(row.archetype_match)}>{matchLabel(row.archetype_match)}</span></td>
                      <td><span className={matchBadge(row.transition_state_match)}>{matchLabel(row.transition_state_match)}</span></td>
                      <td><span className={matchBadge(row.sequence_class_match)}>{matchLabel(row.sequence_class_match)}</span></td>
                      <td><span className={matchBadge(row.archetype_family_rank_match)}>{matchLabel(row.archetype_family_rank_match)}</span></td>
                      <td><span className={matchBadge(row.archetype_composite_family_rank_match)}>{matchLabel(row.archetype_composite_family_rank_match)}</span></td>
                      <td className="mono-cell text-sm">{reasonList(row.drift_reason_codes)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* Silence unused var warning for sequence badges while still keeping the import. */}
          <span style={{ display: "none" }} className={SEQUENCE_BADGE.reinforcing_path}>hidden</span>
        </>
      )}
    </section>
  );
}
