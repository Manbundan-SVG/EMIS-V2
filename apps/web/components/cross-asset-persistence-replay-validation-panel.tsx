"use client";

import type {
  CrossAssetPersistenceReplayValidationSummaryRow,
  CrossAssetFamilyPersistenceReplayStabilitySummaryRow,
  CrossAssetPersistenceReplayStabilityAggregateRow,
} from "@/lib/queries/metrics";

type Props = {
  persistenceReplayValidationSummary: CrossAssetPersistenceReplayValidationSummaryRow[];
  familyPersistenceReplayStabilitySummary: CrossAssetFamilyPersistenceReplayStabilitySummaryRow[];
  persistenceReplayStabilityAggregate: CrossAssetPersistenceReplayStabilityAggregateRow | null;
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

const STATE_BADGE: Record<string, string> = {
  validated:           "badge-green",
  drift_detected:      "badge-red",
  insufficient_source: "badge-muted",
  insufficient_replay: "badge-muted",
  context_mismatch:    "badge-yellow",
  timing_mismatch:     "badge-yellow",
  transition_mismatch: "badge-yellow",
  archetype_mismatch:  "badge-yellow",
  cluster_mismatch:    "badge-yellow",
  persistence_mismatch:"badge-yellow",
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

function fmtScore(value: number | string | null | undefined, digits = 3): string {
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

export function CrossAssetPersistenceReplayValidationPanel({
  persistenceReplayValidationSummary,
  familyPersistenceReplayStabilitySummary,
  persistenceReplayStabilityAggregate,
  loading,
}: Props) {
  return (
    <section className="section">
      <div className="panel-header">
        <div>
          <h2>Persistence Replay Validation</h2>
          <p className="panel-subtitle">
            Phase 4.6D. Deterministic replay comparison across 4.6A persistence diagnostics +
            4.6B persistence-aware attribution + 4.6C persistence-aware composite. Numeric
            tolerance 1e-9 (memory score 1e-6); drift reason codes explicit. validation_state
            separates context_mismatch, timing_mismatch, transition_mismatch, archetype_mismatch,
            cluster_mismatch, and persistence_mismatch so operators can see the primary driver.
          </p>
        </div>
      </div>

      {loading && <p className="muted">Loading persistence replay validation…</p>}

      {!loading && (
        <>
          {/* ── Persistence Replay Stability Aggregate ─────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Persistence Replay Stability Aggregate</h3>
                <p className="panel-subtitle">Workspace rollup of persistence-layer match rates.</p>
              </div>
            </div>
            {!persistenceReplayStabilityAggregate ? (
              <p className="muted">No persistence replay validations recorded yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Validations</th>
                    <th>Context</th>
                    <th>Regime</th>
                    <th>Timing</th>
                    <th>Transition</th>
                    <th>Sequence</th>
                    <th>Archetype</th>
                    <th>Cluster state</th>
                    <th>Persist state</th>
                    <th>Memory</th>
                    <th>State age</th>
                    <th>Persist event</th>
                    <th>Persist attr</th>
                    <th>Persist comp</th>
                    <th>Persist dom family</th>
                    <th>Drift count</th>
                    <th>Latest</th>
                  </tr>
                </thead>
                <tbody>
                  <tr>
                    <td className="text-sm">{persistenceReplayStabilityAggregate.validation_count}</td>
                    <td><span className={rateBadge(persistenceReplayStabilityAggregate.context_match_rate)}>{fmtRate(persistenceReplayStabilityAggregate.context_match_rate)}</span></td>
                    <td><span className={rateBadge(persistenceReplayStabilityAggregate.regime_match_rate)}>{fmtRate(persistenceReplayStabilityAggregate.regime_match_rate)}</span></td>
                    <td><span className={rateBadge(persistenceReplayStabilityAggregate.timing_class_match_rate)}>{fmtRate(persistenceReplayStabilityAggregate.timing_class_match_rate)}</span></td>
                    <td><span className={rateBadge(persistenceReplayStabilityAggregate.transition_state_match_rate)}>{fmtRate(persistenceReplayStabilityAggregate.transition_state_match_rate)}</span></td>
                    <td><span className={rateBadge(persistenceReplayStabilityAggregate.sequence_class_match_rate)}>{fmtRate(persistenceReplayStabilityAggregate.sequence_class_match_rate)}</span></td>
                    <td><span className={rateBadge(persistenceReplayStabilityAggregate.archetype_match_rate)}>{fmtRate(persistenceReplayStabilityAggregate.archetype_match_rate)}</span></td>
                    <td><span className={rateBadge(persistenceReplayStabilityAggregate.cluster_state_match_rate)}>{fmtRate(persistenceReplayStabilityAggregate.cluster_state_match_rate)}</span></td>
                    <td><span className={rateBadge(persistenceReplayStabilityAggregate.persistence_state_match_rate)}>{fmtRate(persistenceReplayStabilityAggregate.persistence_state_match_rate)}</span></td>
                    <td><span className={rateBadge(persistenceReplayStabilityAggregate.memory_score_match_rate)}>{fmtRate(persistenceReplayStabilityAggregate.memory_score_match_rate)}</span></td>
                    <td><span className={rateBadge(persistenceReplayStabilityAggregate.state_age_match_rate)}>{fmtRate(persistenceReplayStabilityAggregate.state_age_match_rate)}</span></td>
                    <td><span className={rateBadge(persistenceReplayStabilityAggregate.persistence_event_match_rate)}>{fmtRate(persistenceReplayStabilityAggregate.persistence_event_match_rate)}</span></td>
                    <td><span className={rateBadge(persistenceReplayStabilityAggregate.persistence_attribution_match_rate)}>{fmtRate(persistenceReplayStabilityAggregate.persistence_attribution_match_rate)}</span></td>
                    <td><span className={rateBadge(persistenceReplayStabilityAggregate.persistence_composite_match_rate)}>{fmtRate(persistenceReplayStabilityAggregate.persistence_composite_match_rate)}</span></td>
                    <td><span className={rateBadge(persistenceReplayStabilityAggregate.persistence_dominant_family_match_rate)}>{fmtRate(persistenceReplayStabilityAggregate.persistence_dominant_family_match_rate)}</span></td>
                    <td className="text-sm">{persistenceReplayStabilityAggregate.drift_detected_count}</td>
                    <td className="text-sm muted">{fmtTs(persistenceReplayStabilityAggregate.latest_validated_at)}</td>
                  </tr>
                </tbody>
              </table>
            )}
          </div>

          {/* ── Persistence Replay Validation Summary ────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Persistence Replay Validation Summary</h3>
                <p className="panel-subtitle">Per-pair match flags and drift reason codes.</p>
              </div>
            </div>
            {persistenceReplayValidationSummary.length === 0 ? (
              <p className="muted">No persistence replay validation rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>State</th>
                    <th>Source run</th>
                    <th>Replay run</th>
                    <th>Src persist</th>
                    <th>Replay persist</th>
                    <th>Src memory</th>
                    <th>Replay memory</th>
                    <th>Src age</th>
                    <th>Replay age</th>
                    <th>Src event</th>
                    <th>Replay event</th>
                    <th>Ctx</th>
                    <th>Regime</th>
                    <th>Timing</th>
                    <th>Trans</th>
                    <th>Seq</th>
                    <th>Arch</th>
                    <th>Cluster</th>
                    <th>Persist</th>
                    <th>Mem</th>
                    <th>Age</th>
                    <th>Event</th>
                    <th>Pe attr</th>
                    <th>Pe comp</th>
                    <th>Pe dom</th>
                    <th>Drift codes</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {persistenceReplayValidationSummary.map((row) => (
                    <tr key={`${row.source_run_id}:${row.replay_run_id}`}>
                      <td>
                        <span className={STATE_BADGE[row.validation_state] ?? "badge-muted"}>
                          {row.validation_state}
                        </span>
                      </td>
                      <td className="mono-cell text-sm">{shortId(row.source_run_id)}</td>
                      <td className="mono-cell text-sm">{shortId(row.replay_run_id)}</td>
                      <td>
                        {row.source_persistence_state ? (
                          <span className={PERSISTENCE_STATE_BADGE[row.source_persistence_state] ?? "badge-muted"}>
                            {row.source_persistence_state}
                          </span>
                        ) : <span className="badge-muted">—</span>}
                      </td>
                      <td>
                        {row.replay_persistence_state ? (
                          <span className={PERSISTENCE_STATE_BADGE[row.replay_persistence_state] ?? "badge-muted"}>
                            {row.replay_persistence_state}
                          </span>
                        ) : <span className="badge-muted">—</span>}
                      </td>
                      <td className="text-sm">{fmtScore(row.source_memory_score)}</td>
                      <td className="text-sm">{fmtScore(row.replay_memory_score)}</td>
                      <td className="text-sm">{row.source_state_age_runs ?? "—"}</td>
                      <td className="text-sm">{row.replay_state_age_runs ?? "—"}</td>
                      <td>
                        {row.source_latest_persistence_event_type ? (
                          <span className={MEMORY_BREAK_BADGE[row.source_latest_persistence_event_type] ?? "badge-muted"}>
                            {row.source_latest_persistence_event_type}
                          </span>
                        ) : <span className="badge-muted">—</span>}
                      </td>
                      <td>
                        {row.replay_latest_persistence_event_type ? (
                          <span className={MEMORY_BREAK_BADGE[row.replay_latest_persistence_event_type] ?? "badge-muted"}>
                            {row.replay_latest_persistence_event_type}
                          </span>
                        ) : <span className="badge-muted">—</span>}
                      </td>
                      <td><span className={matchBadge(row.context_hash_match)}>{matchLabel(row.context_hash_match)}</span></td>
                      <td><span className={matchBadge(row.regime_match)}>{matchLabel(row.regime_match)}</span></td>
                      <td><span className={matchBadge(row.timing_class_match)}>{matchLabel(row.timing_class_match)}</span></td>
                      <td><span className={matchBadge(row.transition_state_match)}>{matchLabel(row.transition_state_match)}</span></td>
                      <td><span className={matchBadge(row.sequence_class_match)}>{matchLabel(row.sequence_class_match)}</span></td>
                      <td><span className={matchBadge(row.archetype_match)}>{matchLabel(row.archetype_match)}</span></td>
                      <td><span className={matchBadge(row.cluster_state_match)}>{matchLabel(row.cluster_state_match)}</span></td>
                      <td><span className={matchBadge(row.persistence_state_match)}>{matchLabel(row.persistence_state_match)}</span></td>
                      <td><span className={matchBadge(row.memory_score_match)}>{matchLabel(row.memory_score_match)}</span></td>
                      <td><span className={matchBadge(row.state_age_match)}>{matchLabel(row.state_age_match)}</span></td>
                      <td><span className={matchBadge(row.persistence_event_match)}>{matchLabel(row.persistence_event_match)}</span></td>
                      <td><span className={matchBadge(row.persistence_attribution_match)}>{matchLabel(row.persistence_attribution_match)}</span></td>
                      <td><span className={matchBadge(row.persistence_composite_match)}>{matchLabel(row.persistence_composite_match)}</span></td>
                      <td><span className={matchBadge(row.persistence_dominant_family_match)}>{matchLabel(row.persistence_dominant_family_match)}</span></td>
                      <td className="mono-cell text-sm">{reasonList(row.drift_reason_codes)}</td>
                      <td className="text-sm muted">{fmtTs(row.created_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* ── Family Persistence Replay Stability ──────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Family Persistence Replay Stability</h3>
                <p className="panel-subtitle">
                  Per-family deltas for persistence-adjusted attribution and persistence integration contribution.
                </p>
              </div>
            </div>
            {familyPersistenceReplayStabilitySummary.length === 0 ? (
              <p className="muted">No family persistence replay stability rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Source run</th>
                    <th>Replay run</th>
                    <th>Family</th>
                    <th>Src persist</th>
                    <th>Replay persist</th>
                    <th>Src memory</th>
                    <th>Replay memory</th>
                    <th>Src age</th>
                    <th>Replay age</th>
                    <th>Src event</th>
                    <th>Replay event</th>
                    <th>Persist adj Δ</th>
                    <th>Persist int Δ</th>
                    <th>Persist</th>
                    <th>Mem</th>
                    <th>Age</th>
                    <th>Event</th>
                    <th>Rank (attr)</th>
                    <th>Rank (comp)</th>
                    <th>Drift codes</th>
                  </tr>
                </thead>
                <tbody>
                  {familyPersistenceReplayStabilitySummary.slice(0, 80).map((row, idx) => (
                    <tr key={`${row.source_run_id}:${row.replay_run_id}:${row.dependency_family}:${idx}`}>
                      <td className="mono-cell text-sm">{shortId(row.source_run_id)}</td>
                      <td className="mono-cell text-sm">{shortId(row.replay_run_id)}</td>
                      <td>
                        <span className={FAMILY_BADGE[row.dependency_family] ?? "badge-muted"}>
                          {row.dependency_family}
                        </span>
                      </td>
                      <td>
                        {row.source_persistence_state ? (
                          <span className={PERSISTENCE_STATE_BADGE[row.source_persistence_state] ?? "badge-muted"}>
                            {row.source_persistence_state}
                          </span>
                        ) : <span className="badge-muted">—</span>}
                      </td>
                      <td>
                        {row.replay_persistence_state ? (
                          <span className={PERSISTENCE_STATE_BADGE[row.replay_persistence_state] ?? "badge-muted"}>
                            {row.replay_persistence_state}
                          </span>
                        ) : <span className="badge-muted">—</span>}
                      </td>
                      <td className="text-sm">{fmtScore(row.source_memory_score)}</td>
                      <td className="text-sm">{fmtScore(row.replay_memory_score)}</td>
                      <td className="text-sm">{row.source_state_age_runs ?? "—"}</td>
                      <td className="text-sm">{row.replay_state_age_runs ?? "—"}</td>
                      <td>
                        {row.source_latest_persistence_event_type ? (
                          <span className={MEMORY_BREAK_BADGE[row.source_latest_persistence_event_type] ?? "badge-muted"}>
                            {row.source_latest_persistence_event_type}
                          </span>
                        ) : <span className="badge-muted">—</span>}
                      </td>
                      <td>
                        {row.replay_latest_persistence_event_type ? (
                          <span className={MEMORY_BREAK_BADGE[row.replay_latest_persistence_event_type] ?? "badge-muted"}>
                            {row.replay_latest_persistence_event_type}
                          </span>
                        ) : <span className="badge-muted">—</span>}
                      </td>
                      <td className="text-sm">{fmtDelta(row.persistence_adjusted_delta)}</td>
                      <td className="text-sm">{fmtDelta(row.persistence_integration_delta)}</td>
                      <td><span className={matchBadge(row.persistence_state_match)}>{matchLabel(row.persistence_state_match)}</span></td>
                      <td><span className={matchBadge(row.memory_score_match)}>{matchLabel(row.memory_score_match)}</span></td>
                      <td><span className={matchBadge(row.state_age_match)}>{matchLabel(row.state_age_match)}</span></td>
                      <td><span className={matchBadge(row.persistence_event_match)}>{matchLabel(row.persistence_event_match)}</span></td>
                      <td><span className={matchBadge(row.persistence_family_rank_match)}>{matchLabel(row.persistence_family_rank_match)}</span></td>
                      <td><span className={matchBadge(row.persistence_composite_family_rank_match)}>{matchLabel(row.persistence_composite_family_rank_match)}</span></td>
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
