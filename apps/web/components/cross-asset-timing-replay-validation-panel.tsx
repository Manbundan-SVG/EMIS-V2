"use client";

import type {
  CrossAssetTimingReplayValidationSummaryRow,
  CrossAssetFamilyTimingReplayStabilitySummaryRow,
  CrossAssetTimingReplayStabilityAggregateRow,
} from "@/lib/queries/metrics";

type Props = {
  timingReplayValidationSummary: CrossAssetTimingReplayValidationSummaryRow[];
  familyTimingReplayStabilitySummary: CrossAssetFamilyTimingReplayStabilitySummaryRow[];
  timingReplayStabilityAggregate: CrossAssetTimingReplayStabilityAggregateRow | null;
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

const STATE_BADGE: Record<string, string> = {
  validated: "badge-green",
  drift_detected: "badge-red",
  insufficient_source: "badge-muted",
  insufficient_replay: "badge-muted",
  context_mismatch: "badge-yellow",
  timing_mismatch: "badge-yellow",
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

export function CrossAssetTimingReplayValidationPanel({
  timingReplayValidationSummary,
  familyTimingReplayStabilitySummary,
  timingReplayStabilityAggregate,
  loading,
}: Props) {
  return (
    <section className="section">
      <div className="panel-header">
        <div>
          <h2>Timing Replay Validation</h2>
          <p className="panel-subtitle">
            Phase 4.2D. Deterministic replay comparison across 4.2A timing + 4.2B timing-aware
            attribution + 4.2C timing-aware composite. Numeric tolerance 1e-9; drift reason codes
            explicit. validation_state separates context_mismatch from timing_mismatch so operators
            can see the primary driver.
          </p>
        </div>
      </div>

      {loading && <p className="muted">Loading timing replay validation…</p>}

      {!loading && (
        <>
          {/* ── Timing Replay Stability Aggregate ──────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Timing Replay Stability Aggregate</h3>
                <p className="panel-subtitle">Workspace rollup of timing-layer match rates.</p>
              </div>
            </div>
            {!timingReplayStabilityAggregate ? (
              <p className="muted">No timing replay validations recorded yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Validations</th>
                    <th>Context</th>
                    <th>Regime</th>
                    <th>Timing class</th>
                    <th>Timing attr</th>
                    <th>Timing composite</th>
                    <th>Timing dom family</th>
                    <th>Drift count</th>
                    <th>Latest</th>
                  </tr>
                </thead>
                <tbody>
                  <tr>
                    <td className="text-sm">{timingReplayStabilityAggregate.validation_count}</td>
                    <td><span className={rateBadge(timingReplayStabilityAggregate.context_match_rate)}>{fmtRate(timingReplayStabilityAggregate.context_match_rate)}</span></td>
                    <td><span className={rateBadge(timingReplayStabilityAggregate.regime_match_rate)}>{fmtRate(timingReplayStabilityAggregate.regime_match_rate)}</span></td>
                    <td><span className={rateBadge(timingReplayStabilityAggregate.timing_class_match_rate)}>{fmtRate(timingReplayStabilityAggregate.timing_class_match_rate)}</span></td>
                    <td><span className={rateBadge(timingReplayStabilityAggregate.timing_attribution_match_rate)}>{fmtRate(timingReplayStabilityAggregate.timing_attribution_match_rate)}</span></td>
                    <td><span className={rateBadge(timingReplayStabilityAggregate.timing_composite_match_rate)}>{fmtRate(timingReplayStabilityAggregate.timing_composite_match_rate)}</span></td>
                    <td><span className={rateBadge(timingReplayStabilityAggregate.timing_dominant_family_match_rate)}>{fmtRate(timingReplayStabilityAggregate.timing_dominant_family_match_rate)}</span></td>
                    <td className="text-sm">{timingReplayStabilityAggregate.drift_detected_count}</td>
                    <td className="text-sm muted">{fmtTs(timingReplayStabilityAggregate.latest_validated_at)}</td>
                  </tr>
                </tbody>
              </table>
            )}
          </div>

          {/* ── Timing Replay Validation Summary ───────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Timing Replay Validation Summary</h3>
                <p className="panel-subtitle">Per-pair match flags and drift reason codes.</p>
              </div>
            </div>
            {timingReplayValidationSummary.length === 0 ? (
              <p className="muted">No timing replay validation rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>State</th>
                    <th>Source run</th>
                    <th>Replay run</th>
                    <th>Src class</th>
                    <th>Replay class</th>
                    <th>Ctx</th>
                    <th>Regime</th>
                    <th>Timing class</th>
                    <th>Timing attr</th>
                    <th>Timing comp</th>
                    <th>Timing dom</th>
                    <th>Drift codes</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {timingReplayValidationSummary.map((row) => (
                    <tr key={`${row.source_run_id}:${row.replay_run_id}`}>
                      <td>
                        <span className={STATE_BADGE[row.validation_state] ?? "badge-muted"}>
                          {row.validation_state}
                        </span>
                      </td>
                      <td className="mono-cell text-sm">{shortId(row.source_run_id)}</td>
                      <td className="mono-cell text-sm">{shortId(row.replay_run_id)}</td>
                      <td>
                        {row.source_dominant_timing_class
                          ? <span className={BUCKET_BADGE[row.source_dominant_timing_class] ?? "badge-muted"}>
                              {row.source_dominant_timing_class}
                            </span>
                          : <span className="badge-muted">—</span>}
                      </td>
                      <td>
                        {row.replay_dominant_timing_class
                          ? <span className={BUCKET_BADGE[row.replay_dominant_timing_class] ?? "badge-muted"}>
                              {row.replay_dominant_timing_class}
                            </span>
                          : <span className="badge-muted">—</span>}
                      </td>
                      <td><span className={matchBadge(row.context_hash_match)}>{matchLabel(row.context_hash_match)}</span></td>
                      <td><span className={matchBadge(row.regime_match)}>{matchLabel(row.regime_match)}</span></td>
                      <td><span className={matchBadge(row.timing_class_match)}>{matchLabel(row.timing_class_match)}</span></td>
                      <td><span className={matchBadge(row.timing_attribution_match)}>{matchLabel(row.timing_attribution_match)}</span></td>
                      <td><span className={matchBadge(row.timing_composite_match)}>{matchLabel(row.timing_composite_match)}</span></td>
                      <td><span className={matchBadge(row.timing_dominant_family_match)}>{matchLabel(row.timing_dominant_family_match)}</span></td>
                      <td className="mono-cell text-sm">{reasonList(row.drift_reason_codes)}</td>
                      <td className="text-sm muted">{fmtTs(row.created_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* ── Family Timing Replay Stability ─────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Family Timing Replay Stability</h3>
                <p className="panel-subtitle">
                  Per-family deltas for timing-adjusted attribution and timing integration contribution.
                </p>
              </div>
            </div>
            {familyTimingReplayStabilitySummary.length === 0 ? (
              <p className="muted">No family timing replay stability rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Source run</th>
                    <th>Replay run</th>
                    <th>Family</th>
                    <th>Src class</th>
                    <th>Replay class</th>
                    <th>Timing adj Δ</th>
                    <th>Timing int Δ</th>
                    <th>Class</th>
                    <th>Rank (attr)</th>
                    <th>Rank (comp)</th>
                    <th>Drift codes</th>
                  </tr>
                </thead>
                <tbody>
                  {familyTimingReplayStabilitySummary.slice(0, 80).map((row, idx) => (
                    <tr key={`${row.source_run_id}:${row.replay_run_id}:${row.dependency_family}:${idx}`}>
                      <td className="mono-cell text-sm">{shortId(row.source_run_id)}</td>
                      <td className="mono-cell text-sm">{shortId(row.replay_run_id)}</td>
                      <td>
                        <span className={FAMILY_BADGE[row.dependency_family] ?? "badge-muted"}>
                          {row.dependency_family}
                        </span>
                      </td>
                      <td>
                        {row.source_dominant_timing_class
                          ? <span className={BUCKET_BADGE[row.source_dominant_timing_class] ?? "badge-muted"}>
                              {row.source_dominant_timing_class}
                            </span>
                          : <span className="badge-muted">—</span>}
                      </td>
                      <td>
                        {row.replay_dominant_timing_class
                          ? <span className={BUCKET_BADGE[row.replay_dominant_timing_class] ?? "badge-muted"}>
                              {row.replay_dominant_timing_class}
                            </span>
                          : <span className="badge-muted">—</span>}
                      </td>
                      <td className="text-sm">{fmtDelta(row.timing_adjusted_delta)}</td>
                      <td className="text-sm">{fmtDelta(row.timing_integration_delta)}</td>
                      <td><span className={matchBadge(row.timing_class_match)}>{matchLabel(row.timing_class_match)}</span></td>
                      <td><span className={matchBadge(row.timing_family_rank_match)}>{matchLabel(row.timing_family_rank_match)}</span></td>
                      <td><span className={matchBadge(row.timing_composite_family_rank_match)}>{matchLabel(row.timing_composite_family_rank_match)}</span></td>
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
