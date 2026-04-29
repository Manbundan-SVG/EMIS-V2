"use client";

import type {
  CrossAssetReplayValidationSummaryRow,
  CrossAssetFamilyReplayStabilitySummaryRow,
  CrossAssetReplayStabilityAggregateRow,
} from "@/lib/queries/metrics";

type Props = {
  replayValidationSummary: CrossAssetReplayValidationSummaryRow[];
  familyReplayStabilitySummary: CrossAssetFamilyReplayStabilitySummaryRow[];
  replayStabilityAggregate: CrossAssetReplayStabilityAggregateRow | null;
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

const STATE_BADGE: Record<string, string> = {
  validated: "badge-green",
  drift_detected: "badge-red",
  insufficient_source: "badge-muted",
  insufficient_replay: "badge-muted",
  context_mismatch: "badge-yellow",
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

function matchBadge(value: boolean | null | undefined): string {
  if (value === true) return "badge-green";
  if (value === false) return "badge-red";
  return "badge-muted";
}

function matchLabel(value: boolean | null | undefined): string {
  if (value === true) return "match";
  if (value === false) return "differ";
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

export function CrossAssetReplayValidationPanel({
  replayValidationSummary,
  familyReplayStabilitySummary,
  replayStabilityAggregate,
  loading,
}: Props) {
  return (
    <section className="section">
      <div className="panel-header">
        <div>
          <h2>Cross-Asset Replay Validation</h2>
          <p className="panel-subtitle">
            Phase 4.1D. Deterministic replay comparison across raw / weighted / regime-aware
            attribution + context + regime. Numeric tolerance 1e-9; drift reason codes explicit and
            enumerated.
          </p>
        </div>
      </div>

      {loading && <p className="muted">Loading replay validation…</p>}

      {!loading && (
        <>
          {/* ── Replay Stability Aggregate ─────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Replay Stability Aggregate</h3>
                <p className="panel-subtitle">
                  Workspace rollup: match rates across layers + recent drift count.
                </p>
              </div>
            </div>
            {!replayStabilityAggregate ? (
              <p className="muted">No replay validations recorded yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Validations</th>
                    <th>Context match</th>
                    <th>Regime match</th>
                    <th>Raw attr</th>
                    <th>Weighted attr</th>
                    <th>Regime attr</th>
                    <th>Dominant (raw)</th>
                    <th>Dominant (weighted)</th>
                    <th>Dominant (regime)</th>
                    <th>Drift count</th>
                    <th>Latest</th>
                  </tr>
                </thead>
                <tbody>
                  <tr>
                    <td className="text-sm">{replayStabilityAggregate.validation_count}</td>
                    <td><span className={rateBadge(replayStabilityAggregate.context_match_rate)}>{fmtRate(replayStabilityAggregate.context_match_rate)}</span></td>
                    <td><span className={rateBadge(replayStabilityAggregate.regime_match_rate)}>{fmtRate(replayStabilityAggregate.regime_match_rate)}</span></td>
                    <td><span className={rateBadge(replayStabilityAggregate.raw_match_rate)}>{fmtRate(replayStabilityAggregate.raw_match_rate)}</span></td>
                    <td><span className={rateBadge(replayStabilityAggregate.weighted_match_rate)}>{fmtRate(replayStabilityAggregate.weighted_match_rate)}</span></td>
                    <td><span className={rateBadge(replayStabilityAggregate.regime_match_rate_attribution)}>{fmtRate(replayStabilityAggregate.regime_match_rate_attribution)}</span></td>
                    <td><span className={rateBadge(replayStabilityAggregate.dominant_family_match_rate)}>{fmtRate(replayStabilityAggregate.dominant_family_match_rate)}</span></td>
                    <td><span className={rateBadge(replayStabilityAggregate.weighted_dominant_family_match_rate)}>{fmtRate(replayStabilityAggregate.weighted_dominant_family_match_rate)}</span></td>
                    <td><span className={rateBadge(replayStabilityAggregate.regime_dominant_family_match_rate)}>{fmtRate(replayStabilityAggregate.regime_dominant_family_match_rate)}</span></td>
                    <td className="text-sm">{replayStabilityAggregate.drift_detected_count}</td>
                    <td className="text-sm muted">{fmtTs(replayStabilityAggregate.latest_validated_at)}</td>
                  </tr>
                </tbody>
              </table>
            )}
          </div>

          {/* ── Replay Validation Summary ──────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Replay Validation Summary</h3>
                <p className="panel-subtitle">Per-pair match flags and drift reason codes.</p>
              </div>
            </div>
            {replayValidationSummary.length === 0 ? (
              <p className="muted">No replay validation rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>State</th>
                    <th>Source run</th>
                    <th>Replay run</th>
                    <th>Ctx</th>
                    <th>Regime</th>
                    <th>Raw</th>
                    <th>Weighted</th>
                    <th>Regime attr</th>
                    <th>Dom (raw)</th>
                    <th>Dom (wt)</th>
                    <th>Dom (regime)</th>
                    <th>Drift codes</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {replayValidationSummary.map((row) => (
                    <tr key={`${row.source_run_id}:${row.replay_run_id}`}>
                      <td>
                        <span className={STATE_BADGE[row.validation_state] ?? "badge-muted"}>
                          {row.validation_state}
                        </span>
                      </td>
                      <td className="mono-cell text-sm">{shortId(row.source_run_id)}</td>
                      <td className="mono-cell text-sm">{shortId(row.replay_run_id)}</td>
                      <td><span className={matchBadge(row.context_hash_match)}>{matchLabel(row.context_hash_match)}</span></td>
                      <td><span className={matchBadge(row.regime_match)}>{matchLabel(row.regime_match)}</span></td>
                      <td><span className={matchBadge(row.raw_attribution_match)}>{matchLabel(row.raw_attribution_match)}</span></td>
                      <td><span className={matchBadge(row.weighted_attribution_match)}>{matchLabel(row.weighted_attribution_match)}</span></td>
                      <td><span className={matchBadge(row.regime_attribution_match)}>{matchLabel(row.regime_attribution_match)}</span></td>
                      <td><span className={matchBadge(row.dominant_family_match)}>{matchLabel(row.dominant_family_match)}</span></td>
                      <td><span className={matchBadge(row.weighted_dominant_family_match)}>{matchLabel(row.weighted_dominant_family_match)}</span></td>
                      <td><span className={matchBadge(row.regime_dominant_family_match)}>{matchLabel(row.regime_dominant_family_match)}</span></td>
                      <td className="mono-cell text-sm">{reasonList(row.drift_reason_codes)}</td>
                      <td className="text-sm muted">{fmtTs(row.created_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* ── Family Replay Stability ────────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Family Replay Stability</h3>
                <p className="panel-subtitle">
                  Per-family deltas across raw, weighted, and regime-adjusted layers.
                </p>
              </div>
            </div>
            {familyReplayStabilitySummary.length === 0 ? (
              <p className="muted">No family replay stability rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Source run</th>
                    <th>Replay run</th>
                    <th>Family</th>
                    <th>Raw Δ</th>
                    <th>Weighted Δ</th>
                    <th>Regime Δ</th>
                    <th>Raw rank</th>
                    <th>Wt rank</th>
                    <th>Regime rank</th>
                    <th>Drift codes</th>
                  </tr>
                </thead>
                <tbody>
                  {familyReplayStabilitySummary.slice(0, 80).map((row, idx) => (
                    <tr key={`${row.source_run_id}:${row.replay_run_id}:${row.dependency_family}:${idx}`}>
                      <td className="mono-cell text-sm">{shortId(row.source_run_id)}</td>
                      <td className="mono-cell text-sm">{shortId(row.replay_run_id)}</td>
                      <td>
                        <span className={FAMILY_BADGE[row.dependency_family] ?? "badge-muted"}>
                          {row.dependency_family}
                        </span>
                      </td>
                      <td className="text-sm">{fmtDelta(row.raw_delta)}</td>
                      <td className="text-sm">{fmtDelta(row.weighted_delta)}</td>
                      <td className="text-sm">{fmtDelta(row.regime_delta)}</td>
                      <td><span className={matchBadge(row.family_rank_match)}>{matchLabel(row.family_rank_match)}</span></td>
                      <td><span className={matchBadge(row.weighted_family_rank_match)}>{matchLabel(row.weighted_family_rank_match)}</span></td>
                      <td><span className={matchBadge(row.regime_family_rank_match)}>{matchLabel(row.regime_family_rank_match)}</span></td>
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
