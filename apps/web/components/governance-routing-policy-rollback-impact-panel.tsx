"use client";

import type {
  GovernanceRoutingPolicyRollbackImpactRow,
  GovernanceRoutingPolicyRollbackEffectivenessSummaryRow,
  GovernanceRoutingPolicyRollbackPendingEvaluationRow,
} from "@/lib/queries/metrics";

type Props = {
  impactRows: GovernanceRoutingPolicyRollbackImpactRow[];
  effectivenessSummary: GovernanceRoutingPolicyRollbackEffectivenessSummaryRow | null;
  pendingEvaluations: GovernanceRoutingPolicyRollbackPendingEvaluationRow[];
  loading: boolean;
};

const IMPACT_BADGE: Record<string, string> = {
  improved: "badge-green",
  neutral: "badge-muted",
  degraded: "badge-red",
  insufficient_data: "badge-yellow",
};

const IMPACT_LABEL: Record<string, string> = {
  improved: "improved",
  neutral: "neutral",
  degraded: "degraded",
  insufficient_data: "pending",
};

function fmtTs(ts: string | null | undefined): string {
  if (!ts) return "—";
  return new Date(ts).toLocaleString(undefined, { dateStyle: "short", timeStyle: "short" });
}

function fmtPct(v: number | null | undefined): string {
  if (v === null || v === undefined || Number.isNaN(v)) return "—";
  return `${(v * 100).toFixed(1)}%`;
}

function fmtDelta(v: number | null | undefined): string {
  if (v === null || v === undefined || Number.isNaN(v)) return "—";
  const pct = v * 100;
  const sign = pct > 0 ? "+" : "";
  return `${sign}${pct.toFixed(1)}%`;
}

function deltaClass(v: number | null | undefined, lowerIsBetter = true): string {
  if (v === null || v === undefined || Number.isNaN(v)) return "";
  const improved = lowerIsBetter ? v < -0.02 : v > 0.02;
  const degraded = lowerIsBetter ? v > 0.02 : v < -0.02;
  if (improved) return "badge-green";
  if (degraded) return "badge-red";
  return "badge-muted";
}

function fmtCount(v: number | null | undefined): string {
  if (v === null || v === undefined) return "—";
  return String(v);
}

export function GovernanceRoutingPolicyRollbackImpactPanel({
  impactRows,
  effectivenessSummary,
  pendingEvaluations,
  loading,
}: Props) {
  return (
    <section className="section">
      <div className="panel-header">
        <div>
          <h2>Rollback Impact Analysis</h2>
          <p className="panel-subtitle">
            Post-rollback outcome measurement. Compares routing feedback metrics before and after
            each rollback execution to classify impact. Classification is conservative:
            requires at least 3 days of post-rollback data and ≥10 cases.
          </p>
        </div>
      </div>

      {loading && <p className="muted">Loading rollback impact state…</p>}

      {!loading && (
        <>
          {/* ── 1. Rollback Effectiveness Summary ─────────────────────────── */}
          {effectivenessSummary && effectivenessSummary.rollback_count > 0 && (
            <div className="panel">
              <div className="panel-header">
                <div>
                  <h3>Rollback Effectiveness Summary</h3>
                  <p className="panel-subtitle">Aggregate rollback outcomes across all executions.</p>
                </div>
              </div>
              <table className="table">
                <thead>
                  <tr>
                    <th>Total</th>
                    <th>Improved</th>
                    <th>Neutral</th>
                    <th>Degraded</th>
                    <th>Insufficient data</th>
                    <th>Improved rate</th>
                    <th>Degraded rate</th>
                    <th>Avg Δ recurrence</th>
                    <th>Avg Δ escalation</th>
                    <th>Latest rollback</th>
                  </tr>
                </thead>
                <tbody>
                  <tr>
                    <td>{effectivenessSummary.rollback_count}</td>
                    <td><span className="badge-green">{effectivenessSummary.improved_count}</span></td>
                    <td><span className="badge-muted">{effectivenessSummary.neutral_count}</span></td>
                    <td><span className="badge-red">{effectivenessSummary.degraded_count}</span></td>
                    <td><span className="badge-yellow">{effectivenessSummary.insufficient_data_count}</span></td>
                    <td>{fmtPct(effectivenessSummary.improved_rate)}</td>
                    <td>{fmtPct(effectivenessSummary.degraded_rate)}</td>
                    <td>
                      <span className={deltaClass(effectivenessSummary.average_delta_recurrence_rate)}>
                        {fmtDelta(effectivenessSummary.average_delta_recurrence_rate)}
                      </span>
                    </td>
                    <td>
                      <span className={deltaClass(effectivenessSummary.average_delta_escalation_rate)}>
                        {fmtDelta(effectivenessSummary.average_delta_escalation_rate)}
                      </span>
                    </td>
                    <td className="text-sm">{fmtTs(effectivenessSummary.latest_rollback_at)}</td>
                  </tr>
                </tbody>
              </table>
            </div>
          )}

          {/* ── 2. Recent Rollback Impact ──────────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Recent Rollback Impact</h3>
                <p className="panel-subtitle">
                  Before/after routing metrics per rollback execution. Negative Δ = improvement (lower is better).
                </p>
              </div>
            </div>
            {impactRows.length === 0 ? (
              <p className="muted">
                No impact snapshots yet. Snapshots are computed automatically after rollbacks mature (≥3 days post-execution).
              </p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Key</th>
                    <th>Scope</th>
                    <th>Target</th>
                    <th>Classification</th>
                    <th>Window</th>
                    <th>Δ Recurrence</th>
                    <th>Δ Reassignment</th>
                    <th>Δ Escalation</th>
                    <th>Δ Resolve latency</th>
                    <th>Evaluated at</th>
                  </tr>
                </thead>
                <tbody>
                  {impactRows.map((row) => (
                    <tr key={row.rollback_execution_id}>
                      <td className="mono-cell text-sm">{row.recommendation_key.slice(0, 12)}</td>
                      <td>
                        <span className="badge-muted">{row.scope_type}</span>
                        {" "}<span className="text-sm">{row.scope_value}</span>
                      </td>
                      <td><span className="badge-muted">{row.target_type}</span></td>
                      <td>
                        <span className={IMPACT_BADGE[row.impact_classification] ?? "badge-muted"}>
                          {IMPACT_LABEL[row.impact_classification] ?? row.impact_classification}
                        </span>
                      </td>
                      <td className="text-sm muted">{row.evaluation_window_label}</td>
                      <td>
                        <span className={deltaClass(row.delta_recurrence_rate)}>
                          {fmtDelta(row.delta_recurrence_rate)}
                        </span>
                      </td>
                      <td>
                        <span className={deltaClass(row.delta_reassignment_rate)}>
                          {fmtDelta(row.delta_reassignment_rate)}
                        </span>
                      </td>
                      <td>
                        <span className={deltaClass(row.delta_escalation_rate)}>
                          {fmtDelta(row.delta_escalation_rate)}
                        </span>
                      </td>
                      <td>
                        <span className={deltaClass(row.delta_avg_resolve_latency_seconds)}>
                          {row.delta_avg_resolve_latency_seconds !== null
                            ? `${row.delta_avg_resolve_latency_seconds > 0 ? "+" : ""}${row.delta_avg_resolve_latency_seconds?.toFixed(0)}s`
                            : "—"}
                        </span>
                      </td>
                      <td className="text-sm">{fmtTs(row.created_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* ── 3. Pending Evaluation ──────────────────────────────────────── */}
          {pendingEvaluations.length > 0 && (
            <div className="panel">
              <div className="panel-header">
                <div>
                  <h3>Pending Evaluation</h3>
                  <p className="panel-subtitle">
                    Recent rollbacks that are too fresh or awaiting impact snapshot creation.
                  </p>
                </div>
              </div>
              <table className="table">
                <thead>
                  <tr>
                    <th>Key</th>
                    <th>Scope</th>
                    <th>Target</th>
                    <th>Executed at</th>
                    <th>Days since</th>
                    <th>Has snapshot</th>
                    <th>Reason</th>
                  </tr>
                </thead>
                <tbody>
                  {pendingEvaluations.map((pe) => (
                    <tr key={pe.rollback_execution_id}>
                      <td className="mono-cell text-sm">{pe.recommendation_key.slice(0, 12)}</td>
                      <td>
                        <span className="badge-muted">{pe.scope_type}</span>
                        {" "}<span className="text-sm">{pe.scope_value}</span>
                      </td>
                      <td><span className="badge-muted">{pe.target_type}</span></td>
                      <td className="text-sm">{fmtTs(pe.executed_at)}</td>
                      <td className="text-sm">{pe.days_since_execution?.toFixed(1)}</td>
                      <td>
                        {pe.has_impact_snapshot ? (
                          <span className="badge-green">yes</span>
                        ) : (
                          <span className="badge-muted">no</span>
                        )}
                      </td>
                      <td>
                        <span className="badge-muted text-xs">{pe.pending_reason_code}</span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </>
      )}
    </section>
  );
}
