"use client";

import { formatNullable, formatTimestamp } from "@/lib/formatters/ops";
import type {
  GovernanceEscalationEffectivenessAnalyticsRow,
  GovernanceIncidentAnalyticsSnapshotRow,
  GovernanceIncidentAnalyticsSummaryRow,
  GovernancePromotionRollbackRiskSummaryRow,
  GovernanceRecurrenceBurdenRow,
  GovernanceRootCauseTrendRow,
  GovernanceRoutingPromotionImpactSummaryRow,
  GovernanceThresholdPromotionImpactSummaryRow,
} from "@/lib/queries/metrics";

type Props = {
  summary: GovernanceIncidentAnalyticsSummaryRow | null;
  rootCauseTrends: GovernanceRootCauseTrendRow[];
  recurrenceBurden: GovernanceRecurrenceBurdenRow[];
  escalationEffectiveness: GovernanceEscalationEffectivenessAnalyticsRow | null;
  snapshots: GovernanceIncidentAnalyticsSnapshotRow[];
  thresholdPromotionImpact: GovernanceThresholdPromotionImpactSummaryRow[];
  routingPromotionImpact: GovernanceRoutingPromotionImpactSummaryRow[];
  rollbackRisk: GovernancePromotionRollbackRiskSummaryRow[];
  loading: boolean;
};

function formatHours(value: number | null): string {
  if (value === null || Number.isNaN(value)) return "-";
  return `${value.toFixed(1)}h`;
}

function formatRate(value: number | null): string {
  if (value === null || Number.isNaN(value)) return "-";
  return `${(value * 100).toFixed(1)}%`;
}

function formatLatencyDelta(beforeMs: number | null, afterMs: number | null): string {
  if (beforeMs === null || afterMs === null || Number.isNaN(beforeMs) || Number.isNaN(afterMs)) return "-";
  return `${((afterMs - beforeMs) / 3600000).toFixed(1)}h`;
}

export function GovernanceIncidentAnalyticsPanel({
  summary,
  rootCauseTrends,
  recurrenceBurden,
  escalationEffectiveness,
  snapshots,
  thresholdPromotionImpact,
  routingPromotionImpact,
  rollbackRisk,
  loading,
}: Props) {
  const latestSnapshot = snapshots[0] ?? null;

  return (
    <section className="section">
      <div className="panel-header">
        <div>
          <h2>Incident Analytics</h2>
          <p className="panel-subtitle">Program-level incident volume, recurrence burden, and escalation effectiveness across the governance workflow.</p>
        </div>
      </div>

      {loading && <p className="muted">Loading incident analytics...</p>}

      {!loading && (
        <>
          <div className="kpi-grid">
            <div className="kpi-card">
              <div className="kpi-label">Open cases</div>
              <div className="kpi-value">{summary?.open_case_count ?? 0}</div>
            </div>
            <div className="kpi-card">
              <div className="kpi-label">Recurring cases</div>
              <div className="kpi-value">{summary?.recurring_case_count ?? 0}</div>
            </div>
            <div className="kpi-card">
              <div className="kpi-label">Mean ack</div>
              <div className="kpi-value">{formatHours(summary?.mean_ack_hours ?? null)}</div>
            </div>
            <div className="kpi-card">
              <div className="kpi-label">Mean resolve</div>
              <div className="kpi-value">{formatHours(summary?.mean_resolve_hours ?? null)}</div>
            </div>
          </div>

          <div className="ops-phase24-grid">
            <div className="panel">
              <div className="panel-header">
                <div>
                  <h3>Root Cause Burden</h3>
                  <p className="panel-subtitle">Which issue families are driving the most case volume and repeated incident load.</p>
                </div>
              </div>
              {rootCauseTrends.length === 0 ? (
                <p className="muted">No incident trend rows yet.</p>
              ) : (
                <table className="table">
                  <thead>
                    <tr>
                      <th>Root cause</th>
                      <th>Cases</th>
                      <th>Recurring</th>
                      <th>Reopened</th>
                      <th>Avg age</th>
                    </tr>
                  </thead>
                  <tbody>
                    {rootCauseTrends.slice(0, 10).map((row) => (
                      <tr key={row.root_cause_code}>
                        <td>{row.root_cause_code}</td>
                        <td>{row.case_count}</td>
                        <td>{row.recurring_count}</td>
                        <td>{row.reopened_count}</td>
                        <td>{formatHours(row.avg_case_age_hours)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>

            <div className="panel">
              <div className="panel-header">
                <div>
                  <h3>Recurrence Burden</h3>
                  <p className="panel-subtitle">Where repeat incidents are accumulating by watchlist and recurrence group density.</p>
                </div>
              </div>
              {recurrenceBurden.length === 0 ? (
                <p className="muted">No recurrence burden rows yet.</p>
              ) : (
                <table className="table">
                  <thead>
                    <tr>
                      <th>Watchlist</th>
                      <th>Recurring</th>
                      <th>Groups</th>
                      <th>Reopened</th>
                      <th>Max repeat</th>
                    </tr>
                  </thead>
                  <tbody>
                    {recurrenceBurden.slice(0, 10).map((row, index) => (
                      <tr key={`${row.watchlist_id ?? "workspace"}-${index}`}>
                        <td>{formatNullable(row.watchlist_id)}</td>
                        <td>{row.recurring_case_count}</td>
                        <td>{row.recurrence_group_count}</td>
                        <td>{row.reopened_case_count}</td>
                        <td>{row.max_repeat_count ?? "-"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>
          </div>

          <div className="ops-phase24-grid">
            <div className="panel">
              <div className="panel-header">
                <div>
                  <h3>Escalation Effectiveness</h3>
                  <p className="panel-subtitle">Whether escalations are resolving cases or feeding back into reopen burden.</p>
                </div>
              </div>
              {!escalationEffectiveness ? (
                <p className="muted">No escalation effectiveness rows yet.</p>
              ) : (
                <div className="kpi-grid">
                  <div className="kpi-card">
                    <div className="kpi-label">Escalated cases</div>
                    <div className="kpi-value">{escalationEffectiveness.escalated_case_count}</div>
                  </div>
                  <div className="kpi-card">
                    <div className="kpi-label">Resolved after escalation</div>
                    <div className="kpi-value">{escalationEffectiveness.escalated_resolved_count}</div>
                    <div className="muted">{formatRate(escalationEffectiveness.escalation_resolution_rate)}</div>
                  </div>
                  <div className="kpi-card">
                    <div className="kpi-label">Reopened after escalation</div>
                    <div className="kpi-value">{escalationEffectiveness.escalated_reopened_count}</div>
                    <div className="muted">{formatRate(escalationEffectiveness.escalation_reopen_rate)}</div>
                  </div>
                </div>
              )}
            </div>

            <div className="panel">
              <div className="panel-header">
                <div>
                  <h3>Latest Snapshot</h3>
                  <p className="panel-subtitle">Persisted analytics snapshot for time-series trend tracking and later impact analysis.</p>
                </div>
              </div>
              {!latestSnapshot ? (
                <p className="muted">No persisted analytics snapshots yet.</p>
              ) : (
                <table className="table">
                  <tbody>
                    <tr>
                      <th>Snapshot date</th>
                      <td>{formatTimestamp(latestSnapshot.snapshot_date)}</td>
                    </tr>
                    <tr>
                      <th>High severity open</th>
                      <td>{latestSnapshot.high_severity_open_count}</td>
                    </tr>
                    <tr>
                      <th>Stale case count</th>
                      <td>{latestSnapshot.stale_case_count}</td>
                    </tr>
                    <tr>
                      <th>Escalated cases</th>
                      <td>{latestSnapshot.escalated_case_count}</td>
                    </tr>
                    <tr>
                      <th>Escalation resolve rate</th>
                      <td>{formatRate(escalationEffectiveness?.escalation_resolution_rate ?? null)}</td>
                    </tr>
                  </tbody>
                </table>
              )}
            </div>
          </div>

          <div className="ops-phase24-grid">
            <div className="panel">
              <div className="panel-header">
                <div>
                  <h3>Threshold Promotion Impact</h3>
                  <p className="panel-subtitle">Whether threshold autopromotions reduced recurrence or escalation burden after activation.</p>
                </div>
              </div>
              {thresholdPromotionImpact.length === 0 ? (
                <p className="muted">No threshold promotion impact rows yet.</p>
              ) : (
                <table className="table">
                  <thead>
                    <tr>
                      <th>Scope</th>
                      <th>Impact</th>
                      <th>Recurrence</th>
                      <th>Escalation</th>
                      <th>Rollback risk</th>
                    </tr>
                  </thead>
                  <tbody>
                    {thresholdPromotionImpact.slice(0, 8).map((row) => (
                      <tr key={row.execution_id}>
                        <td>{`${row.scope_type}:${formatNullable(row.scope_value)}`}</td>
                        <td>{row.impact_classification}</td>
                        <td>{`${formatRate(row.recurrence_rate_before)} -> ${formatRate(row.recurrence_rate_after)}`}</td>
                        <td>{`${formatRate(row.escalation_rate_before)} -> ${formatRate(row.escalation_rate_after)}`}</td>
                        <td>{formatRate(row.rollback_risk_score)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>

            <div className="panel">
              <div className="panel-header">
                <div>
                  <h3>Routing Promotion Impact</h3>
                  <p className="panel-subtitle">How routing autopromotions affected recurrence, escalation, reassignment pressure, and resolution speed.</p>
                </div>
              </div>
              {routingPromotionImpact.length === 0 ? (
                <p className="muted">No routing promotion impact rows yet.</p>
              ) : (
                <table className="table">
                  <thead>
                    <tr>
                      <th>Scope</th>
                      <th>Impact</th>
                      <th>Reassign</th>
                      <th>Resolve delta</th>
                      <th>Rollback risk</th>
                    </tr>
                  </thead>
                  <tbody>
                    {routingPromotionImpact.slice(0, 8).map((row) => (
                      <tr key={row.execution_id}>
                        <td>{`${row.scope_type}:${formatNullable(row.scope_value)}`}</td>
                        <td>{row.impact_classification}</td>
                        <td>{`${formatRate(row.reassignment_rate_before)} -> ${formatRate(row.reassignment_rate_after)}`}</td>
                        <td>{formatLatencyDelta(row.resolution_latency_before_ms, row.resolution_latency_after_ms)}</td>
                        <td>{formatRate(row.rollback_risk_score)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>
          </div>

          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Rollback Risk</h3>
                <p className="panel-subtitle">Promotions whose downstream incident signals suggest degraded behavior and deserve review.</p>
              </div>
            </div>
            {rollbackRisk.length === 0 ? (
              <p className="muted">No rollback-risk promotions yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Promotion type</th>
                    <th>Scope</th>
                    <th>Impact</th>
                    <th>Rollback risk</th>
                    <th>Recorded</th>
                  </tr>
                </thead>
                <tbody>
                  {rollbackRisk.slice(0, 10).map((row) => (
                    <tr key={`${row.promotion_type}-${row.execution_id}`}>
                      <td>{row.promotion_type}</td>
                      <td>{`${row.scope_type}:${formatNullable(row.scope_value)}`}</td>
                      <td>{row.impact_classification}</td>
                      <td>{formatRate(row.rollback_risk_score)}</td>
                      <td>{formatTimestamp(row.created_at)}</td>
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
