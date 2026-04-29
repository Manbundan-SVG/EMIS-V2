"use client";

import { formatNullable, formatTimestamp } from "@/lib/formatters/ops";
import type {
  GovernanceChronicWatchlistSummaryRow,
  GovernanceManagerOverviewSummaryRow,
  GovernanceOperatingRiskSummaryRow,
  GovernanceOperatorTeamComparisonSummaryRow,
  GovernancePromotionHealthOverviewRow,
  GovernanceReviewPriorityRow,
  GovernanceTrendWindowRow,
} from "@/lib/queries/metrics";

type Props = {
  managerOverview: GovernanceManagerOverviewSummaryRow[];
  chronicWatchlists: GovernanceChronicWatchlistSummaryRow[];
  operatorTeamComparison: GovernanceOperatorTeamComparisonSummaryRow[];
  promotionHealth: GovernancePromotionHealthOverviewRow[];
  operatingRisk: GovernanceOperatingRiskSummaryRow[];
  reviewPriorities: GovernanceReviewPriorityRow[];
  trendWindows: GovernanceTrendWindowRow[];
  loading: boolean;
};

function formatRate(value: number | null): string {
  if (value === null || Number.isNaN(value)) return "-";
  return `${(value * 100).toFixed(1)}%`;
}

function formatSecondsAsHours(value: number | null): string {
  if (value === null || Number.isNaN(value)) return "-";
  return `${(value / 3600).toFixed(1)}h`;
}

function formatReason(code: string): string {
  switch (code) {
    case "recurring_burden_spike":
      return "Recurring burden spike";
    case "stale_severe_backlog":
      return "Stale severe backlog";
    case "degraded_promotion_health":
      return "Degraded promotion health";
    case "operator_overload":
      return "Operator overload";
    case "team_overload":
      return "Team overload";
    case "escalation_concentration":
      return "Escalation concentration";
    case "promotion_review":
      return "Promotion review";
    case "open_burden_concentration":
      return "Open burden concentration";
    default:
      return code.replaceAll("_", " ");
  }
}

function formatMetricName(metricName: string): string {
  return metricName.replace(/_/g, " ").replace(/\b\w/g, (match) => match.toUpperCase());
}

function formatTrendValue(value: number | null): string {
  if (value === null || Number.isNaN(value)) return "-";
  return Number.isInteger(value) ? `${value}` : value.toFixed(2);
}

export function GovernanceManagerOverviewPanel({
  managerOverview,
  chronicWatchlists,
  operatorTeamComparison,
  promotionHealth,
  operatingRisk,
  reviewPriorities,
  trendWindows,
  loading,
}: Props) {
  const latestOverview = managerOverview[0] ?? null;
  const latestRisk = operatingRisk[0] ?? null;
  const topHotspots = operatorTeamComparison.slice(0, 6);
  const trendGroups = ["7d", "30d", "90d"].map((windowLabel) => ({
    windowLabel,
    rows: trendWindows.filter((row) => row.window_label === windowLabel),
  }));

  return (
    <section className="section">
      <div className="panel-header">
        <div>
          <h2>Manager Overview</h2>
          <p className="panel-subtitle">Cross-panel operating review for chronic burden, handling hotspots, promotion health, and current risk posture.</p>
        </div>
      </div>

      {loading && <p className="muted">Loading manager overview...</p>}

      {!loading && (
        <>
          <div className="kpi-grid">
            <div className="kpi-card">
              <div className="kpi-label">Operating burden</div>
              <div className="kpi-value">{latestOverview?.total_operating_burden ?? 0}</div>
              <div className="muted">{latestOverview ? `${latestOverview.window_days}d window` : "No overview yet"}</div>
            </div>
            <div className="kpi-card">
              <div className="kpi-label">Chronic watchlists</div>
              <div className="kpi-value">{latestOverview?.chronic_watchlist_count ?? 0}</div>
            </div>
            <div className="kpi-card">
              <div className="kpi-label">Degraded promotions</div>
              <div className="kpi-value">{latestOverview?.degraded_promotion_count ?? 0}</div>
            </div>
            <div className="kpi-card">
              <div className="kpi-label">Operating risk</div>
              <div className="kpi-value">{latestRisk?.operating_risk ?? "low"}</div>
              <div className="muted">{latestRisk ? formatTimestamp(latestRisk.snapshot_at) : "No risk row yet"}</div>
            </div>
          </div>

          <div className="ops-phase24-grid">
            <div className="panel">
              <div className="panel-header">
                <div>
                  <h3>Top Review Priorities</h3>
                  <p className="panel-subtitle">Ranked operating review targets with transparent score, reason, and supporting burden counts.</p>
                </div>
              </div>
              {reviewPriorities.length === 0 ? (
                <p className="muted">No ranked review priorities yet.</p>
              ) : (
                <table className="table">
                  <thead>
                    <tr>
                      <th>Rank</th>
                      <th>Target</th>
                      <th>Reason</th>
                      <th>Score</th>
                      <th>Open</th>
                      <th>Recurring</th>
                      <th>Escalated</th>
                      <th>Stale</th>
                      <th>Rollback</th>
                    </tr>
                  </thead>
                  <tbody>
                    {reviewPriorities.map((row) => (
                      <tr key={`${row.entity_type}-${row.entity_key}`}>
                        <td>{row.priority_rank}</td>
                        <td>
                          <div>{row.entity_label}</div>
                          <div className="muted">{row.entity_type}</div>
                          {(row.latest_regime || row.latest_root_cause) && (
                            <div className="muted">
                              {[row.latest_regime, row.latest_root_cause].filter(Boolean).join(" / ")}
                            </div>
                          )}
                        </td>
                        <td>{formatReason(row.priority_reason_code)}</td>
                        <td>{row.priority_score.toFixed(1)}</td>
                        <td>{row.open_case_count}</td>
                        <td>{row.recurring_case_count}</td>
                        <td>{row.escalated_case_count}</td>
                        <td>{row.stale_case_count}</td>
                        <td>{row.rollback_risk_count}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>

            <div className="panel">
              <div className="panel-header">
                <div>
                  <h3>Trend Windows</h3>
                  <p className="panel-subtitle">Simple current-versus-prior window comparisons for 7d, 30d, and 90d operating burden.</p>
                </div>
              </div>
              {trendWindows.length === 0 ? (
                <p className="muted">No trend-window rows yet.</p>
              ) : (
                <div className="ops-phase24-grid">
                  {trendGroups.map((group) => (
                    <div className="panel" key={group.windowLabel}>
                      <div className="panel-header">
                        <div>
                          <h4>{group.windowLabel}</h4>
                          <p className="panel-subtitle">Current window versus the immediately prior equal-length window.</p>
                        </div>
                      </div>
                      {group.rows.length === 0 ? (
                        <p className="muted">No rows for this window.</p>
                      ) : (
                        <table className="table">
                          <thead>
                            <tr>
                              <th>Metric</th>
                              <th>Current</th>
                              <th>Prior</th>
                              <th>Delta</th>
                              <th>Trend</th>
                            </tr>
                          </thead>
                          <tbody>
                            {group.rows.map((row) => (
                              <tr key={`${row.window_label}-${row.metric_name}`}>
                                <td>{formatMetricName(row.metric_name)}</td>
                                <td>{formatTrendValue(row.current_value)}</td>
                                <td>{formatTrendValue(row.prior_value)}</td>
                                <td>
                                  {formatTrendValue(row.delta_abs)}
                                  {row.delta_pct !== null ? <span className="muted"> ({row.delta_pct.toFixed(1)}%)</span> : null}
                                </td>
                                <td>{row.trend_direction}</td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      )}
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>

          <div className="ops-phase24-grid">
            <div className="panel">
              <div className="panel-header">
                <div>
                  <h3>Chronic Watchlists</h3>
                  <p className="panel-subtitle">Where recurring and reopened burden is concentrating most heavily.</p>
                </div>
              </div>
              {chronicWatchlists.length === 0 ? (
                <p className="muted">No chronic watchlists yet.</p>
              ) : (
                <table className="table">
                  <thead>
                    <tr>
                      <th>Watchlist</th>
                      <th>Recurring</th>
                      <th>Reopened</th>
                      <th>Max repeat</th>
                      <th>Latest case</th>
                    </tr>
                  </thead>
                  <tbody>
                    {chronicWatchlists.slice(0, 8).map((row) => (
                      <tr key={row.watchlist_id ?? `${row.workspace_id}-workspace`}>
                        <td>{row.watchlist_name ?? row.watchlist_slug ?? formatNullable(row.watchlist_id)}</td>
                        <td>{row.recurring_case_count}</td>
                        <td>{row.reopened_case_count}</td>
                        <td>{row.max_repeat_count ?? "-"}</td>
                        <td>{formatTimestamp(row.latest_case_at)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>

            <div className="panel">
              <div className="panel-header">
                <div>
                  <h3>Promotion Health</h3>
                  <p className="panel-subtitle">Threshold and routing promotion classes ranked by impact quality and rollback exposure.</p>
                </div>
              </div>
              {promotionHealth.length === 0 ? (
                <p className="muted">No promotion health rows yet.</p>
              ) : (
                <table className="table">
                  <thead>
                    <tr>
                      <th>Type</th>
                      <th>Improved</th>
                      <th>Degraded</th>
                      <th>Rollback</th>
                      <th>Avg risk</th>
                    </tr>
                  </thead>
                  <tbody>
                    {promotionHealth.map((row) => (
                      <tr key={`${row.workspace_id}-${row.promotion_type}`}>
                        <td>{row.promotion_type}</td>
                        <td>{row.improved_count}</td>
                        <td>{row.degraded_count}</td>
                        <td>{row.rollback_candidate_count}</td>
                        <td>{formatRate(row.avg_rollback_risk_score)}</td>
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
                  <h3>Operator / Team Hotspots</h3>
                  <p className="panel-subtitle">High-load entities with weaker quality or heavier chronic burden that deserve manager attention.</p>
                </div>
              </div>
              {topHotspots.length === 0 ? (
                <p className="muted">No comparison rows yet.</p>
              ) : (
                <table className="table">
                  <thead>
                    <tr>
                      <th>Entity</th>
                      <th>Open</th>
                      <th>Quality</th>
                      <th>Reopen</th>
                      <th>Load</th>
                      <th>Avg resolve</th>
                    </tr>
                  </thead>
                  <tbody>
                    {topHotspots.map((row, index) => (
                      <tr key={`${row.entity_type}-${row.actor_name}-${index}`}>
                        <td>
                          <div>{row.actor_name}</div>
                          <div className="muted">{row.entity_type}</div>
                        </td>
                        <td>{row.active_open_case_count}</td>
                        <td>{formatRate(row.resolution_quality_proxy)}</td>
                        <td>{formatRate(row.reopen_rate)}</td>
                        <td>{row.severity_weighted_load?.toFixed(1) ?? "-"}</td>
                        <td>{formatSecondsAsHours(row.avg_resolve_seconds)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>
          </div>
        </>
      )}
    </section>
  );
}
