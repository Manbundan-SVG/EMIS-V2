"use client";

import { formatNullable, formatTimestamp } from "@/lib/formatters/ops";
import type {
  GovernanceCaseMixSummaryRow,
  GovernanceOperatorPerformanceSummaryRow,
  GovernancePerformanceSnapshotRow,
  GovernanceTeamPerformanceSummaryRow,
} from "@/lib/queries/metrics";

type Props = {
  operatorSummary: GovernanceOperatorPerformanceSummaryRow[];
  teamSummary: GovernanceTeamPerformanceSummaryRow[];
  operatorCaseMix: GovernanceCaseMixSummaryRow[];
  teamCaseMix: GovernanceCaseMixSummaryRow[];
  snapshots: GovernancePerformanceSnapshotRow[];
  loading: boolean;
};

function formatSecondsAsHours(value: number | null): string {
  if (value === null || Number.isNaN(value)) return "-";
  return `${(value / 3600).toFixed(1)}h`;
}

function formatRate(value: number): string {
  return `${(value * 100).toFixed(1)}%`;
}

export function GovernanceOperatorPerformancePanel({
  operatorSummary,
  teamSummary,
  operatorCaseMix,
  teamCaseMix,
  snapshots,
  loading,
}: Props) {
  const topOperator = operatorSummary[0] ?? null;
  const topTeam = teamSummary[0] ?? null;
  const latestSnapshot = snapshots[0] ?? null;

  return (
    <section className="section">
      <div className="panel-header">
        <div>
          <h2>Operator / Team Performance</h2>
          <p className="panel-subtitle">Handling effectiveness, reopen burden, and case-mix difficulty across operators and teams.</p>
        </div>
      </div>

      {loading && <p className="muted">Loading operator and team performance...</p>}

      {!loading && (
        <>
          <div className="kpi-grid">
            <div className="kpi-card">
              <div className="kpi-label">Operators tracked</div>
              <div className="kpi-value">{operatorSummary.length}</div>
            </div>
            <div className="kpi-card">
              <div className="kpi-label">Top operator</div>
              <div className="kpi-value">{topOperator?.operator_name ?? "none"}</div>
              <div className="muted">{topOperator ? formatRate(topOperator.resolution_quality_proxy) : "No operator rows yet"}</div>
            </div>
            <div className="kpi-card">
              <div className="kpi-label">Top team</div>
              <div className="kpi-value">{topTeam?.assigned_team ?? "none"}</div>
              <div className="muted">{topTeam ? formatRate(topTeam.resolution_quality_proxy) : "No team rows yet"}</div>
            </div>
            <div className="kpi-card">
              <div className="kpi-label">Latest snapshot</div>
              <div className="kpi-value">{latestSnapshot ? formatTimestamp(latestSnapshot.snapshot_at) : "-"}</div>
              <div className="muted">
                {latestSnapshot ? `${latestSnapshot.operator_case_mix_count} operator mix rows` : "No snapshots yet"}
              </div>
            </div>
          </div>

          <div className="ops-phase24-grid">
            <div className="panel">
              <div className="panel-header">
                <div>
                  <h3>Operator Effectiveness</h3>
                  <p className="panel-subtitle">Resolution quality proxy alongside reopen, escalation, and reassignment drag.</p>
                </div>
              </div>
              {operatorSummary.length === 0 ? (
                <p className="muted">No operator performance rows yet.</p>
              ) : (
                <table className="table">
                  <thead>
                    <tr>
                      <th>Operator</th>
                      <th>Cases</th>
                      <th>Resolve</th>
                      <th>Reopen</th>
                      <th>Escalate</th>
                      <th>Avg resolve</th>
                    </tr>
                  </thead>
                  <tbody>
                    {operatorSummary.slice(0, 10).map((row) => (
                      <tr key={row.operator_name}>
                        <td>
                          <div>{row.operator_name}</div>
                          <div className="muted">{row.chronic_case_count} chronic / {row.severe_case_count} severe</div>
                        </td>
                        <td>{row.assigned_case_count}</td>
                        <td>{formatRate(row.resolution_quality_proxy)}</td>
                        <td>{formatRate(row.reopen_rate)}</td>
                        <td>{formatRate(row.escalation_rate)}</td>
                        <td>{formatSecondsAsHours(row.avg_resolve_seconds)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>

            <div className="panel">
              <div className="panel-header">
                <div>
                  <h3>Team Effectiveness</h3>
                  <p className="panel-subtitle">Team-level handling quality under incident burden and escalation pressure.</p>
                </div>
              </div>
              {teamSummary.length === 0 ? (
                <p className="muted">No team performance rows yet.</p>
              ) : (
                <table className="table">
                  <thead>
                    <tr>
                      <th>Team</th>
                      <th>Cases</th>
                      <th>Resolve</th>
                      <th>Reopen</th>
                      <th>Escalate</th>
                      <th>Avg resolve</th>
                    </tr>
                  </thead>
                  <tbody>
                    {teamSummary.slice(0, 10).map((row) => (
                      <tr key={row.assigned_team}>
                        <td>
                          <div>{row.assigned_team}</div>
                          <div className="muted">{row.chronic_case_count} chronic / {row.severe_case_count} severe</div>
                        </td>
                        <td>{row.assigned_case_count}</td>
                        <td>{formatRate(row.resolution_quality_proxy)}</td>
                        <td>{formatRate(row.reopen_rate)}</td>
                        <td>{formatRate(row.escalation_rate)}</td>
                        <td>{formatSecondsAsHours(row.avg_resolve_seconds)}</td>
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
                  <h3>Operator Case Mix</h3>
                  <p className="panel-subtitle">Difficulty context so performance can be read alongside root-cause and severity burden.</p>
                </div>
              </div>
              {operatorCaseMix.length === 0 ? (
                <p className="muted">No operator case-mix rows yet.</p>
              ) : (
                <table className="table">
                  <thead>
                    <tr>
                      <th>Operator</th>
                      <th>Root cause</th>
                      <th>Severity</th>
                      <th>Regime</th>
                      <th>Cases</th>
                    </tr>
                  </thead>
                  <tbody>
                    {operatorCaseMix.slice(0, 12).map((row, index) => (
                      <tr key={`${row.actor_name}-${row.root_cause_code}-${row.severity}-${row.regime}-${index}`}>
                        <td>{row.actor_name}</td>
                        <td>{row.root_cause_code}</td>
                        <td>{row.severity}</td>
                        <td>{row.regime}</td>
                        <td>{row.case_count}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>

            <div className="panel">
              <div className="panel-header">
                <div>
                  <h3>Team Case Mix</h3>
                  <p className="panel-subtitle">Team-level concentration by incident class, severity, and recurring burden.</p>
                </div>
              </div>
              {teamCaseMix.length === 0 ? (
                <p className="muted">No team case-mix rows yet.</p>
              ) : (
                <table className="table">
                  <thead>
                    <tr>
                      <th>Team</th>
                      <th>Root cause</th>
                      <th>Severity</th>
                      <th>Regime</th>
                      <th>Cases</th>
                    </tr>
                  </thead>
                  <tbody>
                    {teamCaseMix.slice(0, 12).map((row, index) => (
                      <tr key={`${row.actor_name}-${row.root_cause_code}-${row.severity}-${row.regime}-${index}`}>
                        <td>{row.actor_name}</td>
                        <td>{row.root_cause_code}</td>
                        <td>{row.severity}</td>
                        <td>{row.regime}</td>
                        <td>{row.case_count}</td>
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
