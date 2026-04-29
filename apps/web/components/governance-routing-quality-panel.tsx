"use client";

import { formatNullable, formatTimestamp } from "@/lib/formatters/ops";
import type {
  GovernanceReassignmentPressureRow,
  GovernanceRoutingQualityRow,
} from "@/lib/queries/metrics";

type Props = {
  routingQuality: GovernanceRoutingQualityRow[];
  reassignmentPressure: GovernanceReassignmentPressureRow[];
  loading: boolean;
};

function formatRate(value: number): string {
  return `${(value * 100).toFixed(1)}%`;
}

export function GovernanceRoutingQualityPanel({
  routingQuality,
  reassignmentPressure,
  loading,
}: Props) {
  const weakestRoute = routingQuality[0] ?? null;
  const busiestReassignmentTeam = reassignmentPressure[0] ?? null;

  return (
    <section className="section">
      <div className="panel-header">
        <div>
          <h2>Routing Quality</h2>
          <p className="panel-subtitle">Feedback on whether routing stuck, got manually changed, or escalated into reassignment.</p>
        </div>
      </div>
      {loading && <p className="muted">Loading routing quality...</p>}
      {!loading && (
        <>
          <div className="kpi-grid">
            <div className="kpi-card">
              <div className="kpi-label">Feedback rows</div>
              <div className="kpi-value">{routingQuality.reduce((sum, row) => sum + row.feedback_count, 0)}</div>
            </div>
            <div className="kpi-card">
              <div className="kpi-label">Lowest acceptance</div>
              <div className="kpi-value">
                {weakestRoute ? formatRate(weakestRoute.acceptance_rate) : "none"}
              </div>
              <div className="muted">
                {weakestRoute ? `${weakestRoute.assigned_team} / ${weakestRoute.root_cause_code}` : "No routing feedback yet"}
              </div>
            </div>
            <div className="kpi-card">
              <div className="kpi-label">Busiest reassignment team</div>
              <div className="kpi-value">{busiestReassignmentTeam ? busiestReassignmentTeam.assigned_team : "none"}</div>
              <div className="muted">
                {busiestReassignmentTeam ? `${busiestReassignmentTeam.reassignment_count} reassignments` : "No reassignments yet"}
              </div>
            </div>
          </div>

          <div className="ops-phase24-grid">
            <div className="panel">
              <div className="panel-header">
                <div>
                  <h3>Acceptance by Team / Root Cause</h3>
                  <p className="panel-subtitle">Lower acceptance usually means routing needed human correction.</p>
                </div>
              </div>
              {routingQuality.length === 0 ? (
                <p className="muted">No routing quality feedback yet.</p>
              ) : (
                <table className="table">
                  <thead>
                    <tr>
                      <th>Team</th>
                      <th>Root cause</th>
                      <th>Accepted</th>
                      <th>Rerouted</th>
                      <th>Rate</th>
                      <th>Latest</th>
                    </tr>
                  </thead>
                  <tbody>
                    {routingQuality.slice(0, 10).map((row) => (
                      <tr key={`${row.assigned_team}-${row.root_cause_code}`}>
                        <td>{row.assigned_team}</td>
                        <td>{row.root_cause_code}</td>
                        <td>{row.accepted_count}/{row.feedback_count}</td>
                        <td>{row.rerouted_count}</td>
                        <td>{formatRate(row.acceptance_rate)}</td>
                        <td>{formatTimestamp(row.latest_feedback_at)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>

            <div className="panel">
              <div className="panel-header">
                <div>
                  <h3>Reassignment Pressure</h3>
                  <p className="panel-subtitle">Shows where manual overrides and escalation-driven reroutes are clustering.</p>
                </div>
              </div>
              {reassignmentPressure.length === 0 ? (
                <p className="muted">No reassignment pressure yet.</p>
              ) : (
                <table className="table">
                  <thead>
                    <tr>
                      <th>Team</th>
                      <th>Reassignments</th>
                      <th>Manual</th>
                      <th>Escalation</th>
                      <th>Avg since open</th>
                      <th>Latest</th>
                    </tr>
                  </thead>
                  <tbody>
                    {reassignmentPressure.slice(0, 10).map((row) => (
                      <tr key={row.assigned_team}>
                        <td>{row.assigned_team}</td>
                        <td>{row.reassignment_count}</td>
                        <td>{row.manual_override_count}</td>
                        <td>{row.escalation_reassign_count}</td>
                        <td>{formatNullable(row.avg_minutes_since_open)}</td>
                        <td>{formatTimestamp(row.latest_reassignment_at)}</td>
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
