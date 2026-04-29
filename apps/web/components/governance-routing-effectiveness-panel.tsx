"use client";

import { formatNullable, formatTimestamp } from "@/lib/formatters/ops";
import type {
  GovernanceOperatorEffectivenessRow,
  GovernanceRoutingRecommendationInputRow,
  GovernanceTeamEffectivenessRow,
} from "@/lib/queries/metrics";

type Props = {
  operators: GovernanceOperatorEffectivenessRow[];
  teams: GovernanceTeamEffectivenessRow[];
  recommendationInputs: GovernanceRoutingRecommendationInputRow[];
  loading: boolean;
};

function formatRate(value: number | null): string {
  if (value === null || Number.isNaN(value)) return "-";
  return `${(value * 100).toFixed(1)}%`;
}

function formatHours(value: number | null): string {
  if (value === null || Number.isNaN(value)) return "-";
  return `${value.toFixed(1)}h`;
}

export function GovernanceRoutingEffectivenessPanel({
  operators,
  teams,
  recommendationInputs,
  loading,
}: Props) {
  const topOperator = operators[0] ?? null;
  const topTeam = teams[0] ?? null;
  const strongestInput = recommendationInputs[0] ?? null;

  return (
    <section className="section">
      <div className="panel-header">
        <div>
          <h2>Routing Effectiveness</h2>
          <p className="panel-subtitle">Outcome-based routing performance by operator, team, and repeatable recommendation inputs.</p>
        </div>
      </div>

      {loading && <p className="muted">Loading routing effectiveness...</p>}

      {!loading && (
        <>
          <div className="kpi-grid">
            <div className="kpi-card">
              <div className="kpi-label">Operator rows</div>
              <div className="kpi-value">{operators.length}</div>
            </div>
            <div className="kpi-card">
              <div className="kpi-label">Strongest operator</div>
              <div className="kpi-value">{topOperator?.assigned_to ?? "none"}</div>
              <div className="muted">{topOperator ? formatRate(topOperator.resolution_rate) : "No operator outcomes yet"}</div>
            </div>
            <div className="kpi-card">
              <div className="kpi-label">Strongest team</div>
              <div className="kpi-value">{topTeam?.assigned_team ?? "none"}</div>
              <div className="muted">{topTeam ? formatRate(topTeam.resolution_rate) : "No team outcomes yet"}</div>
            </div>
            <div className="kpi-card">
              <div className="kpi-label">Top recommendation input</div>
              <div className="kpi-value">{strongestInput?.routing_target ?? "none"}</div>
              <div className="muted">{strongestInput ? `${strongestInput.resolved_count} resolves` : "No inputs yet"}</div>
            </div>
          </div>

          <div className="ops-phase24-grid">
            <div className="panel">
              <div className="panel-header">
                <div>
                  <h3>Operator Outcomes</h3>
                  <p className="panel-subtitle">Resolution-heavy operators with lower reassignment and escalation rates should bias future routing.</p>
                </div>
              </div>
              {operators.length === 0 ? (
                <p className="muted">No operator effectiveness rows yet.</p>
              ) : (
                <table className="table">
                  <thead>
                    <tr>
                      <th>Operator</th>
                      <th>Assignments</th>
                      <th>Resolution rate</th>
                      <th>Avg ack</th>
                      <th>Avg resolve</th>
                      <th>Latest</th>
                    </tr>
                  </thead>
                  <tbody>
                    {operators.slice(0, 10).map((row) => (
                      <tr key={row.assigned_to}>
                        <td>{row.assigned_to}</td>
                        <td>{row.assignments}</td>
                        <td>{formatRate(row.resolution_rate)}</td>
                        <td>{formatHours(row.avg_ack_hours)}</td>
                        <td>{formatHours(row.avg_resolve_hours)}</td>
                        <td>{formatTimestamp(row.latest_outcome_at)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>

            <div className="panel">
              <div className="panel-header">
                <div>
                  <h3>Team Outcomes</h3>
                  <p className="panel-subtitle">Team-level routing effectiveness and escalation drag across the current workflow model.</p>
                </div>
              </div>
              {teams.length === 0 ? (
                <p className="muted">No team effectiveness rows yet.</p>
              ) : (
                <table className="table">
                  <thead>
                    <tr>
                      <th>Team</th>
                      <th>Assignments</th>
                      <th>Resolution rate</th>
                      <th>Escalation rate</th>
                      <th>Avg resolve</th>
                      <th>Latest</th>
                    </tr>
                  </thead>
                  <tbody>
                    {teams.slice(0, 10).map((row) => (
                      <tr key={row.assigned_team}>
                        <td>{row.assigned_team}</td>
                        <td>{row.assignments}</td>
                        <td>{formatRate(row.resolution_rate)}</td>
                        <td>{formatRate(row.escalation_rate)}</td>
                        <td>{formatHours(row.avg_resolve_hours)}</td>
                        <td>{formatTimestamp(row.latest_outcome_at)}</td>
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
                <h3>Recommendation Inputs</h3>
                <p className="panel-subtitle">Aggregated outcome inputs that can steer future routing policy by root cause, severity, and version tuple.</p>
              </div>
            </div>
            {recommendationInputs.length === 0 ? (
              <p className="muted">No recommendation inputs yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Target</th>
                    <th>Root cause</th>
                    <th>Severity</th>
                    <th>Resolved</th>
                    <th>Avg ack</th>
                    <th>Avg resolve</th>
                    <th>Latest</th>
                  </tr>
                </thead>
                <tbody>
                  {recommendationInputs.slice(0, 12).map((row) => (
                    <tr key={`${row.routing_target}-${row.root_cause_code ?? "none"}-${row.severity ?? "none"}`}>
                      <td>{row.routing_target}</td>
                      <td>{formatNullable(row.root_cause_code)}</td>
                      <td>{formatNullable(row.severity)}</td>
                      <td>{row.resolved_count}</td>
                      <td>{formatHours(row.avg_ack_hours)}</td>
                      <td>{formatHours(row.avg_resolve_hours)}</td>
                      <td>{formatTimestamp(row.latest_outcome_at)}</td>
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
