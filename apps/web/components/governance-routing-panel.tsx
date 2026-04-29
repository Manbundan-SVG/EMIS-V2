import { formatNullable, formatTimestamp } from "@/lib/formatters/ops";
import type {
  GovernanceOperatorCaseMetricsRow,
  GovernanceRoutingDecisionRow,
  GovernanceTeamCaseMetricsRow,
} from "@/lib/queries/metrics";

type Props = {
  routingDecisions: GovernanceRoutingDecisionRow[];
  operatorMetrics: GovernanceOperatorCaseMetricsRow[];
  teamMetrics: GovernanceTeamCaseMetricsRow[];
  loading: boolean;
};

export function GovernanceRoutingPanel({
  routingDecisions,
  operatorMetrics,
  teamMetrics,
  loading,
}: Props) {
  const busiestTeam = teamMetrics[0] ?? null;
  const busiestOperator = operatorMetrics[0] ?? null;

  return (
    <section className="section">
      <div className="panel-header">
        <div>
          <h2>Assignment Routing</h2>
          <p className="panel-subtitle">Rule-driven case routing with workload-aware fallback and latest assignment decisions.</p>
        </div>
      </div>
      {loading && <p className="muted">Loading routing metrics...</p>}
      {!loading && (
        <>
          <div className="kpi-grid">
            <div className="kpi-card"><div className="kpi-label">Routing decisions</div><div className="kpi-value">{routingDecisions.length}</div></div>
            <div className="kpi-card">
              <div className="kpi-label">Busiest team</div>
              <div className="kpi-value">{busiestTeam ? busiestTeam.assigned_team : "none"}</div>
              <div className="muted">{busiestTeam ? `${busiestTeam.open_case_count} open / ${busiestTeam.severe_open_case_count} severe` : "No team workload yet"}</div>
            </div>
            <div className="kpi-card">
              <div className="kpi-label">Busiest operator</div>
              <div className="kpi-value">{busiestOperator ? busiestOperator.operator_id : "none"}</div>
              <div className="muted">{busiestOperator ? `${busiestOperator.open_case_count} open / ${busiestOperator.stale_open_case_count} stale` : "No operator workload yet"}</div>
            </div>
          </div>

          <div className="ops-phase24-grid">
            <div className="panel">
              <div className="panel-header">
                <div>
                  <h3>Latest Routing Decisions</h3>
                  <p className="panel-subtitle">Recent rule or override outcomes persisted when cases were auto-routed.</p>
                </div>
              </div>
              {routingDecisions.length === 0 ? (
                <p className="muted">No routing decisions recorded yet.</p>
              ) : (
                <table className="table">
                  <thead>
                    <tr>
                      <th>Case</th>
                      <th>Root cause</th>
                      <th>Target</th>
                      <th>Reason</th>
                      <th>At</th>
                    </tr>
                  </thead>
                  <tbody>
                    {routingDecisions.slice(0, 10).map((row) => (
                      <tr key={row.id}>
                        <td>
                          <div>{row.case_title}</div>
                          <div className="muted">{row.case_status} / {row.severity}</div>
                        </td>
                        <td>{formatNullable(row.root_cause_code)}</td>
                        <td>{formatNullable(row.assigned_user ?? row.assigned_team)}</td>
                        <td>{row.routing_reason}</td>
                        <td>{formatTimestamp(row.created_at)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>

            <div className="panel">
              <div className="panel-header">
                <div>
                  <h3>Workload Pressure</h3>
                  <p className="panel-subtitle">Current open and stale load by team and operator.</p>
                </div>
              </div>
              <div className="ops-phase24-grid">
                <div>
                  <h4 className="muted" style={{ marginBottom: 12 }}>Teams</h4>
                  {teamMetrics.length === 0 ? (
                    <p className="muted">No team workload yet.</p>
                  ) : (
                    <table className="table">
                      <thead>
                        <tr>
                          <th>Team</th>
                          <th>Open</th>
                          <th>Severe</th>
                          <th>Stale</th>
                        </tr>
                      </thead>
                      <tbody>
                        {teamMetrics.slice(0, 8).map((row) => (
                          <tr key={`${row.workspace_id}-${row.assigned_team}`}>
                            <td>{row.assigned_team}</td>
                            <td>{row.open_case_count}</td>
                            <td>{row.severe_open_case_count}</td>
                            <td>{row.stale_open_case_count}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  )}
                </div>
                <div>
                  <h4 className="muted" style={{ marginBottom: 12 }}>Operators</h4>
                  {operatorMetrics.length === 0 ? (
                    <p className="muted">No operator workload yet.</p>
                  ) : (
                    <table className="table">
                      <thead>
                        <tr>
                          <th>Operator</th>
                          <th>Team</th>
                          <th>Open</th>
                          <th>Stale</th>
                        </tr>
                      </thead>
                      <tbody>
                        {operatorMetrics.slice(0, 8).map((row) => (
                          <tr key={`${row.workspace_id}-${row.operator_id}-${row.assigned_team ?? "none"}`}>
                            <td>{row.operator_id}</td>
                            <td>{formatNullable(row.assigned_team)}</td>
                            <td>{row.open_case_count}</td>
                            <td>{row.stale_open_case_count}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  )}
                </div>
              </div>
            </div>
          </div>
        </>
      )}
    </section>
  );
}
