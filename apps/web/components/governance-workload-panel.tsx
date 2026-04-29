import { formatNullable, formatTimestamp } from "@/lib/formatters/ops";
import type {
  GovernanceCaseAgingSummaryRow,
  GovernanceCaseSlaSummaryRow,
  GovernanceOperatorPressureRow,
  GovernanceStaleCaseSummaryRow,
  GovernanceTeamPressureRow,
} from "@/lib/queries/metrics";

type Props = {
  aging: GovernanceCaseAgingSummaryRow[];
  stale: GovernanceStaleCaseSummaryRow[];
  operatorPressure: GovernanceOperatorPressureRow[];
  teamPressure: GovernanceTeamPressureRow[];
  sla: GovernanceCaseSlaSummaryRow[];
  loading: boolean;
};

function roundMinutes(value: number | null): string {
  if (value == null) return "n/a";
  return `${Math.round(value)}m`;
}

export function GovernanceWorkloadPanel({
  aging,
  stale,
  operatorPressure,
  teamPressure,
  sla,
  loading,
}: Props) {
  const ackBreachedCount = sla.filter((row) => row.ack_breached).length;
  const resolveBreachedCount = sla.filter((row) => row.resolve_breached).length;

  return (
    <section className="section">
      <div className="panel-header">
        <div>
          <h2>Workload &amp; SLA</h2>
          <p className="panel-subtitle">Case aging, stale-case risk, and operator/team pressure built on current SLA evaluations.</p>
        </div>
      </div>
      {loading && <p className="muted">Loading workload and SLA metrics...</p>}
      {!loading && (
        <>
          <div className="kpi-grid">
            <div className="kpi-card"><div className="kpi-label">Tracked cases</div><div className="kpi-value">{aging.length}</div></div>
            <div className="kpi-card"><div className="kpi-label">Stale cases</div><div className="kpi-value">{stale.length}</div></div>
            <div className="kpi-card"><div className="kpi-label">Ack breaches</div><div className="kpi-value">{ackBreachedCount}</div></div>
            <div className="kpi-card"><div className="kpi-label">Resolve breaches</div><div className="kpi-value">{resolveBreachedCount}</div></div>
          </div>

          <div className="ops-phase24-grid">
            <div className="panel">
              <div className="panel-header">
                <div>
                  <h3>Stale Cases</h3>
                  <p className="panel-subtitle">Open or acknowledged cases that are aging out or breaching SLA windows.</p>
                </div>
              </div>
              {stale.length === 0 ? (
                <p className="muted">No stale governance cases.</p>
              ) : (
                <table className="table">
                  <thead>
                    <tr>
                      <th>Case</th>
                      <th>Owner</th>
                      <th>Age</th>
                      <th>Breaches</th>
                    </tr>
                  </thead>
                  <tbody>
                    {stale.slice(0, 10).map((row) => (
                      <tr key={row.case_id}>
                        <td>
                          <div>{row.title}</div>
                          <div className="muted">{row.status} / {row.severity}</div>
                        </td>
                        <td>{formatNullable(row.current_assignee ?? row.current_team)}</td>
                        <td>{roundMinutes(row.age_minutes)}</td>
                        <td>
                          <div>{row.ack_breached ? "ack" : "ack ok"}</div>
                          <div className="muted">{row.resolve_breached ? "resolve breached" : "resolve ok"}</div>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>

            <div className="panel">
              <div className="panel-header">
                <div>
                  <h3>Pressure Snapshot</h3>
                  <p className="panel-subtitle">Severity-weighted load and open-case pressure by operator and team.</p>
                </div>
              </div>
              <div className="ops-phase24-grid">
                <div>
                  <h4 className="muted" style={{ marginBottom: 12 }}>Operators</h4>
                  {operatorPressure.length === 0 ? (
                    <p className="muted">No operator workload yet.</p>
                  ) : (
                    <table className="table">
                      <thead>
                        <tr>
                          <th>Operator</th>
                          <th>Load</th>
                          <th>Open</th>
                          <th>Breaches</th>
                        </tr>
                      </thead>
                      <tbody>
                        {operatorPressure.slice(0, 8).map((row) => (
                          <tr key={`${row.workspace_id}-${row.assigned_to}`}>
                            <td>
                              <div>{row.assigned_to}</div>
                              <div className="muted">{formatNullable(row.assigned_team)}</div>
                            </td>
                            <td>{row.severity_weighted_load ?? 0}</td>
                            <td>{row.open_case_count}</td>
                            <td>{row.ack_breached_case_count + row.resolve_breached_case_count}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  )}
                </div>
                <div>
                  <h4 className="muted" style={{ marginBottom: 12 }}>Teams</h4>
                  {teamPressure.length === 0 ? (
                    <p className="muted">No team workload yet.</p>
                  ) : (
                    <table className="table">
                      <thead>
                        <tr>
                          <th>Team</th>
                          <th>Load</th>
                          <th>Open</th>
                          <th>Recurring</th>
                        </tr>
                      </thead>
                      <tbody>
                        {teamPressure.slice(0, 8).map((row) => (
                          <tr key={`${row.workspace_id}-${row.assigned_team}`}>
                            <td>{row.assigned_team}</td>
                            <td>{row.severity_weighted_load ?? 0}</td>
                            <td>{row.open_case_count}</td>
                            <td>{row.recurring_case_count}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  )}
                </div>
              </div>
            </div>
          </div>

          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>SLA Evaluations</h3>
                <p className="panel-subtitle">Latest persisted SLA windows and breach state for current cases.</p>
              </div>
            </div>
            {sla.length === 0 ? (
              <p className="muted">No SLA evaluations recorded yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Case</th>
                    <th>Ack due</th>
                    <th>Resolve due</th>
                    <th>Evaluated</th>
                  </tr>
                </thead>
                <tbody>
                  {sla.slice(0, 10).map((row) => (
                    <tr key={row.case_id}>
                      <td>
                        <div>{row.title}</div>
                        <div className="muted">{row.chronicity_class ?? "baseline"} / {row.severity}</div>
                      </td>
                      <td>{formatTimestamp(row.ack_due_at)}</td>
                      <td>{formatTimestamp(row.resolve_due_at)}</td>
                      <td>{formatTimestamp(row.evaluated_at)}</td>
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
