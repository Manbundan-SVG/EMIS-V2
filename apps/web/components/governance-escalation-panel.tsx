import { formatNullable, formatTimestamp } from "@/lib/formatters/ops";
import type {
  GovernanceEscalationEventRow,
  GovernanceEscalationSummaryRow,
  GovernanceStaleCaseSummaryRow,
} from "@/lib/queries/metrics";

type Props = {
  activeEscalations: GovernanceEscalationSummaryRow[];
  recentEvents: GovernanceEscalationEventRow[];
  candidateCases: GovernanceStaleCaseSummaryRow[];
  loading: boolean;
};

function roundMinutes(value: number | null): string {
  if (value == null) return "n/a";
  return `${Math.round(value)}m`;
}

export function GovernanceEscalationPanel({
  activeEscalations,
  recentEvents,
  candidateCases,
  loading,
}: Props) {
  const repeatedCount = activeEscalations.filter((row) => row.repeated_count > 1).length;

  return (
    <section className="section">
      <div className="panel-header">
        <div>
          <h2>Escalations</h2>
          <p className="panel-subtitle">Active escalation state, repeated escalations, and stale cases approaching escalation pressure.</p>
        </div>
      </div>
      {loading && <p className="muted">Loading escalation metrics...</p>}
      {!loading && (
        <>
          <div className="kpi-grid">
            <div className="kpi-card"><div className="kpi-label">Active escalations</div><div className="kpi-value">{activeEscalations.length}</div></div>
            <div className="kpi-card"><div className="kpi-label">Repeated escalations</div><div className="kpi-value">{repeatedCount}</div></div>
            <div className="kpi-card"><div className="kpi-label">Candidates</div><div className="kpi-value">{candidateCases.length}</div></div>
            <div className="kpi-card"><div className="kpi-label">Recent events</div><div className="kpi-value">{recentEvents.length}</div></div>
          </div>

          <div className="ops-phase24-grid">
            <div className="panel">
              <div className="panel-header">
                <div>
                  <h3>Active Escalations</h3>
                  <p className="panel-subtitle">Cases currently under formal escalation with target team or user.</p>
                </div>
              </div>
              {activeEscalations.length === 0 ? (
                <p className="muted">No active escalations.</p>
              ) : (
                <table className="table">
                  <thead>
                    <tr>
                      <th>Case</th>
                      <th>Level</th>
                      <th>Target</th>
                      <th>Reason</th>
                      <th>At</th>
                    </tr>
                  </thead>
                  <tbody>
                    {activeEscalations.slice(0, 10).map((row) => (
                      <tr key={row.id}>
                        <td>
                          <div>{row.case_title}</div>
                          <div className="muted">{row.case_status} / {row.severity} / repeat {row.repeat_count}</div>
                        </td>
                        <td>
                          <div>{row.escalation_level}</div>
                          <div className="muted">{row.repeated_count > 1 ? `repeated ${row.repeated_count}x` : "first escalation"}</div>
                        </td>
                        <td>{formatNullable(row.escalated_to_user ?? row.escalated_to_team)}</td>
                        <td>{formatNullable(row.reason)}</td>
                        <td>{formatTimestamp(row.escalated_at)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>

            <div className="panel">
              <div className="panel-header">
                <div>
                  <h3>Escalation Candidates</h3>
                  <p className="panel-subtitle">Stale cases not yet escalated but already breaching SLA windows.</p>
                </div>
              </div>
              {candidateCases.length === 0 ? (
                <p className="muted">No near-term escalation candidates.</p>
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
                    {candidateCases.slice(0, 10).map((row) => (
                      <tr key={row.case_id}>
                        <td>
                          <div>{row.title}</div>
                          <div className="muted">{row.status} / {row.severity}</div>
                        </td>
                        <td>{formatNullable(row.current_assignee ?? row.current_team)}</td>
                        <td>{roundMinutes(row.age_minutes)}</td>
                        <td>
                          <div>{row.ack_breached ? "ack breached" : "ack ok"}</div>
                          <div className="muted">{row.resolve_breached ? "resolve breached" : "resolve ok"}</div>
                        </td>
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
                <h3>Recent Escalation Events</h3>
                <p className="panel-subtitle">Latest escalation, repeat, and clear events written by the worker.</p>
              </div>
            </div>
            {recentEvents.length === 0 ? (
              <p className="muted">No escalation events recorded yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Event</th>
                    <th>Level</th>
                    <th>Target</th>
                    <th>Reason</th>
                    <th>At</th>
                  </tr>
                </thead>
                <tbody>
                  {recentEvents.slice(0, 10).map((row) => (
                    <tr key={row.id}>
                      <td>{row.event_type}</td>
                      <td>{formatNullable(row.escalation_level)}</td>
                      <td>{formatNullable(row.escalated_to_user ?? row.escalated_to_team)}</td>
                      <td>{formatNullable(row.reason)}</td>
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
