import { formatNullable, formatTimestamp } from "@/lib/formatters/ops";
import type { GovernanceLifecycleRow, GovernanceRecoveryEventRow } from "@/lib/queries/metrics";

type Props = {
  activeStates: GovernanceLifecycleRow[];
  acknowledgedStates: GovernanceLifecycleRow[];
  resolvedStates: GovernanceLifecycleRow[];
  recoveries: GovernanceRecoveryEventRow[];
  loading: boolean;
};

function StateTable({ title, rows }: { title: string; rows: GovernanceLifecycleRow[] }) {
  return (
    <div className="panel">
      <div className="panel-header">
        <div>
          <h3>{title}</h3>
          <p className="panel-subtitle">{rows.length} states</p>
        </div>
      </div>
      {rows.length === 0 ? (
        <p className="muted">No states in this slice.</p>
      ) : (
        <table className="table">
          <thead>
            <tr>
              <th>Type</th>
              <th>Severity</th>
              <th>Watchlist</th>
              <th>Ack</th>
              <th>Mute</th>
              <th>Last seen</th>
            </tr>
          </thead>
          <tbody>
            {rows.slice(0, 8).map((row) => (
              <tr key={row.degradation_state_id}>
                <td>{row.degradation_type}</td>
                <td><span className={`severity-${row.severity}`}>{row.severity}</span></td>
                <td>{formatNullable(row.watchlist_slug ?? row.watchlist_name)}</td>
                <td>{row.acknowledged_at ? `${formatNullable(row.acknowledged_by)} @ ${formatTimestamp(row.acknowledged_at)}` : "-"}</td>
                <td>{row.muting_rule_id ? `${formatNullable(row.mute_target_type)} until ${formatTimestamp(row.muted_until)}` : "-"}</td>
                <td>{formatTimestamp(row.last_seen_at)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}

export function GovernanceLifecyclePanel({
  activeStates,
  acknowledgedStates,
  resolvedStates,
  recoveries,
  loading,
}: Props) {
  return (
    <section className="section">
      <div className="panel-header">
        <div>
          <h2>Governance Lifecycle</h2>
          <p className="panel-subtitle">Acknowledgment, muting, and resolution context on chronic degradation states.</p>
        </div>
      </div>
      {loading && <p className="muted">Loading governance lifecycle...</p>}
      {!loading && (
        <>
          <div className="kpi-grid">
            <div className="kpi-card"><div className="kpi-label">Active</div><div className="kpi-value">{activeStates.length}</div></div>
            <div className="kpi-card"><div className="kpi-label">Acknowledged</div><div className="kpi-value">{acknowledgedStates.length}</div></div>
            <div className="kpi-card"><div className="kpi-label">Resolved</div><div className="kpi-value">{resolvedStates.length}</div></div>
            <div className="kpi-card"><div className="kpi-label">Recoveries</div><div className="kpi-value">{recoveries.length}</div></div>
          </div>

          <div className="ops-phase24-grid">
            <StateTable title="Active" rows={activeStates} />
            <StateTable title="Acknowledged" rows={acknowledgedStates} />
          </div>

          <div className="panel" style={{ marginTop: 16 }}>
            <div className="panel-header">
              <div>
                <h3>Recent Recoveries</h3>
                <p className="panel-subtitle">Latest recovery events for chronic degradation states.</p>
              </div>
            </div>
            {recoveries.length === 0 ? (
              <p className="muted">No recoveries have been recorded yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Type</th>
                    <th>Reason</th>
                    <th>Watchlist</th>
                    <th>Recovered at</th>
                    <th>Prior severity</th>
                  </tr>
                </thead>
                <tbody>
                  {recoveries.slice(0, 8).map((row) => (
                    <tr key={row.id}>
                      <td>{row.degradation_type}</td>
                      <td>{row.recovery_reason}</td>
                      <td>{formatNullable(row.watchlist_slug ?? row.watchlist_name)}</td>
                      <td>{formatTimestamp(row.recovered_at)}</td>
                      <td>{row.prior_severity}</td>
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
