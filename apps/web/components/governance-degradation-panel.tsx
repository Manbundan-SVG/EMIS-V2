"use client";

import { formatNullable, formatTimestamp } from "@/lib/formatters/ops";
import type { GovernanceDegradationStateRow, GovernanceRecoveryEventRow } from "@/lib/queries/metrics";

interface Props {
  activeStates: GovernanceDegradationStateRow[];
  resolvedStates: GovernanceDegradationStateRow[];
  recoveries: GovernanceRecoveryEventRow[];
  loading: boolean;
}

function formatHours(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) return "-";
  return `${value.toFixed(1)}h`;
}

export function GovernanceDegradationPanel({ activeStates, resolvedStates, recoveries, loading }: Props) {
  const openStateCount = activeStates.filter((row) => row.state_status === "active").length;
  const escalatedStateCount = activeStates.filter((row) => row.state_status === "escalated").length;

  return (
    <section className="card">
      <div className="panel-header">
        <div>
          <h2 className="section-title">Chronic Degradation</h2>
          <p className="panel-subtitle">Persistent instability states, escalations, and recoveries layered on top of governance alerts and anomaly clusters.</p>
        </div>
        <div className="muted">
          Active: {openStateCount} · Escalated: {escalatedStateCount} · Recoveries: {recoveries.length}
        </div>
      </div>

      {loading && <p className="muted">Loading chronic degradation states...</p>}
      {!loading && activeStates.length === 0 && resolvedStates.length === 0 && recoveries.length === 0 && (
        <p className="muted">No chronic degradation states have been recorded for this workspace.</p>
      )}

      {activeStates.length > 0 && (
        <div className="scroll-table">
          <table className="table">
            <thead>
              <tr>
                <th>State</th>
                <th>Status</th>
                <th>Watchlist</th>
                <th>Tuple</th>
                <th>Counts</th>
                <th>Last Seen</th>
              </tr>
            </thead>
            <tbody>
              {activeStates.map((state) => (
                <tr key={state.id}>
                  <td>
                    {state.degradation_type}
                    <div className="muted">{formatNullable(state.regime)}</div>
                  </td>
                  <td>
                    {state.severity} · {state.state_status}
                    <div className="muted">Duration {formatHours(state.state_duration_hours)}</div>
                  </td>
                  <td>{formatNullable(state.watchlist_name ?? state.watchlist_slug)}</td>
                  <td className="mono-cell">{state.version_tuple}</td>
                  <td>
                    {state.event_count} events · {state.cluster_count} clusters
                    <div className="muted">{state.member_count} members</div>
                  </td>
                  <td>{formatTimestamp(state.last_seen_at)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {recoveries.length > 0 && (
        <div className="scroll-table">
          <table className="table">
            <thead>
              <tr>
                <th>Recovered State</th>
                <th>Reason</th>
                <th>Watchlist</th>
                <th>Tuple</th>
                <th>Recovered</th>
              </tr>
            </thead>
            <tbody>
              {recoveries.map((recovery) => (
                <tr key={recovery.id}>
                  <td>
                    {recovery.degradation_type}
                    <div className="muted">{recovery.prior_severity}</div>
                  </td>
                  <td>{recovery.recovery_reason}</td>
                  <td>{formatNullable(recovery.watchlist_name ?? recovery.watchlist_slug)}</td>
                  <td className="mono-cell">{recovery.version_tuple}</td>
                  <td>{formatTimestamp(recovery.recovered_at)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}
