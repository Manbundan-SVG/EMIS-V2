"use client";

import { formatNullable, formatTimestamp } from "@/lib/formatters/ops";
import type { GovernanceAlertEventRow, GovernanceAlertStateRow } from "@/lib/queries/metrics";

interface Props {
  events: GovernanceAlertEventRow[];
  state: GovernanceAlertStateRow[];
  loading: boolean;
}

function formatNumber(value: number | null | undefined, digits = 4): string {
  if (value === null || value === undefined || Number.isNaN(value)) return "-";
  return value.toFixed(digits);
}

export function GovernanceAlertsPanel({ events, state, loading }: Props) {
  return (
    <section className="card">
      <div className="panel-header">
        <div>
          <h2 className="section-title">Governance Alerts</h2>
          <p className="panel-subtitle">Threshold-triggered governance events across latest stability and version tuples.</p>
        </div>
      </div>

      {loading && <p className="muted">Loading governance alerts...</p>}
      {!loading && state.length === 0 && events.length === 0 && (
        <p className="muted">No governance alerts have been emitted for this workspace.</p>
      )}

      {state.length > 0 && (
        <div className="scroll-table">
          <table className="table">
            <thead>
              <tr>
                <th>Event</th>
                <th>Severity</th>
                <th>Watchlist</th>
                <th>Tuple</th>
                <th>Triggers</th>
                <th>Latest</th>
              </tr>
            </thead>
            <tbody>
              {state.map((row) => (
                <tr key={`${row.rule_name}:${row.compute_version}:${row.signal_registry_version}:${row.model_version}:${row.watchlist_id ?? "all"}`}>
                  <td>{row.event_type}</td>
                  <td>{row.severity}</td>
                  <td>{formatNullable(row.watchlist_name ?? row.watchlist_slug)}</td>
                  <td className="mono-cell">
                    {formatNullable(row.compute_version)} / {formatNullable(row.signal_registry_version)} / {formatNullable(row.model_version)}
                  </td>
                  <td>{row.trigger_count}</td>
                  <td>{formatTimestamp(row.latest_triggered_at)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {events.length > 0 && (
        <div className="scroll-table">
          <table className="table">
            <thead>
              <tr>
                <th>Recent Event</th>
                <th>Metric</th>
                <th>Value</th>
                <th>Threshold</th>
                <th>Run</th>
                <th>Created</th>
              </tr>
            </thead>
            <tbody>
              {events.map((event) => (
                <tr key={event.id}>
                  <td>
                    {event.event_type}
                    <div className="muted">{event.rule_name}</div>
                  </td>
                  <td>{event.metric_source}.{event.metric_name}</td>
                  <td>
                    {formatNumber(event.metric_value_numeric)}
                    <div className="muted">{formatNullable(event.metric_value_text)}</div>
                  </td>
                  <td>
                    {formatNumber(event.threshold_numeric)}
                    <div className="muted">{formatNullable(event.threshold_text)}</div>
                  </td>
                  <td className="mono-cell">{event.run_id ? event.run_id.slice(0, 8) : "-"}</td>
                  <td>{formatTimestamp(event.created_at)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}
