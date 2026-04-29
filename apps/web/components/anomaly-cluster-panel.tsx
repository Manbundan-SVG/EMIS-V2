"use client";

import { formatNullable, formatTimestamp } from "@/lib/formatters/ops";
import type { GovernanceAnomalyClusterRow, WatchlistAnomalySummaryRow } from "@/lib/queries/metrics";

interface Props {
  clusters: GovernanceAnomalyClusterRow[];
  summary: WatchlistAnomalySummaryRow[];
  loading: boolean;
}

export function AnomalyClusterPanel({ clusters, summary, loading }: Props) {
  return (
    <section className="card">
      <div className="panel-header">
        <div>
          <h2 className="section-title">Anomaly Clusters</h2>
          <p className="panel-subtitle">Grouped governance anomalies by watchlist, version tuple, alert type, and regime context.</p>
        </div>
      </div>

      {loading && <p className="muted">Loading anomaly clusters...</p>}
      {!loading && summary.length === 0 && clusters.length === 0 && (
        <p className="muted">No anomaly clusters are open for this workspace.</p>
      )}

      {summary.length > 0 && (
        <div className="scroll-table">
          <table className="table">
            <thead>
              <tr>
                <th>Watchlist</th>
                <th>Open Clusters</th>
                <th>High Severity</th>
                <th>Open Events</th>
                <th>Last Seen</th>
              </tr>
            </thead>
            <tbody>
              {summary.map((row) => (
                <tr key={`${row.watchlist_id ?? "all"}:${row.workspace_id}`}>
                  <td>{formatNullable(row.watchlist_name ?? row.watchlist_slug)}</td>
                  <td>{row.open_cluster_count}</td>
                  <td>{row.high_open_cluster_count}</td>
                  <td>{row.open_event_count}</td>
                  <td>{formatTimestamp(row.last_seen_at)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {clusters.length > 0 && (
        <div className="scroll-table">
          <table className="table">
            <thead>
              <tr>
                <th>Alert</th>
                <th>Severity</th>
                <th>Watchlist</th>
                <th>Regime</th>
                <th>Tuple</th>
                <th>Events</th>
                <th>First Seen</th>
                <th>Last Seen</th>
              </tr>
            </thead>
            <tbody>
              {clusters.map((cluster) => (
                <tr key={cluster.id}>
                  <td>{cluster.alert_type}</td>
                  <td>{cluster.severity}</td>
                  <td>{formatNullable(cluster.watchlist_name ?? cluster.watchlist_slug)}</td>
                  <td>{formatNullable(cluster.regime)}</td>
                  <td className="mono-cell">{cluster.version_tuple}</td>
                  <td>{cluster.event_count}</td>
                  <td>{formatTimestamp(cluster.first_seen_at)}</td>
                  <td>{formatTimestamp(cluster.last_seen_at)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}
