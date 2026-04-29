"use client";

import type { WatchlistSlaRow } from "@/lib/queries/metrics";

interface Props {
  items: WatchlistSlaRow[];
  loading: boolean;
}

function formatDateTime(value: string | null): string {
  return value ? new Date(value).toLocaleString() : "-";
}

function formatDurationSeconds(value: number | null): string {
  if (value == null) return "-";
  if (value < 60) return `${value}s`;
  if (value < 3600) return `${Math.floor(value / 60)}m`;
  return `${(value / 3600).toFixed(1)}h`;
}

export function SlaPanel({ items, loading }: Props) {
  return (
    <section className="card">
      <div className="panel-header">
        <div>
          <h2 className="section-title">Watchlist SLA</h2>
          <p className="panel-subtitle">Operational freshness for each watchlist over the last 24 hours.</p>
        </div>
      </div>

      {loading && <p className="muted">Loading SLA metrics...</p>}
      {!loading && items.length === 0 && <p className="muted">No SLA data is available yet.</p>}

      {items.length > 0 && (
        <div className="scroll-table">
          <table className="table">
            <thead>
              <tr>
                <th>Watchlist</th>
                <th>Completed 24h</th>
                <th>Failed 24h</th>
                <th>Last success</th>
                <th>Age</th>
                <th>Avg runtime</th>
              </tr>
            </thead>
            <tbody>
              {items.map((item) => (
                <tr key={`${item.workspace_id}:${item.watchlist_id ?? "all"}`}>
                  <td>{item.watchlist_name ?? item.watchlist_slug ?? "workspace"}</td>
                  <td>{item.completed_24h}</td>
                  <td className={item.failed_24h > 0 ? "signal-negative" : undefined}>{item.failed_24h}</td>
                  <td>{formatDateTime(item.last_success_at)}</td>
                  <td>{formatDurationSeconds(item.seconds_since_last_success)}</td>
                  <td>{item.avg_runtime_ms_24h == null ? "-" : `${item.avg_runtime_ms_24h} ms`}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}
